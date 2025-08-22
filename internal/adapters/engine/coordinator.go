package engine

import (
	"context"
	"strings"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type WorkflowCoordinator struct {
	engine   *Engine
	executor *NodeExecutor
}

func NewWorkflowCoordinator(engine *Engine) *WorkflowCoordinator {
	return &WorkflowCoordinator{
		engine:   engine,
		executor: NewNodeExecutor(engine),
	}
}

func (wc *WorkflowCoordinator) ProcessReadyNodes(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := wc.processNextReadyNode(ctx); err != nil {
				if isNotFoundError(err) {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				return err
			}
		}
	}
}

func (wc *WorkflowCoordinator) processNextReadyNode(ctx context.Context) error {
	if wc.engine.queue == nil {
		wc.engine.logger.Debug("queue not available, skipping ready node processing")
		return domain.NewNotFoundError("queue_item", "queue not available")
	}

	// Use claim duration based on node execution timeout or default to 5 minutes
	claimDuration := wc.engine.config.NodeExecutionTimeout
	if claimDuration <= 0 {
		claimDuration = 5 * time.Minute
	}

	item, err := wc.engine.queue.DequeueReady(ctx, ports.WithClaim(wc.engine.nodeID, claimDuration))

	if err != nil {
		if domain.IsKeyNotFound(err) || isNotFoundError(err) {
			return domain.NewNotFoundError("queue_item", "ready queue is empty")
		}
		return err
	}

	if item == nil {
		return domain.NewNotFoundError("queue_item", "dequeued item is nil")
	}

	wc.engine.logger.Debug("processing ready node",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
		"item_id", item.ID,
	)

	wc.engine.logger.Debug("about to start node execution goroutine",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
		"item_id", item.ID,
	)

	go func() {
		wc.engine.logger.Debug("starting node execution goroutine",
			"workflow_id", item.WorkflowID,
			"node_name", item.NodeName,
			"item_id", item.ID,
		)
		if err := wc.executor.ExecuteNode(ctx, item); err != nil {
			wc.engine.logger.Error("node execution failed",
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"error", err.Error(),
			)
		} else {
			wc.engine.logger.Info("node execution completed successfully",
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"item_id", item.ID,
			)
		}
	}()

	return nil
}

func (wc *WorkflowCoordinator) CheckWorkflowCompletions(ctx context.Context) error {
	ticker := time.NewTicker(wc.engine.config.StateUpdateInterval * 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := wc.checkCompletedWorkflows(ctx); err != nil {
				wc.engine.logger.Error("failed to check workflow completions", "error", err.Error())
			}
		}
	}
}

func (wc *WorkflowCoordinator) checkCompletedWorkflows(ctx context.Context) error {
	wc.engine.mu.RLock()
	workflowsToCheck := make([]*WorkflowInstance, 0, len(wc.engine.activeWorkflows))
	for _, workflow := range wc.engine.activeWorkflows {
		workflowsToCheck = append(workflowsToCheck, workflow)
	}
	wc.engine.mu.RUnlock()

	for _, workflow := range workflowsToCheck {
		if err := wc.CheckWorkflowCompletion(ctx, workflow); err != nil {
			wc.engine.logger.Error("failed to check workflow completion",
				"workflow_id", workflow.ID,
				"error", err.Error(),
			)
		}
	}

	return nil
}

func (wc *WorkflowCoordinator) CheckWorkflowCompletion(ctx context.Context, workflow *WorkflowInstance) error {
	workflow.mu.RLock()
	if workflow.Status != ports.WorkflowStateRunning {
		workflow.mu.RUnlock()
		return nil
	}
	workflowID := workflow.ID
	workflow.mu.RUnlock()

	isEmpty, err := wc.engine.queue.IsEmpty(ctx)
	if err != nil {
		return err
	}

	pendingItems, err := wc.engine.queue.GetPendingItems(ctx)
	if err != nil && !strings.Contains(err.Error(), "not found") {
		return err
	}

	hasWorkflowNodes := wc.hasNodesForWorkflow(ctx, workflowID, isEmpty, pendingItems)

	if !hasWorkflowNodes {
		wc.engine.logger.Info("workflow completed - no more nodes to execute",
			"workflow_id", workflowID,
		)

		workflow.mu.Lock()
		workflow.Status = ports.WorkflowStateCompleted
		now := time.Now()
		workflow.CompletedAt = &now
		workflow.mu.Unlock()

		if err := wc.executor.persistWorkflowState(ctx, workflow); err != nil {
			wc.engine.logger.Error("failed to persist completed workflow state",
				"workflow_id", workflowID,
				"error", err.Error(),
			)
		}

		var handlerError error
		if wc.engine.lifecycleManager != nil && wc.engine.dataCollector != nil {
			completionData, err := wc.engine.dataCollector.CollectWorkflowData(ctx, workflow)
			if err != nil {
				wc.engine.logger.Error("failed to collect workflow data",
					"workflow_id", workflowID,
					"error", err.Error(),
				)
			} else {
				handlerError = wc.engine.lifecycleManager.TriggerCompletion(ctx, *completionData)
				if handlerError != nil {
					wc.engine.logger.Error("completion handlers failed - cleanup postponed",
						"workflow_id", workflowID,
						"error", handlerError.Error(),
					)
				}
			}
		}

		if handlerError == nil {
			if wc.engine.cleanupScheduler != nil {
				orchestrator := wc.engine.cleanupScheduler.GetOrchestrator()
				if orchestrator != nil {
					cleanupOptions := CleanupOptions{
						PreserveState:   false,
						PreserveAudit:   false,
						RetentionPeriod: 0,
					}
					if err := orchestrator.CleanupWorkflow(ctx, workflowID, cleanupOptions); err != nil {
						wc.engine.logger.Error("failed to execute immediate workflow cleanup",
							"workflow_id", workflowID,
							"error", err.Error(),
						)
					} else {
						wc.engine.logger.Info("immediate workflow cleanup completed",
							"workflow_id", workflowID,
						)
					}
				}
			}

			wc.engine.mu.Lock()
			delete(wc.engine.activeWorkflows, workflowID)
			wc.engine.mu.Unlock()

			wc.engine.logger.Info("workflow completed and cleaned up",
				"workflow_id", workflowID,
			)
		} else {
			wc.engine.logger.Warn("workflow completion handlers failed - retaining data for retry",
				"workflow_id", workflowID,
			)
		}
	}

	return nil
}

func (wc *WorkflowCoordinator) hasNodesForWorkflow(ctx context.Context, workflowID string, isEmpty bool, pendingItems []ports.QueueItem) bool {
	if !isEmpty {
		return true
	}

	for _, item := range pendingItems {
		if item.WorkflowID == workflowID {
			return true
		}
	}

	// Check for active claims for this workflow
	if wc.hasActiveClaimsForWorkflow(ctx, workflowID) {
		return true
	}

	return false
}

func (wc *WorkflowCoordinator) hasActiveClaimsForWorkflow(ctx context.Context, workflowID string) bool {
	// Check if there are any active claims by looking for claim keys in storage
	// Since claims use the pattern "claims:{itemID}", we need to check each one
	claimPrefix := "claims:"
	items, err := wc.engine.storage.List(ctx, claimPrefix)
	if err != nil {
		wc.engine.logger.Error("failed to list claims for workflow completion check",
			"workflow_id", workflowID,
			"error", err.Error(),
		)
		// If we can't check claims, assume there might be active ones to be safe
		return true
	}

	// Check each claim to see if the associated item belongs to this workflow
	for _, item := range items {
		// Extract item ID from claim key (format: "claims:{itemID}")
		itemID := strings.TrimPrefix(item.Key, "claims:")

		// Get the item data using the queue's item data key format
		itemDataKey := "item:" + itemID
		itemData, err := wc.engine.storage.Get(ctx, itemDataKey)
		if err != nil {
			// If we can't get item data, this claim might be stale - skip it
			continue
		}

		// Check if this item data contains our workflow ID (simple string search)
		// This is a simple check since we don't have access to deserializeItem
		if strings.Contains(string(itemData), `"workflow_id":"`+workflowID+`"`) {
			wc.engine.logger.Debug("found active claim for workflow",
				"workflow_id", workflowID,
				"item_id", itemID,
			)
			return true
		}
	}

	return false
}

func (wc *WorkflowCoordinator) PauseWorkflow(ctx context.Context, workflowID string) error {
	workflow, err := wc.getAndLockWorkflow(workflowID)
	if err != nil {
		return err
	}
	defer workflow.mu.Unlock()

	if workflow.Status != ports.WorkflowStateRunning {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "workflow is not running",
			Details: map[string]interface{}{
				"workflow_id":    workflowID,
				"current_status": workflow.Status,
			},
		}
	}

	workflow.Status = ports.WorkflowStatePaused

	wc.engine.logger.Info("workflow paused",
		"workflow_id", workflowID,
	)

	return wc.executor.persistWorkflowState(ctx, workflow)
}

func (wc *WorkflowCoordinator) ResumeWorkflow(ctx context.Context, workflowID string) error {
	workflow, err := wc.getAndLockWorkflow(workflowID)
	if err != nil {
		return err
	}
	defer workflow.mu.Unlock()

	if workflow.Status != ports.WorkflowStatePaused {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "workflow is not paused",
			Details: map[string]interface{}{
				"workflow_id":    workflowID,
				"current_status": workflow.Status,
			},
		}
	}

	workflow.Status = ports.WorkflowStateRunning

	wc.engine.logger.Info("workflow resumed",
		"workflow_id", workflowID,
	)

	return wc.executor.persistWorkflowState(ctx, workflow)
}

func (wc *WorkflowCoordinator) StopWorkflow(ctx context.Context, workflowID string) error {
	wc.engine.mu.Lock()
	workflow, exists := wc.engine.activeWorkflows[workflowID]
	if exists {
		delete(wc.engine.activeWorkflows, workflowID)
	}
	wc.engine.mu.Unlock()

	if !exists {
		return domain.NewNotFoundError("workflow", workflowID)
	}

	workflow.mu.Lock()
	workflow.Status = ports.WorkflowStateFailed
	now := time.Now()
	workflow.CompletedAt = &now
	errorStr := "workflow stopped by request"
	workflow.LastError = &errorStr
	workflow.mu.Unlock()

	wc.engine.logger.Info("workflow stopped",
		"workflow_id", workflowID,
	)

	return wc.executor.persistWorkflowState(ctx, workflow)
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if domainErr, ok := err.(domain.Error); ok {
		return domainErr.Type == domain.ErrorTypeNotFound
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "empty")
}

func (wc *WorkflowCoordinator) getAndLockWorkflow(workflowID string) (*WorkflowInstance, error) {
	wc.engine.mu.RLock()
	workflow, exists := wc.engine.activeWorkflows[workflowID]
	wc.engine.mu.RUnlock()

	if !exists {
		return nil, domain.NewNotFoundError("workflow", workflowID)
	}

	workflow.mu.Lock()
	return workflow, nil
}
