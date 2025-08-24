package engine

import (
	"context"
	"encoding/json"
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
	if wc.engine.readyQueue == nil {
		wc.engine.logger.Debug("ready queue not available, skipping ready node processing")
		return domain.NewNotFoundError("queue_item", "ready queue not available")
	}

	claimDuration := wc.engine.config.NodeExecutionTimeout
	if claimDuration <= 0 {
		claimDuration = 5 * time.Minute
	}

	item, err := wc.engine.readyQueue.Dequeue(ctx, ports.WithClaim(wc.engine.nodeID, claimDuration))

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
			wc.engine.logger.Debug("node execution completed successfully",
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
	if wc.engine.storage == nil {
		return nil
	}

	workflowKeys, err := wc.engine.storage.List(ctx, "workflow:state:")
	if err != nil {
		return err
	}

	for _, kv := range workflowKeys {
		var workflow WorkflowInstance
		if err := json.Unmarshal(kv.Value, &workflow); err != nil {
			wc.engine.logger.Warn("failed to unmarshal workflow", "key", kv.Key, "error", err.Error())
			continue
		}

		if workflow.Status == ports.WorkflowStateRunning {
			if err := wc.CheckWorkflowCompletion(ctx, &workflow); err != nil {
				wc.engine.logger.Error("failed to check workflow completion",
					"workflow_id", workflow.ID,
					"error", err.Error(),
				)
			}
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

	isEmpty, err := wc.engine.readyQueue.IsEmpty(ctx)
	if err != nil {
		return err
	}

	pendingItems, err := wc.engine.pendingQueue.GetItems(ctx)
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
			if wc.engine.cleaner != nil {
				if err := wc.engine.cleaner.NukeWorkflow(ctx, workflowID); err != nil {
					wc.engine.logger.Error("failed to cleanup completed workflow",
						"workflow_id", workflowID,
						"error", err.Error(),
					)
				}
			}

			wc.engine.logger.Debug("workflow completed and cleaned up",
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

	if wc.hasActiveClaimsForWorkflow(ctx, workflowID) {
		return true
	}

	return false
}

func (wc *WorkflowCoordinator) hasActiveClaimsForWorkflow(ctx context.Context, workflowID string) bool {
	claimPrefix := "claims:"
	items, err := wc.engine.storage.List(ctx, claimPrefix)
	if err != nil {
		wc.engine.logger.Error("failed to list claims for workflow completion check",
			"workflow_id", workflowID,
			"error", err.Error(),
		)
		return true
	}

	activeClaimsCount := 0
	for _, item := range items {
		itemID := strings.TrimPrefix(item.Key, "claims:")

		itemDataKey := "item:" + itemID
		itemData, err := wc.engine.storage.Get(ctx, itemDataKey)
		if err != nil {
			wc.engine.logger.Debug("failed to get item data for claim check",
				"workflow_id", workflowID,
				"item_id", itemID,
				"error", err.Error(),
			)
			continue
		}

		if strings.Contains(string(itemData), `"workflow_id":"`+workflowID+`"`) {
			activeClaimsCount++
			wc.engine.logger.Debug("found active claim for workflow",
				"workflow_id", workflowID,
				"item_id", itemID,
			)
		}
	}

	if activeClaimsCount > 0 {
		wc.engine.logger.Debug("workflow has active claims, cannot complete yet",
			"workflow_id", workflowID,
			"active_claims", activeClaimsCount,
		)
		return true
	}

	wc.engine.logger.Debug("no active claims found for workflow",
		"workflow_id", workflowID,
	)
	return false
}

func (wc *WorkflowCoordinator) PauseWorkflow(ctx context.Context, workflowID string) error {
	workflow, err := wc.getWorkflow(ctx, workflowID)
	if err != nil {
		return err
	}

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

	return wc.engine.stateManager.SaveWorkflowState(ctx, workflow)
}

func (wc *WorkflowCoordinator) ResumeWorkflow(ctx context.Context, workflowID string) error {
	workflow, err := wc.getWorkflow(ctx, workflowID)
	if err != nil {
		return err
	}

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

	return wc.engine.stateManager.SaveWorkflowState(ctx, workflow)
}

func (wc *WorkflowCoordinator) StopWorkflow(ctx context.Context, workflowID string) error {
	workflow, err := wc.engine.stateManager.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return err
	}

	if workflow == nil {
		return domain.NewNotFoundError("workflow", workflowID)
	}

	workflow.Status = ports.WorkflowStateFailed
	now := time.Now()
	workflow.CompletedAt = &now
	errorStr := "workflow stopped by request"
	workflow.LastError = &errorStr

	if err := wc.engine.stateManager.SaveWorkflowState(ctx, workflow); err != nil {
		return err
	}

	if wc.engine.cleaner != nil {
		if err := wc.engine.cleaner.NukeWorkflow(ctx, workflowID); err != nil {
			wc.engine.logger.Error("failed to cleanup stopped workflow",
				"workflow_id", workflowID,
				"error", err.Error(),
			)
		}
	}

	wc.engine.logger.Debug("workflow stopped and cleaned up",
		"workflow_id", workflowID,
	)

	return nil
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

func (wc *WorkflowCoordinator) getWorkflow(ctx context.Context, workflowID string) (*WorkflowInstance, error) {
	workflow, err := wc.engine.stateManager.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	if workflow == nil {
		return nil, domain.NewNotFoundError("workflow", workflowID)
	}

	return workflow, nil
}
