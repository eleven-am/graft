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
	
	item, err := wc.engine.queue.DequeueReady(ctx)
	
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
		finalState := workflow.CurrentState
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

		if wc.engine.lifecycleManager != nil {
			if err := wc.engine.lifecycleManager.TriggerCompletion(workflowID, finalState); err != nil {
				wc.engine.logger.Error("failed to trigger completion handlers",
					"workflow_id", workflowID,
					"error", err.Error(),
				)
			}
		}

		if wc.engine.cleanupScheduler != nil {
			cleanupOptions := CleanupOptions{
				PreserveState:   false,
				PreserveAudit:   true,
				RetentionPeriod: time.Hour * 24,
				Force:           false,
			}
			if err := wc.engine.cleanupScheduler.ScheduleWorkflowCleanup(workflowID, ports.WorkflowStateCompleted, cleanupOptions); err != nil {
				wc.engine.logger.Error("failed to schedule workflow cleanup",
					"workflow_id", workflowID,
					"error", err.Error(),
				)
			}
		}

		wc.engine.mu.Lock()
		delete(wc.engine.activeWorkflows, workflowID)
		wc.engine.mu.Unlock()

		wc.engine.logger.Info("workflow removed from active workflows",
			"workflow_id", workflowID,
		)
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
				"workflow_id":     workflowID,
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
				"workflow_id":     workflowID,
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