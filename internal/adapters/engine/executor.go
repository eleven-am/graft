package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type NodeExecutor struct {
	engine              *Engine
	recoverableExecutor *RecoverableExecutor
}

func NewNodeExecutor(engine *Engine) *NodeExecutor {
	return &NodeExecutor{
		engine:              engine,
		recoverableExecutor: NewRecoverableExecutor(engine.logger, engine.metricsTracker),
	}
}

func (ne *NodeExecutor) ExecuteNode(ctx context.Context, item *ports.QueueItem) error {
	ne.engine.logger.Debug("starting node execution",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
		"item_id", item.ID,
	)

	defer func() {
		if err := ne.engine.readyQueue.ReleaseWorkClaim(ctx, item.ID, ne.engine.nodeID); err != nil {
			ne.engine.logger.Error("failed to release work claim",
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"item_id", item.ID,
				"error", err.Error(),
			)
		} else {
			ne.engine.logger.Debug("work claim released successfully",
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"item_id", item.ID,
			)
		}
	}()

	ne.engine.logger.Debug("about to get workflow",
		"workflow_id", item.WorkflowID,
		"item_id", item.ID,
	)

	workflow, err := ne.getWorkflow(ctx, item.WorkflowID)
	if err != nil {
		ne.engine.logger.Error("failed to get workflow",
			"workflow_id", item.WorkflowID,
			"error", err.Error(),
		)
		return err
	}

	ne.engine.logger.Debug("about to get node from registry",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
	)

	node, err := ne.engine.nodeRegistry.GetNode(item.NodeName)
	if err != nil {
		ne.engine.logger.Error("node not found in registry",
			"workflow_id", item.WorkflowID,
			"node_name", item.NodeName,
			"error", err.Error(),
		)
		return err
	}

	if !ne.engine.resourceManager.CanExecuteNode(item.NodeName) {
		ne.engine.logger.Debug("insufficient resources for node execution",
			"workflow_id", item.WorkflowID,
			"node_name", item.NodeName,
		)
		if err := ne.requeueNode(ctx, item, "insufficient_resources"); err != nil {
			return err
		}
		return nil
	}

	if err := ne.engine.resourceManager.AcquireNode(item.NodeName); err != nil {
		ne.engine.logger.Error("failed to acquire resources",
			"workflow_id", item.WorkflowID,
			"node_name", item.NodeName,
			"error", err.Error(),
		)
		return err
	}

	defer func() {
		if err := ne.engine.resourceManager.ReleaseNode(item.NodeName); err != nil {
			ne.engine.logger.Error("failed to release resources",
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"error", err.Error(),
			)
		}
	}()

	workflow.mu.RLock()
	currentState := workflow.CurrentState
	workflow.mu.RUnlock()

	workflowCtx := &domain.WorkflowContext{
		WorkflowID:  item.WorkflowID,
		NodeName:    item.NodeName,
		ExecutionID: item.ID,
		StartedAt:   workflow.StartedAt,
		Metadata:    workflow.Metadata,
		ClusterInfo: domain.ClusterBasicInfo{
			NodeID:   ne.engine.nodeID,
			IsLeader: ne.engine.raft != nil && ne.engine.raft.IsLeader(),
			Status:   "running",
		},
		RetryCount: item.RetryCount,
		Priority:   item.Priority,
	}

	enrichedCtx := domain.WithWorkflowContext(ctx, workflowCtx)

	canStart := node.CanStart(enrichedCtx, currentState, item.Config)
	ne.engine.logger.Debug("checking if node can start",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
		"can_start", canStart,
		"current_state", currentState,
		"config", item.Config,
	)

	if !canStart {
		ne.engine.logger.Debug("node cannot start with current state",
			"workflow_id", item.WorkflowID,
			"node_name", item.NodeName,
		)
		if err := ne.requeueToPending(ctx, item, "start_condition_not_met"); err != nil {
			return err
		}
		return nil
	}

	ne.engine.logger.Debug("executing node",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
		"current_state_type", fmt.Sprintf("%T", currentState),
	)

	startTime := time.Now()

	results, nextNodes, err := ne.recoverableExecutor.ExecuteWithRecovery(enrichedCtx, node, currentState, item.Config, item, workflowCtx)
	duration := time.Since(startTime)

	ne.engine.logger.Debug("node execution completed",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
		"duration", duration,
		"error", err,
		"result_type", fmt.Sprintf("%T", results),
		"next_nodes_count", len(nextNodes),
		"next_nodes_count", len(nextNodes),
	)

	executedNode := ports.ExecutedNode{
		NodeName:   item.NodeName,
		ExecutedAt: startTime,
		Duration:   duration,
		Status:     ports.NodeExecutionStatusCompleted,
		Config:     item.Config,
		Results:    results,
	}

	if err != nil {
		errorStr := err.Error()
		executedNode.Error = &errorStr

		if panicErr, isPanic := err.(*domain.WorkflowPanicError); isPanic {
			executedNode.Status = ports.NodeExecutionStatusPanicFailed
			ne.engine.logger.Error("node execution panicked",
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"duration", duration,
				"panic_value", panicErr.PanicValue,
				"stack_trace", panicErr.StackTrace,
			)
		} else {
			executedNode.Status = ports.NodeExecutionStatusFailed
			ne.engine.logger.Error("node execution failed",
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"duration", duration,
				"error", err.Error(),
			)
		}

		if err := ne.handleExecutionFailure(ctx, workflow, item, err); err != nil {
			return err
		}
		return nil
	}

	ne.engine.logger.Debug("node execution completed successfully",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
		"duration", duration,
		"next_nodes", len(nextNodes),
	)

	if err := ne.updateWorkflowState(ctx, workflow, results, &executedNode); err != nil {
		ne.engine.logger.Error("updateWorkflowState failed - aborting node processing",
			"workflow_id", item.WorkflowID,
			"node_name", item.NodeName,
			"error", err.Error(),
		)
		return err
	}

	ne.engine.logger.Debug("workflow state updated successfully",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
	)

	if err := ne.triggerEvaluationAfterExecution(ctx, item.WorkflowID, item.NodeName, workflow.CurrentState); err != nil {
		ne.engine.logger.Error("failed to trigger evaluation after execution",
			"workflow_id", item.WorkflowID,
			"node_name", item.NodeName,
			"error", err.Error(),
		)
	}

	ne.engine.logger.Debug("queueing next nodes",
		"workflow_id", item.WorkflowID,
		"current_node", item.NodeName,
		"next_nodes_count", len(nextNodes),
	)

	for i, nextNode := range nextNodes {
		ne.engine.logger.Debug("queueing next node",
			"workflow_id", item.WorkflowID,
			"current_node", item.NodeName,
			"next_node_index", i,
			"node_name", nextNode.NodeName,
		)

		if err := ne.queueNextNode(ctx, item.WorkflowID, nextNode); err != nil {
			ne.engine.logger.Error("failed to queue next node",
				"workflow_id", item.WorkflowID,
				"current_node", item.NodeName,
				"next_node", nextNode.NodeName,
				"error", err.Error(),
			)
		} else {
			ne.engine.logger.Debug("next node queued successfully",
				"workflow_id", item.WorkflowID,
				"current_node", item.NodeName,
				"next_node", nextNode.NodeName,
			)
		}
	}

	if len(nextNodes) == 0 {
		ne.engine.logger.Debug("no next nodes, checking workflow completion immediately",
			"workflow_id", item.WorkflowID,
			"completed_node", item.NodeName,
		)
		if err := ne.engine.coordinator.CheckWorkflowCompletion(ctx, workflow); err != nil {
			ne.engine.logger.Error("failed immediate workflow completion check",
				"workflow_id", item.WorkflowID,
				"error", err.Error(),
			)
		}
	}

	return nil
}

func (ne *NodeExecutor) getWorkflow(ctx context.Context, workflowID string) (*WorkflowInstance, error) {
	workflow, err := ne.engine.stateManager.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	if workflow == nil {
		return nil, domain.NewNotFoundError("workflow", workflowID)
	}

	return workflow, nil
}

func (ne *NodeExecutor) requeueNode(ctx context.Context, item *ports.QueueItem, reason string) error {
	ne.engine.logger.Debug("requeuing node to ready queue",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
		"reason", reason,
	)

	time.Sleep(ne.engine.config.RetryBackoff)
	return ne.engine.readyQueue.Enqueue(ctx, *item)
}

func (ne *NodeExecutor) requeueToPending(ctx context.Context, item *ports.QueueItem, reason string) error {
	ne.engine.logger.Debug("requeuing node to pending queue",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
		"reason", reason,
	)

	return ne.engine.pendingQueue.Enqueue(ctx, *item)
}

func (ne *NodeExecutor) handleExecutionFailure(ctx context.Context, workflow *WorkflowInstance, item *ports.QueueItem, execErr error) error {
	workflow.mu.Lock()
	errorStr := execErr.Error()
	workflow.LastError = &errorStr
	workflow.mu.Unlock()

	ne.engine.logger.Error("marking workflow as failed",
		"workflow_id", workflow.ID,
		"failed_node", item.NodeName,
		"error", execErr.Error(),
	)

	workflow.mu.Lock()
	workflow.Status = ports.WorkflowStateFailed
	now := time.Now()
	workflow.CompletedAt = &now
	workflow.mu.Unlock()

	if err := ne.persistWorkflowState(ctx, workflow); err != nil {
		return err
	}

	if ne.engine.lifecycleManager != nil && ne.engine.dataCollector != nil {
		stackTrace := ""
		if panicErr, isPanic := execErr.(*domain.WorkflowPanicError); isPanic {
			stackTrace = panicErr.StackTrace
		}

		errorData, collectErr := ne.engine.dataCollector.CollectWorkflowErrorData(ctx, workflow, execErr, item.NodeName, stackTrace)
		if collectErr != nil {
			ne.engine.logger.Error("failed to collect error data",
				"workflow_id", workflow.ID,
				"error", collectErr.Error(),
			)
		} else {
			if err := ne.engine.lifecycleManager.TriggerError(ctx, *errorData); err != nil {
				ne.engine.logger.Error("failed to trigger error handlers",
					"workflow_id", workflow.ID,
					"error", err.Error(),
				)
			}
		}
	}

	if ne.engine.cleaner != nil {
		if err := ne.engine.cleaner.NukeWorkflow(ctx, workflow.ID); err != nil {
			ne.engine.logger.Error("failed to cleanup failed workflow",
				"workflow_id", workflow.ID,
				"error", err.Error(),
			)
		}
	}

	return nil
}

func (ne *NodeExecutor) updateWorkflowState(ctx context.Context, workflow *WorkflowInstance, results interface{}, executedNode *ports.ExecutedNode) error {
	ne.engine.logger.Debug("updating workflow state",
		"workflow_id", workflow.ID,
		"node_name", executedNode.NodeName,
		"semaphore_nil", ne.engine.semaphore == nil,
	)

	if ne.engine.semaphore == nil {
		ne.engine.logger.Warn("semaphore is nil - proceeding without atomic locking",
			"workflow_id", workflow.ID,
			"node_name", executedNode.NodeName,
		)
	} else {
		// Normal path with semaphore locking
		semaphoreTimeout := 30 * time.Second
		if ne.engine.config.NodeExecutionTimeout > 0 {
			semaphoreTimeout = ne.engine.config.NodeExecutionTimeout
		}

		if err := ne.engine.semaphore.Acquire(ctx, workflow.ID, ne.engine.nodeID, semaphoreTimeout); err != nil {
			ne.engine.logger.Error("failed to acquire workflow state lock",
				"workflow_id", workflow.ID,
				"node_name", executedNode.NodeName,
				"error", err.Error(),
			)
			return domain.Error{
				Type:    domain.ErrorTypeConflict,
				Message: "failed to acquire workflow state lock",
				Details: map[string]interface{}{
					"workflow_id": workflow.ID,
					"node_name":   executedNode.NodeName,
					"error":       err.Error(),
				},
			}
		}

		defer func() {
			if err := ne.engine.semaphore.Release(ctx, workflow.ID, ne.engine.nodeID); err != nil {
				ne.engine.logger.Error("failed to release workflow state lock",
					"workflow_id", workflow.ID,
					"node_name", executedNode.NodeName,
					"error", err.Error(),
				)
			}
		}()
	}

	workflow.mu.Lock()
	defer workflow.mu.Unlock()

	if results != nil {
		if workflow.CurrentState == nil {
			workflow.CurrentState = results
		} else {
			ne.engine.logger.Debug("merging states",
				"current_state_type", fmt.Sprintf("%T", workflow.CurrentState),
				"results_type", fmt.Sprintf("%T", results),
				"workflow_id", workflow.ID,
				"node_name", executedNode.NodeName,
			)

			merged, err := MergeStates(workflow.CurrentState, results)
			if err != nil {
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to merge workflow states",
					Details: map[string]interface{}{
						"workflow_id": workflow.ID,
						"node_name":   executedNode.NodeName,
						"error":       err.Error(),
					},
				}
			}

			workflow.CurrentState = merged
		}
	}

	if err := ne.persistExecutedNode(ctx, workflow.ID, executedNode); err != nil {
		ne.engine.logger.Error("failed to persist executed node",
			"workflow_id", workflow.ID,
			"node_name", executedNode.NodeName,
			"error", err.Error(),
		)
	}

	stateKeyCount := 0
	if stateMap, ok := workflow.CurrentState.(map[string]interface{}); ok {
		stateKeyCount = len(stateMap)
	}

	ne.engine.logger.Debug("workflow state updated",
		"workflow_id", workflow.ID,
		"node_name", executedNode.NodeName,
		"state_keys", stateKeyCount,
	)

	return ne.persistWorkflowState(ctx, workflow)
}

func (ne *NodeExecutor) persistWorkflowState(ctx context.Context, workflow *WorkflowInstance) error {
	stateKey := fmt.Sprintf("workflow:state:%s", workflow.ID)

	workflowData := map[string]interface{}{
		"id":            workflow.ID,
		"status":        string(workflow.Status),
		"current_state": workflow.CurrentState,
		"started_at":    workflow.StartedAt,
		"completed_at":  workflow.CompletedAt,
		"metadata":      workflow.Metadata,
		"last_error":    workflow.LastError,
	}

	serializedState, err := serializeWorkflowData(workflowData)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to serialize workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflow.ID,
				"error":       err.Error(),
			},
		}
	}

	if err := ne.engine.storage.Put(ctx, stateKey, serializedState); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to persist workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflow.ID,
				"error":       err.Error(),
			},
		}
	}

	ne.engine.logger.Debug("workflow state persisted",
		"workflow_id", workflow.ID,
		"status", workflow.Status,
	)

	return nil
}

func (ne *NodeExecutor) queueNextNode(ctx context.Context, workflowID string, nextNode ports.NextNode) error {
	ne.engine.logger.Debug("queueing next node",
		"workflow_id", workflowID,
		"node_name", nextNode.NodeName,
	)

	item := &ports.QueueItem{
		ID:         generateItemID(),
		WorkflowID: workflowID,
		NodeName:   nextNode.NodeName,
		Config:     nextNode.Config,
		EnqueuedAt: time.Now(),
	}

	ne.engine.logger.Debug("queue item created",
		"workflow_id", workflowID,
		"node_name", item.NodeName,
	)

	node, err := ne.engine.nodeRegistry.GetNode(nextNode.NodeName)
	if err != nil {
		ne.engine.logger.Error("next node not found in registry",
			"workflow_id", workflowID,
			"node_name", nextNode.NodeName,
			"error", err.Error(),
		)
		return err
	}

	ne.engine.logger.Debug("node found in registry",
		"workflow_id", workflowID,
		"node_name", nextNode.NodeName,
	)

	workflow, err := ne.getWorkflow(ctx, workflowID)
	if err != nil {
		ne.engine.logger.Error("failed to get workflow",
			"workflow_id", workflowID,
			"error", err.Error(),
		)
		return err
	}

	ne.engine.logger.Debug("workflow state retrieved",
		"workflow_id", workflowID,
		"workflow_status", workflow.Status,
	)

	if nextNode.IdempotencyKey != nil && *nextNode.IdempotencyKey != "" {
		if err := ne.checkAndClaimIdempotencyKey(ctx, workflowID, *nextNode.IdempotencyKey); err != nil {
			if _, isAlreadyClaimed := err.(*IdempotencyKeyClaimedError); isAlreadyClaimed {
				ne.engine.logger.Debug("skipping node due to idempotency key already claimed",
					"workflow_id", workflowID,
					"node_name", nextNode.NodeName,
					"idempotency_key", *nextNode.IdempotencyKey,
				)
				return nil
			}
			return err
		}

		ne.engine.logger.Debug("claimed idempotency key for node",
			"workflow_id", workflowID,
			"node_name", nextNode.NodeName,
			"idempotency_key", *nextNode.IdempotencyKey,
		)
	}

	workflow.mu.RLock()
	currentState := workflow.CurrentState
	workflow.mu.RUnlock()

	ne.engine.logger.Debug("checking node start conditions",
		"workflow_id", workflowID,
		"node_name", nextNode.NodeName,
		"current_state_type", fmt.Sprintf("%T", currentState),
	)

	canStart := node.CanStart(ctx, currentState, nextNode.Config)
	ne.engine.logger.Debug("node start check result",
		"workflow_id", workflowID,
		"node_name", nextNode.NodeName,
		"can_start", canStart,
	)

	if canStart {
		ne.engine.logger.Debug("queueing to ready queue",
			"workflow_id", workflowID,
			"node_name", nextNode.NodeName,
		)
		err := ne.engine.readyQueue.Enqueue(ctx, *item)
		if err != nil {
			ne.engine.logger.Error("failed to enqueue to ready queue",
				"workflow_id", workflowID,
				"node_name", nextNode.NodeName,
				"error", err.Error(),
			)
		} else {
			ne.engine.logger.Debug("node enqueued to ready queue",
				"workflow_id", workflowID,
				"node_name", nextNode.NodeName,
			)
		}
		return err
	} else {
		ne.engine.logger.Debug("queueing to pending queue",
			"workflow_id", workflowID,
			"node_name", nextNode.NodeName,
		)
		err := ne.engine.pendingQueue.Enqueue(ctx, *item)
		if err != nil {
			ne.engine.logger.Error("failed to enqueue to pending queue",
				"workflow_id", workflowID,
				"node_name", nextNode.NodeName,
				"error", err.Error(),
			)
		} else {
			ne.engine.logger.Debug("node enqueued to pending queue",
				"workflow_id", workflowID,
				"node_name", nextNode.NodeName,
			)
		}
		return err
	}
}

func serializeWorkflowData(data map[string]interface{}) ([]byte, error) {
	return json.Marshal(data)
}

type IdempotencyKeyClaimedError struct {
	WorkflowID     string
	IdempotencyKey string
}

func (e *IdempotencyKeyClaimedError) Error() string {
	return fmt.Sprintf("idempotency key '%s' already claimed for workflow '%s'", e.IdempotencyKey, e.WorkflowID)
}

func (ne *NodeExecutor) checkAndClaimIdempotencyKey(ctx context.Context, workflowID, idempotencyKey string) error {
	if ne.engine.storage == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "storage not available for idempotency check",
			Details: map[string]interface{}{
				"workflow_id":     workflowID,
				"idempotency_key": idempotencyKey,
			},
		}
	}

	storageKey := fmt.Sprintf("workflow:idempotency:%s:%s", workflowID, idempotencyKey)

	_, err := ne.engine.storage.Get(ctx, storageKey)
	if err == nil {
		return &IdempotencyKeyClaimedError{
			WorkflowID:     workflowID,
			IdempotencyKey: idempotencyKey,
		}
	}

	keyData := map[string]interface{}{
		"workflow_id":     workflowID,
		"idempotency_key": idempotencyKey,
		"claimed_at":      time.Now(),
	}

	serializedData, err := json.Marshal(keyData)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to serialize idempotency key data",
			Details: map[string]interface{}{
				"workflow_id":     workflowID,
				"idempotency_key": idempotencyKey,
				"error":           err.Error(),
			},
		}
	}

	if err := ne.engine.storage.Put(ctx, storageKey, serializedData); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to claim idempotency key",
			Details: map[string]interface{}{
				"workflow_id":     workflowID,
				"idempotency_key": idempotencyKey,
				"error":           err.Error(),
			},
		}
	}

	return nil
}

func (ne *NodeExecutor) triggerEvaluationAfterExecution(ctx context.Context, workflowID, nodeName string, currentState interface{}) error {
	if ne.engine.evaluationTrigger == nil {
		return nil
	}

	event := StateChangeEvent{
		WorkflowID: workflowID,
		ChangedBy:  nodeName,
		NewState:   currentState,
		Timestamp:  time.Now(),
		NodeName:   nodeName,
		EventType:  EventTypeNodeCompleted,
	}

	return ne.engine.evaluationTrigger.TriggerEvaluation(ctx, event)
}

func (ne *NodeExecutor) persistExecutedNode(ctx context.Context, workflowID string, executedNode *ports.ExecutedNode) error {
	executionKey := fmt.Sprintf("workflow:execution:%s:%s_%d", workflowID, executedNode.NodeName, executedNode.ExecutedAt.UnixNano())

	nodeData := map[string]interface{}{
		"node_name":   executedNode.NodeName,
		"executed_at": executedNode.ExecutedAt.Format(time.RFC3339Nano),
		"duration":    executedNode.Duration.Nanoseconds(),
		"status":      string(executedNode.Status),
		"config":      executedNode.Config,
		"results":     executedNode.Results,
	}

	if executedNode.Error != nil {
		nodeData["error"] = *executedNode.Error
	}

	serializedData, err := json.Marshal(nodeData)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to serialize executed node",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"node_name":   executedNode.NodeName,
				"error":       err.Error(),
			},
		}
	}

	if err := ne.engine.storage.Put(ctx, executionKey, serializedData); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to persist executed node",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"node_name":   executedNode.NodeName,
				"error":       err.Error(),
			},
		}
	}

	return nil
}
