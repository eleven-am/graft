package engine

import (
	"context"
	"errors"
	"fmt"
	json "github.com/goccy/go-json"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Executor struct {
	config       domain.EngineConfig
	nodeRegistry ports.NodeRegistryPort
	stateManager *StateManager
	queue        ports.QueuePort
	storage      ports.StoragePort
	logger       *slog.Logger
	metrics      *domain.ExecutionMetrics
}

func NewExecutor(config domain.EngineConfig, nodeRegistry ports.NodeRegistryPort, stateManager *StateManager, queue ports.QueuePort, storage ports.StoragePort, logger *slog.Logger, metrics *domain.ExecutionMetrics) *Executor {
	return &Executor{
		config:       config,
		nodeRegistry: nodeRegistry,
		stateManager: stateManager,
		queue:        queue,
		storage:      storage,
		logger:       logger.With("component", "executor"),
		metrics:      metrics,
	}
}

type WorkItem struct {
	WorkflowID   string          `json:"workflow_id"`
	NodeName     string          `json:"node_name"`
	Config       json.RawMessage `json:"config"`
	EnqueuedAt   time.Time       `json:"enqueued_at"`
	ProcessAfter time.Time       `json:"process_after"`
	RetryCount   int             `json:"retry_count"`
}

func (e *Executor) ExecuteNode(ctx context.Context, workflowID, nodeName string, config json.RawMessage) error {
	return e.ExecuteNodeWithRetry(ctx, workflowID, nodeName, config, 0)
}

func (e *Executor) ExecuteNodeWithRetry(ctx context.Context, workflowID, nodeName string, config json.RawMessage, currentRetryCount int) error {
	node, nodeErr := e.nodeRegistry.GetNode(nodeName)
	if nodeErr != nil {
		e.logger.Error("node not found in registry",
			"workflow_id", workflowID,
			"node_name", nodeName,
			"error", nodeErr)
		return domain.ErrNotFound
	}

	workflow, workflowErr := e.stateManager.LoadWorkflowState(ctx, workflowID)
	if workflowErr != nil {
		return domain.NewDiscoveryError("executor", "load_workflow_state", workflowErr)
	}

	if workflow.Status == domain.WorkflowStatePaused {
		e.logger.Debug("workflow is paused, skipping execution",
			"workflow_id", workflowID,
			"node_name", nodeName)
		return nil
	}

	workflowCtx := &domain.WorkflowContext{
		WorkflowID:  workflowID,
		NodeName:    nodeName,
		ExecutionID: fmt.Sprintf("%s-%s-%d", workflowID, nodeName, time.Now().UnixNano()),
		StartedAt:   workflow.StartedAt,
		Metadata:    workflow.Metadata,
	}

	enrichedCtx := domain.WithWorkflowContext(ctx, workflowCtx)

	fmt.Printf("🚀🚀🚀 EXECUTOR: About to call CanStart for node %s\n", nodeName)
	fmt.Printf("🚀 workflow.CurrentState type: %T\n", workflow.CurrentState)
	fmt.Printf("🚀 config type: %T\n", config)

	canStart := node.CanStart(enrichedCtx, workflow.CurrentState, config)
	fmt.Printf("🚀 CanStart returned: %v\n", canStart)
	if !canStart {
		e.logger.Debug("node cannot start with current state",
			"workflow_id", workflowID,
			"node_name", nodeName)
		return e.requeueNode(ctx, workflowID, nodeName, config)
	}

	startTime := time.Now()

	timeoutCtx, cancel := context.WithTimeout(enrichedCtx, e.config.NodeExecutionTimeout)
	defer cancel()

	e.metrics.IncrementNodesExecuted()

	var portResult *ports.NodeResult
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				e.logger.Error("node execution panicked",
					"workflow_id", workflowID,
					"node_name", nodeName,
					"panic", r)

				err = fmt.Errorf("node execution panicked: %v", r)

				e.metrics.IncrementNodesFailed()
			}
		}()

		portResult, err = node.Execute(timeoutCtx, workflow.CurrentState, config)
	}()

	nodeResult := portResult.ToInternal()
	duration := time.Since(startTime)

	e.metrics.AddExecutionTime(duration)

	executedNode := domain.ExecutedNodeData{
		NodeName:   nodeName,
		ExecutedAt: startTime,
		Duration:   duration,
		Status:     string(domain.NodeExecutionStatusCompleted),
		Config:     config,
	}

	if err != nil {
		errorStr := err.Error()
		executedNode.Error = &errorStr
		executedNode.Status = string(domain.NodeExecutionStatusFailed)

		e.metrics.IncrementNodesFailed()

		if errors.Is(err, context.DeadlineExceeded) {
			e.metrics.IncrementNodesTimedOut()

			e.logger.Warn("node execution timed out",
				"workflow_id", workflowID,
				"node_name", nodeName,
				"duration", duration,
				"timeout", e.config.NodeExecutionTimeout,
				"retry_count", currentRetryCount)
		} else {
			e.logger.Error("node execution failed",
				"workflow_id", workflowID,
				"node_name", nodeName,
				"duration", duration,
				"error", err,
				"retry_count", currentRetryCount)
		}

		return e.requeueNodeWithBackoff(ctx, workflowID, nodeName, config, currentRetryCount)
	}

	e.metrics.IncrementNodesSucceeded()

	var resultsBytes json.RawMessage
	if nodeResult.GlobalState != nil {
		var err error
		resultsBytes, err = json.Marshal(nodeResult.GlobalState)
		if err != nil {
			return domain.NewDiscoveryError("executor", "marshal_results", err)
		}
	}
	executedNode.Results = resultsBytes

	err = e.stateManager.UpdateWorkflowState(ctx, workflowID, func(wf *domain.WorkflowInstance) error {
		if resultsBytes != nil && len(resultsBytes) > 0 {
			if len(wf.CurrentState) == 0 {
				wf.CurrentState = resultsBytes
			} else {
				merged, mergeErr := domain.MergeStates(wf.CurrentState, resultsBytes)
				if mergeErr != nil {
					return domain.NewDiscoveryError("executor", "merge_states", mergeErr)
				}
				wf.CurrentState = merged
			}
		}

		return e.persistExecutedNode(ctx, workflowID, &executedNode)
	})

	if err != nil {
		e.logger.Error("failed to update workflow state",
			"workflow_id", workflowID,
			"node_name", nodeName,
			"error", err)
		return err
	}

	e.logger.Debug("node executed successfully",
		"workflow_id", workflowID,
		"node_name", nodeName,
		"duration", duration,
		"next_nodes_count", len(nodeResult.NextNodes))

	if len(nodeResult.NextNodes) == 0 {
		return e.checkWorkflowCompletion(ctx, workflowID)
	}

	for _, nextNode := range nodeResult.NextNodes {
		if err := e.enqueueNextNode(ctx, workflowID, nextNode); err != nil {
			e.logger.Error("failed to enqueue next node",
				"workflow_id", workflowID,
				"current_node", nodeName,
				"next_node", nextNode.NodeName,
				"error", err)
		}
	}

	return nil
}

func (e *Executor) enqueueNextNode(ctx context.Context, workflowID string, nextNode domain.NextNode) error {
	e.logger.Debug("enqueuing next node",
		"workflow_id", workflowID,
		"node_name", nextNode.NodeName)

	now := time.Now()
	workItem := WorkItem{
		WorkflowID:   workflowID,
		NodeName:     nextNode.NodeName,
		Config:       nextNode.Config,
		EnqueuedAt:   now,
		ProcessAfter: now,
	}

	itemBytes, err := json.Marshal(workItem)
	if err != nil {
		return domain.NewDiscoveryError("executor", "marshal_work_item", err)
	}

	if err := e.queue.Enqueue(itemBytes); err != nil {
		return domain.NewDiscoveryError("executor", "enqueue_work_item", err)
	}

	e.metrics.IncrementItemsEnqueued()

	e.logger.Debug("next node enqueued successfully",
		"workflow_id", workflowID,
		"node_name", nextNode.NodeName)

	return nil
}

func (e *Executor) requeueNode(ctx context.Context, workflowID, nodeName string, config json.RawMessage) error {
	e.logger.Debug("requeuing node",
		"workflow_id", workflowID,
		"node_name", nodeName)

	now := time.Now()
	workItem := WorkItem{
		WorkflowID:   workflowID,
		NodeName:     nodeName,
		Config:       config,
		EnqueuedAt:   now,
		ProcessAfter: now,
	}

	itemBytes, err := json.Marshal(workItem)
	if err != nil {
		return domain.NewDiscoveryError("executor", "marshal_requeue_item", err)
	}

	if err := e.queue.Enqueue(itemBytes); err != nil {
		return domain.NewDiscoveryError("executor", "requeue_work_item", err)
	}

	return nil
}

func (e *Executor) handleExecutionFailure(ctx context.Context, workflow *domain.WorkflowInstance, executedNode *domain.ExecutedNodeData, execErr error) error {
	e.logger.Error("handling execution failure",
		"workflow_id", workflow.ID,
		"node_name", executedNode.NodeName,
		"error", execErr)

	err := e.stateManager.UpdateWorkflowState(ctx, workflow.ID, func(wf *domain.WorkflowInstance) error {
		wf.Status = domain.WorkflowStateFailed
		now := time.Now()
		wf.CompletedAt = &now
		errorStr := execErr.Error()
		wf.LastError = &errorStr

		return e.persistExecutedNode(ctx, workflow.ID, executedNode)
	})

	if err != nil {
		return domain.NewDiscoveryError("executor", "update_workflow_failed_state", err)
	}

	e.metrics.IncrementWorkflowsFailed()

	return nil
}

func (e *Executor) checkWorkflowCompletion(ctx context.Context, workflowID string) error {
	e.logger.Debug("checking workflow completion", "workflow_id", workflowID)

	workflowPrefix := fmt.Sprintf(`"workflow_id":"%s"`, workflowID)
	hasPendingNodes, err := e.queue.HasItemsWithPrefix(workflowPrefix)
	if err != nil {
		e.logger.Error("failed to check pending workflow items",
			"workflow_id", workflowID,
			"error", err)
		return nil
	}

	if !hasPendingNodes {
		e.logger.Info("workflow completed",
			"workflow_id", workflowID)

		err := e.stateManager.UpdateWorkflowState(ctx, workflowID, func(wf *domain.WorkflowInstance) error {
			wf.Status = domain.WorkflowStateCompleted
			now := time.Now()
			wf.CompletedAt = &now
			return nil
		})

		if err == nil {
			e.metrics.IncrementWorkflowsCompleted()

			if publishErr := e.publishWorkflowCompletedEvent(ctx, workflowID); publishErr != nil {
				e.logger.Error("failed to publish workflow completed event",
					"workflow_id", workflowID,
					"error", publishErr)
			}
		}

		return err
	}

	e.logger.Debug("workflow has pending nodes, not marking as complete",
		"workflow_id", workflowID)
	return nil
}

func (e *Executor) persistExecutedNode(ctx context.Context, workflowID string, executedNode *domain.ExecutedNodeData) error {
	executionKey := fmt.Sprintf("workflow:execution:%s:%s_%d",
		workflowID,
		executedNode.NodeName,
		executedNode.ExecutedAt.UnixNano())

	nodeBytes, err := json.Marshal(executedNode)
	if err != nil {
		return domain.NewDiscoveryError("executor", "marshal_executed_node", err)
	}

	if err := e.storage.Put(executionKey, nodeBytes, 1); err != nil {
		return domain.NewDiscoveryError("executor", "persist_executed_node", err)
	}

	e.logger.Debug("executed node persisted",
		"workflow_id", workflowID,
		"node_name", executedNode.NodeName)

	return nil
}

func (e *Executor) requeueNodeWithBackoff(ctx context.Context, workflowID, nodeName string, config json.RawMessage, retryCount int) error {
	if retryCount >= e.config.RetryAttempts {
		e.logger.Info("max retry attempts exceeded, sending to dead letter queue",
			"workflow_id", workflowID,
			"node_name", nodeName,
			"retry_count", retryCount,
			"max_retries", e.config.RetryAttempts)

		if err := e.sendToDeadLetterQueue(ctx, workflowID, nodeName, config, retryCount, "max_retries_exceeded"); err != nil {
			e.logger.Error("failed to send to dead letter queue, failing workflow",
				"workflow_id", workflowID,
				"node_name", nodeName,
				"error", err)

			workflow, loadErr := e.stateManager.LoadWorkflowState(ctx, workflowID)
			if loadErr != nil {
				return domain.NewDiscoveryError("executor", "load_workflow_for_dlq_failure", loadErr)
			}

			executedNode := domain.ExecutedNodeData{
				NodeName:   nodeName,
				ExecutedAt: time.Now(),
				Status:     string(domain.NodeExecutionStatusFailed),
				Config:     config,
			}
			errorStr := fmt.Sprintf("failed to send to DLQ after %d retries: %v", retryCount, err)
			executedNode.Error = &errorStr

			return e.handleExecutionFailure(ctx, workflow, &executedNode, err)
		}

		e.logger.Debug("work item successfully sent to dead letter queue",
			"workflow_id", workflowID,
			"node_name", nodeName,
			"retry_count", retryCount)

		return nil
	}

	baseDelay := 1 * time.Second
	maxDelay := 5 * time.Minute
	delay := time.Duration(int64(baseDelay) * (1 << uint(retryCount)))
	if delay > maxDelay {
		delay = maxDelay
	}

	e.logger.Debug("applying backoff delay",
		"workflow_id", workflowID,
		"node_name", nodeName,
		"delay", delay,
		"retry_count", retryCount)

	now := time.Now()
	workItem := WorkItem{
		WorkflowID:   workflowID,
		NodeName:     nodeName,
		Config:       config,
		EnqueuedAt:   now,
		ProcessAfter: now.Add(delay),
		RetryCount:   retryCount + 1,
	}

	itemBytes, err := json.Marshal(workItem)
	if err != nil {
		return domain.NewDiscoveryError("executor", "marshal_backoff_item", err)
	}

	if err := e.queue.Enqueue(itemBytes); err != nil {
		return domain.NewDiscoveryError("executor", "requeue_backoff_item", err)
	}

	e.metrics.IncrementNodesRetried()
	e.metrics.IncrementItemsEnqueued()

	e.logger.Debug("node requeued with backoff",
		"workflow_id", workflowID,
		"node_name", nodeName,
		"new_retry_count", retryCount+1,
		"delay", delay)

	return nil
}

func (e *Executor) sendToDeadLetterQueue(ctx context.Context, workflowID, nodeName string, config json.RawMessage, retryCount int, reason string) error {
	e.logger.Warn("sending work item to dead letter queue",
		"workflow_id", workflowID,
		"node_name", nodeName,
		"retry_count", retryCount,
		"reason", reason)

	now := time.Now()
	workItem := WorkItem{
		WorkflowID:   workflowID,
		NodeName:     nodeName,
		Config:       config,
		EnqueuedAt:   now,
		ProcessAfter: now,
		RetryCount:   retryCount,
	}

	itemBytes, err := json.Marshal(workItem)
	if err != nil {
		return domain.NewDiscoveryError("executor", "marshal_dead_letter_item", err)
	}

	if err := e.queue.SendToDeadLetter(itemBytes, reason); err != nil {
		return domain.NewDiscoveryError("executor", "send_to_dead_letter", err)
	}

	e.metrics.IncrementItemsSentToDeadLetter()

	e.logger.Debug("work item sent to dead letter queue",
		"workflow_id", workflowID,
		"node_name", nodeName,
		"reason", reason)

	return nil
}

func (e *Executor) publishWorkflowCompletedEvent(ctx context.Context, workflowID string) error {
	workflow, err := e.stateManager.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return domain.NewDiscoveryError("executor", "get_workflow_for_event", err)
	}

	executedNodes, err := e.loadExecutedNodeNames(ctx, workflowID)
	if err != nil {
		e.logger.Warn("failed to load executed nodes for event", "workflow_id", workflowID, "error", err)
		executedNodes = []string{}
	}

	duration := time.Duration(0)
	if workflow.CompletedAt != nil {
		duration = workflow.CompletedAt.Sub(workflow.StartedAt)
	}

	event := domain.WorkflowCompletedEvent{
		WorkflowID:    workflowID,
		FinalState:    workflow.CurrentState,
		CompletedAt:   *workflow.CompletedAt,
		ExecutedNodes: executedNodes,
		Duration:      duration,
		Metadata:      convertMetadata(workflow.Metadata),
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return domain.NewDiscoveryError("executor", "marshal_workflow_completed_event", err)
	}

	eventKey := fmt.Sprintf("workflow:%s:completed", workflowID)
	if err := e.storage.Put(eventKey, eventBytes, 1); err != nil {
		return domain.NewDiscoveryError("executor", "publish_workflow_completed_event", err)
	}

	e.logger.Debug("workflow completed event published",
		"workflow_id", workflowID,
		"event_key", eventKey)

	return nil
}

func (e *Executor) loadExecutedNodeNames(ctx context.Context, workflowID string) ([]string, error) {
	prefix := fmt.Sprintf("workflow:execution:%s:", workflowID)
	kvList, err := e.storage.ListByPrefix(prefix)
	if err != nil {
		return nil, err
	}

	nodeNames := make(map[string]bool)
	for _, kv := range kvList {
		if len(kv.Key) > len(prefix) {
			remaining := kv.Key[len(prefix):]
			underscorePos := -1
			for i, char := range remaining {
				if char == '_' {
					underscorePos = i
					break
				}
			}
			if underscorePos > 0 {
				nodeName := remaining[:underscorePos]
				nodeNames[nodeName] = true
			}
		}
	}

	result := make([]string, 0, len(nodeNames))
	for name := range nodeNames {
		result = append(result, name)
	}
	return result, nil
}

func convertMetadata(metadata map[string]string) map[string]interface{} {
	if metadata == nil {
		return nil
	}
	result := make(map[string]interface{})
	for k, v := range metadata {
		result[k] = v
	}
	return result
}
