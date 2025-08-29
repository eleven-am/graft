package engine

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Engine struct {
	config       domain.EngineConfig
	nodeID       string
	nodeRegistry ports.NodeRegistryPort
	stateManager *StateManager
	executor     *Executor
	queue        ports.QueuePort
	storage      ports.StoragePort
	eventManager ports.EventManager
	loadBalancer ports.LoadBalancer
	logger       *slog.Logger
	metrics      *domain.ExecutionMetrics

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewEngine(config domain.EngineConfig, nodeID string, nodeRegistry ports.NodeRegistryPort, queue ports.QueuePort, storage ports.StoragePort, eventManager ports.EventManager, loadBalancer ports.LoadBalancer, logger *slog.Logger) *Engine {
	stateManager := NewStateManager(storage, logger)
	metrics := domain.NewExecutionMetrics()
	executor := NewExecutor(config, nodeID, nodeRegistry, stateManager, queue, storage, eventManager, loadBalancer, logger, metrics)

	return &Engine{
		config:       config,
		nodeID:       nodeID,
		nodeRegistry: nodeRegistry,
		stateManager: stateManager,
		executor:     executor,
		queue:        queue,
		storage:      storage,
		eventManager: eventManager,
		loadBalancer: loadBalancer,
		logger:       logger.With("component", "engine"),
		metrics:      metrics,
	}
}

func (e *Engine) Start(ctx context.Context) error {
	e.logger.Info("starting workflow engine", "worker_count", e.config.WorkerCount)

	e.ctx, e.cancel = context.WithCancel(ctx)

	for i := 0; i < e.config.WorkerCount; i++ {
		e.wg.Add(1)
		go e.processWork()
	}

	return nil
}

func (e *Engine) Stop() error {

	if e.cancel != nil {
		e.cancel()
	}

	e.wg.Wait()

	if err := e.queue.Close(); err != nil {
		e.logger.Error("failed to close queue", "error", err)
		return err
	}

	return nil
}

func (e *Engine) ProcessTrigger(trigger domain.WorkflowTrigger) error {
	workflow := &domain.WorkflowInstance{
		ID:           trigger.WorkflowID,
		Status:       domain.WorkflowStateRunning,
		CurrentState: trigger.InitialState,
		StartedAt:    time.Now(),
		Metadata:     trigger.Metadata,
		Version:      1,
	}

	if err := e.stateManager.SaveWorkflowState(e.ctx, workflow); err != nil {
		e.logger.Error("failed to save initial workflow state",
			"workflow_id", trigger.WorkflowID,
			"error", err)
		return domain.NewDiscoveryError("engine", "save_initial_workflow_state", err)
	}

	e.metrics.IncrementWorkflowsStarted()

	var initialNodeNames []string
	for _, node := range trigger.InitialNodes {
		initialNodeNames = append(initialNodeNames, node.Name)
	}

	startedEvent := domain.WorkflowStartedEvent{
		WorkflowID:   trigger.WorkflowID,
		Trigger:      trigger,
		StartedAt:    workflow.StartedAt,
		InitialNodes: initialNodeNames,
		NodeID:       e.nodeID,
		Metadata:     convertMetadata(trigger.Metadata),
	}

	if err := e.emitWorkflowStartedEvent(startedEvent); err != nil {
		e.logger.Error("failed to emit workflow started event",
			"workflow_id", trigger.WorkflowID,
			"error", err)
	}

	for _, nodeConfig := range trigger.InitialNodes {
		if err := e.enqueueInitialNode(trigger.WorkflowID, nodeConfig); err != nil {
			e.logger.Error("failed to enqueue initial node",
				"workflow_id", trigger.WorkflowID,
				"node_name", nodeConfig.Name,
				"error", err)
			return domain.NewDiscoveryError("engine", "enqueue_initial_node", err)
		}
	}

	return nil
}

func (e *Engine) GetWorkflowStatus(workflowID string) (*domain.WorkflowStatus, error) {

	workflow, err := e.stateManager.LoadWorkflowState(e.ctx, workflowID)
	if err != nil {
		return nil, domain.NewDiscoveryError("engine", "load_workflow", err)
	}

	executedNodes, err := e.loadExecutedNodes(workflowID)
	if err != nil {
		e.logger.Warn("failed to load executed nodes",
			"workflow_id", workflowID,
			"error", err)
		executedNodes = []domain.ExecutedNodeData{}
	}

	executingNodes, err := e.loadExecutingNodes(workflowID)
	if err != nil {
		e.logger.Warn("failed to load executing nodes",
			"workflow_id", workflowID,
			"error", err)
		executingNodes = []domain.ExecutingNodeData{}
	}

	pendingNodes, err := e.loadPendingNodes(workflowID)
	if err != nil {
		e.logger.Warn("failed to load pending nodes",
			"workflow_id", workflowID,
			"error", err)
		pendingNodes = []domain.NodeConfig{}
	}

	status := &domain.WorkflowStatus{
		WorkflowID:     workflow.ID,
		Status:         workflow.Status,
		CurrentState:   workflow.CurrentState,
		StartedAt:      workflow.StartedAt,
		CompletedAt:    workflow.CompletedAt,
		ExecutedNodes:  executedNodes,
		ExecutingNodes: executingNodes,
		PendingNodes:   pendingNodes,
		LastError:      workflow.LastError,
	}

	return status, nil
}

func (e *Engine) PauseWorkflow(ctx context.Context, workflowID string) error {

	err := e.stateManager.UpdateWorkflowState(ctx, workflowID, func(wf *domain.WorkflowInstance) error {
		if wf.Status != domain.WorkflowStateRunning {
			return domain.ErrInvalidInput
		}
		wf.Status = domain.WorkflowStatePaused
		return nil
	})

	if err == nil {
		e.metrics.IncrementWorkflowsPaused()
	}

	return err
}

func (e *Engine) ResumeWorkflow(ctx context.Context, workflowID string) error {

	err := e.stateManager.UpdateWorkflowState(ctx, workflowID, func(wf *domain.WorkflowInstance) error {
		if wf.Status != domain.WorkflowStatePaused {
			return domain.ErrInvalidInput
		}
		wf.Status = domain.WorkflowStateRunning
		return nil
	})

	if err == nil {
		e.metrics.IncrementWorkflowsResumed()
	}

	return err
}

func (e *Engine) StopWorkflow(ctx context.Context, workflowID string) error {

	err := e.stateManager.UpdateWorkflowState(ctx, workflowID, func(wf *domain.WorkflowInstance) error {
		wf.Status = domain.WorkflowStateFailed
		now := time.Now()
		wf.CompletedAt = &now
		errorStr := "workflow stopped by request"
		wf.LastError = &errorStr
		return nil
	})

	if err == nil {
		e.metrics.IncrementWorkflowsFailed()
	}

	return err
}

func (e *Engine) processWork() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-e.queue.WaitForItem(e.ctx):
			for {
				processed, err := e.processNextItem()
				if err != nil {
					e.logger.Error("failed to process work item", "error", err)
					time.Sleep(100 * time.Millisecond)
				}
				if !processed {
					break
				}
			}
		case <-time.After(1 * time.Second):
			// Periodic check for delayed items that are now ready
			for {
				processed, err := e.processNextItem()
				if err != nil {
					e.logger.Error("failed to process work item", "error", err)
					time.Sleep(100 * time.Millisecond)
				}
				if !processed {
					break
				}
			}
		}
	}
}

func (e *Engine) processNextItem() (bool, error) {
	select {
	case <-e.ctx.Done():
		return false, nil
	default:
	}

	item, exists, err := e.queue.Peek()
	if err != nil {
		e.logger.Error("Failed to peek item", "error", err)
		return false, domain.NewDiscoveryError("engine", "peek_work_item", err)
	}

	if !exists {
		return false, nil
	}

	var workItem WorkItem
	if err := json.Unmarshal(item, &workItem); err != nil {
		e.logger.Error("Failed to unmarshal peeked work item", "error", err)
		return false, domain.NewDiscoveryError("engine", "unmarshal_peeked_work_item", err)
	}

	shouldExecute, err := e.loadBalancer.ShouldExecuteNode(e.nodeID, workItem.WorkflowID, workItem.NodeName)
	if err != nil {
		e.logger.Error("Failed to check load balancer decision", "error", err)
		return false, domain.NewDiscoveryError("engine", "load_balancer_decision", err)
	}

	if !shouldExecute {
		return false, nil
	}

	_, claimID, exists, err := e.queue.Claim()
	if err != nil {
		e.logger.Error("Failed to claim item", "error", err)
		return false, domain.NewDiscoveryError("engine", "claim_work_item", err)
	}

	if !exists {
		return false, nil
	}

	e.metrics.IncrementItemsProcessed()

	execErr := e.executor.ExecuteNodeWithRetry(e.ctx, workItem.WorkflowID, workItem.NodeName, workItem.Config, workItem.RetryCount)

	if err := e.queue.Complete(claimID); err != nil {
		e.logger.Error("failed to complete work item",
			"claim_id", claimID,
			"error", err)
	}

	return true, execErr
}

func (e *Engine) enqueueInitialNode(workflowID string, nodeConfig domain.NodeConfig) error {
	now := time.Now()
	workItem := WorkItem{
		WorkflowID:   workflowID,
		NodeName:     nodeConfig.Name,
		Config:       nodeConfig.Config,
		EnqueuedAt:   now,
		ProcessAfter: now,
		RetryCount:   0,
	}

	itemBytes, err := json.Marshal(workItem)
	if err != nil {
		return domain.NewDiscoveryError("engine", "marshal_work_item", err)
	}

	if err := e.queue.Enqueue(itemBytes); err != nil {
		return domain.NewDiscoveryError("engine", "enqueue_work_item", err)
	}

	e.metrics.IncrementItemsEnqueued()

	return nil
}

func (e *Engine) loadExecutedNodes(workflowID string) ([]domain.ExecutedNodeData, error) {
	prefix := fmt.Sprintf("workflow:execution:%s:", workflowID)

	items, err := e.storage.ListByPrefix(prefix)
	if err != nil {
		return nil, domain.NewDiscoveryError("engine", "list_executed_nodes", err)
	}

	var executedNodes []domain.ExecutedNodeData
	for _, item := range items {
		var node domain.ExecutedNodeData
		if err := json.Unmarshal(item.Value, &node); err != nil {
			e.logger.Warn("failed to unmarshal executed node",
				"key", item.Key,
				"error", err)
			continue
		}
		executedNodes = append(executedNodes, node)
	}

	return executedNodes, nil
}

func (e *Engine) loadPendingNodes(workflowID string) ([]domain.NodeConfig, error) {
	workflowPrefix := fmt.Sprintf(`"workflow_id":"%s"`, workflowID)

	pendingItems, err := e.queue.GetItemsWithPrefix(workflowPrefix)
	if err != nil {
		return nil, domain.NewDiscoveryError("engine", "get_pending_workflow_items", err)
	}

	var pendingNodes []domain.NodeConfig
	for _, itemBytes := range pendingItems {
		var workItem struct {
			WorkflowID string          `json:"workflow_id"`
			NodeName   string          `json:"node_name"`
			Config     json.RawMessage `json:"config"`
		}
		if err := json.Unmarshal(itemBytes, &workItem); err != nil {
			e.logger.Warn("failed to unmarshal work item",
				"error", err)
			continue
		}

		pendingNodes = append(pendingNodes, domain.NodeConfig{
			Name:   workItem.NodeName,
			Config: workItem.Config,
		})
	}

	return pendingNodes, nil
}

func (e *Engine) loadExecutingNodes(workflowID string) ([]domain.ExecutingNodeData, error) {
	workflowPrefix := fmt.Sprintf(`"workflow_id":"%s"`, workflowID)

	claimedItems, err := e.queue.GetClaimedItemsWithPrefix(workflowPrefix)
	if err != nil {
		return nil, domain.NewDiscoveryError("engine", "get_claimed_workflow_items", err)
	}

	var executingNodes []domain.ExecutingNodeData
	for _, claimedItem := range claimedItems {
		var workItem struct {
			WorkflowID string          `json:"workflow_id"`
			NodeName   string          `json:"node_name"`
			Config     json.RawMessage `json:"config"`
		}
		if err := json.Unmarshal(claimedItem.Data, &workItem); err != nil {
			e.logger.Warn("failed to unmarshal claimed work item",
				"error", err)
			continue
		}

		executingNodes = append(executingNodes, domain.ExecutingNodeData{
			NodeName:  workItem.NodeName,
			StartedAt: claimedItem.ClaimedAt,
			ClaimID:   claimedItem.ClaimID,
			Config:    workItem.Config,
		})
	}

	return executingNodes, nil
}

func (e *Engine) GetDeadLetterItems(limit int) ([]ports.DeadLetterItem, error) {

	items, err := e.queue.GetDeadLetterItems(limit)
	if err != nil {
		return nil, domain.NewDiscoveryError("engine", "get_dead_letter_items", err)
	}

	return items, nil
}

func (e *Engine) GetDeadLetterSize() (int, error) {

	size, err := e.queue.GetDeadLetterSize()
	if err != nil {
		return 0, domain.NewDiscoveryError("engine", "get_dead_letter_size", err)
	}

	return size, nil
}

func (e *Engine) RetryFromDeadLetter(itemID string) error {

	if err := e.queue.RetryFromDeadLetter(itemID); err != nil {
		return domain.NewDiscoveryError("engine", "retry_from_dead_letter", err)
	}

	e.metrics.IncrementItemsRetriedFromDeadLetter()

	return nil
}

func (e *Engine) GetMetrics() domain.ExecutionMetrics {
	return e.metrics.GetSnapshot()
}

func (e *Engine) emitWorkflowStartedEvent(event domain.WorkflowStartedEvent) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal workflow started event: %w", err)
	}

	eventKey := fmt.Sprintf("workflow:%s:started", event.WorkflowID)
	if err := e.storage.Put(eventKey, eventBytes, 1); err != nil {
		return fmt.Errorf("failed to store workflow started event: %w", err)
	}

	domainEvent := domain.Event{
		Type:      domain.EventPut,
		Key:       eventKey,
		Timestamp: time.Now(),
	}

	return e.eventManager.Broadcast(domainEvent)
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
