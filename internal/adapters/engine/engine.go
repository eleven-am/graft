package engine

import (
	"context"
	"fmt"
	json "github.com/eleven-am/graft/internal/xjson"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Engine struct {
	config       domain.EngineConfig
	nodeID       string
	nodeRegistry ports.NodeRegistryPort
	stateManager StateManagerInterface
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

type ExponentialBackoff struct {
	baseDelay time.Duration
	maxDelay  time.Duration
	factor    float64
	current   time.Duration
}

func NewExponentialBackoff(baseDelay, maxDelay time.Duration, factor float64) *ExponentialBackoff {
	return &ExponentialBackoff{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		factor:    factor,
	}
}

func (b *ExponentialBackoff) NextDelay() time.Duration {
	if b.current == 0 {
		b.current = b.baseDelay
	} else {
		b.current = time.Duration(float64(b.current) * b.factor)
		if b.current > b.maxDelay {
			b.current = b.maxDelay
		}
	}

	jitter := time.Duration(rand.Float64() * 0.5 * float64(b.current))
	if rand.Float64() < 0.5 {
		return b.current - jitter
	}
	return b.current + jitter
}

func (b *ExponentialBackoff) Reset() {
	b.current = 0
}

func NewEngine(config domain.EngineConfig, nodeID string, nodeRegistry ports.NodeRegistryPort, queue ports.QueuePort, storage ports.StoragePort, eventManager ports.EventManager, loadBalancer ports.LoadBalancer, logger *slog.Logger) *Engine {
	var stateManager StateManagerInterface

	if config.StateOptimization.Strategy != "" {
		stateManager = NewOptimizedStateManager(storage, config.StateOptimization, logger)
	} else {
		stateManager = NewStateManager(storage, logger)
	}

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
	if starter, ok := e.queue.(interface{ Start(context.Context) error }); ok {
		_ = starter.Start(e.ctx)
	}
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

	if osm, ok := e.stateManager.(*OptimizedStateManager); ok {
		if err := osm.Stop(); err != nil {
			e.logger.Error("failed to stop optimized state manager", "error", err)
		}
	}

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
		Version:      0,
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

		if emitErr := e.emitWorkflowLifecycleEvent(workflowID, "paused", nil); emitErr != nil {
			e.logger.Error("failed to emit workflow paused event", "workflow_id", workflowID, "error", emitErr)
		}
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

		if emitErr := e.emitWorkflowLifecycleEvent(workflowID, "resumed", nil); emitErr != nil {
			e.logger.Error("failed to emit workflow resumed event", "workflow_id", workflowID, "error", emitErr)
		}
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

		if emitErr := e.emitWorkflowLifecycleEvent(workflowID, "failed", fmt.Errorf("workflow stopped by request")); emitErr != nil {
			e.logger.Error("failed to emit workflow failed event", "workflow_id", workflowID, "error", emitErr)
		}
	}

	return err
}

func (e *Engine) processWork() {
	defer e.wg.Done()
	backoff := NewExponentialBackoff(50*time.Millisecond, 5*time.Second, 2.0)

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-e.queue.WaitForItem(e.ctx):
			for {
				processed, err := e.processNextItem()
				if err != nil {
					delay := backoff.NextDelay()
					e.logger.Debug("processWork error, backing off", "error", err, "delay", delay)

					select {
					case <-time.After(delay):
					case <-e.ctx.Done():
						return
					}
				} else {
					backoff.Reset()
				}
				if !processed {
					break
				}
			}
		case <-time.After(1 * time.Second):

			for {
				processed, err := e.processNextItem()
				if err != nil {
					delay := backoff.NextDelay()
					e.logger.Debug("processWork periodic error, backing off", "error", err, "delay", delay)

					select {
					case <-time.After(delay):
					case <-e.ctx.Done():
						return
					}
				} else {
					backoff.Reset()
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

	item, claimID, exists, err := e.queue.Claim()
	if err != nil {
		e.logger.Error("Failed to claim item", "error", err)
		return false, domain.NewDiscoveryError("engine", "claim_work_item", err)
	}

	if !exists {
		return false, nil
	}

	var workItem WorkItem
	if err := json.Unmarshal(item, &workItem); err != nil {
		e.logger.Error("Failed to unmarshal claimed work item", "error", err)
		if compErr := e.queue.Complete(claimID); compErr != nil {
			e.logger.Error("failed to complete malformed work item", "claim_id", claimID, "error", compErr)
		}
		return true, domain.NewDiscoveryError("engine", "unmarshal_claimed_work_item", err)
	}

	shouldExecute, err := e.loadBalancer.ShouldExecuteNode(e.nodeID, workItem.WorkflowID, workItem.NodeName)
	if err != nil {
		e.logger.Error("Failed to check load balancer decision", "error", err)
		time.Sleep(50*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond)
		_ = e.queue.Release(claimID)
		return true, domain.NewDiscoveryError("engine", "load_balancer_decision", err)
	}

	if !shouldExecute {
		time.Sleep(50*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond)
		if relErr := e.queue.Release(claimID); relErr != nil {
			e.logger.Error("failed to release unassigned claim", "claim_id", claimID, "error", relErr)
		}
		return true, nil
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
	if err := e.storage.Put(eventKey, eventBytes, 0); err != nil {
		return fmt.Errorf("failed to store workflow started event: %w", err)
	}

	return nil
}

func (e *Engine) emitWorkflowLifecycleEvent(workflowID, eventType string, reason error) error {
	eventData := map[string]interface{}{
		"workflow_id": workflowID,
		"timestamp":   time.Now().Unix(),
		"node_id":     e.nodeID,
	}

	if reason != nil {
		eventData["error"] = reason.Error()
	}

	eventBytes, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal workflow %s event: %w", eventType, err)
	}

	eventKey := fmt.Sprintf("workflow:%s:%s", workflowID, eventType)
	if err := e.storage.Put(eventKey, eventBytes, 0); err != nil {
		return fmt.Errorf("failed to store workflow %s event: %w", eventType, err)
	}

	return nil
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
