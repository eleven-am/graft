package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/google/uuid"
)

type Engine struct {
	nodeRegistry     ports.NodeRegistryPort
	resourceManager  ports.ResourceManagerPort
	storage          ports.StoragePort
	queue            ports.QueuePort
	transport        ports.TransportPort
	discovery        ports.DiscoveryPort

	activeWorkflows   map[string]*WorkflowInstance
	coordinator       *WorkflowCoordinator
	stateManager      *StateManager
	lifecycleManager  *LifecycleManager
	metricsTracker    *MetricsTracker
	pendingEvaluator  PendingEvaluator
	evaluationTrigger EvaluationTrigger
	cleanupScheduler  *CleanupScheduler
	mu                sync.RWMutex
	logger            *slog.Logger
	config            Config

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type Config struct {
	MaxConcurrentWorkflows int           `json:"max_concurrent_workflows"`
	NodeExecutionTimeout   time.Duration `json:"node_execution_timeout"`
	StateUpdateInterval    time.Duration `json:"state_update_interval"`
	RetryAttempts          int           `json:"retry_attempts"`
	RetryBackoff           time.Duration `json:"retry_backoff"`
}

type WorkflowInstance struct {
	ID           string              `json:"id"`
	Status       ports.WorkflowState `json:"status"`
	CurrentState interface{}         `json:"current_state"`
	StartedAt    time.Time           `json:"started_at"`
	CompletedAt  *time.Time          `json:"completed_at,omitempty"`
	Metadata     map[string]string   `json:"metadata"`
	LastError    *string             `json:"last_error,omitempty"`
	mu           sync.RWMutex        `json:"-"`
}

func NewEngine(config Config, logger *slog.Logger) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	
	engine := &Engine{
		activeWorkflows: make(map[string]*WorkflowInstance),
		logger:          logger,
		config:          config,
		ctx:             ctx,
		cancel:          cancel,
	}

	engine.coordinator = NewWorkflowCoordinator(engine)
	engine.stateManager = NewStateManager(engine)
	engine.metricsTracker = NewMetricsTracker()
	engine.lifecycleManager = NewLifecycleManager(logger, HandlerConfig{
		Timeout:    30 * time.Second,
		MaxRetries: 3,
	}, engine.metricsTracker)
	
	engine.pendingEvaluator = NewPendingEvaluator(engine, logger)
	engine.evaluationTrigger = NewEvaluationTrigger(3, logger)
	engine.evaluationTrigger.RegisterEvaluator(engine.pendingEvaluator)
	
	return engine
}

func (e *Engine) SetNodeRegistry(registry ports.NodeRegistryPort) {
	e.nodeRegistry = registry
}

func (e *Engine) SetResourceManager(manager ports.ResourceManagerPort) {
	e.resourceManager = manager
}

func (e *Engine) SetStorage(storage ports.StoragePort) {
	e.storage = storage
}

func (e *Engine) SetQueue(queue ports.QueuePort) {
	e.queue = queue
}

func (e *Engine) SetTransport(transport ports.TransportPort) {
	e.transport = transport
}

func (e *Engine) SetDiscovery(discovery ports.DiscoveryPort) {
	e.discovery = discovery
}

func (e *Engine) RegisterLifecycleHandlers(completion []ports.CompletionHandler, error []ports.ErrorHandler) {
	if e.lifecycleManager != nil {
		e.lifecycleManager.RegisterHandlers(completion, error)
	}
}

func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.nodeRegistry == nil || e.resourceManager == nil {
		e.mu.Unlock()
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "missing required dependencies",
			Details: map[string]interface{}{
				"node_registry":    e.nodeRegistry != nil,
				"resource_manager": e.resourceManager != nil,
				"storage":          e.storage != nil,
				"queue":            e.queue != nil,
			},
		}
	}
	
	if e.storage == nil {
		return domain.Error{
		Type:    domain.ErrorTypeInternal,
		Message: "FATAL: workflow engine requires storage for state persistence",
		Details: map[string]interface{}{
			"component": "workflow_engine",
			"requirement": "storage",
		},
	}
	}
	if e.queue == nil {
		return domain.Error{
		Type:    domain.ErrorTypeInternal,
		Message: "FATAL: workflow engine requires queue for distributed execution",
		Details: map[string]interface{}{
			"component": "workflow_engine",
			"requirement": "queue",
		},
	}
	}
	e.mu.Unlock()

	e.logger.Info("starting workflow engine",
		"max_concurrent_workflows", e.config.MaxConcurrentWorkflows,
		"node_execution_timeout", e.config.NodeExecutionTimeout,
	)

	if err := e.stateManager.RecoverActiveWorkflows(ctx); err != nil {
		e.logger.Error("failed to recover active workflows", "error", err.Error())
		return err
	}

	if err := e.evaluationTrigger.Start(ctx); err != nil {
		e.logger.Error("failed to start evaluation trigger", "error", err.Error())
		return err
	}

	e.wg.Add(2)
	go e.processReadyNodes()
	go e.processWorkflowCompletions()

	return nil
}

func (e *Engine) Stop() error {
	e.logger.Info("stopping workflow engine")

	if e.evaluationTrigger != nil {
		if err := e.evaluationTrigger.Stop(); err != nil {
			e.logger.Error("failed to stop evaluation trigger", "error", err.Error())
		}
	}

	e.cancel()
	e.wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()

	for id, workflow := range e.activeWorkflows {
		workflow.mu.Lock()
		if workflow.Status == ports.WorkflowStateRunning {
			workflow.Status = ports.WorkflowStatePaused
			e.logger.Info("paused workflow during shutdown", "workflow_id", id)
		}
		workflow.mu.Unlock()
	}

	e.logger.Info("workflow engine stopped")
	return nil
}

func (e *Engine) ProcessTrigger(trigger ports.WorkflowTrigger) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.activeWorkflows) >= e.config.MaxConcurrentWorkflows {
		return domain.Error{
			Type:    domain.ErrorTypeRateLimit,
			Message: "maximum concurrent workflows reached",
			Details: map[string]interface{}{
				"current_count": len(e.activeWorkflows),
				"max_allowed":   e.config.MaxConcurrentWorkflows,
			},
		}
	}

	workflow := &WorkflowInstance{
		ID:           trigger.WorkflowID,
		Status:       ports.WorkflowStateRunning,
		CurrentState: trigger.InitialState,
		StartedAt:    time.Now(),
		Metadata:     trigger.Metadata,
	}

	e.activeWorkflows[trigger.WorkflowID] = workflow

	e.logger.Info("workflow triggered",
		"workflow_id", trigger.WorkflowID,
		"initial_nodes", len(trigger.InitialNodes),
	)

	for _, nodeConfig := range trigger.InitialNodes {
		if err := e.queueNodeForExecution(trigger.WorkflowID, nodeConfig, "initial"); err != nil {
			e.logger.Error("failed to queue initial node",
				"workflow_id", trigger.WorkflowID,
				"node_name", nodeConfig.Name,
				"error", err.Error(),
			)
		}
	}

	return nil
}

func (e *Engine) GetWorkflowStatus(workflowID string) (*ports.WorkflowStatus, error) {
	e.mu.RLock()
	workflow, exists := e.activeWorkflows[workflowID]
	e.mu.RUnlock()

	if !exists {
		return nil, domain.NewNotFoundError("workflow", workflowID)
	}

	workflow.mu.RLock()
	defer workflow.mu.RUnlock()

	status := &ports.WorkflowStatus{
		WorkflowID:    workflow.ID,
		Status:        workflow.Status,
		CurrentState:  workflow.CurrentState,
		StartedAt:     workflow.StartedAt,
		CompletedAt:   workflow.CompletedAt,
		ExecutedNodes: []ports.ExecutedNode{},
		PendingNodes:  []ports.PendingNode{},
		ReadyNodes:    []ports.ReadyNode{},
		LastError:     workflow.LastError,
	}

	return status, nil
}

func (e *Engine) GetExecutionMetrics() ports.EngineMetrics {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var activeCount, completedCount, failedCount int64
	for _, workflow := range e.activeWorkflows {
		workflow.mu.RLock()
		switch workflow.Status {
		case ports.WorkflowStateRunning:
			activeCount++
		case ports.WorkflowStateCompleted:
			completedCount++
		case ports.WorkflowStateFailed:
			failedCount++
		}
		workflow.mu.RUnlock()
	}

	queueSizes := ports.QueueSizes{}
	if e.queue != nil {
		ctx := context.Background()
		
		isEmpty, err := e.queue.IsEmpty(ctx)
		if err == nil && !isEmpty {
			queueSizes.Ready = 1
		}
		
		pendingItems, err := e.queue.GetPendingItems(ctx)
		if err == nil {
			queueSizes.Pending = len(pendingItems)
		}
	}

	return ports.EngineMetrics{
		TotalWorkflows:     int64(len(e.activeWorkflows)),
		ActiveWorkflows:    activeCount,
		CompletedWorkflows: completedCount,
		FailedWorkflows:    failedCount,
		QueueSizes:         queueSizes,
		WorkerPoolSize:     e.config.MaxConcurrentWorkflows,
		PanicMetrics:       e.metricsTracker.GetPanicMetrics(),
		HandlerMetrics:     e.metricsTracker.GetHandlerMetrics(),
	}
}

func (e *Engine) processReadyNodes() {
	defer e.wg.Done()

	for {
		if err := e.coordinator.ProcessReadyNodes(e.ctx); err != nil {
			if e.ctx.Err() != nil {
				return
			}
			e.logger.Error("error processing ready nodes", "error", err.Error())
			time.Sleep(time.Second)
		}
	}
}


func (e *Engine) processWorkflowCompletions() {
	defer e.wg.Done()

	for {
		if err := e.coordinator.CheckWorkflowCompletions(e.ctx); err != nil {
			if e.ctx.Err() != nil {
				return
			}
			e.logger.Error("error checking workflow completions", "error", err.Error())
			time.Sleep(time.Second)
		}
	}
}

func (e *Engine) queueNodeForExecution(workflowID string, nodeConfig ports.NodeConfig, reason string) error {
	if e.queue == nil {
		e.logger.Debug("queue not available, executing node directly",
			"workflow_id", workflowID,
			"node_name", nodeConfig.Name,
			"reason", reason)
		
		return domain.Error{
		Type:    domain.ErrorTypeUnavailable,
		Message: "queue not available for node execution",
		Details: map[string]interface{}{
			"component": "workflow_engine",
		},
	}
	}
	
	item := &ports.QueueItem{
		ID:         generateItemID(),
		WorkflowID: workflowID,
		NodeName:   nodeConfig.Name,
		Config:     nodeConfig.Config,
		EnqueuedAt: time.Now(),
	}

	return e.queue.EnqueueReady(context.Background(), *item)
}

func generateItemID() string {
	return time.Now().Format("20060102150405") + "_" + uuid.New().String()[:8]
}