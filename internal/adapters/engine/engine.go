package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/google/uuid"
)

type Engine struct {
	nodeRegistry    ports.NodeRegistryPort
	resourceManager ports.ResourceManagerPort
	storage         ports.StoragePort
	raft            ports.RaftPort
	readyQueue      ports.QueuePort
	pendingQueue    ports.QueuePort
	semaphore       ports.SemaphorePort

	coordinator       *WorkflowCoordinator
	stateManager      *StateManager
	lifecycleManager  *LifecycleManager
	dataCollector     *WorkflowDataCollector
	metricsTracker    *MetricsTracker
	pendingEvaluator  PendingEvaluator
	evaluationTrigger EvaluationTrigger
	cleaner           *WorkflowCleaner
	logger            *slog.Logger
	config            Config
	nodeID            string

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
		logger: logger,
		config: config,
		nodeID: uuid.New().String(),
		ctx:    ctx,
		cancel: cancel,
	}

	engine.metricsTracker = NewMetricsTracker()
	engine.coordinator = NewWorkflowCoordinator(engine)
	engine.stateManager = NewStateManager(engine)
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
	e.initializeDataCollector()
}

func (e *Engine) SetResourceManager(manager ports.ResourceManagerPort) {
	e.resourceManager = manager
}

func (e *Engine) SetStorage(storage ports.StoragePort) {
	e.storage = storage
	e.initializeDataCollector()
	e.initializeCleaner()
}

func (e *Engine) SetReadyQueue(queue ports.QueuePort) {
	e.readyQueue = queue
	e.initializeDataCollector()
	e.initializeCleaner()
}

func (e *Engine) SetPendingQueue(queue ports.QueuePort) {
	e.pendingQueue = queue
	e.initializeDataCollector()
	e.initializeCleaner()
}

func (e *Engine) SetSemaphore(semaphore ports.SemaphorePort) {
	e.semaphore = semaphore
}

func (e *Engine) SetRaft(raft ports.RaftPort) {
	e.raft = raft
	e.initializeRaftCleanup()
}

func (e *Engine) RegisterLifecycleHandlers(completion []ports.CompletionHandler, error []ports.ErrorHandler) {
	if e.lifecycleManager != nil {
		e.lifecycleManager.RegisterHandlers(completion, error)
	}
}

func (e *Engine) initializeDataCollector() {
	if e.storage != nil && e.pendingQueue != nil && e.readyQueue != nil && e.nodeRegistry != nil && e.dataCollector == nil {
		e.dataCollector = NewWorkflowDataCollector(e.storage, e.pendingQueue, e.readyQueue, e.nodeRegistry, e.resourceManager, e.metricsTracker, e.nodeID, e.logger)
	}
}

func (e *Engine) initializeCleaner() {
	if e.storage != nil && e.cleaner == nil {
		e.cleaner = NewWorkflowCleaner(e.storage, e.readyQueue, e.pendingQueue, e.logger)
	}
}

func (e *Engine) initializeRaftCleanup() {
	if e.raft != nil && e.lifecycleManager != nil {
		cleanupCallback := func(workflowID string) error {
			e.logger.Debug("executing Raft-based nuclear cleanup", "workflow_id", workflowID)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			command := domain.NewCompleteWorkflowPurgeCommand(workflowID)
			result, err := e.raft.Apply(ctx, command)

			if err != nil {
				return fmt.Errorf("raft cleanup command failed: %w", err)
			}

			if !result.Success {
				return fmt.Errorf("raft cleanup command rejected: %s", result.Error)
			}

			e.logger.Debug("Raft nuclear cleanup completed successfully", "workflow_id", workflowID)
			return nil
		}

		e.lifecycleManager.SetCleanupCallback(cleanupCallback)
		e.logger.Debug("Raft cleanup callback initialized")
	}
}

func (e *Engine) Start(ctx context.Context) error {
	if e.nodeRegistry == nil || e.resourceManager == nil ||
		e.storage == nil || e.readyQueue == nil || e.pendingQueue == nil ||
		e.semaphore == nil || e.raft == nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "missing required dependencies",
			Details: map[string]interface{}{
				"node_registry":    e.nodeRegistry != nil,
				"resource_manager": e.resourceManager != nil,
				"storage":          e.storage != nil,
				"ready_queue":      e.readyQueue != nil,
				"pending_queue":    e.pendingQueue != nil,
				"semaphore":        e.semaphore != nil,
				"raft":             e.raft != nil,
			},
		}
	}

	if e.storage == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "FATAL: workflow engine requires storage for state persistence",
			Details: map[string]interface{}{
				"component":   "workflow_engine",
				"requirement": "storage",
			},
		}
	}
	if e.readyQueue == nil || e.pendingQueue == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "FATAL: workflow engine requires both ready and pending queues for distributed execution",
			Details: map[string]interface{}{
				"component":     "workflow_engine",
				"ready_queue":   e.readyQueue != nil,
				"pending_queue": e.pendingQueue != nil,
			},
		}
	}

	e.logger.Info("starting workflow engine",
		"max_concurrent_workflows", e.config.MaxConcurrentWorkflows,
		"node_execution_timeout", e.config.NodeExecutionTimeout,
	)

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

	e.logger.Info("workflow engine stopped")
	return nil
}

func (e *Engine) ProcessTrigger(trigger ports.WorkflowTrigger) error {
	workflow := &WorkflowInstance{
		ID:           trigger.WorkflowID,
		Status:       ports.WorkflowStateRunning,
		CurrentState: trigger.InitialState,
		StartedAt:    time.Now(),
		Metadata:     trigger.Metadata,
	}

	if err := e.stateManager.SaveWorkflowState(context.Background(), workflow); err != nil {
		e.logger.Error("failed to save initial workflow state",
			"workflow_id", trigger.WorkflowID,
			"error", err.Error(),
		)
		return err
	}

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
	workflow, err := e.stateManager.LoadWorkflowState(context.Background(), workflowID)
	if err != nil {
		e.logger.Error("failed to load workflow state for status",
			"workflow_id", workflowID,
			"error", err.Error())
		return nil, err
	}

	if workflow == nil {
		e.logger.Debug("workflow state not found, returning not found error",
			"workflow_id", workflowID)
		return nil, domain.NewNotFoundError("workflow", workflowID)
	}

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

	e.logger.Debug("workflow status retrieved successfully",
		"workflow_id", workflowID,
		"status", workflow.Status,
		"has_state", workflow.CurrentState != nil)

	return status, nil
}

func (e *Engine) GetExecutionMetrics() ports.EngineMetrics {
	e.logger.Debug("calculating execution metrics")
	var totalWorkflows, activeCount, completedCount, failedCount, nodesExecuted int64
	var totalExecutionTime time.Duration

	if e.storage != nil {
		e.logger.Debug("retrieving workflow states")
		ctx := context.Background()
		workflowKeys, err := e.storage.List(ctx, "workflow:state:")
		if err == nil {
			e.logger.Debug("workflow keys retrieved", "count", len(workflowKeys))
			totalWorkflows = int64(len(workflowKeys))

			for _, kv := range workflowKeys {
				// Skip empty data to avoid unmarshal errors
				if len(kv.Value) == 0 {
					e.logger.Debug("skipping empty workflow state data for metrics",
						"key", kv.Key)
					continue
				}

				var workflow WorkflowInstance
				if err := json.Unmarshal(kv.Value, &workflow); err != nil {
					e.logger.Debug("failed to unmarshal workflow state for metrics",
						"key", kv.Key,
						"data_length", len(kv.Value),
						"error", err.Error())
					continue
				}

				switch workflow.Status {
				case ports.WorkflowStateRunning:
					activeCount++
				case ports.WorkflowStateCompleted:
					completedCount++
					// Calculate execution time for completed workflows
					if workflow.CompletedAt != nil {
						duration := workflow.CompletedAt.Sub(workflow.StartedAt)
						totalExecutionTime += duration
					}
				case ports.WorkflowStateFailed:
					failedCount++
				}
			}
		} else {
			e.logger.Error("storage list failed", "error", err.Error())
		}
	} else {
		e.logger.Warn("storage is nil in GetExecutionMetrics")
	}

	// Get node execution count from metrics tracker
	if e.metricsTracker != nil {
		nodesExecuted = e.metricsTracker.GetNodesExecuted()
	}

	// Calculate average execution time
	var avgExecutionTime time.Duration
	if completedCount > 0 {
		avgExecutionTime = totalExecutionTime / time.Duration(completedCount)
	}

	queueSizes := ports.QueueSizes{}
	ctx := context.Background()

	if e.readyQueue != nil {
		isEmpty, err := e.readyQueue.IsEmpty(ctx)
		if err == nil && !isEmpty {
			queueSizes.Ready = 1
		}
	}

	if e.pendingQueue != nil {
		pendingItems, err := e.pendingQueue.GetItems(ctx)
		if err == nil {
			queueSizes.Pending = len(pendingItems)
		}
	}

	metrics := ports.EngineMetrics{
		TotalWorkflows:       totalWorkflows,
		ActiveWorkflows:      activeCount,
		CompletedWorkflows:   completedCount,
		FailedWorkflows:      failedCount,
		NodesExecuted:        nodesExecuted,
		AverageExecutionTime: avgExecutionTime,
		QueueSizes:           queueSizes,
		WorkerPoolSize:       e.config.MaxConcurrentWorkflows,
		PanicMetrics:         e.metricsTracker.GetPanicMetrics(),
		HandlerMetrics:       e.metricsTracker.GetHandlerMetrics(),
	}

	e.logger.Debug("execution metrics calculated",
		"total_workflows", totalWorkflows,
		"active_workflows", activeCount,
		"completed_workflows", completedCount,
		"failed_workflows", failedCount,
		"nodes_executed", nodesExecuted)

	return metrics
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
	if e.readyQueue == nil {
		e.logger.Debug("ready queue not available, cannot execute node",
			"workflow_id", workflowID,
			"node_name", nodeConfig.Name,
			"reason", reason)

		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "ready queue not available for node execution",
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

	return e.readyQueue.Enqueue(context.Background(), *item)
}

func (e *Engine) PauseWorkflow(ctx context.Context, workflowID string) error {
	if e.coordinator == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "coordinator not initialized",
		}
	}
	return e.coordinator.PauseWorkflow(ctx, workflowID)
}

func (e *Engine) ResumeWorkflow(ctx context.Context, workflowID string) error {
	if e.coordinator == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "coordinator not initialized",
		}
	}
	return e.coordinator.ResumeWorkflow(ctx, workflowID)
}

func (e *Engine) StopWorkflow(ctx context.Context, workflowID string) error {
	if e.coordinator == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "coordinator not initialized",
		}
	}
	return e.coordinator.StopWorkflow(ctx, workflowID)
}

func (e *Engine) EvaluatePendingNodes(ctx context.Context, workflowID string, currentState interface{}) error {
	if e.pendingEvaluator == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "pending evaluator not initialized",
		}
	}
	return e.pendingEvaluator.EvaluatePendingNodes(ctx, workflowID, currentState)
}

func (e *Engine) CheckNodeReadiness(node *ports.PendingNode, state interface{}, config interface{}) bool {
	if e.pendingEvaluator == nil {
		return false
	}
	return e.pendingEvaluator.CheckNodeReadiness(node, state, config)
}

func (e *Engine) ProcessReadyNodes(ctx context.Context) error {
	if e.coordinator == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "coordinator not initialized",
		}
	}
	return e.coordinator.ProcessReadyNodes(ctx)
}

func (e *Engine) SaveWorkflowState(ctx context.Context, workflowID string) error {
	if e.stateManager == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "state manager not initialized",
		}
	}

	workflow, err := e.stateManager.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return err
	}

	if workflow == nil {
		return domain.NewNotFoundError("workflow", workflowID)
	}

	return e.stateManager.SaveWorkflowState(ctx, workflow)
}

func (e *Engine) LoadWorkflowState(ctx context.Context, workflowID string) (*ports.WorkflowInstance, error) {
	if e.stateManager == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "state manager not initialized",
		}
	}

	workflow, err := e.stateManager.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	if workflow == nil {
		return nil, domain.NewNotFoundError("workflow", workflowID)
	}

	return &ports.WorkflowInstance{
		ID:           workflow.ID,
		Status:       workflow.Status,
		CurrentState: workflow.CurrentState,
		StartedAt:    workflow.StartedAt,
		CompletedAt:  workflow.CompletedAt,
		Metadata:     workflow.Metadata,
		LastError:    workflow.LastError,
	}, nil
}

func (e *Engine) UpdateWorkflowState(ctx context.Context, workflowID string, updates map[string]interface{}) error {
	if e.stateManager == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "state manager not initialized",
		}
	}
	return e.stateManager.UpdateWorkflowState(ctx, workflowID, updates)
}

func (e *Engine) RecoverActiveWorkflows(ctx context.Context) error {
	if e.stateManager == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "state manager not initialized",
		}
	}

	e.logger.Debug("starting active workflow recovery")

	workflows, err := e.stateManager.ListWorkflowStates(ctx)
	if err != nil {
		return fmt.Errorf("failed to list workflows for recovery: %w", err)
	}

	recoveredCount := 0
	for _, workflow := range workflows {
		if workflow.Status == ports.WorkflowStateRunning {
			e.logger.Debug("recovering active workflow",
				"workflow_id", workflow.ID,
				"started_at", workflow.StartedAt,
			)

			if e.pendingEvaluator != nil {
				if err := e.pendingEvaluator.EvaluatePendingNodes(ctx, workflow.ID, workflow.CurrentState); err != nil {
					e.logger.Error("failed to evaluate workflow during recovery",
						"workflow_id", workflow.ID,
						"error", err.Error(),
					)
				}
			}
			recoveredCount++
		}
	}

	e.logger.Debug("workflow recovery completed",
		"total_workflows", len(workflows),
		"recovered_active", recoveredCount,
	)

	return nil
}

func (e *Engine) RestoreWorkflowFromState(ctx context.Context, importData *domain.WorkflowStateImport) (*domain.ValidationResult, error) {
	e.logger.Debug("starting workflow state import",
		"workflow_id", importData.WorkflowState.WorkflowID,
		"resumption_mode", importData.ResumptionMode,
		"validation_level", importData.ValidationLevel)

	validator := NewStateValidator(e.logger)
	validationResult, err := validator.ValidateState(ctx, &importData.WorkflowState, importData.ValidationLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to validate workflow state: %w", err)
	}

	if !validationResult.Valid {
		e.logger.Error("workflow state validation failed",
			"workflow_id", importData.WorkflowState.WorkflowID,
			"errors", len(validationResult.Errors))
		return validationResult, nil
	}

	reconstructor := NewDAGReconstructor(e, validator, e.logger)
	if err := reconstructor.RestoreWorkflowState(ctx, &importData.WorkflowState); err != nil {
		return nil, fmt.Errorf("failed to restore workflow state: %w", err)
	}

	e.logger.Debug("workflow state successfully restored",
		"workflow_id", importData.WorkflowState.WorkflowID,
		"resumption_mode", importData.ResumptionMode)

	return validationResult, nil
}

func (e *Engine) ValidateWorkflowState(state *domain.CompleteWorkflowState) (*domain.ValidationResult, error) {
	e.logger.Debug("validating workflow state", "workflow_id", state.WorkflowID)

	validator := NewStateValidator(e.logger)
	result, err := validator.ValidateState(context.Background(), state, domain.ValidationStandard)
	if err != nil {
		return nil, fmt.Errorf("failed to validate workflow state: %w", err)
	}

	e.logger.Debug("workflow state validation completed",
		"workflow_id", state.WorkflowID,
		"valid", result.Valid,
		"errors", len(result.Errors),
		"warnings", len(result.Warnings))

	return result, nil
}

func (e *Engine) ExportWorkflowState(ctx context.Context, workflowID string) (*domain.CompleteWorkflowState, error) {
	e.logger.Debug("exporting workflow state", "workflow_id", workflowID)

	workflow, err := e.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to load workflow for export: %w", err)
	}

	workflowInstance := &WorkflowInstance{
		ID:           workflow.ID,
		Status:       workflow.Status,
		CurrentState: workflow.CurrentState,
		StartedAt:    workflow.StartedAt,
		CompletedAt:  workflow.CompletedAt,
		Metadata:     workflow.Metadata,
		LastError:    workflow.LastError,
	}

	if e.dataCollector == nil {
		e.initializeDataCollector()
	}

	state, err := e.dataCollector.ExportCompleteWorkflowState(ctx, workflowInstance)
	if err != nil {
		return nil, fmt.Errorf("failed to export workflow state: %w", err)
	}

	e.logger.Debug("workflow state successfully exported",
		"workflow_id", workflowID,
		"executed_nodes", len(state.ExecutedNodes),
		"pending_nodes", len(state.PendingNodes),
		"ready_nodes", len(state.ReadyNodes))

	return state, nil
}

func (e *Engine) ResumeWorkflowFromPoint(ctx context.Context, workflowID string, resumptionPoint *domain.ResumptionPoint) error {
	e.logger.Debug("resuming workflow from point",
		"workflow_id", workflowID,
		"resumption_node", resumptionPoint.NodeID)

	workflow, err := e.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to load workflow for resumption: %w", err)
	}

	if workflow.Status == ports.WorkflowStateRunning {
		return fmt.Errorf("workflow %s is already running", workflowID)
	}

	workflowKey := fmt.Sprintf("workflow:state:%s", workflowID)
	workflow.Status = ports.WorkflowStateRunning
	workflowBytes, err := json.Marshal(workflow)
	if err != nil {
		return fmt.Errorf("failed to marshal workflow for storage: %w", err)
	}
	if err := e.storage.Put(ctx, workflowKey, workflowBytes); err != nil {
		return fmt.Errorf("failed to update workflow status for resumption: %w", err)
	}

	if resumptionPoint.NodeID != "" {
		nodeConfig := ports.NodeConfig{
			Name:   resumptionPoint.NodeID,
			Config: resumptionPoint.State,
		}
		if err := e.queueNodeForExecution(workflowID, nodeConfig, "resumption_point"); err != nil {
			return fmt.Errorf("failed to queue resumption node: %w", err)
		}
	}

	if e.pendingEvaluator != nil {
		if err := e.pendingEvaluator.EvaluatePendingNodes(ctx, workflowID, workflow.CurrentState); err != nil {
			e.logger.Error("failed to evaluate pending nodes during resumption",
				"workflow_id", workflowID,
				"error", err.Error())
		}
	}

	e.logger.Debug("workflow successfully resumed from point",
		"workflow_id", workflowID,
		"resumption_node", resumptionPoint.NodeID)

	return nil
}

func (e *Engine) ResumeFromCheckpoint(ctx context.Context, workflowID string, checkpointID string) error {
	e.logger.Debug("resuming workflow from checkpoint",
		"workflow_id", workflowID,
		"checkpoint_id", checkpointID)

	checkpointKey := fmt.Sprintf("workflow:checkpoint:%s:%s", workflowID, checkpointID)
	checkpointBytes, err := e.storage.Get(ctx, checkpointKey)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint %s: %w", checkpointID, err)
	}

	var checkpoint domain.CheckpointData
	if err := json.Unmarshal(checkpointBytes, &checkpoint); err != nil {
		return fmt.Errorf("failed to unmarshal checkpoint %s: %w", checkpointID, err)
	}

	resumptionPoint := &domain.ResumptionPoint{
		NodeID:         checkpoint.NodeID,
		State:          checkpoint.State,
		Timestamp:      checkpoint.CreatedAt,
		ValidationHash: fmt.Sprintf("checkpoint_%s_%d", checkpointID, checkpoint.CreatedAt.Unix()),
		Metadata:       checkpoint.Metadata,
	}

	return e.ResumeWorkflowFromPoint(ctx, workflowID, resumptionPoint)
}

func generateItemID() string {
	return time.Now().Format("20060102150405") + "_" + uuid.New().String()[:8]
}
