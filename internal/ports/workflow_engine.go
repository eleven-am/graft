package ports

import (
	"context"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type EnginePort interface {
	Start(ctx context.Context) error
	Stop() error
	ProcessTrigger(trigger WorkflowTrigger) error
	GetWorkflowStatus(workflowID string) (*WorkflowStatus, error)
	GetExecutionMetrics() EngineMetrics

	PauseWorkflow(ctx context.Context, workflowID string) error
	ResumeWorkflow(ctx context.Context, workflowID string) error
	StopWorkflow(ctx context.Context, workflowID string) error

	EvaluatePendingNodes(ctx context.Context, workflowID string, currentState interface{}) error
	CheckNodeReadiness(node *PendingNode, state interface{}, config interface{}) bool
	ProcessReadyNodes(ctx context.Context) error

	SaveWorkflowState(ctx context.Context, workflowID string) error
	LoadWorkflowState(ctx context.Context, workflowID string) (*WorkflowInstance, error)
	RecoverActiveWorkflows(ctx context.Context) error
	UpdateWorkflowState(ctx context.Context, workflowID string, updates map[string]interface{}) error

	RegisterLifecycleHandlers(completion []CompletionHandler, error []ErrorHandler)

	RestoreWorkflowFromState(ctx context.Context, importData *domain.WorkflowStateImport) (*domain.ValidationResult, error)
	ValidateWorkflowState(state *domain.CompleteWorkflowState) (*domain.ValidationResult, error)
	ExportWorkflowState(ctx context.Context, workflowID string) (*domain.CompleteWorkflowState, error)
	ResumeWorkflowFromPoint(ctx context.Context, workflowID string, resumptionPoint *domain.ResumptionPoint) error
	ResumeFromCheckpoint(ctx context.Context, workflowID string, checkpointID string) error
}

type WorkflowTrigger struct {
	WorkflowID   string            `json:"workflow_id"`
	InitialNodes []NodeConfig      `json:"initial_nodes"`
	InitialState interface{}       `json:"initial_state"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type WorkflowInstance struct {
	ID           string            `json:"id"`
	Status       WorkflowState     `json:"status"`
	CurrentState interface{}       `json:"current_state"`
	StartedAt    time.Time         `json:"started_at"`
	CompletedAt  *time.Time        `json:"completed_at,omitempty"`
	Metadata     map[string]string `json:"metadata"`
	LastError    *string           `json:"last_error,omitempty"`
}

type NodeConfig struct {
	Name   string      `json:"name"`
	Config interface{} `json:"config"`
}

type WorkflowStatus struct {
	WorkflowID    string         `json:"workflow_id"`
	Status        WorkflowState  `json:"status"`
	CurrentState  interface{}    `json:"current_state"`
	StartedAt     time.Time      `json:"started_at"`
	CompletedAt   *time.Time     `json:"completed_at,omitempty"`
	ExecutedNodes []ExecutedNode `json:"executed_nodes"`
	PendingNodes  []PendingNode  `json:"pending_nodes"`
	ReadyNodes    []ReadyNode    `json:"ready_nodes"`
	LastError     *string        `json:"last_error,omitempty"`
}

type WorkflowState string

const (
	WorkflowStateRunning   WorkflowState = "running"
	WorkflowStateCompleted WorkflowState = "completed"
	WorkflowStateFailed    WorkflowState = "failed"
	WorkflowStatePaused    WorkflowState = "paused"
)

type NodeExecutionStatus string

const (
	NodeExecutionStatusCompleted   NodeExecutionStatus = "completed"
	NodeExecutionStatusFailed      NodeExecutionStatus = "failed"
	NodeExecutionStatusCancelled   NodeExecutionStatus = "cancelled"
	NodeExecutionStatusPanicFailed NodeExecutionStatus = "panic_failed"
)

type ExecutedNode struct {
	NodeName   string              `json:"node_name"`
	ExecutedAt time.Time           `json:"executed_at"`
	Duration   time.Duration       `json:"duration"`
	Status     NodeExecutionStatus `json:"status"`
	Config     interface{}         `json:"config"`
	Results    interface{}         `json:"results,omitempty"`
	Error      *string             `json:"error,omitempty"`
}

type PendingNode struct {
	NodeName string      `json:"node_name"`
	Config   interface{} `json:"config"`
	QueuedAt time.Time   `json:"queued_at"`
	Priority int         `json:"priority"`
	Reason   string      `json:"reason,omitempty"`
}

type ReadyNode struct {
	NodeName       string      `json:"node_name"`
	Config         interface{} `json:"config"`
	QueuedAt       time.Time   `json:"queued_at"`
	Priority       int         `json:"priority"`
	WorkflowID     string      `json:"workflow_id"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
}

type EngineMetrics struct {
	TotalWorkflows       int64          `json:"total_workflows"`
	ActiveWorkflows      int64          `json:"active_workflows"`
	CompletedWorkflows   int64          `json:"completed_workflows"`
	FailedWorkflows      int64          `json:"failed_workflows"`
	NodesExecuted        int64          `json:"nodes_executed"`
	AverageExecutionTime time.Duration  `json:"average_execution_time"`
	WorkerPoolSize       int            `json:"worker_pool_size"`
	QueueSizes           QueueSizes     `json:"queue_sizes"`
	PanicMetrics         PanicMetrics   `json:"panic_metrics"`
	HandlerMetrics       HandlerMetrics `json:"handler_metrics"`
}

type PanicMetrics struct {
	TotalPanics         int64         `json:"total_panics"`
	PanicsLastHour      int64         `json:"panics_last_hour"`
	AverageRecoveryTime time.Duration `json:"average_recovery_time"`
	LastPanicAt         *time.Time    `json:"last_panic_at,omitempty"`
}

type HandlerMetrics struct {
	CompletionHandlersExecuted int64         `json:"completion_handlers_executed"`
	ErrorHandlersExecuted      int64         `json:"error_handlers_executed"`
	HandlerFailures            int64         `json:"handler_failures"`
	AverageHandlerTime         time.Duration `json:"average_handler_time"`
	HandlerTimeouts            int64         `json:"handler_timeouts"`
}

type QueueSizes struct {
	Ready   int `json:"ready"`
	Pending int `json:"pending"`
}

type CompletionHandler func(ctx context.Context, data domain.WorkflowCompletionData) error
type ErrorHandler func(ctx context.Context, data domain.WorkflowErrorData) error
