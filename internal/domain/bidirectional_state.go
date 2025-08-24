package domain

import (
	"time"
)

type WorkflowState string

const (
	WorkflowStateRunning   WorkflowState = "running"
	WorkflowStateCompleted WorkflowState = "completed"
	WorkflowStateFailed    WorkflowState = "failed"
	WorkflowStatePaused    WorkflowState = "paused"
)

type CompleteWorkflowState struct {
	WorkflowID  string            `json:"workflow_id"`
	Status      WorkflowState     `json:"status"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Duration    time.Duration     `json:"duration"`
	Metadata    map[string]string `json:"metadata"`

	CurrentState  interface{}        `json:"current_state"`
	ExecutedNodes []ExecutedNodeData `json:"executed_nodes"`
	PendingNodes  []PendingNodeData  `json:"pending_nodes"`
	ReadyNodes    []ReadyNodeData    `json:"ready_nodes"`

	QueueItems      []QueueItemData      `json:"queue_items"`
	ClaimsData      []ClaimData          `json:"claims"`
	IdempotencyKeys []IdempotencyKeyData `json:"idempotency_keys"`
	Checkpoints     []CheckpointData     `json:"checkpoints"`

	ExecutionDAG   WorkflowDAG         `json:"execution_dag"`
	ResourceStates []ResourceStateData `json:"resource_states"`
	Dependencies   []DependencyData    `json:"dependencies"`

	LastExecutedNode string           `json:"last_executed_node,omitempty"`
	ResumptionPoint  *ResumptionPoint `json:"resumption_point,omitempty"`

	StateHash  string     `json:"state_hash"`
	Version    int        `json:"version"`
	ImportedAt *time.Time `json:"imported_at,omitempty"`
}

type WorkflowStateImport struct {
	WorkflowState   CompleteWorkflowState  `json:"workflow_state"`
	ResumptionMode  ResumptionMode         `json:"resumption_mode"`
	FromNodeID      string                 `json:"from_node_id,omitempty"`
	ValidationLevel ValidationLevel        `json:"validation_level"`
	ClientMetadata  map[string]interface{} `json:"client_metadata,omitempty"`
}

type ResumptionMode string

const (
	ResumptionFromLastNode     ResumptionMode = "from_last_node"
	ResumptionFromSpecificNode ResumptionMode = "from_specific_node"
	ResumptionFullRestart      ResumptionMode = "full_restart"
	ResumptionFromCheckpoint   ResumptionMode = "from_checkpoint"
)

type ValidationLevel string

const (
	ValidationStrict   ValidationLevel = "strict"
	ValidationStandard ValidationLevel = "standard"
	ValidationLenient  ValidationLevel = "lenient"
	ValidationSkip     ValidationLevel = "skip"
)

type ResumptionPoint struct {
	NodeID         string                 `json:"node_id"`
	Position       DAGPosition            `json:"position"`
	Prerequisites  []string               `json:"prerequisites"`
	State          interface{}            `json:"state"`
	Timestamp      time.Time              `json:"timestamp"`
	ValidationHash string                 `json:"validation_hash"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

type DAGPosition struct {
	Level        int      `json:"level"`
	Dependencies []string `json:"dependencies"`
	Dependents   []string `json:"dependents"`
}

type WorkflowDAG struct {
	Nodes  []DAGNode `json:"nodes"`
	Edges  []DAGEdge `json:"edges"`
	Roots  []string  `json:"roots"`
	Leaves []string  `json:"leaves"`
}

type DAGNode struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"`
	Status       NodeExecutionStatus    `json:"status"`
	Dependencies []string               `json:"dependencies"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

type DAGEdge struct {
	From     string                 `json:"from"`
	To       string                 `json:"to"`
	Type     string                 `json:"type"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type NodeExecutionStatus string

const (
	NodeStatusPending   NodeExecutionStatus = "pending"
	NodeStatusReady     NodeExecutionStatus = "ready"
	NodeStatusExecuting NodeExecutionStatus = "executing"
	NodeStatusCompleted NodeExecutionStatus = "completed"
	NodeStatusFailed    NodeExecutionStatus = "failed"
	NodeStatusSkipped   NodeExecutionStatus = "skipped"
)

type ValidationResult struct {
	Valid               bool                `json:"valid"`
	Errors              []ValidationError   `json:"errors,omitempty"`
	Warnings            []ValidationWarning `json:"warnings,omitempty"`
	RecommendedActions  []RecommendedAction `json:"recommended_actions,omitempty"`
	StateConsistency    ConsistencyReport   `json:"state_consistency"`
	ResumptionReadiness ResumptionReadiness `json:"resumption_readiness"`
}

type ValidationError struct {
	Code     string                 `json:"code"`
	Message  string                 `json:"message"`
	Field    string                 `json:"field,omitempty"`
	NodeID   string                 `json:"node_id,omitempty"`
	Severity ValidationSeverity     `json:"severity"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type ValidationWarning struct {
	Code       string                 `json:"code"`
	Message    string                 `json:"message"`
	Field      string                 `json:"field,omitempty"`
	NodeID     string                 `json:"node_id,omitempty"`
	Suggestion string                 `json:"suggestion,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type RecommendedAction struct {
	Action      string                 `json:"action"`
	Description string                 `json:"description"`
	NodeID      string                 `json:"node_id,omitempty"`
	Priority    ActionPriority         `json:"priority"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

type ValidationSeverity string

const (
	SeverityCritical ValidationSeverity = "critical"
	SeverityHigh     ValidationSeverity = "high"
	SeverityMedium   ValidationSeverity = "medium"
	SeverityLow      ValidationSeverity = "low"
)

type ActionPriority string

const (
	PriorityImmediate ActionPriority = "immediate"
	PriorityHigh      ActionPriority = "high"
	PriorityMedium    ActionPriority = "medium"
	PriorityLow       ActionPriority = "low"
)

type ConsistencyReport struct {
	DAGConsistent      bool               `json:"dag_consistent"`
	TemporalConsistent bool               `json:"temporal_consistent"`
	ResourceConsistent bool               `json:"resource_consistent"`
	DataIntegrity      bool               `json:"data_integrity"`
	Issues             []ConsistencyIssue `json:"issues,omitempty"`
	Recommendations    []string           `json:"recommendations,omitempty"`
}

type ConsistencyIssue struct {
	Type        string                 `json:"type"`
	Description string                 `json:"description"`
	NodeID      string                 `json:"node_id,omitempty"`
	Severity    ValidationSeverity     `json:"severity"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

type ResumptionReadiness struct {
	Ready                bool       `json:"ready"`
	MissingPrerequisites []string   `json:"missing_prerequisites,omitempty"`
	InvalidResources     []string   `json:"invalid_resources,omitempty"`
	StateInconsistencies []string   `json:"state_inconsistencies,omitempty"`
	EstimatedReadiness   *time.Time `json:"estimated_readiness,omitempty"`
	Recommendations      []string   `json:"recommendations,omitempty"`
}

type ExecutionPlan struct {
	WorkflowID        string                 `json:"workflow_id"`
	ResumptionPoint   ResumptionPoint        `json:"resumption_point"`
	NextExecutable    []string               `json:"next_executable"`
	RequiredResources []string               `json:"required_resources"`
	EstimatedDuration time.Duration          `json:"estimated_duration"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
}
