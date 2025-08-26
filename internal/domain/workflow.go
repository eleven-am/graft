package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type WorkflowState string

const (
	WorkflowStateRunning   WorkflowState = "running"
	WorkflowStateCompleted WorkflowState = "completed" 
	WorkflowStateFailed    WorkflowState = "failed"
	WorkflowStatePaused    WorkflowState = "paused"
)

type WorkflowInstance struct {
	ID           string            `json:"id"`
	Status       WorkflowState     `json:"status"`
	CurrentState json.RawMessage   `json:"current_state"`
	StartedAt    time.Time         `json:"started_at"`
	CompletedAt  *time.Time        `json:"completed_at,omitempty"`
	Metadata     map[string]string `json:"metadata"`
	LastError    *string           `json:"last_error,omitempty"`
	Version      int64             `json:"version"`
	RetryCount   map[string]int    `json:"retry_count,omitempty"`
}

type NodeResult struct {
	GlobalState interface{} `json:"global_state"`
	NextNodes   []NextNode  `json:"next_nodes"`
}

type NextNode struct {
	NodeName       string          `json:"node_name"`
	Config         json.RawMessage `json:"config"`
	Priority       int             `json:"priority,omitempty"`
	Delay          *time.Duration  `json:"delay,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
}

type WorkflowTrigger struct {
	WorkflowID   string            `json:"workflow_id"`
	InitialNodes []NodeConfig      `json:"initial_nodes"`
	InitialState json.RawMessage   `json:"initial_state"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type NodeConfig struct {
	Name   string          `json:"name"`
	Config json.RawMessage `json:"config"`
}

type ExecutedNodeData struct {
	NodeName   string          `json:"node_name"`
	ExecutedAt time.Time       `json:"executed_at"`
	Duration   time.Duration   `json:"duration"`
	Status     string          `json:"status"`
	Config     json.RawMessage `json:"config"`
	Results    json.RawMessage `json:"results"`
	Error      *string         `json:"error,omitempty"`
}

type WorkflowStatus struct {
	WorkflowID    string             `json:"workflow_id"`
	Status        WorkflowState      `json:"status"`
	CurrentState  json.RawMessage    `json:"current_state"`
	StartedAt     time.Time          `json:"started_at"`
	CompletedAt   *time.Time         `json:"completed_at,omitempty"`
	ExecutedNodes []ExecutedNodeData `json:"executed_nodes"`
	PendingNodes  []NodeConfig       `json:"pending_nodes"`
	LastError     *string            `json:"last_error,omitempty"`
}

type NodeExecutionStatus string

const (
	NodeExecutionStatusCompleted   NodeExecutionStatus = "completed"
	NodeExecutionStatusFailed      NodeExecutionStatus = "failed"
	NodeExecutionStatusCancelled   NodeExecutionStatus = "cancelled"
	NodeExecutionStatusPanicFailed NodeExecutionStatus = "panic_failed"
)

type WorkflowContext struct {
	WorkflowID  string            `json:"workflow_id"`
	NodeName    string            `json:"node_name"`
	ExecutionID string            `json:"execution_id"`
	StartedAt   time.Time         `json:"started_at"`
	Metadata    map[string]string `json:"metadata"`
}

type NodePort interface {
	GetName() string
	CanStart(ctx context.Context, state []byte, config []byte) bool
	Execute(ctx context.Context, state []byte, config []byte) (*NodeResult, error)
}

type NodeRegistryPort interface {
	RegisterNode(node NodePort) error
	GetNode(nodeName string) (NodePort, error)
	ListNodes() []string
	HasNode(nodeName string) bool
}

type WorkflowEvent struct {
	Type        string    `json:"type"`
	WorkflowID  string    `json:"workflow_id"`
	NodeName    string    `json:"node_name,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Data        json.RawMessage `json:"data,omitempty"`
}

const (
	EventTypeWorkflowStarted   = "workflow_started"
	EventTypeNodeCompleted     = "node_completed"
	EventTypeWorkflowCompleted = "workflow_completed"
	EventTypeWorkflowFailed    = "workflow_failed"
)

type NotFoundError struct {
	Type string
	ID   string
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("%s not found: %s", e.Type, e.ID)
}

func NewNotFoundError(typ, id string) error {
	return NotFoundError{Type: typ, ID: id}
}