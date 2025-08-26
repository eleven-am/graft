package graft

import (
	"time"
)

type WorkflowTrigger struct {
	WorkflowID   string            `json:"workflow_id"`
	InitialNodes []NodeConfig      `json:"initial_nodes"`
	InitialState interface{}       `json:"initial_state"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type NodeConfig struct {
	Name   string      `json:"name"`
	Config interface{} `json:"config"`
}

type NextNode struct {
	NodeName       string         `json:"node_name"`
	Config         interface{}    `json:"config"`
	Priority       int            `json:"priority,omitempty"`
	Delay          *time.Duration `json:"delay,omitempty"`
	IdempotencyKey *string        `json:"idempotency_key,omitempty"`
}

type NodeResult struct {
	GlobalState interface{} `json:"global_state"`
	NextNodes   []NextNode  `json:"next_nodes"`
}

type WorkflowStatus struct {
	WorkflowID    string             `json:"workflow_id"`
	Status        WorkflowState      `json:"status"`
	CurrentState  interface{}        `json:"current_state"`
	StartedAt     time.Time          `json:"started_at"`
	CompletedAt   *time.Time         `json:"completed_at,omitempty"`
	ExecutedNodes []ExecutedNodeData `json:"executed_nodes"`
	PendingNodes  []NodeConfig       `json:"pending_nodes"`
	LastError     *string            `json:"last_error,omitempty"`
}

type ExecutedNodeData struct {
	NodeName   string        `json:"node_name"`
	ExecutedAt time.Time     `json:"executed_at"`
	Duration   time.Duration `json:"duration"`
	Status     string        `json:"status"`
	Config     interface{}   `json:"config"`
	Results    interface{}   `json:"results"`
	Error      *string       `json:"error,omitempty"`
}

type WorkflowState string

const (
	WorkflowStateRunning   WorkflowState = "running"
	WorkflowStateCompleted WorkflowState = "completed"
	WorkflowStateFailed    WorkflowState = "failed"
	WorkflowStatePaused    WorkflowState = "paused"
)
