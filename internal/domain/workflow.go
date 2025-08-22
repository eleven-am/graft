package domain

import (
	"time"
)

type Workflow struct {
	ID          string
	Status      WorkflowStatus
	GlobalState map[string]interface{}
	StartedAt   time.Time
	CompletedAt *time.Time
	Error       string
	Metadata    map[string]string
}

type WorkflowStatus string

const (
	WorkflowStatusPending   WorkflowStatus = "pending"
	WorkflowStatusRunning   WorkflowStatus = "running"
	WorkflowStatusCompleted WorkflowStatus = "completed"
	WorkflowStatusFailed    WorkflowStatus = "failed"
	WorkflowStatusCancelled WorkflowStatus = "cancelled"
)

type Trigger struct {
	ID           string
	WorkflowID   string
	InitialNodes []NodeConfig
	InitialState map[string]interface{}
	ReceivedAt   time.Time
}

type NodeConfig struct {
	Name   string
	Config map[string]interface{}
}