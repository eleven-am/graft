package domain

import (
	"time"
)

type NodeExecution struct {
	ID              string
	WorkflowID      string
	NodeName        string
	Status          NodeStatus
	Config          map[string]interface{}
	Results         map[string]interface{}
	Error           string
	StartedAt       time.Time
	CompletedAt     *time.Time
	RetryCount      int
	MaxRetries      int
}

type NodeStatus string

const (
	NodeStatusPending   NodeStatus = "pending"
	NodeStatusReady     NodeStatus = "ready"
	NodeStatusRunning   NodeStatus = "running"
	NodeStatusCompleted NodeStatus = "completed"
	NodeStatusFailed    NodeStatus = "failed"
	NodeStatusSkipped   NodeStatus = "skipped"
)

type NodeDefinition struct {
	Name        string
	Description string
	Version     string
	Metadata    map[string]string
}