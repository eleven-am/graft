package ports

import (
	"context"
	"time"
)

type DistributedEventManager interface {
	Start(ctx context.Context) error
	Stop() error
	BroadcastEvent(ctx context.Context, event *StateChangeEvent) error
	SubscribeToWorkflow(workflowID string, nodeID string) (<-chan *WorkflowStatus, func(), error)
	RegisterNode(nodeID string, address string) error
	UnregisterNode(nodeID string) error
	GetConnectedNodes() []EventNodeInfo
}

type StateChangeEvent struct {
	EventID        string                 `json:"event_id"`
	WorkflowID     string                 `json:"workflow_id"`
	ChangedBy      string                 `json:"changed_by"`
	NodeName       string                 `json:"node_name,omitempty"`
	EventType      StateChangeEventType   `json:"event_type"`
	Timestamp      time.Time              `json:"timestamp"`
	SequenceNumber int64                  `json:"sequence_number"`
	StateData      map[string]interface{} `json:"state_data"`
	SourceNodeID   string                 `json:"source_node_id"`
}

type StateChangeEventType string

const (
	EventTypeNodeStarted       StateChangeEventType = "node_started"
	EventTypeNodeCompleted     StateChangeEventType = "node_completed"
	EventTypeNodeFailed        StateChangeEventType = "node_failed"
	EventTypeStateUpdated      StateChangeEventType = "state_updated"
	EventTypeWorkflowCompleted StateChangeEventType = "workflow_completed"
	EventTypeWorkflowFailed    StateChangeEventType = "workflow_failed"
)

type EventStore interface {
	StoreEvent(ctx context.Context, event *StateChangeEvent) error
	GetEventsFromSequence(ctx context.Context, workflowID string, fromSequence int64) ([]*StateChangeEvent, error)
	GetLatestSequence(ctx context.Context, workflowID string) (int64, error)
	CleanupOldEvents(ctx context.Context, before time.Time) error
}

type EventSubscription struct {
	ID           string
	WorkflowID   string
	NodeID       string
	Channel      chan *WorkflowStatus
	FromSequence int64
	CreatedAt    time.Time
	LastUpdated  time.Time
}

type EventNodeInfo struct {
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	Connected bool      `json:"connected"`
	LastSeen  time.Time `json:"last_seen"`
}
