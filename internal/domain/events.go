package domain

import "time"

type EventType string

const (
	EventTypeWorkflowStarted    EventType = "workflow_started"
	EventTypeWorkflowCompleted  EventType = "workflow_completed"
	EventTypeWorkflowFailed     EventType = "workflow_failed"
	EventTypeNodeStarted        EventType = "node_started"
	EventTypeNodeCompleted      EventType = "node_completed"
	EventTypeNodeFailed         EventType = "node_failed"
	EventTypeStateUpdated       EventType = "state_updated"
	EventTypeNodeQueued         EventType = "node_queued"
	EventTypeClusterNodeJoined  EventType = "cluster_node_joined"
	EventTypeClusterNodeLeft    EventType = "cluster_node_left"
	EventTypeLeaderElected      EventType = "leader_elected"
)

type Event struct {
	ID         string
	Type       EventType
	WorkflowID string
	NodeID     string
	Timestamp  time.Time
	Data       map[string]interface{}
}

type EventHandler interface {
	Handle(event Event) error
}