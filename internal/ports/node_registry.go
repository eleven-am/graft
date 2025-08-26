package ports

import (
	"context"
	"encoding/json"
	"time"
)

type NodePort interface {
	GetName() string
	CanStart(ctx context.Context, args ...interface{}) bool
	Execute(ctx context.Context, args ...interface{}) (*NodeResult, error)
}

type NodeResult struct {
	GlobalState json.RawMessage
	NextNodes   []NextNode
}

type NextNode struct {
	NodeName       string         `json:"node_name"`
	Config         interface{}    `json:"config"`
	Priority       int            `json:"priority,omitempty"`
	Delay          *time.Duration `json:"delay,omitempty"`
	IdempotencyKey *string        `json:"idempotency_key,omitempty"`
}

type NodeRegistryPort interface {
	RegisterNode(node interface{}) error
	GetNode(nodeName string) (NodePort, error)
	ListNodes() []string
	UnregisterNode(nodeName string) error
	HasNode(nodeName string) bool
	GetNodeCount() int
}

type NodeRegistrationError struct {
	NodeName string
	Reason   string
}

func (e NodeRegistrationError) Error() string {
	return "node registration failed for '" + e.NodeName + "': " + e.Reason
}
