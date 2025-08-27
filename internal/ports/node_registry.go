package ports

import (
	"context"
	"github.com/eleven-am/graft/internal/domain"
	json "github.com/goccy/go-json"
	"time"
)

type NodePort interface {
	GetName() string
	CanStart(ctx context.Context, state json.RawMessage, config json.RawMessage) bool
	Execute(ctx context.Context, state json.RawMessage, config json.RawMessage) (*NodeResult, error)
}

type NodeResult struct {
	GlobalState interface{} `json:"global_state"`
	NextNodes   []NextNode  `json:"next_nodes"`
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

func (n *NodeResult) ToInternal() *domain.NodeResult {
	if n == nil {
		return nil
	}

	return &domain.NodeResult{
		GlobalState: interfaceToRawMessage(n.GlobalState),
		NextNodes:   toInternalNextNodes(n.NextNodes),
	}
}

func toInternalNextNodes(next []NextNode) []domain.NextNode {
	if next == nil {
		return nil
	}

	internalNext := make([]domain.NextNode, len(next))
	for i, n := range next {
		internalNext[i] = domain.NextNode{
			NodeName:       n.NodeName,
			Config:         interfaceToRawMessage(n.Config),
			Priority:       n.Priority,
			Delay:          n.Delay,
			IdempotencyKey: n.IdempotencyKey,
		}
	}

	return internalNext
}

func interfaceToRawMessage(data interface{}) json.RawMessage {
	if data == nil {
		return nil
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return nil
	}

	return bytes
}
