package graft

import (
	"encoding/json"
	"fmt"

	"github.com/eleven-am/graft/internal/domain"
)

func (w WorkflowTrigger) toInternal() (domain.WorkflowTrigger, error) {
	initialState, err := marshalToRawMessage(w.InitialState)
	if err != nil {
		return domain.WorkflowTrigger{}, fmt.Errorf("failed to marshal initial state: %w", err)
	}

	nodes := make([]domain.NodeConfig, len(w.InitialNodes))
	for i, node := range w.InitialNodes {
		internalNode, err := node.toInternal()
		if err != nil {
			return domain.WorkflowTrigger{}, fmt.Errorf("failed to convert node %s: %w", node.Name, err)
		}
		nodes[i] = internalNode
	}

	return domain.WorkflowTrigger{
		WorkflowID:   w.WorkflowID,
		InitialNodes: nodes,
		InitialState: initialState,
		Metadata:     w.Metadata,
	}, nil
}

func (n NodeConfig) toInternal() (domain.NodeConfig, error) {
	config, err := marshalToRawMessage(n.Config)
	if err != nil {
		return domain.NodeConfig{}, fmt.Errorf("failed to marshal config: %w", err)
	}

	return domain.NodeConfig{
		Name:   n.Name,
		Config: config,
	}, nil
}

func workflowStatusFromInternal(w domain.WorkflowStatus) (WorkflowStatus, error) {
	var currentState interface{}
	if len(w.CurrentState) > 0 {
		if err := json.Unmarshal(w.CurrentState, &currentState); err != nil {
			return WorkflowStatus{}, fmt.Errorf("failed to unmarshal current state: %w", err)
		}
	}

	executedNodes := make([]ExecutedNodeData, len(w.ExecutedNodes))
	for i, node := range w.ExecutedNodes {
		publicNode, err := executedNodeFromInternal(node)
		if err != nil {
			return WorkflowStatus{}, fmt.Errorf("failed to convert executed node: %w", err)
		}
		executedNodes[i] = publicNode
	}

	pendingNodes := make([]NodeConfig, len(w.PendingNodes))
	for i, node := range w.PendingNodes {
		publicNode, err := nodeConfigFromInternal(node)
		if err != nil {
			return WorkflowStatus{}, fmt.Errorf("failed to convert pending node: %w", err)
		}
		pendingNodes[i] = publicNode
	}

	return WorkflowStatus{
		WorkflowID:    w.WorkflowID,
		Status:        WorkflowState(w.Status),
		CurrentState:  currentState,
		StartedAt:     w.StartedAt,
		CompletedAt:   w.CompletedAt,
		ExecutedNodes: executedNodes,
		PendingNodes:  pendingNodes,
		LastError:     w.LastError,
	}, nil
}

func executedNodeFromInternal(e domain.ExecutedNodeData) (ExecutedNodeData, error) {
	var config interface{}
	if len(e.Config) > 0 {
		if err := json.Unmarshal(e.Config, &config); err != nil {
			return ExecutedNodeData{}, fmt.Errorf("failed to unmarshal config: %w", err)
		}
	}

	var results interface{}
	if len(e.Results) > 0 {
		if err := json.Unmarshal(e.Results, &results); err != nil {
			return ExecutedNodeData{}, fmt.Errorf("failed to unmarshal results: %w", err)
		}
	}

	return ExecutedNodeData{
		NodeName:   e.NodeName,
		ExecutedAt: e.ExecutedAt,
		Duration:   e.Duration,
		Status:     e.Status,
		Config:     config,
		Results:    results,
		Error:      e.Error,
	}, nil
}

func nodeConfigFromInternal(n domain.NodeConfig) (NodeConfig, error) {
	var config interface{}
	if len(n.Config) > 0 {
		if err := json.Unmarshal(n.Config, &config); err != nil {
			return NodeConfig{}, fmt.Errorf("failed to unmarshal config: %w", err)
		}
	}

	return NodeConfig{
		Name:   n.Name,
		Config: config,
	}, nil
}

func marshalToRawMessage(v interface{}) (json.RawMessage, error) {
	if v == nil {
		return json.RawMessage("null"), nil
	}

	// If it's already json.RawMessage, use it directly
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}

	// If it's already []byte, convert to json.RawMessage
	if bytes, ok := v.([]byte); ok {
		return json.RawMessage(bytes), nil
	}

	// Marshal the value to JSON
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(data), nil
}
