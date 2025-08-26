package node_registry

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/eleven-am/graft/internal/ports"
)

type TestState struct {
	Value string `json:"value"`
}

type TestConfig struct {
	Name string `json:"name"`
}

type TestNode struct {
	name string
}

func (n *TestNode) GetName() string {
	return n.name
}

func (n *TestNode) Execute(ctx context.Context, state TestState, config TestConfig) (*ports.NodeResult, error) {
	globalStateBytes, err := json.Marshal(TestState{Value: "test_state"})
	if err != nil {
		return nil, err
	}
	return &ports.NodeResult{
		GlobalState: globalStateBytes,
		NextNodes:   []ports.NextNode{},
	}, nil
}

type TestNodeWithCanStart struct {
	name     string
	canStart bool
}

func (n *TestNodeWithCanStart) GetName() string {
	return n.name
}

func (n *TestNodeWithCanStart) CanStart(ctx context.Context, state TestState, config TestConfig) bool {
	return n.canStart
}

func (n *TestNodeWithCanStart) Execute(ctx context.Context, state TestState, config TestConfig) (*ports.NodeResult, error) {
	globalStateBytes, err := json.Marshal(TestState{Value: "test_state_with_canstart"})
	if err != nil {
		return nil, err
	}
	return &ports.NodeResult{
		GlobalState: globalStateBytes,
		NextNodes:   []ports.NextNode{},
	}, nil
}

func TestNodeRegistry_RegisterNode(t *testing.T) {
	manager := NewManager(nil)

	node := &TestNode{name: "test_node"}
	err := manager.RegisterNode(node)
	if err != nil {
		t.Fatalf("Failed to register node: %v", err)
	}

	if !manager.HasNode("test_node") {
		t.Error("Node should be registered")
	}

	if manager.GetNodeCount() != 1 {
		t.Errorf("Expected node count to be 1, got %d", manager.GetNodeCount())
	}
}

func TestNodeRegistry_RegisterNodeTwice(t *testing.T) {
	manager := NewManager(nil)

	node := &TestNode{name: "test_node"}
	err := manager.RegisterNode(node)
	if err != nil {
		t.Fatalf("Failed to register node first time: %v", err)
	}

	err = manager.RegisterNode(node)
	if err == nil {
		t.Error("Expected error when registering node twice")
	}

	regErr, ok := err.(*ports.NodeRegistrationError)
	if !ok {
		t.Error("Expected NodeRegistrationError")
	}

	if regErr.NodeName != "test_node" {
		t.Errorf("Expected node name 'test_node', got '%s'", regErr.NodeName)
	}
}

func TestNodeRegistry_GetNode(t *testing.T) {
	manager := NewManager(nil)

	node := &TestNode{name: "test_node"}
	err := manager.RegisterNode(node)
	if err != nil {
		t.Fatalf("Failed to register node: %v", err)
	}

	retrievedNode, err := manager.GetNode("test_node")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	if retrievedNode.GetName() != "test_node" {
		t.Errorf("Expected node name 'test_node', got '%s'", retrievedNode.GetName())
	}
}

func TestNodeRegistry_GetNonexistentNode(t *testing.T) {
	manager := NewManager(nil)

	_, err := manager.GetNode("nonexistent")
	if err == nil {
		t.Error("Expected error when getting nonexistent node")
	}
}

func TestNodeRegistry_ListNodes(t *testing.T) {
	manager := NewManager(nil)

	node1 := &TestNode{name: "test_node_1"}
	node2 := &TestNode{name: "test_node_2"}

	manager.RegisterNode(node1)
	manager.RegisterNode(node2)

	nodes := manager.ListNodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}

	nodeSet := make(map[string]bool)
	for _, name := range nodes {
		nodeSet[name] = true
	}

	if !nodeSet["test_node_1"] || !nodeSet["test_node_2"] {
		t.Error("Missing expected node names in list")
	}
}

func TestNodeRegistry_UnregisterNode(t *testing.T) {
	manager := NewManager(nil)

	node := &TestNode{name: "test_node"}
	manager.RegisterNode(node)

	if !manager.HasNode("test_node") {
		t.Error("Node should be registered")
	}

	err := manager.UnregisterNode("test_node")
	if err != nil {
		t.Fatalf("Failed to unregister node: %v", err)
	}

	if manager.HasNode("test_node") {
		t.Error("Node should not be registered after unregistering")
	}

	if manager.GetNodeCount() != 0 {
		t.Errorf("Expected node count to be 0, got %d", manager.GetNodeCount())
	}
}

func TestNodeRegistry_NodeExecution(t *testing.T) {
	manager := NewManager(nil)

	node := &TestNode{name: "test_node"}
	manager.RegisterNode(node)

	retrievedNode, err := manager.GetNode("test_node")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	ctx := context.Background()
	state := TestState{Value: "input_state"}
	config := TestConfig{Name: "test_config"}
	
	result, err := retrievedNode.Execute(ctx, state, config)
	if err != nil {
		t.Fatalf("Failed to execute node: %v", err)
	}

	var resultState TestState
	if err := json.Unmarshal(result.GlobalState, &resultState); err != nil {
		t.Fatalf("Failed to unmarshal result GlobalState: %v", err)
	}

	if resultState.Value != "test_state" {
		t.Errorf("Expected global state value 'test_state', got '%v'", resultState.Value)
	}
}

func TestNodeRegistry_NodeCanStart(t *testing.T) {
	manager := NewManager(nil)

	node := &TestNodeWithCanStart{name: "test_node", canStart: false}
	manager.RegisterNode(node)

	retrievedNode, err := manager.GetNode("test_node")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	ctx := context.Background()
	state := TestState{Value: "input_state"}
	config := TestConfig{Name: "test_config"}
	
	canStart := retrievedNode.CanStart(ctx, state, config)
	if canStart {
		t.Error("Expected CanStart to return false")
	}

	node2 := &TestNodeWithCanStart{name: "test_node_2", canStart: true}
	manager.RegisterNode(node2)

	retrievedNode2, err := manager.GetNode("test_node_2")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	canStart2 := retrievedNode2.CanStart(ctx, state, config)
	if !canStart2 {
		t.Error("Expected CanStart to return true")
	}
}