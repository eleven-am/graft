package node_registry

import (
	"context"
	"testing"

	"github.com/eleven-am/graft/internal/ports"
)

type MockNode struct {
	name   string
	health bool
}

func (m *MockNode) GetName() string                                        { return m.name }
func (m *MockNode) CanStart(ctx context.Context, args ...interface{}) bool { return m.health }
func (m *MockNode) Execute(ctx context.Context, args ...interface{}) (*ports.NodeResult, error) {
	return &ports.NodeResult{}, nil
}

func TestAdapter_RegisterNode_Success(t *testing.T) {
	adapter := NewAdapter(nil)

	node := &MockNode{name: "test-node", health: true}

	err := adapter.RegisterNode(node)
	if err != nil {
		t.Errorf("Failed to register node: %v", err)
	}

	registered, err := adapter.GetNode("test-node")
	if err != nil {
		t.Errorf("Node should exist after registration: %v", err)
	}

	if registered.GetName() != "test-node" {
		t.Errorf("Expected node name 'test-node', got '%s'", registered.GetName())
	}
}

func TestAdapter_RegisterNode_Nil(t *testing.T) {
	adapter := NewAdapter(nil)

	err := adapter.RegisterNode(nil)
	if err == nil {
		t.Error("Expected error when registering nil node")
	}

	if regErr, ok := err.(*ports.NodeRegistrationError); ok {
		if regErr.NodeName != "<nil>" {
			t.Errorf("Expected node name '<nil>', got '%s'", regErr.NodeName)
		}
		if regErr.Reason != "node cannot be nil" {
			t.Errorf("Expected reason 'node cannot be nil', got '%s'", regErr.Reason)
		}
	} else {
		t.Error("Expected NodeRegistrationError")
	}
}

func TestAdapter_RegisterNode_EmptyName(t *testing.T) {
	adapter := NewAdapter(nil)

	node := &MockNode{name: "", health: true}

	err := adapter.RegisterNode(node)
	if err == nil {
		t.Error("Expected error when registering node with empty name")
	}

	if regErr, ok := err.(*ports.NodeRegistrationError); ok {
		if regErr.Reason != "node name cannot be empty" {
			t.Errorf("Expected reason 'node name cannot be empty', got '%s'", regErr.Reason)
		}
	} else {
		t.Error("Expected NodeRegistrationError")
	}
}

func TestAdapter_RegisterNode_AlreadyExists(t *testing.T) {
	adapter := NewAdapter(nil)

	node1 := &MockNode{name: "test-node", health: true}
	node2 := &MockNode{name: "test-node", health: false}

	err := adapter.RegisterNode(node1)
	if err != nil {
		t.Errorf("Failed to register first node: %v", err)
	}

	err = adapter.RegisterNode(node2)
	if err == nil {
		t.Error("Expected error when registering node with duplicate name")
	}

	if regErr, ok := err.(*ports.NodeRegistrationError); ok {
		if regErr.NodeName != "test-node" {
			t.Errorf("Expected node name 'test-node', got '%s'", regErr.NodeName)
		}
		if regErr.Reason != "node already registered" {
			t.Errorf("Expected reason 'node already registered', got '%s'", regErr.Reason)
		}
	} else {
		t.Error("Expected NodeRegistrationError")
	}

	registered, err := adapter.GetNode("test-node")
	if err != nil {
		t.Errorf("Original node should still be registered: %v", err)
	}

	if !registered.CanStart(context.Background()) {
		t.Error("Original node should still be healthy")
	}
}

func TestAdapter_GetNode_Exists(t *testing.T) {
	adapter := NewAdapter(nil)

	node := &MockNode{name: "test-node", health: true}
	adapter.RegisterNode(node)

	retrieved, err := adapter.GetNode("test-node")
	if err != nil {
		t.Errorf("Node should exist: %v", err)
	}

	if retrieved.GetName() != "test-node" {
		t.Errorf("Expected node name 'test-node', got '%s'", retrieved.GetName())
	}
}

func TestAdapter_GetNode_NotExists(t *testing.T) {
	adapter := NewAdapter(nil)

	_, err := adapter.GetNode("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent node")
	}
}

func TestAdapter_UnregisterNode_Success(t *testing.T) {
	adapter := NewAdapter(nil)

	node := &MockNode{name: "test-node", health: true}
	adapter.RegisterNode(node)

	err := adapter.UnregisterNode("test-node")
	if err != nil {
		t.Errorf("Should successfully unregister existing node: %v", err)
	}

	_, err = adapter.GetNode("test-node")
	if err == nil {
		t.Error("Expected error for unregistered node")
	}
}

func TestAdapter_UnregisterNode_NotExists(t *testing.T) {
	adapter := NewAdapter(nil)

	err := adapter.UnregisterNode("nonexistent")
	if err == nil {
		t.Error("Expected error when unregistering nonexistent node")
	}
}

func TestAdapter_ListNodes(t *testing.T) {
	adapter := NewAdapter(nil)

	nodeNames := adapter.ListNodes()
	if len(nodeNames) != 0 {
		t.Errorf("Expected 0 nodes initially, got %d", len(nodeNames))
	}

	node1 := &MockNode{name: "node-1", health: true}
	node2 := &MockNode{name: "node-2", health: false}
	node3 := &MockNode{name: "node-3", health: true}

	adapter.RegisterNode(node1)
	adapter.RegisterNode(node2)
	adapter.RegisterNode(node3)

	nodeNames = adapter.ListNodes()
	if len(nodeNames) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodeNames))
	}

	nameMap := make(map[string]bool)
	for _, name := range nodeNames {
		nameMap[name] = true
	}

	expectedNames := []string{"node-1", "node-2", "node-3"}
	for _, name := range expectedNames {
		if !nameMap[name] {
			t.Errorf("Expected node '%s' not found", name)
		}
	}
}

func TestAdapter_HasNode(t *testing.T) {
	adapter := NewAdapter(nil)

	if adapter.HasNode("test-node") {
		t.Error("Expected node to not exist initially")
	}

	node := &MockNode{name: "test-node", health: true}
	adapter.RegisterNode(node)

	if !adapter.HasNode("test-node") {
		t.Error("Expected node to exist after registration")
	}

	adapter.UnregisterNode("test-node")

	if adapter.HasNode("test-node") {
		t.Error("Expected node to not exist after unregistration")
	}
}

func TestAdapter_GetNodeCount(t *testing.T) {
	adapter := NewAdapter(nil)

	if adapter.GetNodeCount() != 0 {
		t.Errorf("Expected 0 nodes initially, got %d", adapter.GetNodeCount())
	}

	node1 := &MockNode{name: "node-1", health: true}
	node2 := &MockNode{name: "node-2", health: false}

	adapter.RegisterNode(node1)
	if adapter.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node after first registration, got %d", adapter.GetNodeCount())
	}

	adapter.RegisterNode(node2)
	if adapter.GetNodeCount() != 2 {
		t.Errorf("Expected 2 nodes after second registration, got %d", adapter.GetNodeCount())
	}

	adapter.UnregisterNode("node-1")
	if adapter.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node after unregistration, got %d", adapter.GetNodeCount())
	}
}
