package memory

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/eleven-am/graft/internal/ports"
)

type mockNode struct {
	name string
}

func (m *mockNode) GetName() string {
	return m.name
}

func (m *mockNode) CanStart(ctx context.Context, args ...interface{}) bool {
	return true
}

func (m *mockNode) Execute(ctx context.Context, args ...interface{}) (*ports.NodeResult, error) {
	return &ports.NodeResult{GlobalState: "result"}, nil
}

func TestNewMemoryNodeRegistry(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	if registry == nil {
		t.Fatal("expected registry to be created")
	}

	if registry.nodes == nil {
		t.Fatal("expected nodes map to be initialized")
	}

	if registry.logger == nil {
		t.Fatal("expected logger to be set")
	}
}

func TestNewMemoryNodeRegistry_NilLogger(t *testing.T) {
	registry := NewMemoryNodeRegistry(nil)

	if registry == nil {
		t.Fatal("expected registry to be created with nil logger")
	}

	if registry.logger == nil {
		t.Fatal("expected default logger to be set")
	}
}

func TestMemoryNodeRegistry_RegisterNode(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	node := &mockNode{name: "test-node"}
	err := registry.RegisterNode(node)
	if err != nil {
		t.Fatalf("failed to register node: %v", err)
	}

	if !registry.HasNode("test-node") {
		t.Fatal("node should be registered")
	}
}

func TestMemoryNodeRegistry_RegisterNode_Nil(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	err := registry.RegisterNode(nil)
	if err == nil {
		t.Fatal("expected error when registering nil node")
	}

	regErr, ok := err.(*ports.NodeRegistrationError)
	if !ok {
		t.Fatalf("expected NodeRegistrationError, got %T", err)
	}

	if regErr.NodeName != "<nil>" {
		t.Errorf("expected node name '<nil>', got %s", regErr.NodeName)
	}

	if regErr.Reason != "node cannot be nil" {
		t.Errorf("expected reason 'node cannot be nil', got %s", regErr.Reason)
	}
}

func TestMemoryNodeRegistry_RegisterNode_EmptyName(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	node := &mockNode{name: ""}
	err := registry.RegisterNode(node)
	if err == nil {
		t.Fatal("expected error when registering node with empty name")
	}

	regErr, ok := err.(*ports.NodeRegistrationError)
	if !ok {
		t.Fatalf("expected NodeRegistrationError, got %T", err)
	}

	if regErr.Reason != "node name cannot be empty" {
		t.Errorf("expected reason 'node name cannot be empty', got %s", regErr.Reason)
	}
}

func TestMemoryNodeRegistry_RegisterNode_Duplicate(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	node1 := &mockNode{name: "test-node"}
	node2 := &mockNode{name: "test-node"}

	err := registry.RegisterNode(node1)
	if err != nil {
		t.Fatalf("failed to register first node: %v", err)
	}

	err = registry.RegisterNode(node2)
	if err == nil {
		t.Fatal("expected error when registering duplicate node")
	}

	regErr, ok := err.(*ports.NodeRegistrationError)
	if !ok {
		t.Fatalf("expected NodeRegistrationError, got %T", err)
	}

	if regErr.NodeName != "test-node" {
		t.Errorf("expected node name 'test-node', got %s", regErr.NodeName)
	}

	if regErr.Reason != "node already registered" {
		t.Errorf("expected reason 'node already registered', got %s", regErr.Reason)
	}
}

func TestMemoryNodeRegistry_GetNode(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	originalNode := &mockNode{name: "test-node"}
	err := registry.RegisterNode(originalNode)
	if err != nil {
		t.Fatalf("failed to register node: %v", err)
	}

	retrievedNode, err := registry.GetNode("test-node")
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}

	if retrievedNode != originalNode {
		t.Fatal("retrieved node should be the same as registered node")
	}
}

func TestMemoryNodeRegistry_GetNode_NotFound(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	_, err := registry.GetNode("non-existent")
	if err == nil {
		t.Fatal("expected error when getting non-existent node")
	}

	expectedError := "not_found: node not found in registry"
	if err.Error() != expectedError {
		t.Errorf("expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestMemoryNodeRegistry_ListNodes(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	nodeNames := registry.ListNodes()
	if len(nodeNames) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodeNames))
	}

	node1 := &mockNode{name: "node1"}
	node2 := &mockNode{name: "node2"}

	registry.RegisterNode(node1)
	registry.RegisterNode(node2)

	nodeNames = registry.ListNodes()
	if len(nodeNames) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodeNames))
	}

	found1 := false
	found2 := false
	for _, name := range nodeNames {
		if name == "node1" {
			found1 = true
		} else if name == "node2" {
			found2 = true
		}
	}

	if !found1 || !found2 {
		t.Errorf("expected to find both nodes, found1=%v, found2=%v", found1, found2)
	}
}

func TestMemoryNodeRegistry_UnregisterNode(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	node := &mockNode{name: "test-node"}
	registry.RegisterNode(node)

	if !registry.HasNode("test-node") {
		t.Fatal("node should be registered")
	}

	err := registry.UnregisterNode("test-node")
	if err != nil {
		t.Fatalf("failed to unregister node: %v", err)
	}

	if registry.HasNode("test-node") {
		t.Fatal("node should not exist after unregistration")
	}
}

func TestMemoryNodeRegistry_UnregisterNode_NotFound(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	err := registry.UnregisterNode("non-existent")
	if err == nil {
		t.Fatal("expected error when unregistering non-existent node")
	}

	expectedError := "not_found: node not found for unregistration"
	if err.Error() != expectedError {
		t.Errorf("expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestMemoryNodeRegistry_HasNode(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	if registry.HasNode("test-node") {
		t.Fatal("node should not exist initially")
	}

	node := &mockNode{name: "test-node"}
	registry.RegisterNode(node)

	if !registry.HasNode("test-node") {
		t.Fatal("node should exist after registration")
	}
}

func TestMemoryNodeRegistry_GetNodeCount(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	if registry.GetNodeCount() != 0 {
		t.Errorf("expected 0 nodes initially, got %d", registry.GetNodeCount())
	}

	node1 := &mockNode{name: "node1"}
	node2 := &mockNode{name: "node2"}
	node3 := &mockNode{name: "node3"}

	registry.RegisterNode(node1)
	if registry.GetNodeCount() != 1 {
		t.Errorf("expected 1 node after first registration, got %d", registry.GetNodeCount())
	}

	registry.RegisterNode(node2)
	registry.RegisterNode(node3)
	if registry.GetNodeCount() != 3 {
		t.Errorf("expected 3 nodes after all registrations, got %d", registry.GetNodeCount())
	}

	registry.UnregisterNode("node2")
	if registry.GetNodeCount() != 2 {
		t.Errorf("expected 2 nodes after unregistration, got %d", registry.GetNodeCount())
	}
}

func TestMemoryNodeRegistry_ConcurrentAccess(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	var wg sync.WaitGroup
	numWorkers := 10
	nodesPerWorker := 10

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < nodesPerWorker; j++ {
				nodeName := fmt.Sprintf("worker%d-node%d", workerID, j)
				node := &mockNode{name: nodeName}

				err := registry.RegisterNode(node)
				if err != nil {
					t.Errorf("worker %d failed to register node %s: %v", workerID, nodeName, err)
				}

				if !registry.HasNode(nodeName) {
					t.Errorf("worker %d: node %s should exist after registration", workerID, nodeName)
				}

				retrievedNode, err := registry.GetNode(nodeName)
				if err != nil {
					t.Errorf("worker %d failed to get node %s: %v", workerID, nodeName, err)
				}

				if retrievedNode != node {
					t.Errorf("worker %d: retrieved node should match registered node for %s", workerID, nodeName)
				}
			}
		}(i)
	}

	wg.Wait()

	expectedCount := numWorkers * nodesPerWorker
	actualCount := registry.GetNodeCount()
	if actualCount != expectedCount {
		t.Errorf("expected %d nodes after concurrent registration, got %d", expectedCount, actualCount)
	}

	nodeNames := registry.ListNodes()
	if len(nodeNames) != expectedCount {
		t.Errorf("expected %d nodes in list, got %d", expectedCount, len(nodeNames))
	}
}

func TestMemoryNodeRegistry_ConcurrentReadWrite(t *testing.T) {
	logger := slog.Default()
	registry := NewMemoryNodeRegistry(logger)

	numNodes := 50
	for i := 0; i < numNodes; i++ {
		node := &mockNode{name: fmt.Sprintf("node%d", i)}
		registry.RegisterNode(node)
	}

	var wg sync.WaitGroup
	numReaders := 5
	numWriters := 3
	readsPerWorker := 100
	writesPerWorker := 20

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for j := 0; j < readsPerWorker; j++ {
				nodeNames := registry.ListNodes()
				if len(nodeNames) < numNodes {
					t.Errorf("reader %d: expected at least %d nodes, got %d", readerID, numNodes, len(nodeNames))
				}

				for _, nodeName := range nodeNames {
					if registry.HasNode(nodeName) {
						_, err := registry.GetNode(nodeName)
						if err != nil {
							t.Errorf("reader %d failed to get existing node %s: %v", readerID, nodeName, err)
						}
					}
				}
			}
		}(i)
	}

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < writesPerWorker; j++ {
				nodeName := fmt.Sprintf("writer%d-node%d", writerID, j)
				node := &mockNode{name: nodeName}

				err := registry.RegisterNode(node)
				if err != nil {
					t.Errorf("writer %d failed to register node %s: %v", writerID, nodeName, err)
				}

				if registry.HasNode(nodeName) {
					err = registry.UnregisterNode(nodeName)
					if err != nil {
						t.Errorf("writer %d failed to unregister node %s: %v", writerID, nodeName, err)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	finalCount := registry.GetNodeCount()
	if finalCount < numNodes {
		t.Errorf("expected at least %d nodes after concurrent operations, got %d", numNodes, finalCount)
	}
}

func TestNodeRegistrationError_Error(t *testing.T) {
	err := &ports.NodeRegistrationError{
		NodeName: "test-node",
		Reason:   "test reason",
	}

	expectedError := "node registration failed for 'test-node': test reason"
	if err.Error() != expectedError {
		t.Errorf("expected error message '%s', got '%s'", expectedError, err.Error())
	}
}

func TestValidateNode(t *testing.T) {
	err := validateNode(nil)
	if err == nil {
		t.Fatal("expected error when validating nil node")
	}

	node := &mockNode{name: ""}
	err = validateNode(node)
	if err == nil {
		t.Fatal("expected error when validating node with empty name")
	}

	node = &mockNode{name: "valid-node"}
	err = validateNode(node)
	if err != nil {
		t.Fatalf("expected no error when validating valid node: %v", err)
	}
}

func TestValidateNodeName(t *testing.T) {
	err := validateNodeName("")
	if err == nil {
		t.Fatal("expected error when validating empty node name")
	}

	err = validateNodeName("valid-name")
	if err != nil {
		t.Fatalf("expected no error when validating valid node name: %v", err)
	}
}
