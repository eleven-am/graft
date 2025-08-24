package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDAGManagerImplementation(t *testing.T) {
	logger := slog.Default()
	dm := NewDAGManager(logger)

	state := &domain.CompleteWorkflowState{
		WorkflowID: "test-workflow",
		ExecutionDAG: domain.WorkflowDAG{
			Nodes: []domain.DAGNode{
				{ID: "node1", Type: "start", Status: domain.NodeStatusCompleted, Dependencies: []string{}},
				{ID: "node2", Type: "process", Status: domain.NodeStatusPending, Dependencies: []string{"node1"}},
				{ID: "node3", Type: "process", Status: domain.NodeStatusPending, Dependencies: []string{"node1"}},
				{ID: "node4", Type: "end", Status: domain.NodeStatusPending, Dependencies: []string{"node2", "node3"}},
			},
			Edges: []domain.DAGEdge{
				{From: "node1", To: "node2", Type: "execution"},
				{From: "node1", To: "node3", Type: "execution"},
				{From: "node2", To: "node4", Type: "execution"},
				{From: "node3", To: "node4", Type: "execution"},
			},
		},
	}

	// Test building DAG from state
	dag, err := dm.BuildDAGFromState(state)
	require.NoError(t, err)
	assert.NotNil(t, dag)

	// Test getting roots
	roots, err := dm.GetRoots(state.WorkflowID)
	require.NoError(t, err)
	assert.Contains(t, roots, "node1")

	// Test getting leaves
	leaves, err := dm.GetLeaves(state.WorkflowID)
	require.NoError(t, err)
	assert.Contains(t, leaves, "node4")

	// Test getting next executable nodes
	completedNodes := map[string]bool{"node1": true}
	nextNodes, err := dm.GetNextExecutableNodes(state.WorkflowID, completedNodes)
	require.NoError(t, err)
	assert.Len(t, nextNodes, 2)
	assert.Contains(t, nextNodes, "node2")
	assert.Contains(t, nextNodes, "node3")

	// Test topological order
	order, err := dm.GetTopologicalOrder(state.WorkflowID)
	require.NoError(t, err)
	assert.NotEmpty(t, order)

	// Verify node1 comes before node2 and node3
	node1Idx := -1
	node2Idx := -1
	node3Idx := -1
	node4Idx := -1
	for i, nodeID := range order {
		switch nodeID {
		case "node1":
			node1Idx = i
		case "node2":
			node2Idx = i
		case "node3":
			node3Idx = i
		case "node4":
			node4Idx = i
		}
	}
	assert.Less(t, node1Idx, node2Idx)
	assert.Less(t, node1Idx, node3Idx)
	assert.Less(t, node2Idx, node4Idx)
	assert.Less(t, node3Idx, node4Idx)
}

func TestStateHashComputation(t *testing.T) {
	logger := slog.Default()
	wdc := &WorkflowDataCollector{
		logger: logger,
	}

	state := &domain.CompleteWorkflowState{
		WorkflowID:   "test-workflow",
		Status:       domain.WorkflowStateRunning,
		StartedAt:    time.Now(),
		Version:      1,
		CurrentState: map[string]interface{}{"key": "value"},
		ExecutedNodes: []domain.ExecutedNodeData{
			{
				NodeName:    "node1",
				Status:      "completed",
				ExecutedAt:  time.Now(),
				Duration:    time.Second,
				TriggeredBy: "",
			},
		},
		ExecutionDAG: domain.WorkflowDAG{
			Nodes: []domain.DAGNode{
				{ID: "node1", Type: "start", Status: domain.NodeStatusCompleted},
			},
			Edges: []domain.DAGEdge{},
			Roots: []string{"node1"},
		},
		IdempotencyKeys: []domain.IdempotencyKeyData{
			{
				Key:        "test-key",
				WorkflowID: "test-workflow",
				NodeID:     "node1",
				CreatedAt:  time.Now(),
				ExpiresAt:  time.Now().Add(time.Hour),
			},
		},
	}

	// Compute hash
	hash1 := wdc.computeStateHash(state)
	assert.NotEmpty(t, hash1)

	// Same state should produce same hash
	hash2 := wdc.computeStateHash(state)
	assert.Equal(t, hash1, hash2)

	// Modify state slightly
	state.ExecutedNodes = append(state.ExecutedNodes, domain.ExecutedNodeData{
		NodeName:   "node2",
		Status:     "completed",
		ExecutedAt: time.Now(),
		Duration:   time.Second,
	})

	// Hash should be different
	hash3 := wdc.computeStateHash(state)
	assert.NotEqual(t, hash1, hash3)
}

func TestDependencyCollection(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	// Create mocks
	mockStorage := mocks.NewMockStoragePort(t)
	mockPendingQueue := mocks.NewMockQueuePort(t)
	mockReadyQueue := mocks.NewMockQueuePort(t)

	wdc := &WorkflowDataCollector{
		logger:       logger,
		storage:      mockStorage,
		pendingQueue: mockPendingQueue,
		readyQueue:   mockReadyQueue,
	}

	// Set up mock expectations
	mockStorage.EXPECT().List(ctx, "workflow:dependency:test-workflow:").Return(nil, domain.Error{Type: domain.ErrorTypeNotFound}).Once()
	mockPendingQueue.EXPECT().GetItems(ctx).Return([]ports.QueueItem{}, nil).Once()
	mockReadyQueue.EXPECT().GetItems(ctx).Return([]ports.QueueItem{}, nil).Once()

	// Mock executed nodes response
	executedNodeData := map[string]interface{}{
		"node_name":   "node1",
		"executed_at": "2024-01-01T00:00:00Z",
		"duration":    float64(1000000000),
		"status":      "completed",
		"config":      map[string]interface{}{},
		"results": map[string]interface{}{
			"next_nodes": []interface{}{
				map[string]interface{}{"node_name": "node2", "config": map[string]interface{}{"key": "value"}},
				map[string]interface{}{"node_name": "node3", "config": map[string]interface{}{"key": "value"}},
			},
		},
	}

	nodeDataBytes, _ := json.Marshal(executedNodeData)

	mockStorage.EXPECT().List(ctx, "workflow:execution:test-workflow:").Return([]ports.KeyValue{
		{
			Key:   "workflow:execution:test-workflow:node1",
			Value: nodeDataBytes,
		},
	}, nil).Once()

	deps, err := wdc.collectDependencies(ctx, "test-workflow")
	require.NoError(t, err)
	assert.NotEmpty(t, deps)

	// Should have dependencies from node1 to node2 and node3
	foundNode2Dep := false
	foundNode3Dep := false
	for _, dep := range deps {
		if dep.SourceNode == "node1" && dep.TargetNode == "node2" {
			foundNode2Dep = true
			assert.Equal(t, "trigger", dep.DependencyType)
		}
		if dep.SourceNode == "node1" && dep.TargetNode == "node3" {
			foundNode3Dep = true
			assert.Equal(t, "trigger", dep.DependencyType)
		}
	}
	assert.True(t, foundNode2Dep, "Should have found node1->node2 dependency")
	assert.True(t, foundNode3Dep, "Should have found node1->node3 dependency")
}
