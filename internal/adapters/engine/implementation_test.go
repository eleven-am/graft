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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

type TestNode struct {
	name     string
	canStart bool
}

func (n *TestNode) Execute(ctx context.Context, args ...interface{}) (*ports.NodeResult, error) {
	return &ports.NodeResult{}, nil
}

func (n *TestNode) CanStart(ctx context.Context, args ...interface{}) bool {
	return n.canStart
}

func (n *TestNode) GetName() string {
	return n.name
}

func TestRouteInitialNode(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	mockStorage := mocks.NewMockStoragePort(t)
	mockPendingQueue := mocks.NewMockQueuePort(t)
	mockReadyQueue := mocks.NewMockQueuePort(t)
	mockNodeRegistry := mocks.NewMockNodeRegistryPort(t)

	engine := &Engine{
		storage:      mockStorage,
		pendingQueue: mockPendingQueue,
		readyQueue:   mockReadyQueue,
		nodeRegistry: mockNodeRegistry,
		logger:       logger,
	}
	engine.stateManager = NewStateManager(engine)

	testCases := []struct {
		name                string
		nodeCanStart        bool
		expectedQueue       string
		expectedEnqueueCall bool
	}{
		{
			name:                "node_can_start_goes_to_ready",
			nodeCanStart:        true,
			expectedQueue:       "ready",
			expectedEnqueueCall: true,
		},
		{
			name:                "node_cannot_start_goes_to_pending",
			nodeCanStart:        false,
			expectedQueue:       "pending",
			expectedEnqueueCall: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			workflowID := "test-workflow"
			nodeConfig := ports.NodeConfig{
				Name:   "test-node",
				Config: map[string]interface{}{"test": "value"},
			}

			testNode := &TestNode{
				name:     "test-node",
				canStart: tc.nodeCanStart,
			}

			workflowData := map[string]interface{}{
				"id":             workflowID,
				"status":         "running",
				"version":        1,
				"started_at":     "2024-01-01T00:00:00Z",
				"current_state":  map[string]interface{}{"initial": "state"},
				"executed_nodes": []interface{}{},
				"metadata":       map[string]interface{}{},
			}
			workflowJSON, _ := json.Marshal(workflowData)

			mockStorage.On("Get", ctx, "workflow:state:"+workflowID).Return(workflowJSON, nil).Once()
			mockNodeRegistry.On("GetNode", "test-node").Return(testNode, nil).Once()

			if tc.expectedQueue == "ready" {
				mockReadyQueue.On("Enqueue", ctx, mock.MatchedBy(func(item ports.QueueItem) bool {
					return item.WorkflowID == workflowID && item.NodeName == "test-node"
				})).Return(nil).Once()
			} else {
				mockPendingQueue.On("Enqueue", ctx, mock.MatchedBy(func(item ports.QueueItem) bool {
					return item.WorkflowID == workflowID && item.NodeName == "test-node"
				})).Return(nil).Once()
			}

			err := engine.routeInitialNode(workflowID, nodeConfig)

			assert.NoError(t, err)
			mockStorage.AssertExpectations(t)
			mockNodeRegistry.AssertExpectations(t)
			mockReadyQueue.AssertExpectations(t)
			mockPendingQueue.AssertExpectations(t)
		})
	}
}
