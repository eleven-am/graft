package engine

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStateHashCollisions - Try to create hash collisions
func TestStateHashCollisions(t *testing.T) {
	logger := slog.Default()
	wdc := &WorkflowDataCollector{logger: logger}

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create two states that should have different hashes
	state1 := &domain.CompleteWorkflowState{
		WorkflowID: "workflow1",
		Status:     domain.WorkflowStateRunning,
		StartedAt:  baseTime,
		Version:    1,
		ExecutedNodes: []domain.ExecutedNodeData{
			{NodeName: "node1", Status: "completed", ExecutedAt: baseTime, Duration: time.Second},
			{NodeName: "node2", Status: "completed", ExecutedAt: baseTime.Add(time.Second), Duration: time.Second},
		},
	}

	state2 := &domain.CompleteWorkflowState{
		WorkflowID: "workflow1", // Same ID
		Status:     domain.WorkflowStateRunning,
		StartedAt:  baseTime,
		Version:    1,
		ExecutedNodes: []domain.ExecutedNodeData{
			{NodeName: "node2", Status: "completed", ExecutedAt: baseTime, Duration: time.Second}, // Swapped order
			{NodeName: "node1", Status: "completed", ExecutedAt: baseTime.Add(time.Second), Duration: time.Second},
		},
	}

	hash1 := wdc.computeStateHash(state1)
	hash2 := wdc.computeStateHash(state2)

	// Should be different due to execution timing differences
	assert.NotEqual(t, hash1, hash2, "Different execution order should produce different hashes")

	// Try with identical data
	state3 := &domain.CompleteWorkflowState{
		WorkflowID: "workflow1",
		Status:     domain.WorkflowStateRunning,
		StartedAt:  baseTime,
		Version:    1,
		ExecutedNodes: []domain.ExecutedNodeData{
			{NodeName: "node1", Status: "completed", ExecutedAt: baseTime, Duration: time.Second},
			{NodeName: "node2", Status: "completed", ExecutedAt: baseTime.Add(time.Second), Duration: time.Second},
		},
	}

	hash3 := wdc.computeStateHash(state3)
	assert.Equal(t, hash1, hash3, "Identical states should produce identical hashes")
}

// TestDependencyCollectionWithCorruptedData - Malformed JSON and corrupt data
func TestDependencyCollectionWithCorruptedData(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	mockStorage := mocks.NewMockStoragePort(t)
	mockPendingQueue := mocks.NewMockQueuePort(t)
	mockReadyQueue := mocks.NewMockQueuePort(t)

	wdc := &WorkflowDataCollector{
		logger:       logger,
		storage:      mockStorage,
		pendingQueue: mockPendingQueue,
		readyQueue:   mockReadyQueue,
	}

	// Mock corrupted data
	mockStorage.EXPECT().List(ctx, "workflow:dependency:corrupt-workflow:").Return(nil, domain.Error{Type: domain.ErrorTypeNotFound}).Once()
	mockPendingQueue.EXPECT().GetItems(ctx).Return([]ports.QueueItem{}, nil).Once()
	mockReadyQueue.EXPECT().GetItems(ctx).Return([]ports.QueueItem{}, nil).Once()

	mockStorage.EXPECT().List(ctx, "workflow:execution:corrupt-workflow:").Return([]ports.KeyValue{
		{Key: "node1", Value: []byte(`{"node_name": "node1", "malformed": json`)},                                      // Invalid JSON
		{Key: "node2", Value: []byte(`{"executed_at": "not-a-date", "duration": "not-a-number"}`)},                     // Invalid data types
		{Key: "node3", Value: []byte(`{"node_name": "node3", "results": {"next_nodes": [{"invalid": "structure"}]}}`)}, // Missing node_name in next_nodes
		{Key: "node4", Value: []byte(`null`)},                                                                          // Null data
		{Key: "node5", Value: []byte(``)},                                                                              // Empty data
	}, nil).Once()

	// Should not panic and return empty dependencies
	deps, err := wdc.collectDependencies(ctx, "corrupt-workflow")
	assert.NoError(t, err, "Should handle corrupted data gracefully")
	assert.Empty(t, deps, "Should return empty dependencies for corrupted data")
}

// TestResourceStateCollectionFailure - Resource manager failures
func TestResourceStateCollectionFailure(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	mockResourceManager := mocks.NewMockResourceManagerPort(t)

	wdc := &WorkflowDataCollector{
		logger:          logger,
		resourceManager: mockResourceManager,
	}

	// Test various failure scenarios
	testCases := []struct {
		name        string
		setupMock   func()
		expectError bool
	}{
		{
			name: "timeout_error",
			setupMock: func() {
				mockResourceManager.EXPECT().GetResourceStates(ctx, "timeout_error-workflow").
					Return(nil, fmt.Errorf("timeout: context deadline exceeded")).Once()
			},
			expectError: false, // Should not error but log warning
		},
		{
			name: "permission_denied",
			setupMock: func() {
				mockResourceManager.EXPECT().GetResourceStates(ctx, "permission_denied-workflow").
					Return(nil, fmt.Errorf("permission denied")).Once()
			},
			expectError: false,
		},
		{
			name: "nil_resource_manager",
			setupMock: func() {
				// No mock setup - test nil resource manager
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "nil_resource_manager" {
				wdc.resourceManager = nil
			}

			tc.setupMock()

			result, err := wdc.collectResourceStates(ctx, tc.name+"-workflow")

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}

// TestValidationEdgeCases - Push validation to its limits
func TestValidationEdgeCases(t *testing.T) {
	logger := slog.Default()
	validator := NewStateValidator(logger)
	ctx := context.Background()

	testCases := []struct {
		name           string
		state          *domain.CompleteWorkflowState
		level          domain.ValidationLevel
		expectValid    bool
		expectedErrors int
	}{
		{
			name: "empty_workflow_id",
			state: &domain.CompleteWorkflowState{
				WorkflowID: "",
				Status:     domain.WorkflowStateRunning,
				StartedAt:  time.Now(),
				Version:    1,
				ExecutionDAG: domain.WorkflowDAG{
					Nodes: []domain.DAGNode{{ID: "node1", Type: "test", Status: domain.NodeStatusCompleted}},
				},
			},
			level:          domain.ValidationStrict,
			expectValid:    false,
			expectedErrors: 1,
		},
		{
			name: "negative_version",
			state: &domain.CompleteWorkflowState{
				WorkflowID: "test",
				Status:     domain.WorkflowStateRunning,
				StartedAt:  time.Now(),
				Version:    -1, // Invalid
				ExecutionDAG: domain.WorkflowDAG{
					Nodes: []domain.DAGNode{{ID: "node1", Type: "test", Status: domain.NodeStatusCompleted}},
				},
			},
			level:          domain.ValidationStrict,
			expectValid:    false,
			expectedErrors: 1,
		},
		{
			name: "node_executed_before_workflow_start",
			state: &domain.CompleteWorkflowState{
				WorkflowID: "test",
				Status:     domain.WorkflowStateRunning,
				StartedAt:  time.Now(),
				Version:    1,
				ExecutedNodes: []domain.ExecutedNodeData{
					{
						NodeName:   "time-traveler",
						Status:     "completed",
						ExecutedAt: time.Now().Add(-1 * time.Hour), // Before workflow start
						Duration:   time.Second,
					},
				},
				ExecutionDAG: domain.WorkflowDAG{
					Nodes: []domain.DAGNode{{ID: "time-traveler", Type: "test", Status: domain.NodeStatusCompleted}},
				},
			},
			level:          domain.ValidationStrict,
			expectValid:    false,
			expectedErrors: 1,
		},
		{
			name: "duplicate_nodes_in_dag",
			state: &domain.CompleteWorkflowState{
				WorkflowID: "test",
				Status:     domain.WorkflowStateRunning,
				StartedAt:  time.Now(),
				Version:    1,
				ExecutionDAG: domain.WorkflowDAG{
					Nodes: []domain.DAGNode{
						{ID: "duplicate", Type: "test", Status: domain.NodeStatusCompleted},
						{ID: "duplicate", Type: "test", Status: domain.NodeStatusPending}, // Duplicate ID
					},
				},
			},
			level:          domain.ValidationStrict,
			expectValid:    false,
			expectedErrors: 1,
		},
		{
			name: "execution_order_violation",
			state: &domain.CompleteWorkflowState{
				WorkflowID: "test",
				Status:     domain.WorkflowStateRunning,
				StartedAt:  time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC), // Start before all executions
				Version:    1,
				ExecutedNodes: []domain.ExecutedNodeData{
					{
						NodeName:   "dependent",
						Status:     "completed",
						ExecutedAt: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
						Duration:   time.Second,
					},
					{
						NodeName:   "dependency",
						Status:     "completed",
						ExecutedAt: time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC), // Executed AFTER dependent
						Duration:   time.Second,
					},
				},
				ExecutionDAG: domain.WorkflowDAG{
					Nodes: []domain.DAGNode{
						{ID: "dependent", Type: "test", Status: domain.NodeStatusCompleted, Dependencies: []string{"dependency"}},
						{ID: "dependency", Type: "test", Status: domain.NodeStatusCompleted},
					},
				},
			},
			level:          domain.ValidationStrict,
			expectValid:    false,
			expectedErrors: 1, // Only execution order violation
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validator.ValidateState(ctx, tc.state, tc.level)
			require.NoError(t, err, "Validation should not return error")

			assert.Equal(t, tc.expectValid, result.Valid, "Validation result mismatch")
			assert.Equal(t, tc.expectedErrors, len(result.Errors), "Expected error count mismatch: %v", result.Errors)

			if !tc.expectValid {
				assert.NotEmpty(t, result.Errors, "Should have validation errors")
				t.Logf("Validation errors: %v", result.Errors)
			}
		})
	}
}

// TestStateHashWithExtremeData - Test hash with unusual data
func TestStateHashWithExtremeData(t *testing.T) {
	logger := slog.Default()
	wdc := &WorkflowDataCollector{logger: logger}

	// Test with extreme Unicode, empty strings, very long strings
	extremeData := &domain.CompleteWorkflowState{
		WorkflowID: "🚀🔥💻🌟", // Unicode emoji
		Status:     domain.WorkflowStateRunning,
		StartedAt:  time.Unix(0, 0), // Epoch time
		Version:    999999999,       // Large version
		CurrentState: map[string]interface{}{
			"":                         "empty_key",
			strings.Repeat("x", 10000): "very_long_key", // 10KB key
			"nested": map[string]interface{}{
				"deeply": map[string]interface{}{
					"nested": map[string]interface{}{
						"data": "value",
					},
				},
			},
			"unicode": "こんにちは🌍",
			"null":    nil,
		},
		ExecutedNodes: []domain.ExecutedNodeData{
			{
				NodeName:    "",                        // Empty node name
				Status:      strings.Repeat("x", 1000), // Very long status
				ExecutedAt:  time.Unix(2147483647, 0),  // Max Unix timestamp (32-bit)
				Duration:    time.Duration(1<<63 - 1),  // Max duration
				TriggeredBy: strings.Repeat("🔗", 100),  // Unicode chains
			},
		},
	}

	// Should not panic or error
	hash := wdc.computeStateHash(extremeData)
	assert.NotEmpty(t, hash)
	assert.Equal(t, 64, len(hash), "SHA256 hash should be 64 hex characters")

	// Hash should be deterministic
	hash2 := wdc.computeStateHash(extremeData)
	assert.Equal(t, hash, hash2, "Hash should be deterministic")
}
