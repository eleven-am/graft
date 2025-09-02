package queue

import (
	"fmt"
	"testing"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
)

func TestQueue_WorkflowIndex(t *testing.T) {
	tests := []struct {
		name           string
		workflowItems  map[string][]int64 // workflowID -> sequences
		testWorkflowID string
		expectedExists bool
		expectedSeqs   []int64
	}{
		{
			name: "single workflow with multiple sequences",
			workflowItems: map[string][]int64{
				"workflow-123": {1, 2, 3},
			},
			testWorkflowID: "workflow-123",
			expectedExists: true,
			expectedSeqs:   []int64{1, 2, 3},
		},
		{
			name: "multiple workflows",
			workflowItems: map[string][]int64{
				"workflow-123": {1, 2},
				"workflow-456": {3, 4, 5},
			},
			testWorkflowID: "workflow-456",
			expectedExists: true,
			expectedSeqs:   []int64{3, 4, 5},
		},
		{
			name: "workflow not found",
			workflowItems: map[string][]int64{
				"workflow-123": {1, 2},
			},
			testWorkflowID: "workflow-999",
			expectedExists: false,
			expectedSeqs:   nil,
		},
		{
			name:           "empty index",
			workflowItems:  map[string][]int64{},
			testWorkflowID: "workflow-123",
			expectedExists: false,
			expectedSeqs:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue := NewQueue("test", nil, nil, nil)

			// Populate workflow index
			for workflowID, sequences := range tt.workflowItems {
				for _, seq := range sequences {
					itemData := []byte(`{"workflow_id":"` + workflowID + `","data":"test"}`)
					queue.updateWorkflowIndex(itemData, seq, true)
				}
			}

			// Test hasWorkflowItems
			exists := queue.hasWorkflowItems(tt.testWorkflowID)
			assert.Equal(t, tt.expectedExists, exists, "hasWorkflowItems result mismatch")

			// Test getWorkflowSequences
			sequences := queue.getWorkflowSequences(tt.testWorkflowID)
			if tt.expectedSeqs == nil {
				assert.Nil(t, sequences, "expected nil sequences")
			} else {
				assert.ElementsMatch(t, tt.expectedSeqs, sequences, "sequences mismatch")
			}
		})
	}
}

func TestQueue_WorkflowIndexRemoval(t *testing.T) {
	queue := NewQueue("test", nil, nil, nil)

	// Add items to index
	itemData := []byte(`{"workflow_id":"workflow-123","data":"test"}`)
	queue.updateWorkflowIndex(itemData, 1, true)
	queue.updateWorkflowIndex(itemData, 2, true)
	queue.updateWorkflowIndex(itemData, 3, true)

	// Verify workflow exists
	assert.True(t, queue.hasWorkflowItems("workflow-123"))
	sequences := queue.getWorkflowSequences("workflow-123")
	assert.ElementsMatch(t, []int64{1, 2, 3}, sequences)

	// Remove one item
	queue.updateWorkflowIndex(itemData, 2, false)

	// Verify workflow still exists with remaining items
	assert.True(t, queue.hasWorkflowItems("workflow-123"))
	sequences = queue.getWorkflowSequences("workflow-123")
	assert.ElementsMatch(t, []int64{1, 3}, sequences)

	// Remove remaining items
	queue.updateWorkflowIndex(itemData, 1, false)
	queue.updateWorkflowIndex(itemData, 3, false)

	// Verify workflow is cleaned up
	assert.False(t, queue.hasWorkflowItems("workflow-123"))
	sequences = queue.getWorkflowSequences("workflow-123")
	assert.Nil(t, sequences)
}

func TestQueue_ExtractWorkflowID(t *testing.T) {
	tests := []struct {
		name       string
		itemData   []byte
		expectedID string
	}{
		{
			name:       "valid workflow_id",
			itemData:   []byte(`{"workflow_id":"abc123","other":"data"}`),
			expectedID: "abc123",
		},
		{
			name:       "workflow_id at end",
			itemData:   []byte(`{"other":"data","workflow_id":"xyz789"}`),
			expectedID: "xyz789",
		},
		{
			name:       "no workflow_id field",
			itemData:   []byte(`{"other":"data","task_id":"123"}`),
			expectedID: "",
		},
		{
			name:       "empty workflow_id",
			itemData:   []byte(`{"workflow_id":"","other":"data"}`),
			expectedID: "",
		},
		{
			name:       "malformed JSON",
			itemData:   []byte(`{"workflow_id":"abc`),
			expectedID: "",
		},
	}

	queue := NewQueue("test", nil, nil, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workflowID := queue.extractWorkflowID(tt.itemData)
			assert.Equal(t, tt.expectedID, workflowID)
		})
	}
}

func TestQueue_ExtractWorkflowIDFromPrefix(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		expectedID string
	}{
		{
			name:       "valid prefix",
			prefix:     `"workflow_id":"abc123"`,
			expectedID: "abc123",
		},
		{
			name:       "prefix without end quote",
			prefix:     `"workflow_id":"abc123`,
			expectedID: "abc123",
		},
		{
			name:       "invalid prefix format",
			prefix:     `workflow_id:abc123`,
			expectedID: "",
		},
		{
			name:       "empty value",
			prefix:     `"workflow_id":""`,
			expectedID: "",
		},
	}

	queue := NewQueue("test", nil, nil, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workflowID := queue.extractWorkflowIDFromPrefix(tt.prefix)
			assert.Equal(t, tt.expectedID, workflowID)
		})
	}
}

func TestQueue_HasItemsWithPrefixOptimized(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	// Add workflow to index
	itemData := []byte(`{"workflow_id":"abc123","data":"test"}`)
	queue.updateWorkflowIndex(itemData, 1, true)

	// Test O(1) workflow lookup - no storage calls expected
	exists, err := queue.HasItemsWithPrefix(`"workflow_id":"abc123"`)

	assert.NoError(t, err)
	assert.True(t, exists)

	// Test non-existent workflow - falls back to persistent index in storage
	mockStorage.On("ListByPrefix", "queue:test:wf:nonexistent:").Return([]ports.KeyValueVersion{}, nil).Once()
	exists, err = queue.HasItemsWithPrefix(`"workflow_id":"nonexistent"`)

	assert.NoError(t, err)
	assert.False(t, exists)

	// For non-workflow prefixes, should fallback to storage (not tested here)
	// This would require mocking ListByPrefix calls

	mockStorage.AssertExpectations(t)
}

func TestQueue_GetItemsWithPrefixOptimized(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	// Add multiple workflow items to index
	workflowData1 := []byte(`{"workflow_id":"workflow-123","data":"test1"}`)
	workflowData2 := []byte(`{"workflow_id":"workflow-123","data":"test2"}`)
	queue.updateWorkflowIndex(workflowData1, 1, true)
	queue.updateWorkflowIndex(workflowData2, 2, true)

	// Mock storage Get calls for retrieving items by sequence
	queueItem1 := domain.NewQueueItem(workflowData1, 1)
	itemBytes1, _ := queueItem1.ToBytes()
	queueItem2 := domain.NewQueueItem(workflowData2, 2)
	itemBytes2, _ := queueItem2.ToBytes()

	mockStorage.On("Get", "queue:test:pending:00000000000000000001").
		Return(itemBytes1, int64(0), true, nil).Once()
	mockStorage.On("Get", "queue:test:pending:00000000000000000002").
		Return(itemBytes2, int64(0), true, nil).Once()

	// Test O(1) workflow lookup - should use index and fetch specific items
	items, err := queue.GetItemsWithPrefix(`"workflow_id":"workflow-123"`)

	assert.NoError(t, err)
	assert.Len(t, items, 2)
	assert.Contains(t, string(items[0]), "workflow-123")
	assert.Contains(t, string(items[1]), "workflow-123")

	mockStorage.AssertExpectations(t)
}

func TestQueue_GetItemsWithPrefixFallback(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	// Test non-workflow prefix - should fallback to storage scan
	queueItem := domain.NewQueueItem([]byte(`{"task_id":"task-123","data":"test"}`), 1)
	itemBytes, _ := queueItem.ToBytes()

	mockStorage.On("ListByPrefix", "queue:test:pending:").Return([]ports.KeyValueVersion{
		{Key: "queue:test:pending:00000000000000000001", Value: itemBytes},
	}, nil).Once()

	items, err := queue.GetItemsWithPrefix(`"task_id":"task-123"`)

	assert.NoError(t, err)
	assert.Len(t, items, 1)
	assert.Contains(t, string(items[0]), "task-123")

	mockStorage.AssertExpectations(t)
}

func TestQueue_WorkflowIndexConcurrency(t *testing.T) {
	queue := NewQueue("test", nil, nil, nil)

	// Test concurrent access to workflow index
	done := make(chan bool, 2)

	// Goroutine 1: Add items
	go func() {
		defer func() { done <- true }()
		for i := 0; i < 100; i++ {
			itemData := []byte(`{"workflow_id":"workflow-1","data":"test"}`)
			queue.updateWorkflowIndex(itemData, int64(i), true)
		}
	}()

	// Goroutine 2: Check items
	go func() {
		defer func() { done <- true }()
		for i := 0; i < 100; i++ {
			queue.hasWorkflowItems("workflow-1")
			queue.getWorkflowSequences("workflow-1")
		}
	}()

	// Wait for both goroutines
	<-done
	<-done

	// Verify final state
	assert.True(t, queue.hasWorkflowItems("workflow-1"))
	sequences := queue.getWorkflowSequences("workflow-1")
	assert.Equal(t, 100, len(sequences))
}

// Benchmark to verify O(1) performance
func BenchmarkQueue_WorkflowIndex(b *testing.B) {
	queue := NewQueue("test", nil, nil, nil)

	// Add many workflows to index
	for i := 0; i < 10000; i++ {
		workflowID := fmt.Sprintf("workflow-%d", i)
		itemData := []byte(`{"workflow_id":"` + workflowID + `","data":"test"}`)
		queue.updateWorkflowIndex(itemData, int64(i), true)
	}

	b.ResetTimer()

	// Benchmark workflow lookup - should be O(1) regardless of index size
	for i := 0; i < b.N; i++ {
		queue.hasWorkflowItems("workflow-5000")
	}
}
