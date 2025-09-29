package queue

import (
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestQueue_EnqueueBlocked(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(1), nil).Once()
	mockStorage.On("Put", "queue:test:blocked:00000000000000000001", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()

	err := queue.EnqueueBlocked([]byte("blocked item"))
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
}

func TestQueue_EnqueueReady(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(1), nil).Once()
	mockStorage.On("Put", "queue:test:ready:00000000000000000001", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()

	err := queue.EnqueueReady([]byte("ready item"))
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
}

func TestQueue_GetBlockedForWorkflow(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	workflowData := []byte(`{"workflow_id":"wf-123","data":"test"}`)

	blockedItem := &domain.BlockedItem{
		Data:        workflowData,
		BlockedAt:   time.Now(),
		LastChecked: time.Now(),
		CheckCount:  1,
		Sequence:    1,
	}
	blockedBytes, _ := blockedItem.ToBytes()

	mockStorage.On("ListByPrefix", "queue:test:blocked:").Return([]ports.KeyValueVersion{
		{Key: "queue:test:blocked:00000000000000000001", Value: blockedBytes},
	}, nil).Once()

	items, err := queue.GetBlockedForWorkflow("wf-123")
	assert.NoError(t, err)
	assert.Len(t, items, 1)
	assert.Equal(t, workflowData, items[0].Data)

	mockStorage.AssertExpectations(t)
}

func TestQueue_GetReadyForWorkflow(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	workflowData := []byte(`{"workflow_id":"wf-123","data":"test"}`)

	queueItem := domain.NewQueueItem(workflowData, 1)
	itemBytes, _ := queueItem.ToBytes()

	mockStorage.On("ListByPrefix", "queue:test:ready:").Return([]ports.KeyValueVersion{
		{Key: "queue:test:ready:00000000000000000001", Value: itemBytes},
	}, nil).Once()

	items, err := queue.GetReadyForWorkflow("wf-123")
	assert.NoError(t, err)
	assert.Len(t, items, 1)
	assert.Equal(t, workflowData, items[0])

	mockStorage.AssertExpectations(t)
}

func TestQueue_PromoteBlockedToReady(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	blockedItem := &domain.BlockedItem{
		Data:        []byte(`{"workflow_id":"wf-123","data":"test"}`),
		BlockedAt:   time.Now(),
		LastChecked: time.Now(),
		CheckCount:  1,
		Sequence:    1,
	}

	mockStorage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
		return len(ops) == 2 &&
			ops[0].Type == ports.OpDelete &&
			ops[0].Key == "queue:test:blocked:00000000000000000001" &&
			ops[1].Type == ports.OpPut &&
			ops[1].Key == "queue:test:ready:00000000000000000001"
	})).Return(nil).Once()

	err := queue.PromoteBlockedToReady(blockedItem)
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
}

func TestQueue_GetItemsWithPrefix_ChecksBothQueues(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	workflowData1 := []byte(`{"workflow_id":"wf-123","data":"ready"}`)
	workflowData2 := []byte(`{"workflow_id":"wf-123","data":"blocked"}`)
	queue.updateWorkflowIndex(workflowData1, 1, true)
	queue.updateWorkflowIndex(workflowData2, 2, true)

	queueItem1 := domain.NewQueueItem(workflowData1, 1)
	readyBytes, _ := queueItem1.ToBytes()

	blockedItem := &domain.BlockedItem{
		Data:        workflowData2,
		BlockedAt:   time.Now(),
		LastChecked: time.Now(),
		CheckCount:  1,
		Sequence:    2,
	}
	blockedBytes, _ := blockedItem.ToBytes()

	mockStorage.On("Get", "queue:test:ready:00000000000000000001").
		Return(readyBytes, int64(0), true, nil).Once()

	mockStorage.On("Get", "queue:test:ready:00000000000000000002").
		Return([]byte{}, int64(0), false, nil).Once()
	mockStorage.On("Get", "queue:test:blocked:00000000000000000002").
		Return(blockedBytes, int64(0), true, nil).Once()

	items, err := queue.GetItemsWithPrefix(`"workflow_id":"wf-123"`)
	assert.NoError(t, err)
	assert.Len(t, items, 2)

	contains := func(data []byte, items [][]byte) bool {
		for _, item := range items {
			if string(item) == string(data) {
				return true
			}
		}
		return false
	}

	assert.True(t, contains(workflowData1, items), "Should contain ready item")
	assert.True(t, contains(workflowData2, items), "Should contain blocked item")

	mockStorage.AssertExpectations(t)
}

func TestQueue_HasItemsWithPrefix_ChecksBothQueues(t *testing.T) {
	tests := []struct {
		name           string
		setupWorkflow  func(*Queue)
		setupMocks     func(*mocks.MockStoragePort)
		prefix         string
		expectedExists bool
	}{
		{
			name: "has items in ready queue only",
			setupWorkflow: func(q *Queue) {
				workflowData := []byte(`{"workflow_id":"wf-ready","data":"test"}`)
				q.updateWorkflowIndex(workflowData, 1, true)
			},
			setupMocks: func(mockStorage *mocks.MockStoragePort) {

			},
			prefix:         `"workflow_id":"wf-ready"`,
			expectedExists: true,
		},
		{
			name: "has items in blocked queue only",
			setupWorkflow: func(q *Queue) {
				workflowData := []byte(`{"workflow_id":"wf-blocked","data":"test"}`)
				q.updateWorkflowIndex(workflowData, 1, true)
			},
			setupMocks: func(mockStorage *mocks.MockStoragePort) {

			},
			prefix:         `"workflow_id":"wf-blocked"`,
			expectedExists: true,
		},
		{
			name: "no items for workflow",
			setupWorkflow: func(q *Queue) {

			},
			setupMocks: func(mockStorage *mocks.MockStoragePort) {

			},
			prefix:         `"workflow_id":"wf-none"`,
			expectedExists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := mocks.NewMockStoragePort(t)
			queue := NewQueue("test", mockStorage, nil, nil)

			tt.setupWorkflow(queue)
			tt.setupMocks(mockStorage)

			exists, err := queue.HasItemsWithPrefix(tt.prefix)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedExists, exists)

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestQueue_Size_IncludesBothQueues(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	mockStorage.On("CountPrefix", "queue:test:ready:").Return(5, nil).Once()
	mockStorage.On("CountPrefix", "queue:test:blocked:").Return(3, nil).Once()

	size, err := queue.Size()
	assert.NoError(t, err)
	assert.Equal(t, 8, size)

	mockStorage.AssertExpectations(t)
}

func TestQueue_WorkflowIndexWithBlockedItems(t *testing.T) {
	queue := NewQueue("test", nil, nil, nil)

	workflowData1 := []byte(`{"workflow_id":"wf-123","data":"item1"}`)
	workflowData2 := []byte(`{"workflow_id":"wf-123","data":"item2"}`)
	workflowData3 := []byte(`{"workflow_id":"wf-456","data":"item3"}`)

	queue.updateWorkflowIndex(workflowData1, 1, true)
	queue.updateWorkflowIndex(workflowData2, 2, true)
	queue.updateWorkflowIndex(workflowData3, 3, true)

	assert.True(t, queue.hasWorkflowItems("wf-123"))
	assert.True(t, queue.hasWorkflowItems("wf-456"))
	assert.False(t, queue.hasWorkflowItems("wf-nonexistent"))

	seqs123 := queue.getWorkflowSequences("wf-123")
	assert.ElementsMatch(t, []int64{1, 2}, seqs123)

	seqs456 := queue.getWorkflowSequences("wf-456")
	assert.ElementsMatch(t, []int64{3}, seqs456)

	queue.updateWorkflowIndex(workflowData1, 1, false)
	seqs123Updated := queue.getWorkflowSequences("wf-123")
	assert.ElementsMatch(t, []int64{2}, seqs123Updated)

	queue.updateWorkflowIndex(workflowData2, 2, false)
	assert.False(t, queue.hasWorkflowItems("wf-123"))
	assert.Nil(t, queue.getWorkflowSequences("wf-123"))
}

func TestQueue_BlockedItemPromotion_Integration(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	workflowData := []byte(`{"workflow_id":"wf-promotion","data":"test"}`)

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(1), nil).Once()
	mockStorage.On("Put", "queue:test:blocked:00000000000000000001", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()

	err := queue.EnqueueBlocked(workflowData)
	assert.NoError(t, err)

	queue.updateWorkflowIndex(workflowData, 1, true)

	blockedItem := &domain.BlockedItem{
		Data:        workflowData,
		BlockedAt:   time.Now(),
		LastChecked: time.Now(),
		CheckCount:  1,
		Sequence:    1,
	}

	mockStorage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
		return len(ops) == 2 &&
			ops[0].Type == ports.OpDelete &&
			ops[0].Key == "queue:test:blocked:00000000000000000001" &&
			ops[1].Type == ports.OpPut &&
			ops[1].Key == "queue:test:ready:00000000000000000001"
	})).Return(nil).Once()

	err = queue.PromoteBlockedToReady(blockedItem)
	assert.NoError(t, err)

	assert.True(t, queue.hasWorkflowItems("wf-promotion"))

	mockStorage.AssertExpectations(t)
}

func TestQueue_EnqueueDefaultsToReady(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(1), nil).Once()
	mockStorage.On("Put", "queue:test:ready:00000000000000000001", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()

	err := queue.Enqueue([]byte("default enqueue"))
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
}

func TestQueue_SequenceGenerationConsistency(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(1), nil).Once()
	mockStorage.On("Put", "queue:test:ready:00000000000000000001", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(2), nil).Once()
	mockStorage.On("Put", "queue:test:blocked:00000000000000000002", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()

	err := queue.EnqueueReady([]byte("ready item"))
	assert.NoError(t, err)

	err = queue.EnqueueBlocked([]byte("blocked item"))
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
}
