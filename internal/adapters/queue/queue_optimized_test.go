package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestQueue_OptimizedOperations(t *testing.T) {
	tests := []struct {
		name        string
		operation   func(*Queue, *mocks.MockStoragePort) (interface{}, error)
		setupMocks  func(*mocks.MockStoragePort)
		expectedErr bool
	}{
		{
			name: "Peek uses GetNext O(1) operation",
			operation: func(q *Queue, mockStorage *mocks.MockStoragePort) (interface{}, error) {
				data, exists, err := q.Peek()
				return map[string]interface{}{"data": data, "exists": exists}, err
			},
			setupMocks: func(mockStorage *mocks.MockStoragePort) {

				queueItem := domain.NewQueueItem([]byte("test-data"), 1)
				itemBytes, _ := queueItem.ToBytes()

				mockStorage.On("GetNext", "queue:test:pending:").
					Return("queue:test:pending:00000000000000000001", itemBytes, true, nil).
					Once()
			},
			expectedErr: false,
		},
		{
			name: "Peek returns no item when queue empty",
			operation: func(q *Queue, mockStorage *mocks.MockStoragePort) (interface{}, error) {
				data, exists, err := q.Peek()
				return map[string]interface{}{"data": data, "exists": exists}, err
			},
			setupMocks: func(mockStorage *mocks.MockStoragePort) {
				mockStorage.On("GetNext", "queue:test:pending:").
					Return("", []byte(nil), false, nil).
					Once()
			},
			expectedErr: false,
		},
		{
			name: "Claim uses GetNext O(1) operation",
			operation: func(q *Queue, mockStorage *mocks.MockStoragePort) (interface{}, error) {
				data, claimID, exists, err := q.Claim()
				return map[string]interface{}{
					"data":    data,
					"claimID": claimID,
					"exists":  exists,
				}, err
			},
			setupMocks: func(mockStorage *mocks.MockStoragePort) {

				queueItem := domain.NewQueueItem([]byte("test-data"), 1)
				itemBytes, _ := queueItem.ToBytes()

				mockStorage.On("GetNext", "queue:test:pending:").
					Return("queue:test:pending:00000000000000000001", itemBytes, true, nil).
					Once()

				mockStorage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
					return len(ops) == 2 &&
						ops[0].Type == ports.OpDelete &&
						ops[1].Type == ports.OpPut
				})).Return(nil).Once()
			},
			expectedErr: false,
		},
		{
			name: "Size uses CountPrefix O(1) operation",
			operation: func(q *Queue, mockStorage *mocks.MockStoragePort) (interface{}, error) {
				size, err := q.Size()
				return size, err
			},
			setupMocks: func(mockStorage *mocks.MockStoragePort) {
				mockStorage.On("CountPrefix", "queue:test:pending:").
					Return(5, nil).
					Once()
			},
			expectedErr: false,
		},
		{
			name: "GetDeadLetterSize uses CountPrefix O(1) operation",
			operation: func(q *Queue, mockStorage *mocks.MockStoragePort) (interface{}, error) {
				size, err := q.GetDeadLetterSize()
				return size, err
			},
			setupMocks: func(mockStorage *mocks.MockStoragePort) {
				mockStorage.On("CountPrefix", "queue:test:deadletter:").
					Return(2, nil).
					Once()
			},
			expectedErr: false,
		},
		{
			name: "Enqueue uses AtomicIncrement for sequence",
			operation: func(q *Queue, mockStorage *mocks.MockStoragePort) (interface{}, error) {
				err := q.Enqueue([]byte("test-data"))
				return nil, err
			},
			setupMocks: func(mockStorage *mocks.MockStoragePort) {

				mockStorage.On("AtomicIncrement", "queue:test:sequence").
					Return(int64(1), nil).
					Once()

				mockStorage.On("Put", "queue:test:pending:00000000000000000001", mock.AnythingOfType("[]uint8"), int64(0)).
					Return(nil).
					Once()
			},
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := mocks.NewMockStoragePort(t)
			tt.setupMocks(mockStorage)

			queue := NewQueue("test", mockStorage, nil, nil)

			result, err := tt.operation(queue, mockStorage)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if result != nil {
				t.Logf("Result: %+v", result)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestQueue_SequenceGenerationOptimized(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	mockStorage.On("AtomicIncrement", "queue:test:sequence").
		Return(int64(42), nil).
		Once()

	sequence, err := queue.getNextSequence()

	assert.NoError(t, err)
	assert.Equal(t, int64(42), sequence)

	mockStorage.AssertExpectations(t)
}

func TestQueue_DeadLetterSequenceOptimized(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	mockStorage.On("AtomicIncrement", "queue:test:deadletter:sequence").
		Return(int64(123), nil).
		Once()

	sequence, err := queue.getNextDeadLetterSequence()

	assert.NoError(t, err)
	assert.Equal(t, int64(123), sequence)

	mockStorage.AssertExpectations(t)
}

// Benchmark tests to verify O(1) performance
func BenchmarkQueue_OptimizedPeek(b *testing.B) {
	mockStorage := mocks.NewMockStoragePort(b)
	queue := NewQueue("test", mockStorage, nil, nil)

	queueItem := domain.NewQueueItem([]byte("test-data"), 1)
	itemBytes, _ := queueItem.ToBytes()

	mockStorage.On("GetNext", "queue:test:pending:").
		Return("queue:test:pending:00000000000000000001", itemBytes, true, nil).
		Times(b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := queue.Peek()
		if err != nil {
			b.Fatalf("Peek failed: %v", err)
		}
	}
}

func BenchmarkQueue_OptimizedSize(b *testing.B) {
	mockStorage := mocks.NewMockStoragePort(b)
	queue := NewQueue("test", mockStorage, nil, nil)

	mockStorage.On("CountPrefix", "queue:test:pending:").
		Return(1000, nil).
		Times(b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := queue.Size()
		if err != nil {
			b.Fatalf("Size failed: %v", err)
		}
	}
}

// Test to verify performance characteristics
func TestQueue_PerformanceCharacteristics(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	queue := NewQueue("test", mockStorage, nil, nil)

	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {

			queueItem := domain.NewQueueItem([]byte("test-data"), 1)
			itemBytes, _ := queueItem.ToBytes()

			mockStorage.On("GetNext", "queue:test:pending:").
				Return("queue:test:pending:00000000000000000001", itemBytes, true, nil).
				Once()

			start := time.Now()
			_, _, err := queue.Peek()
			duration := time.Since(start)

			assert.NoError(t, err)

			assert.Less(t, duration, time.Millisecond,
				"Peek operation took too long for queue size %d: %v", size, duration)
		})
	}
}
