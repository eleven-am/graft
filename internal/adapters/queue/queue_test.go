package queue

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewBadgerQueue(t *testing.T) {
	tests := []struct {
		name      string
		setupMock func() ports.StoragePort
		config    Config
		wantError bool
		errorType domain.ErrorType
	}{
		{
			name: "valid ready queue",
			setupMock: func() ports.StoragePort {
				return mocks.NewMockStoragePort(t)
			},
			config: Config{
				QueueType: QueueTypeReady,
			},
			wantError: false,
		},
		{
			name: "valid pending queue",
			setupMock: func() ports.StoragePort {
				return mocks.NewMockStoragePort(t)
			},
			config: Config{
				QueueType: QueueTypePending,
			},
			wantError: false,
		},
		{
			name: "nil storage",
			setupMock: func() ports.StoragePort {
				return nil
			},
			config: Config{
				QueueType: QueueTypeReady,
			},
			wantError: true,
			errorType: domain.ErrorTypeValidation,
		},
		{
			name: "invalid queue type",
			setupMock: func() ports.StoragePort {
				return mocks.NewMockStoragePort(t)
			},
			config: Config{
				QueueType: "invalid",
			},
			wantError: true,
			errorType: domain.ErrorTypeValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupMock()
			queue, err := NewBadgerQueue(storage, tt.config, nil)

			if tt.wantError {
				assert.Error(t, err)
				if domainErr, ok := err.(domain.Error); ok {
					assert.Equal(t, tt.errorType, domainErr.Type)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, queue)
			}
		})
	}
}

func TestIsEmpty(t *testing.T) {
	ctx := context.Background()

	t.Run("empty queue", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)

		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypeReady,
		}, nil)
		require.NoError(t, err)

		mockStorage.EXPECT().List(ctx, "queue:ready:").Return([]ports.KeyValue{}, nil)

		isEmpty, err := queue.IsEmpty(ctx)
		assert.NoError(t, err)
		assert.True(t, isEmpty)
	})

	t.Run("non-empty queue", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)

		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypeReady,
		}, nil)
		require.NoError(t, err)

		mockStorage.EXPECT().List(ctx, "queue:ready:").Return([]ports.KeyValue{
			{Key: "queue:ready:0000000000000000001:id1", Value: []byte("data")},
		}, nil)

		isEmpty, err := queue.IsEmpty(ctx)
		assert.NoError(t, err)
		assert.False(t, isEmpty)
	})
}

func TestGetSize(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypeReady,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	metaData := []byte(`{"v":1,"size":3,"updated":1755776269766210000}`)
	mockStorage.EXPECT().Get(ctx, "queue:meta:ready").Return(metaData, nil)

	size, err := queue.GetSize(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, size)
}

func TestClearReady(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypeReady,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	keys := []ports.KeyValue{
		{Key: "queue:ready:0000000000000000001:id1", Value: []byte("data1")},
		{Key: "queue:ready:0000000000000000002:id2", Value: []byte("data2")},
	}

	mockStorage.EXPECT().List(ctx, "queue:ready:").Return(keys, nil)

	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) >= 2
	})).Return(nil)

	err = queue.Clear(ctx)
	assert.NoError(t, err)
}

func TestClearPending(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypePending,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	keys := []ports.KeyValue{
		{Key: "queue:pending:0000000000000000001:id1", Value: []byte("data1")},
	}

	mockStorage.EXPECT().List(ctx, "queue:pending:").Return(keys, nil)

	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) >= 1
	})).Return(nil)

	err = queue.Clear(ctx)
	assert.NoError(t, err)
}

func TestStorageErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("list error", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypeReady,
		}, nil)
		require.NoError(t, err)

		internalErr := domain.Error{Type: domain.ErrorTypeInternal, Message: "storage error"}
		mockStorage.EXPECT().List(ctx, "queue:ready:").Return(nil, internalErr)

		_, err = queue.IsEmpty(ctx)
		assert.Error(t, err)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeInternal, domainErr.Type)
	})

	t.Run("clear batch error", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypeReady,
		}, nil)
		require.NoError(t, err)

		keys := []ports.KeyValue{
			{Key: "queue:ready:0000000000000000001:id1", Value: []byte("data1")},
		}

		mockStorage.EXPECT().List(ctx, "queue:ready:").Return(keys, nil)

		batchErr := domain.Error{Type: domain.ErrorTypeInternal, Message: "batch error"}
		mockStorage.EXPECT().Batch(ctx, mock.Anything).Return(batchErr)

		err = queue.Clear(ctx)
		assert.Error(t, err)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeInternal, domainErr.Type)
	})
}

func TestEnqueueReady(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypeReady,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Config:     map[string]interface{}{"key": "value"},
		Priority:   1,
	}

	keyNotFoundErr := fmt.Errorf("key not found")
	mockStorage.EXPECT().Get(ctx, "queue:meta:ready").Return(nil, keyNotFoundErr)
	mockStorage.EXPECT().Get(ctx, mock.AnythingOfType("string")).Return(nil, keyNotFoundErr)
	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) >= 3
	})).Return(nil)

	err = queue.EnqueueReady(ctx, item)
	assert.NoError(t, err)
}

func TestEnqueuePending(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypePending,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		ID:         "test-id",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Config:     map[string]interface{}{"key": "value"},
		Priority:   1,
	}

	keyNotFoundErr := fmt.Errorf("key not found")
	mockStorage.EXPECT().Get(ctx, "queue:meta:pending").Return(nil, keyNotFoundErr)
	mockStorage.EXPECT().Get(ctx, "queue:item:test-id").Return(nil, keyNotFoundErr)
	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) >= 3
	})).Return(nil)

	err = queue.EnqueuePending(ctx, item)
	assert.NoError(t, err)
}

func TestDequeueReady(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypeReady,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		ID:         "test-id",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Config:     map[string]interface{}{"key": "value"},
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	itemData, _ := serializeItem(item)
	key := fmt.Sprintf("queue:ready:%019d:%s", item.EnqueuedAt.UnixNano(), item.ID)
	itemDataKey := fmt.Sprintf("queue:item:%s", item.ID)

	mockStorage.EXPECT().List(ctx, "queue:ready:").Return([]ports.KeyValue{
		{Key: key, Value: []byte(item.ID)},
	}, nil)

	mockStorage.EXPECT().Get(ctx, itemDataKey).Return(itemData, nil)
	mockStorage.EXPECT().Get(ctx, "queue:meta:ready").Return([]byte(`{"v":1,"size":1,"updated":0}`), nil)
	// Mock for claim manager access
	mockStorage.EXPECT().Get(ctx, "claims:test-id").Return(nil, fmt.Errorf("not found")).Maybe()
	mockStorage.EXPECT().Put(ctx, "claims:test-id", mock.AnythingOfType("[]uint8")).Return(nil).Maybe()

	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) >= 2
	})).Return(nil)

	dequeuedItem, err := queue.DequeueReady(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, dequeuedItem)
	assert.Equal(t, item.WorkflowID, dequeuedItem.WorkflowID)
}

func TestDequeueReadyEmpty(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypeReady,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	mockStorage.EXPECT().List(ctx, "queue:ready:").Return([]ports.KeyValue{}, nil)

	item, err := queue.DequeueReady(ctx)
	assert.NoError(t, err)
	assert.Nil(t, item)
}

func TestGetPendingItems(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypePending,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	item1 := ports.QueueItem{
		ID:         "id1",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Config:     map[string]interface{}{"key": "value1"},
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	itemData1, _ := serializeItem(item1)
	key1 := fmt.Sprintf("queue:pending:%019d:%s", item1.EnqueuedAt.UnixNano(), item1.ID)
	itemDataKey1 := fmt.Sprintf("queue:item:%s", item1.ID)

	mockStorage.EXPECT().List(ctx, "queue:pending:").Return([]ports.KeyValue{
		{Key: key1, Value: []byte(item1.ID)},
	}, nil)

	mockStorage.EXPECT().Get(ctx, itemDataKey1).Return(itemData1, nil)

	items, err := queue.GetPendingItems(ctx)
	assert.NoError(t, err)
	assert.Len(t, items, 1)
	assert.Equal(t, item1.WorkflowID, items[0].WorkflowID)
}

func TestMovePendingToReady(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypePending,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		ID:         "test-id",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Config:     map[string]interface{}{"key": "value"},
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	itemData, _ := serializeItem(item)
	pendingKey := fmt.Sprintf("queue:pending:%019d:%s", item.EnqueuedAt.UnixNano(), item.ID)
	itemDataKey := fmt.Sprintf("queue:item:%s", item.ID)
	pendingIndexKey := fmt.Sprintf("queue:pending_idx:%s", item.ID)

	mockStorage.EXPECT().Get(ctx, itemDataKey).Return(itemData, nil)
	mockStorage.EXPECT().Get(ctx, pendingIndexKey).Return([]byte(pendingKey), nil)
	mockStorage.EXPECT().Get(ctx, "queue:meta:pending").Return([]byte(`{"v":1,"size":1,"updated":0}`), nil)
	mockStorage.EXPECT().Get(ctx, "queue:meta:ready").Return(nil, fmt.Errorf("key not found"))

	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) == 5
	})).Return(nil)

	err = queue.MovePendingToReady(ctx, item.ID)
	assert.NoError(t, err)
}

func TestRemoveFromPending(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypePending,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	itemID := "test-id"
	pendingKey := fmt.Sprintf("queue:pending:%019d:%s", time.Now().UnixNano(), itemID)
	pendingIndexKey := fmt.Sprintf("queue:pending_idx:%s", itemID)

	mockStorage.EXPECT().Get(ctx, pendingIndexKey).Return([]byte(pendingKey), nil)
	mockStorage.EXPECT().Get(ctx, "queue:meta:pending").Return([]byte(`{"v":1,"size":1,"updated":0}`), nil)

	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) >= 3
	})).Return(nil)

	err = queue.RemoveFromPending(ctx, itemID)
	assert.NoError(t, err)
}

func TestGetSizeWithMissingMetadata(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypeReady,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	keyNotFoundErr := fmt.Errorf("key not found")
	mockStorage.EXPECT().Get(ctx, "queue:meta:ready").Return(nil, keyNotFoundErr)

	size, err := queue.GetSize(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, size)
}

func TestGenerateVisibilityKey(t *testing.T) {
	tests := []struct {
		name      string
		itemID    string
		expiresAt time.Time
		want      string
	}{
		{
			name:      "basic visibility key",
			itemID:    "test-id",
			expiresAt: time.Unix(0, 1234567890123456789),
			want:      "queue:visibility:1234567890123456789:test-id",
		},
		{
			name:      "uuid item id",
			itemID:    "550e8400-e29b-41d4-a716-446655440000",
			expiresAt: time.Unix(0, 987654321098765432),
			want:      "queue:visibility:0987654321098765432:550e8400-e29b-41d4-a716-446655440000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateVisibilityKey(tt.itemID, tt.expiresAt)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestQueueTypeValidationErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("EnqueueReady with pending queue", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypePending,
		}, nil)
		require.NoError(t, err)

		item := ports.QueueItem{
			WorkflowID: "workflow-1",
			NodeName:   "node-1",
			Config:     map[string]interface{}{"key": "value"},
			Priority:   1,
		}

		err = queue.EnqueueReady(ctx, item)
		assert.Error(t, err)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeValidation, domainErr.Type)
	})

	t.Run("EnqueuePending with ready queue", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypeReady,
		}, nil)
		require.NoError(t, err)

		item := ports.QueueItem{
			WorkflowID: "workflow-1",
			NodeName:   "node-1",
			Config:     map[string]interface{}{"key": "value"},
			Priority:   1,
		}

		err = queue.EnqueuePending(ctx, item)
		assert.Error(t, err)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeValidation, domainErr.Type)
	})

	t.Run("DequeueReady with pending queue", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypePending,
		}, nil)
		require.NoError(t, err)

		_, err = queue.DequeueReady(ctx)
		assert.Error(t, err)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeValidation, domainErr.Type)
	})

	t.Run("GetPendingItems with ready queue", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypeReady,
		}, nil)
		require.NoError(t, err)

		_, err = queue.GetPendingItems(ctx)
		assert.Error(t, err)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeValidation, domainErr.Type)
	})

	t.Run("RemoveFromPending with ready queue", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypeReady,
		}, nil)
		require.NoError(t, err)

		err = queue.RemoveFromPending(ctx, "test-id")
		assert.Error(t, err)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeValidation, domainErr.Type)
	})
}

func TestDequeueWithVisibility(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypeReady,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		ID:         "test-id",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Config:     map[string]interface{}{"key": "value"},
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	itemData, _ := serializeItem(item)
	key := fmt.Sprintf("queue:ready:%019d:%s", item.EnqueuedAt.UnixNano(), item.ID)
	itemDataKey := fmt.Sprintf("queue:item:%s", item.ID)

	mockStorage.EXPECT().List(ctx, "queue:ready:").Return([]ports.KeyValue{
		{Key: key, Value: []byte(item.ID)},
	}, nil)
	mockStorage.EXPECT().Get(ctx, itemDataKey).Return(itemData, nil)
	mockStorage.EXPECT().Get(ctx, "queue:meta:ready").Return([]byte(`{"v":1,"size":1,"updated":0}`), nil)
	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) >= 2
	})).Return(nil)
	mockStorage.EXPECT().Put(ctx, mock.MatchedBy(func(key string) bool {
		return strings.HasPrefix(key, "queue:visibility:")
	}), []byte(item.ID)).Return(nil)

	dequeuedItem, err := queue.dequeueWithVisibility(ctx, 30*time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, dequeuedItem)
	assert.Equal(t, item.WorkflowID, dequeuedItem.WorkflowID)
}

func TestDequeueWithVisibilityPutError(t *testing.T) {
	ctx := context.Background()
	mockStorage := mocks.NewMockStoragePort(t)

	queue, err := NewBadgerQueue(mockStorage, Config{
		QueueType: QueueTypeReady,
		NodeID:    "test-node",
	}, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		ID:         "test-id",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Config:     map[string]interface{}{"key": "value"},
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	itemData, _ := serializeItem(item)
	key := fmt.Sprintf("queue:ready:%019d:%s", item.EnqueuedAt.UnixNano(), item.ID)
	itemDataKey := fmt.Sprintf("queue:item:%s", item.ID)

	mockStorage.EXPECT().List(ctx, "queue:ready:").Return([]ports.KeyValue{
		{Key: key, Value: []byte(item.ID)},
	}, nil)
	mockStorage.EXPECT().Get(ctx, itemDataKey).Return(itemData, nil)
	mockStorage.EXPECT().Get(ctx, "queue:meta:ready").Return([]byte(`{"v":1,"size":1,"updated":0}`), nil)
	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) >= 2
	})).Return(nil)
	mockStorage.EXPECT().Put(ctx, mock.MatchedBy(func(key string) bool {
		return strings.HasPrefix(key, "queue:visibility:")
	}), []byte(item.ID)).Return(fmt.Errorf("storage error"))

	mockStorage.EXPECT().Get(ctx, "queue:meta:ready").Return([]byte(`{"v":1,"size":0,"updated":0}`), nil)
	mockStorage.EXPECT().Get(ctx, "queue:item:test-id").Return(nil, fmt.Errorf("key not found"))
	mockStorage.EXPECT().Batch(ctx, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) >= 3
	})).Return(nil)

	_, err = queue.dequeueWithVisibility(ctx, 30*time.Second)
	assert.Error(t, err)
	domainErr, ok := err.(domain.Error)
	assert.True(t, ok)
	assert.Equal(t, domain.ErrorTypeInternal, domainErr.Type)
}

func TestGetPendingItemsWithLimitEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("invalid limit - negative", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypePending,
		}, nil)
		require.NoError(t, err)

		mockStorage.EXPECT().List(ctx, "queue:pending:").Return([]ports.KeyValue{}, nil)

		items, err := queue.getPendingItemsWithLimit(ctx, -1)
		assert.NoError(t, err)
		assert.Empty(t, items)
	})

	t.Run("limit too high", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypePending,
		}, nil)
		require.NoError(t, err)

		mockStorage.EXPECT().List(ctx, "queue:pending:").Return([]ports.KeyValue{}, nil)

		items, err := queue.getPendingItemsWithLimit(ctx, 2000)
		assert.NoError(t, err)
		assert.Empty(t, items)
	})

	t.Run("storage error on list", func(t *testing.T) {
		mockStorage := mocks.NewMockStoragePort(t)
		queue, err := NewBadgerQueue(mockStorage, Config{
			QueueType: QueueTypePending,
		}, nil)
		require.NoError(t, err)

		mockStorage.EXPECT().List(ctx, "queue:pending:").Return(nil, fmt.Errorf("storage error"))

		_, err = queue.getPendingItemsWithLimit(ctx, 10)
		assert.Error(t, err)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeInternal, domainErr.Type)
	})
}
