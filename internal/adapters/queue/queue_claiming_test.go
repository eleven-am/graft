package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockQueueStorage struct {
	data map[string][]byte
}

func newMockQueueStorage() *mockQueueStorage {
	return &mockQueueStorage{
		data: make(map[string][]byte),
	}
}

func (m *mockQueueStorage) Put(ctx context.Context, key string, value []byte) error {
	m.data[key] = value
	return nil
}

func (m *mockQueueStorage) Get(ctx context.Context, key string) ([]byte, error) {
	value, exists := m.data[key]
	if !exists {
		return nil, fmt.Errorf("key not found")
	}
	return value, nil
}

func (m *mockQueueStorage) Delete(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}

func (m *mockQueueStorage) List(ctx context.Context, prefix string) ([]ports.KeyValue, error) {
	var result []ports.KeyValue
	for key, value := range m.data {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, ports.KeyValue{Key: key, Value: value})
		}
	}
	return result, nil
}

func (m *mockQueueStorage) Batch(ctx context.Context, ops []ports.Operation) error {
	for _, op := range ops {
		switch op.Type {
		case ports.OpPut:
			m.data[op.Key] = op.Value
		case ports.OpDelete:
			delete(m.data, op.Key)
		}
	}
	return nil
}

func TestQueue_DequeueReady_WithClaim(t *testing.T) {
	ctx := context.Background()
	storage := newMockQueueStorage()

	config := Config{
		QueueType: QueueTypeReady,
		NodeID:    "node-1",
	}

	queue, err := NewBadgerQueue(storage, config, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		ID:         "item-1",
		WorkflowID: "workflow-1",
		NodeName:   "test-node",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err = queue.EnqueueReady(ctx, item)
	require.NoError(t, err)

	claimDuration := 5 * time.Minute
	dequeuedItem, err := queue.DequeueReady(ctx, ports.WithClaim("node-1", claimDuration))
	require.NoError(t, err)
	require.NotNil(t, dequeuedItem)
	assert.Equal(t, item.ID, dequeuedItem.ID)

	err = queue.VerifyWorkClaim(ctx, item.ID, "node-1")
	require.NoError(t, err)
}

func TestQueue_DequeueReady_WrongQueueType(t *testing.T) {
	ctx := context.Background()
	storage := newMockQueueStorage()

	config := Config{
		QueueType: QueueTypePending,
		NodeID:    "node-1",
	}

	queue, err := NewBadgerQueue(storage, config, nil)
	require.NoError(t, err)

	claimDuration := 5 * time.Minute
	_, err = queue.DequeueReady(ctx, ports.WithClaim("node-1", claimDuration))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this queue is not configured for ready items")
}

func TestQueue_DequeueReady_EmptyQueue(t *testing.T) {
	ctx := context.Background()
	storage := newMockQueueStorage()

	config := Config{
		QueueType: QueueTypeReady,
		NodeID:    "node-1",
	}

	queue, err := NewBadgerQueue(storage, config, nil)
	require.NoError(t, err)

	claimDuration := 5 * time.Minute
	dequeuedItem, err := queue.DequeueReady(ctx, ports.WithClaim("node-1", claimDuration))
	require.NoError(t, err)
	assert.Nil(t, dequeuedItem)
}

func TestQueue_VerifyWorkClaim(t *testing.T) {
	ctx := context.Background()
	storage := newMockQueueStorage()

	config := Config{
		QueueType: QueueTypeReady,
		NodeID:    "node-1",
	}

	queue, err := NewBadgerQueue(storage, config, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		ID:         "item-verify",
		WorkflowID: "workflow-1",
		NodeName:   "test-node",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err = queue.EnqueueReady(ctx, item)
	require.NoError(t, err)

	claimDuration := 5 * time.Minute
	_, err = queue.DequeueReady(ctx, ports.WithClaim("node-1", claimDuration))
	require.NoError(t, err)

	err = queue.VerifyWorkClaim(ctx, item.ID, "node-1")
	require.NoError(t, err)

	err = queue.VerifyWorkClaim(ctx, "non-existent", "node-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active claim found")
}

func TestQueue_ReleaseWorkClaim(t *testing.T) {
	ctx := context.Background()
	storage := newMockQueueStorage()

	config := Config{
		QueueType: QueueTypeReady,
		NodeID:    "node-1",
	}

	queue, err := NewBadgerQueue(storage, config, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		ID:         "item-release",
		WorkflowID: "workflow-1",
		NodeName:   "test-node",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err = queue.EnqueueReady(ctx, item)
	require.NoError(t, err)

	claimDuration := 5 * time.Minute
	_, err = queue.DequeueReady(ctx, ports.WithClaim("node-1", claimDuration))
	require.NoError(t, err)

	err = queue.VerifyWorkClaim(ctx, item.ID, "node-1")
	require.NoError(t, err)

	err = queue.ReleaseWorkClaim(ctx, item.ID, "node-1")
	require.NoError(t, err)

	err = queue.VerifyWorkClaim(ctx, item.ID, "node-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active claim found")
}

func TestQueue_MultiNodeClaimConflict(t *testing.T) {
	ctx := context.Background()
	storage := newMockQueueStorage()

	config1 := Config{
		QueueType: QueueTypeReady,
		NodeID:    "node-1",
	}
	queue1, err := NewBadgerQueue(storage, config1, nil)
	require.NoError(t, err)

	config2 := Config{
		QueueType: QueueTypeReady,
		NodeID:    "node-2",
	}
	queue2, err := NewBadgerQueue(storage, config2, nil)
	require.NoError(t, err)

	item1 := ports.QueueItem{
		ID:         "item-conflict-1",
		WorkflowID: "workflow-1",
		NodeName:   "test-node",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	item2 := ports.QueueItem{
		ID:         "item-conflict-2",
		WorkflowID: "workflow-2",
		NodeName:   "test-node",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err = queue1.EnqueueReady(ctx, item1)
	require.NoError(t, err)
	err = queue1.EnqueueReady(ctx, item2)
	require.NoError(t, err)

	claimDuration := 5 * time.Minute

	dequeuedItem1, err := queue1.DequeueReady(ctx, ports.WithClaim("node-1", claimDuration))
	require.NoError(t, err)
	require.NotNil(t, dequeuedItem1)

	dequeuedItem2, err := queue2.DequeueReady(ctx, ports.WithClaim("node-2", claimDuration))
	require.NoError(t, err)
	require.NotNil(t, dequeuedItem2)

	assert.NotEqual(t, dequeuedItem1.ID, dequeuedItem2.ID)

	err = queue1.VerifyWorkClaim(ctx, dequeuedItem1.ID, "node-1")
	require.NoError(t, err)

	err = queue2.VerifyWorkClaim(ctx, dequeuedItem2.ID, "node-2")
	require.NoError(t, err)

	err = queue1.VerifyWorkClaim(ctx, dequeuedItem2.ID, "node-1")
	require.Error(t, err)

	err = queue2.VerifyWorkClaim(ctx, dequeuedItem1.ID, "node-2")
	require.Error(t, err)
}

func TestQueue_ClaimExpiration(t *testing.T) {
	ctx := context.Background()
	storage := newMockQueueStorage()

	config := Config{
		QueueType: QueueTypeReady,
		NodeID:    "node-1",
	}

	queue, err := NewBadgerQueue(storage, config, nil)
	require.NoError(t, err)

	item := ports.QueueItem{
		ID:         "item-expiry",
		WorkflowID: "workflow-1",
		NodeName:   "test-node",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err = queue.EnqueueReady(ctx, item)
	require.NoError(t, err)

	shortClaimDuration := 1 * time.Millisecond
	_, err = queue.DequeueReady(ctx, ports.WithClaim("node-1", shortClaimDuration))
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	err = queue.VerifyWorkClaim(ctx, item.ID, "node-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has expired")
}

func TestQueue_InvalidNodeIDConfig(t *testing.T) {
	ctx := context.Background()
	storage := newMockQueueStorage()

	config := Config{
		QueueType: QueueTypeReady,
		NodeID:    "",
	}

	queue, err := NewBadgerQueue(storage, config, nil)
	require.NoError(t, err)
	item := ports.QueueItem{
		ID:         "test-item",
		WorkflowID: "workflow-1",
		NodeName:   "test-node",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err = queue.EnqueueReady(ctx, item)
	require.NoError(t, err)

	claimDuration := 5 * time.Minute
	_, err = queue.DequeueReady(ctx, ports.WithClaim("node-1", claimDuration))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queue was created without a valid node ID, claiming not supported")
}
