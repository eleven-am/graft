package queue

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestSimpleDequeueWithClaim(t *testing.T) {
	ctx := context.Background()
	storage := newMockQueueStorage()

	config := Config{
		QueueType: QueueTypeReady,
		NodeID:    "test-node",
	}

	queue, err := NewBadgerQueue(storage, config, nil)
	require.NoError(t, err)

	item, err := queue.DequeueReady(ctx)
	require.NoError(t, err)
	require.Nil(t, item)
	testItem := ports.QueueItem{
		ID:         "test-item",
		WorkflowID: "test-workflow",
		NodeName:   "test-node",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err = queue.EnqueueReady(ctx, testItem)
	require.NoError(t, err)

	item, err = queue.DequeueReady(ctx)
	require.NoError(t, err)
	require.NotNil(t, item)
	require.Equal(t, "test-item", item.ID)
}
