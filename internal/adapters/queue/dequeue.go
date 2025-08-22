package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func (q *Queue) dequeue(ctx context.Context) (*ports.QueueItem, error) {
	prefix := getQueuePrefix(q.queueType)
	items, err := q.storage.List(ctx, prefix)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list queue items",
			Details: map[string]interface{}{
				"error": err.Error(),
				"queue": string(q.queueType),
			},
		}
	}

	if len(items) == 0 {
		return nil, domain.NewNotFoundError("queue_item", "queue is empty")
	}

	firstItem := items[0]
	queueKey := firstItem.Key

	_, _, itemID, err := parseQueueKey(queueKey)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to parse queue key",
			Details: map[string]interface{}{
				"error": err.Error(),
				"key":   queueKey,
			},
		}
	}

	itemDataKey := generateItemDataKey(itemID)
	itemData, err := q.storage.Get(ctx, itemDataKey)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get item data",
			Details: map[string]interface{}{
				"error":   err.Error(),
				"item_id": itemID,
			},
		}
	}

	item, err := deserializeItem(itemData)
	if err != nil {
		return nil, err
	}

	metaKey := generateMetadataKey(q.queueType)
	metaData, err := q.storage.Get(ctx, metaKey)
	if err != nil && !domain.IsKeyNotFound(err) {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get queue metadata",
			Details: map[string]interface{}{
				"error": err.Error(),
				"queue": string(q.queueType),
			},
		}
	}

	meta, err := deserializeMetadata(metaData)
	if err != nil {
		return nil, err
	}

	if meta.Size > 0 {
		meta.Size--
	}
	updatedMeta, err := serializeMetadata(*meta)
	if err != nil {
		return nil, err
	}

	ops := []ports.Operation{
		{Type: ports.OpDelete, Key: queueKey},
		{Type: ports.OpDelete, Key: itemDataKey},
		{Type: ports.OpPut, Key: metaKey, Value: updatedMeta},
	}

	if err := q.storage.Batch(ctx, ops); err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to dequeue item",
			Details: map[string]interface{}{
				"error":   err.Error(),
				"item_id": itemID,
				"queue":   string(q.queueType),
			},
		}
	}

	q.logger.Info("item dequeued",
		"item_id", item.ID,
		"workflow_id", item.WorkflowID,
		"queue_size", meta.Size,
		"wait_time_ms", time.Since(item.EnqueuedAt).Milliseconds(),
	)

	return item, nil
}

func (q *Queue) dequeueWithVisibility(ctx context.Context, visibilityTimeout time.Duration) (*ports.QueueItem, error) {
	item, err := q.dequeue(ctx)
	if err != nil {
		return nil, err
	}

	visibilityKey := generateVisibilityKey(item.ID, time.Now().Add(visibilityTimeout))
	if err := q.storage.Put(ctx, visibilityKey, []byte(item.ID)); err != nil {
		if reEnqueueErr := q.enqueue(ctx, *item, q.queueType); reEnqueueErr != nil {
			q.logger.Error("failed to re-enqueue item after visibility timeout failure",
				"item_id", item.ID,
				"error", reEnqueueErr.Error(),
			)
		}
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to set visibility timeout",
			Details: map[string]interface{}{
				"error":   err.Error(),
				"item_id": item.ID,
			},
		}
	}

	return item, nil
}

func generateVisibilityKey(itemID string, expiresAt time.Time) string {
	return fmt.Sprintf("queue:visibility:%019d:%s", expiresAt.UnixNano(), itemID)
}
