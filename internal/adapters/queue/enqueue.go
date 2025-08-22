package queue

import (
	"context"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/google/uuid"
)

func (q *Queue) enqueue(ctx context.Context, item ports.QueueItem, queueType QueueType) error {
	if item.ID == "" {
		item.ID = uuid.New().String()
	}

	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = time.Now()
	}

	itemData, err := serializeItem(item)
	if err != nil {
		return err
	}

	metaKey := generateMetadataKey(queueType)
	metaData, err := q.storage.Get(ctx, metaKey)
	if err != nil && !domain.IsKeyNotFound(err) {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get queue metadata",
			Details: map[string]interface{}{
				"error": err.Error(),
				"queue": string(queueType),
			},
		}
	}

	meta, err := deserializeMetadata(metaData)
	if err != nil {
		return err
	}

	if meta.Size >= q.maxSize {
		return domain.Error{
			Type:    domain.ErrorTypeRateLimit,
			Message: "queue is full",
			Details: map[string]interface{}{
				"queue":    string(queueType),
				"size":     meta.Size,
				"max_size": q.maxSize,
				"item_id":  item.ID,
			},
		}
	}

	existingItemKey := generateItemDataKey(item.ID)
	if existingData, err := q.storage.Get(ctx, existingItemKey); err == nil && len(existingData) > 0 {
		return domain.NewConflictError("queue_item", "item with this ID already exists in queue")
	}

	queueKey := generateQueueKey(queueType, item.EnqueuedAt, item.ID)
	itemKey := generateItemDataKey(item.ID)

	meta.Size++
	updatedMeta, err := serializeMetadata(*meta)
	if err != nil {
		return err
	}

	ops := []ports.Operation{
		{Type: ports.OpPut, Key: queueKey, Value: []byte(item.ID)},
		{Type: ports.OpPut, Key: itemKey, Value: itemData},
		{Type: ports.OpPut, Key: metaKey, Value: updatedMeta},
	}

	if queueType == QueueTypePending {
		pendingIndexKey := generatePendingIndexKey(item.ID)
		ops = append(ops, ports.Operation{
			Type:  ports.OpPut,
			Key:   pendingIndexKey,
			Value: []byte(queueKey),
		})
	}

	if err := q.storage.Batch(ctx, ops); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to enqueue item",
			Details: map[string]interface{}{
				"error":   err.Error(),
				"item_id": item.ID,
				"queue":   string(queueType),
			},
		}
	}

	q.logger.Info("item enqueued",
		"item_id", item.ID,
		"workflow_id", item.WorkflowID,
		"queue_size", meta.Size,
		"priority", item.Priority,
	)

	return nil
}
