package queue

import (
	"context"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

const (
	defaultPaginationLimit = 100
	maxPaginationLimit     = 1000
)

func (q *Queue) getPendingItems(ctx context.Context) ([]ports.QueueItem, error) {
	return q.getPendingItemsWithLimit(ctx, defaultPaginationLimit)
}

func (q *Queue) getPendingItemsWithLimit(ctx context.Context, limit int) ([]ports.QueueItem, error) {
	if limit <= 0 || limit > maxPaginationLimit {
		limit = defaultPaginationLimit
	}

	prefix := getQueuePrefix(QueueTypePending)
	kvPairs, err := q.storage.List(ctx, prefix)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list pending items",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	targetCount := min(limit, len(kvPairs))
	items := make([]ports.QueueItem, 0, targetCount)

	itemKeys := make([]string, 0, targetCount)
	itemIDs := make([]string, 0, targetCount)

	count := 0
	for _, kv := range kvPairs {
		if count >= limit {
			break
		}

		_, _, itemID, err := parseQueueKey(kv.Key)
		if err != nil {
			q.logger.Error("failed to parse queue key",
				"key", kv.Key,
				"error", err.Error(),
			)
			continue
		}

		itemDataKey := generateItemDataKey(itemID)
		itemKeys = append(itemKeys, itemDataKey)
		itemIDs = append(itemIDs, itemID)
		count++
	}

	for i, itemKey := range itemKeys {
		itemData, err := q.storage.Get(ctx, itemKey)
		if err != nil {
			q.logger.Error("failed to get item data",
				"item_id", itemIDs[i],
				"error", err.Error(),
			)
			continue
		}

		item, err := deserializeItem(itemData)
		if err != nil {
			q.logger.Error("failed to deserialize item",
				"item_id", itemIDs[i],
				"error", err.Error(),
			)
			continue
		}

		items = append(items, *item)
	}

	q.logger.Info("retrieved pending items",
		"count", len(items),
		"limit", limit,
	)

	return items, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (q *Queue) movePendingToReady(ctx context.Context, itemID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if itemID == "" {
		return domain.NewValidationError("itemID", "itemID is required")
	}

	itemDataKey := generateItemDataKey(itemID)
	itemData, err := q.storage.Get(ctx, itemDataKey)
	if err != nil {
		if domain.IsKeyNotFound(err) {
			return domain.NewNotFoundError("queue_item", itemID)
		}
		return domain.Error{
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
		return err
	}

	oldPendingKey, err := q.findPendingKeyByID(ctx, itemID)
	if err != nil {
		return err
	}

	newReadyKey := generateQueueKey(QueueTypeReady, time.Now(), itemID)

	pendingMetaKey := generateMetadataKey(QueueTypePending)
	pendingMetaData, _ := q.storage.Get(ctx, pendingMetaKey)
	pendingMeta, _ := deserializeMetadata(pendingMetaData)
	if pendingMeta.Size > 0 {
		pendingMeta.Size--
	}
	updatedPendingMeta, err := serializeMetadata(*pendingMeta)
	if err != nil {
		return err
	}

	readyMetaKey := generateMetadataKey(QueueTypeReady)
	readyMetaData, _ := q.storage.Get(ctx, readyMetaKey)
	readyMeta, _ := deserializeMetadata(readyMetaData)
	readyMeta.Size++
	updatedReadyMeta, err := serializeMetadata(*readyMeta)
	if err != nil {
		return err
	}

	ops := []ports.Operation{
		{Type: ports.OpDelete, Key: oldPendingKey},
		{Type: ports.OpPut, Key: newReadyKey, Value: []byte(itemID)},
		{Type: ports.OpPut, Key: pendingMetaKey, Value: updatedPendingMeta},
		{Type: ports.OpPut, Key: readyMetaKey, Value: updatedReadyMeta},
		{Type: ports.OpDelete, Key: generatePendingIndexKey(itemID)},
	}

	if err := q.storage.Batch(ctx, ops); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to move item from pending to ready",
			Details: map[string]interface{}{
				"error":   err.Error(),
				"item_id": itemID,
			},
		}
	}

	q.logger.Info("moved item from pending to ready",
		"item_id", itemID,
		"workflow_id", item.WorkflowID,
		"pending_size", pendingMeta.Size,
		"ready_size", readyMeta.Size,
	)

	return nil
}

func (q *Queue) removeFromPending(ctx context.Context, itemID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if itemID == "" {
		return domain.NewValidationError("itemID", "itemID is required")
	}

	pendingKey, err := q.findPendingKeyByID(ctx, itemID)
	if err != nil {
		return err
	}

	itemDataKey := generateItemDataKey(itemID)

	metaKey := generateMetadataKey(QueueTypePending)
	metaData, _ := q.storage.Get(ctx, metaKey)
	meta, _ := deserializeMetadata(metaData)
	if meta.Size > 0 {
		meta.Size--
	}
	updatedMeta, err := serializeMetadata(*meta)
	if err != nil {
		return err
	}

	ops := []ports.Operation{
		{Type: ports.OpDelete, Key: pendingKey},
		{Type: ports.OpDelete, Key: itemDataKey},
		{Type: ports.OpPut, Key: metaKey, Value: updatedMeta},
		{Type: ports.OpDelete, Key: generatePendingIndexKey(itemID)},
	}

	if err := q.storage.Batch(ctx, ops); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to remove item from pending",
			Details: map[string]interface{}{
				"error":   err.Error(),
				"item_id": itemID,
			},
		}
	}

	q.logger.Info("removed item from pending",
		"item_id", itemID,
		"pending_size", meta.Size,
	)

	return nil
}

func (q *Queue) findPendingKeyByID(ctx context.Context, itemID string) (string, error) {
	indexKey := generatePendingIndexKey(itemID)
	pendingKeyData, err := q.storage.Get(ctx, indexKey)
	if err == nil {
		return string(pendingKeyData), nil
	}

	q.logger.Warn("pending item index missing, falling back to list scan",
		"item_id", itemID,
		"index_key", indexKey)

	return q.findPendingKeyByIDSlow(ctx, itemID)
}

func (q *Queue) findPendingKeyByIDSlow(ctx context.Context, itemID string) (string, error) {
	pendingPrefix := getQueuePrefix(QueueTypePending)
	pendingItems, err := q.storage.List(ctx, pendingPrefix)
	if err != nil {
		return "", domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list pending items",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	for _, kv := range pendingItems {
		if string(kv.Value) == itemID {
			return kv.Key, nil
		}
	}

	return "", domain.NewNotFoundError("pending_item", itemID)
}
