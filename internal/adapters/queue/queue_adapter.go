package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

const (
	readyPrefix      = "queue/ready/"
	pendingPrefix    = "queue/pending/"
	claimPrefix      = "queue/claims/"
	visPrefix        = "queue/visibility/"
	metaPrefix       = "queue/meta/"
	deadLetterPrefix = "queue/dead/"
	partitionPrefix  = "queue/partitions/"
	checksumPrefix   = "queue/checksums/"

	defaultClaimDuration = 5 * time.Minute
	maxClaimDuration     = 1 * time.Hour
	maxRetryCount        = 3
	batchSizeLimit       = 100
)

type Adapter struct {
	storage   ports.StoragePort
	nodeID    string
	logger    *slog.Logger
	queueType ports.QueueType
}

func NewAdapter(storage ports.StoragePort, nodeID string, queueType ports.QueueType, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}

	return &Adapter{
		storage:   storage,
		nodeID:    nodeID,
		queueType: queueType,
		logger:    logger.With("component", "queue", "queue_type", queueType),
	}
}

func (q *Adapter) Enqueue(ctx context.Context, item ports.QueueItem) error {
	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}

	if err := q.validateAndPrepareItem(&item); err != nil {
		return err
	}

	if err := q.checkDeduplication(ctx, item); err != nil {
		return err
	}

	prefix := q.getQueuePrefix(q.queueType)
	return q.enqueue(ctx, item, prefix)
}

func (q *Adapter) Dequeue(ctx context.Context, opts ...ports.DequeueOption) (*ports.QueueItem, error) {
	switch q.queueType {
	case ports.QueueTypeReady:
		return q.dequeueReady(ctx, opts...)
	case ports.QueueTypePending:
		return nil, fmt.Errorf("cannot dequeue from pending queue - use MoveItem instead")
	case ports.QueueTypeDeadLetter:
		return nil, fmt.Errorf("cannot dequeue from dead letter queue")
	default:
		return nil, fmt.Errorf("unsupported queue type: %s", q.queueType)
	}
}

func (q *Adapter) GetItems(ctx context.Context) ([]ports.QueueItem, error) {
	prefix := q.getQueuePrefix(q.queueType)
	items, err := q.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list %s items: %w", q.queueType, err)
	}

	if q.queueType == ports.QueueTypeDeadLetter {
		result := make([]ports.QueueItem, 0, len(items))
		for _, kv := range items {
			var deadItem ports.DeadLetterItem
			if err := json.Unmarshal(kv.Value, &deadItem); err != nil {
				q.logger.Error("failed to deserialize dead letter item", "key", kv.Key, "error", err)
				continue
			}
			result = append(result, deadItem.QueueItem)
		}
		return result, nil
	}

	result := make([]ports.QueueItem, 0, len(items))
	for _, kv := range items {
		var item ports.QueueItem
		if err := json.Unmarshal(kv.Value, &item); err != nil {
			q.logger.Error("failed to deserialize queue item", "key", kv.Key, "error", err)
			continue
		}
		result = append(result, item)
	}

	return result, nil
}

func (q *Adapter) RemoveItem(ctx context.Context, itemID string) error {
	prefix := q.getQueuePrefix(q.queueType)
	storageItems, err := q.storage.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("failed to list storage items: %w", err)
	}

	for _, kv := range storageItems {
		var item ports.QueueItem
		if q.queueType == ports.QueueTypeDeadLetter {
			var deadItem ports.DeadLetterItem
			if err := json.Unmarshal(kv.Value, &deadItem); err != nil {
				continue
			}
			item = deadItem.QueueItem
		} else {
			if err := json.Unmarshal(kv.Value, &item); err != nil {
				continue
			}
		}

		if item.ID == itemID {
			if err := q.storage.Delete(ctx, kv.Key); err != nil {
				return fmt.Errorf("failed to delete item from %s queue: %w", q.queueType, err)
			}
			q.logger.Debug("removed item from queue", "item_id", itemID, "queue_type", q.queueType)
			return nil
		}
	}

	return fmt.Errorf("item %s not found in %s queue", itemID, q.queueType)
}

func (q *Adapter) EnqueueBatch(ctx context.Context, items []ports.QueueItem) error {
	if len(items) == 0 {
		return nil
	}

	if len(items) > batchSizeLimit {
		return domain.NewResourceError("batch", "enqueue", domain.ErrCapacityLimit)
	}

	if q.queueType == ports.QueueTypeReady {
		return q.enqueueBatchReady(ctx, items)
	}

	for _, item := range items {
		if err := q.Enqueue(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

func (q *Adapter) DequeueBatch(ctx context.Context, maxItems int, opts ...ports.DequeueOption) ([]ports.QueueItem, error) {
	if maxItems <= 0 || maxItems > batchSizeLimit {
		return nil, domain.NewResourceError("batch", "dequeue", domain.ErrInvalidInput)
	}

	switch q.queueType {
	case ports.QueueTypeReady:
		return q.dequeueBatchReady(ctx, maxItems, opts...)
	case ports.QueueTypePending:
		return nil, fmt.Errorf("cannot dequeue batch from pending queue")
	case ports.QueueTypeDeadLetter:
		return nil, fmt.Errorf("cannot dequeue batch from dead letter queue")
	default:
		return nil, fmt.Errorf("unsupported queue type: %s", q.queueType)
	}
}

func (q *Adapter) IsEmpty(ctx context.Context) (bool, error) {
	prefix := q.getQueuePrefix(q.queueType)
	items, err := q.storage.List(ctx, prefix)
	if err != nil {
		return false, fmt.Errorf("failed to list %s items: %w", q.queueType, err)
	}

	return len(items) == 0, nil
}

func (q *Adapter) VerifyWorkClaim(ctx context.Context, workItemID string, nodeID string) error {
	claimKey := q.getClaimKey(workItemID)
	claimData, err := q.storage.Get(ctx, claimKey)
	if err != nil {
		return fmt.Errorf("claim not found for item %s: %w", workItemID, err)
	}

	var claim WorkClaim
	if err := json.Unmarshal(claimData, &claim); err != nil {
		return fmt.Errorf("failed to deserialize claim: %w", err)
	}

	if claim.NodeID != nodeID {
		return fmt.Errorf("claim belongs to different node: expected %s, got %s", nodeID, claim.NodeID)
	}

	if time.Now().After(claim.ExpiresAt) {
		return fmt.Errorf("claim has expired")
	}

	return nil
}

func (q *Adapter) ReleaseWorkClaim(ctx context.Context, workItemID string, nodeID string) error {
	if err := q.VerifyWorkClaim(ctx, workItemID, nodeID); err != nil {
		return fmt.Errorf("invalid claim: %w", err)
	}

	claimKey := q.getClaimKey(workItemID)
	claimData, err := q.storage.Get(ctx, claimKey)
	if err != nil {
		return fmt.Errorf("failed to get claim: %w", err)
	}

	var claim WorkClaim
	if err := json.Unmarshal(claimData, &claim); err != nil {
		return fmt.Errorf("failed to deserialize claim: %w", err)
	}

	visKey := q.getVisibilityKey(claim.ExpiresAt, workItemID)

	ops := []ports.Operation{
		{Type: ports.OpDelete, Key: claimKey},
		{Type: ports.OpDelete, Key: visKey},
	}

	if err := q.storage.Batch(ctx, ops); err != nil {
		return fmt.Errorf("failed to release claim: %w", err)
	}

	q.logger.Debug("work claim released", "item_id", workItemID, "node_id", nodeID)
	return nil
}

func (q *Adapter) ProcessExpiredClaims(ctx context.Context) error {
	now := time.Now()

	visItems, err := q.storage.List(ctx, visPrefix)
	if err != nil {
		return fmt.Errorf("failed to list visibility items: %w", err)
	}

	var expiredItems []string
	for _, kv := range visItems {
		keyParts := strings.Split(kv.Key, "/")
		if len(keyParts) < 3 {
			continue
		}

		timestampPart := keyParts[2]
		underscoreIdx := strings.Index(timestampPart, "_")
		if underscoreIdx == -1 {
			continue
		}

		timestampStr := timestampPart[:underscoreIdx]
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			continue
		}

		if timestamp <= now.UnixNano() {
			itemID := string(kv.Value)
			expiredItems = append(expiredItems, itemID)
		}
	}

	for _, itemID := range expiredItems {
		if err := q.requeueExpiredItem(ctx, itemID); err != nil {
			q.logger.Error("failed to requeue expired item", "item_id", itemID, "error", err)
		}
	}

	if len(expiredItems) > 0 {
		q.logger.Debug("processed expired claims", "count", len(expiredItems))
	}

	return nil
}

func (q *Adapter) GetQueuePartitions(ctx context.Context) ([]ports.QueuePartition, error) {
	items, err := q.storage.List(ctx, partitionPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list partitions: %w", err)
	}

	result := make([]ports.QueuePartition, 0, len(items))
	for _, kv := range items {
		var partition ports.QueuePartition
		if err := json.Unmarshal(kv.Value, &partition); err != nil {
			q.logger.Error("failed to deserialize partition", "key", kv.Key, "error", err)
			continue
		}
		result = append(result, partition)
	}

	return result, nil
}

func (q *Adapter) getQueuePrefix(queueType ports.QueueType) string {
	switch queueType {
	case ports.QueueTypeReady:
		return readyPrefix
	case ports.QueueTypePending:
		return pendingPrefix
	case ports.QueueTypeDeadLetter:
		return deadLetterPrefix
	default:
		return ""
	}
}

func (q *Adapter) enqueue(ctx context.Context, item ports.QueueItem, prefix string) error {
	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = time.Now()
	}

	key := q.getTimestampKey(prefix, item.EnqueuedAt, item.ID)
	if item.PartitionKey != "" {
		key = q.getPartitionTimestampKey(prefix, item.PartitionKey, item.EnqueuedAt, item.ID)
	}

	itemData, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to serialize queue item: %w", err)
	}

	var ops []ports.Operation
	ops = append(ops, ports.Operation{
		Type:  ports.OpPut,
		Key:   key,
		Value: itemData,
	})

	if item.Checksum != "" {
		ops = append(ops, ports.Operation{
			Type:  ports.OpPut,
			Key:   q.getChecksumKey(item.Checksum),
			Value: []byte(item.ID),
		})
	}

	if item.PartitionKey != "" {
		ops = append(ops, q.updatePartitionOps(item.PartitionKey)...)
	}

	if len(ops) == 1 {
		if err := q.storage.Put(ctx, key, itemData); err != nil {
			return fmt.Errorf("failed to store queue item: %w", err)
		}
	} else {
		if err := q.storage.Batch(ctx, ops); err != nil {
			return fmt.Errorf("failed to store queue item with metadata: %w", err)
		}
	}

	q.logger.Debug("item enqueued", "item_id", item.ID, "priority", item.Priority, "prefix", prefix, "partition", item.PartitionKey)
	return nil
}

func (q *Adapter) dequeueReady(ctx context.Context, opts ...ports.DequeueOption) (*ports.QueueItem, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context cannot be nil")
	}

	options := &ports.DequeueOptions{
		NodeID:        q.nodeID,
		ClaimDuration: defaultClaimDuration,
	}

	for _, opt := range opts {
		opt(options)
	}

	if options.ClaimDuration <= 0 || options.ClaimDuration > maxClaimDuration {
		return nil, fmt.Errorf("invalid claim duration: %v (must be between 1s and %v)",
			options.ClaimDuration, maxClaimDuration)
	}

	if options.NodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	prefix := readyPrefix
	if options.PartitionKey != "" {
		prefix = readyPrefix + options.PartitionKey + "/"
	}

	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		items, err := q.storage.List(ctx, prefix)
		if err != nil {
			return nil, fmt.Errorf("failed to list ready items: %w", err)
		}

		if len(items) == 0 {
			return nil, domain.ErrNotFound
		}

		candidates := make([]*ports.KeyValue, 0, len(items))
		candidateItems := make([]*ports.QueueItem, 0, len(items))

		for _, kv := range items {
			var item ports.QueueItem
			if err := json.Unmarshal(kv.Value, &item); err != nil {
				q.logger.Error("failed to deserialize queue item", "key", kv.Key, "error", err)
				continue
			}
			candidates = append(candidates, &kv)
			candidateItems = append(candidateItems, &item)
		}

		if len(candidates) == 0 {
			return nil, domain.ErrNotFound
		}

		highestPriority := -1
		var bestCandidates []int

		for i, item := range candidateItems {
			if item.Priority > highestPriority {
				highestPriority = item.Priority
				bestCandidates = []int{i}
			} else if item.Priority == highestPriority {
				bestCandidates = append(bestCandidates, i)
			}
		}

		for _, idx := range bestCandidates {
			selectedKV := candidates[idx]
			selectedItem := candidateItems[idx]
			item := *selectedItem

			claim := &WorkClaim{
				ItemID:    item.ID,
				NodeID:    options.NodeID,
				ClaimedAt: time.Now(),
				ExpiresAt: time.Now().Add(options.ClaimDuration),
			}

			claimData, err := json.Marshal(claim)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize claim: %w", err)
			}

			ops := []ports.Operation{
				{Type: ports.OpDelete, Key: selectedKV.Key},
				{Type: ports.OpPut, Key: q.getClaimKey(item.ID), Value: claimData},
				{Type: ports.OpPut, Key: q.getVisibilityKey(claim.ExpiresAt, item.ID), Value: []byte(item.ID)},
			}

			if err := q.storage.Batch(ctx, ops); err != nil {
				if strings.Contains(err.Error(), "key not found for deletion") {
					continue
				}
				return nil, fmt.Errorf("failed to claim work item: %w", err)
			}

			q.logger.Debug("work item dequeued and claimed",
				"item_id", item.ID,
				"node_id", options.NodeID,
				"priority", item.Priority,
				"claim_duration", options.ClaimDuration)

			return &item, nil
		}
	}

	return nil, domain.ErrNotFound
}

func (q *Adapter) enqueueBatchReady(ctx context.Context, items []ports.QueueItem) error {
	seenIDs := make(map[string]bool)
	for i := range items {
		if err := q.validateAndPrepareItem(&items[i]); err != nil {
			return err
		}

		if seenIDs[items[i].ID] {
			return domain.NewResourceError("queue_item", "enqueue", fmt.Errorf("duplicate ID in batch: %s", items[i].ID))
		}
		seenIDs[items[i].ID] = true
	}

	var ops []ports.Operation
	for _, item := range items {
		if err := q.checkDeduplication(ctx, item); err != nil {
			return err
		}

		key := q.getTimestampKey(readyPrefix, item.EnqueuedAt, item.ID)
		itemData, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("failed to serialize batch item %s: %w", item.ID, err)
		}

		ops = append(ops, ports.Operation{
			Type:  ports.OpPut,
			Key:   key,
			Value: itemData,
		})

		ops = append(ops, ports.Operation{
			Type:  ports.OpPut,
			Key:   q.getChecksumKey(item.Checksum),
			Value: []byte(item.ID),
		})

		if item.PartitionKey != "" {
			ops = append(ops, q.updatePartitionOps(item.PartitionKey)...)
		}
	}

	if err := q.storage.Batch(ctx, ops); err != nil {
		return fmt.Errorf("failed to enqueue batch: %w", err)
	}

	q.logger.Debug("batch enqueued", "count", len(items))
	return nil
}

func (q *Adapter) dequeueBatchReady(ctx context.Context, maxItems int, opts ...ports.DequeueOption) ([]ports.QueueItem, error) {
	options := &ports.DequeueOptions{
		NodeID:        q.nodeID,
		ClaimDuration: defaultClaimDuration,
	}

	for _, opt := range opts {
		opt(options)
	}

	prefix := readyPrefix
	if options.PartitionKey != "" {
		prefix = readyPrefix + options.PartitionKey + "/"
	}

	items, err := q.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list ready items: %w", err)
	}

	var result []ports.QueueItem
	var ops []ports.Operation

	for _, kv := range items {
		if len(result) >= maxItems {
			break
		}

		var item ports.QueueItem
		if err := json.Unmarshal(kv.Value, &item); err != nil {
			q.logger.Error("failed to deserialize batch item", "key", kv.Key, "error", err)
			continue
		}

		claim := &WorkClaim{
			ItemID:    item.ID,
			NodeID:    options.NodeID,
			ClaimedAt: time.Now(),
			ExpiresAt: time.Now().Add(options.ClaimDuration),
		}

		claimData, err := json.Marshal(claim)
		if err != nil {
			continue
		}

		ops = append(ops,
			ports.Operation{Type: ports.OpDelete, Key: kv.Key},
			ports.Operation{Type: ports.OpPut, Key: q.getClaimKey(item.ID), Value: claimData},
			ports.Operation{Type: ports.OpPut, Key: q.getVisibilityKey(claim.ExpiresAt, item.ID), Value: []byte(item.ID)},
		)

		result = append(result, item)
	}

	if len(ops) > 0 {
		if err := q.storage.Batch(ctx, ops); err != nil {
			return nil, fmt.Errorf("failed to claim batch items: %w", err)
		}
	}

	q.logger.Debug("batch dequeued", "count", len(result))
	return result, nil
}

func (q *Adapter) requeueExpiredItem(ctx context.Context, itemID string) error {
	claimKey := q.getClaimKey(itemID)

	claimData, err := q.storage.Get(ctx, claimKey)
	if err != nil {
		return fmt.Errorf("failed to get expired claim: %w", err)
	}

	var claim WorkClaim
	if err := json.Unmarshal(claimData, &claim); err != nil {
		return fmt.Errorf("failed to deserialize expired claim: %w", err)
	}

	item := ports.QueueItem{
		ID:         itemID,
		EnqueuedAt: time.Now(),
	}

	readyKey := q.getReadyKey(time.Now(), itemID)
	itemData, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to serialize requeued item: %w", err)
	}

	visKey := q.getVisibilityKey(claim.ExpiresAt, itemID)

	ops := []ports.Operation{
		{Type: ports.OpDelete, Key: claimKey},
		{Type: ports.OpDelete, Key: visKey},
		{Type: ports.OpPut, Key: readyKey, Value: itemData},
	}

	if err := q.storage.Batch(ctx, ops); err != nil {
		return fmt.Errorf("failed to requeue expired item: %w", err)
	}

	q.logger.Debug("requeued expired item", "item_id", itemID)
	return nil
}

func (q *Adapter) validateAndPrepareItem(item *ports.QueueItem) error {
	if item.ID == "" {
		return domain.NewResourceError("queue_item", "validate", domain.ErrInvalidInput)
	}

	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = time.Now()
	}

	if item.MaxRetries == 0 {
		item.MaxRetries = maxRetryCount
	}

	if item.Checksum == "" {
		item.Checksum = q.calculateChecksum(*item)
	}

	return nil
}

func (q *Adapter) checkDeduplication(ctx context.Context, item ports.QueueItem) error {
	if item.Checksum == "" {
		return nil
	}

	checksumKey := q.getChecksumKey(item.Checksum)
	checksumData := []byte(item.ID)

	// Try to store the checksum atomically - if it already exists, storage will handle it
	err := q.storage.Put(ctx, checksumKey, checksumData)
	if err != nil {
		// Check if this is due to the same item being queued again (acceptable)
		existingID, getErr := q.storage.Get(ctx, checksumKey)
		if getErr == nil && string(existingID) == item.ID {
			// Same item ID with same checksum - this is a legitimate retry/duplicate
			q.logger.Debug("duplicate queue item detected, allowing",
				"item_id", item.ID,
				"checksum", item.Checksum)
			return nil
		}

		// Different item ID with same checksum - this is a collision (very rare)
		if getErr == nil && string(existingID) != item.ID {
			q.logger.Warn("checksum collision detected",
				"item_id", item.ID,
				"existing_id", string(existingID),
				"checksum", item.Checksum)
			// Allow the collision and continue - checksums are for optimization, not correctness
			return nil
		}

		// Storage error - propagate it
		return fmt.Errorf("failed to store checksum: %w", err)
	}

	return nil
}

func (q *Adapter) calculateChecksum(item ports.QueueItem) string {
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%s|%s|%s|%s|%d",
		item.ID, item.WorkflowID, item.NodeName, item.PartitionKey, item.Priority)))
	return hex.EncodeToString(hasher.Sum(nil))[:16]
}

func (q *Adapter) updatePartitionOps(partitionKey string) []ports.Operation {
	now := time.Now()
	partition := ports.QueuePartition{
		Key:          partitionKey,
		ItemCount:    1,
		LastActivity: now,
	}

	partitionData, _ := json.Marshal(partition)
	partitionStorageKey := partitionPrefix + partitionKey

	return []ports.Operation{
		{Type: ports.OpPut, Key: partitionStorageKey, Value: partitionData},
	}
}

func (q *Adapter) getTimestampKey(prefix string, timestamp time.Time, itemID string) string {
	return fmt.Sprintf("%s%020d_%s", prefix, timestamp.UnixNano(), itemID)
}

func (q *Adapter) getPartitionTimestampKey(prefix, partitionKey string, timestamp time.Time, itemID string) string {
	return fmt.Sprintf("%s%s/%020d_%s", prefix, partitionKey, timestamp.UnixNano(), itemID)
}

func (q *Adapter) getReadyKey(timestamp time.Time, itemID string) string {
	return q.getTimestampKey(readyPrefix, timestamp, itemID)
}

func (q *Adapter) getClaimKey(itemID string) string {
	return claimPrefix + itemID
}

func (q *Adapter) getVisibilityKey(expiresAt time.Time, itemID string) string {
	return fmt.Sprintf("%s%020d_%s", visPrefix, expiresAt.UnixNano(), itemID)
}

func (q *Adapter) getChecksumKey(checksum string) string {
	return checksumPrefix + checksum
}

func (q *Adapter) getDeadLetterKey(timestamp time.Time, itemID string) string {
	return fmt.Sprintf("%s%020d_%s", deadLetterPrefix, timestamp.UnixNano(), itemID)
}

type WorkClaim struct {
	ItemID    string    `json:"item_id"`
	NodeID    string    `json:"node_id"`
	ClaimedAt time.Time `json:"claimed_at"`
	ExpiresAt time.Time `json:"expires_at"`
}
