package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type QueueType string

const (
	QueueTypeReady      QueueType = "ready"
	QueueTypePending    QueueType = "pending"
	defaultMaxQueueSize           = 10000
)

type Queue struct {
	storage         ports.StoragePort
	queueType       QueueType
	logger          *slog.Logger
	mu              sync.RWMutex
	claimManager    *ClaimManager
	claimingEnabled bool
	maxSize         int
}

type Config struct {
	QueueType      QueueType
	MaxSize        int
	EnableMetrics  bool
	CleanupEnabled bool
	CleanupTTL     time.Duration
	NodeID         string
}

func NewBadgerQueue(storage ports.StoragePort, config Config) (*Queue, error) {
	if storage == nil {
		return nil, domain.NewValidationError("storage", "storage port is required")
	}

	if config.QueueType != QueueTypeReady && config.QueueType != QueueTypePending {
		return nil, domain.NewValidationError("queueType", "must be 'ready' or 'pending'")
	}

	nodeID := config.NodeID
	claimingEnabled := nodeID != ""
	if nodeID == "" {
		nodeID = "default"
	}

	instanceID := fmt.Sprintf("%p", storage)[:8] // Use storage pointer as instance ID for debugging
	logger := slog.Default().With(
		"component", "queue",
		"queue_type", string(config.QueueType),
		"node_id", nodeID,
		"instance_id", instanceID,
	)

	claimManager := NewClaimManager(storage, nodeID)

	maxSize := config.MaxSize
	if maxSize <= 0 {
		maxSize = defaultMaxQueueSize
	}

	queue := &Queue{
		storage:         storage,
		queueType:       config.QueueType,
		logger:          logger,
		claimManager:    claimManager,
		claimingEnabled: claimingEnabled,
		maxSize:         maxSize,
	}

	logger.Info("queue initialized",
		"max_size", maxSize,
		"metrics_enabled", config.EnableMetrics,
		"cleanup_enabled", config.CleanupEnabled,
	)

	return queue, nil
}

func (q *Queue) EnqueueReady(ctx context.Context, item ports.QueueItem) error {
	if q.queueType != QueueTypeReady {
		return domain.NewValidationError("queue_type", "this queue is not configured for ready items")
	}
	return q.enqueue(ctx, item, QueueTypeReady)
}

func (q *Queue) EnqueuePending(ctx context.Context, item ports.QueueItem) error {
	if q.queueType != QueueTypePending {
		return domain.NewValidationError("queue_type", "this queue is not configured for pending items")
	}
	return q.enqueue(ctx, item, QueueTypePending)
}

func (q *Queue) DequeueReady(ctx context.Context, opts ...ports.DequeueOption) (*ports.QueueItem, error) {
	if q.queueType != QueueTypeReady {
		return nil, domain.NewValidationError("queue_type", "this queue is not configured for ready items")
	}

	options := &ports.DequeueOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.NodeID == "" && options.ClaimDuration > 0 {
		return nil, domain.NewValidationError("nodeID", "node ID is required when claiming")
	}

	if !q.claimingEnabled && options.ClaimDuration > 0 {
		return nil, domain.NewValidationError("nodeID", "queue was created without a valid node ID, claiming not supported")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	item, err := q.dequeue(ctx)
	if err != nil {
		if domainErr, ok := err.(domain.Error); ok && domainErr.Type == domain.ErrorTypeNotFound {
			return nil, nil
		}
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	if options.NodeID != "" && options.ClaimDuration > 0 {
		if err := q.claimManager.ClaimWork(ctx, item.ID, options.ClaimDuration); err != nil {
			if putBackErr := q.enqueue(ctx, *item, QueueTypeReady); putBackErr != nil {
				q.logger.Error("failed to put item back after claim failure",
					"item_id", item.ID,
					"error", putBackErr)
			}
			return nil, err
		}

		q.logger.Info("work item dequeued with claim",
			"item_id", item.ID,
			"node_id", options.NodeID,
			"claim_duration", options.ClaimDuration)
	} else {
		q.logger.Info("work item dequeued without claim",
			"item_id", item.ID)
	}

	return item, nil
}

func (q *Queue) GetPendingItems(ctx context.Context) ([]ports.QueueItem, error) {
	if q.queueType != QueueTypePending {
		return nil, domain.NewValidationError("queue_type", "this queue is not configured for pending items")
	}
	return q.getPendingItems(ctx)
}

func (q *Queue) MovePendingToReady(ctx context.Context, itemID string) error {
	return q.movePendingToReady(ctx, itemID)
}

func (q *Queue) RemoveFromPending(ctx context.Context, itemID string) error {
	if q.queueType != QueueTypePending {
		return domain.NewValidationError("queue_type", "this queue is not configured for pending items")
	}
	return q.removeFromPending(ctx, itemID)
}

func (q *Queue) IsEmpty(ctx context.Context) (bool, error) {
	return q.isEmpty(ctx)
}

func (q *Queue) isEmpty(ctx context.Context) (bool, error) {
	prefix := getQueuePrefix(q.queueType)
	items, err := q.storage.List(ctx, prefix)
	if err != nil {
		return false, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to check if queue is empty",
			Details: map[string]interface{}{
				"error": err.Error(),
				"queue": string(q.queueType),
			},
		}
	}

	isEmpty := len(items) == 0

	q.logger.Debug("checked if queue is empty",
		"is_empty", isEmpty,
		"queue_type", string(q.queueType),
	)

	return isEmpty, nil
}

func (q *Queue) GetSize(ctx context.Context) (int, error) {
	metaKey := generateMetadataKey(q.queueType)
	metaData, err := q.storage.Get(ctx, metaKey)
	if err != nil {
		if domain.IsKeyNotFound(err) {
			return 0, nil
		}
		return 0, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get queue size",
			Details: map[string]interface{}{
				"error": err.Error(),
				"queue": string(q.queueType),
			},
		}
	}

	meta, err := deserializeMetadata(metaData)
	if err != nil {
		return 0, err
	}

	return meta.Size, nil
}

func (q *Queue) VerifyWorkClaim(ctx context.Context, workItemID string, nodeID string) error {
	return q.claimManager.VerifyClaim(ctx, workItemID)
}

func (q *Queue) ReleaseWorkClaim(ctx context.Context, workItemID string, nodeID string) error {
	return q.claimManager.ReleaseClaim(ctx, workItemID)
}

func (q *Queue) Clear(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	prefix := getQueuePrefix(q.queueType)
	items, err := q.storage.List(ctx, prefix)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list queue items for clearing",
			Details: map[string]interface{}{
				"error": err.Error(),
				"queue": string(q.queueType),
			},
		}
	}

	ops := make([]ports.Operation, 0, len(items)*2+1)

	for _, kv := range items {
		ops = append(ops, ports.Operation{
			Type: ports.OpDelete,
			Key:  kv.Key,
		})

		_, _, itemID, err := parseQueueKey(kv.Key)
		if err == nil {
			itemDataKey := generateItemDataKey(itemID)
			ops = append(ops, ports.Operation{
				Type: ports.OpDelete,
				Key:  itemDataKey,
			})
		}
	}

	meta := QueueMetadata{
		Size:        0,
		LastUpdated: time.Now().UnixNano(),
	}
	updatedMeta, err := serializeMetadata(meta)
	if err != nil {
		return err
	}

	metaKey := generateMetadataKey(q.queueType)
	ops = append(ops, ports.Operation{
		Type:  ports.OpPut,
		Key:   metaKey,
		Value: updatedMeta,
	})

	if err := q.storage.Batch(ctx, ops); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to clear queue",
			Details: map[string]interface{}{
				"error": err.Error(),
				"queue": string(q.queueType),
			},
		}
	}

	q.logger.Info("queue cleared",
		"queue_type", string(q.queueType),
		"items_removed", len(items),
	)

	return nil
}
