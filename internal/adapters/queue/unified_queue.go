package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type UnifiedQueue struct {
	readyQueue   *Queue
	pendingQueue *Queue
	logger       *slog.Logger
}

func NewUnifiedBadgerQueue(storage ports.StoragePort, config Config, logger *slog.Logger) (*UnifiedQueue, error) {
	if storage == nil {
		return nil, domain.NewValidationError("storage", "storage port is required")
	}

	if logger == nil {
		logger = slog.Default()
	}

	nodeID := config.NodeID
	if nodeID == "" {
		nodeID = "default"
	}

	instanceID := fmt.Sprintf("%p", storage)[:8]
	logger = logger.With(
		"component", "unified-queue",
		"node_id", nodeID,
		"instance_id", instanceID,
	)

	readyConfig := Config{
		QueueType:      QueueTypeReady,
		MaxSize:        config.MaxSize,
		EnableMetrics:  config.EnableMetrics,
		CleanupEnabled: config.CleanupEnabled,
		CleanupTTL:     config.CleanupTTL,
		NodeID:         config.NodeID,
	}

	pendingConfig := Config{
		QueueType:      QueueTypePending,
		MaxSize:        config.MaxSize,
		EnableMetrics:  config.EnableMetrics,
		CleanupEnabled: config.CleanupEnabled,
		CleanupTTL:     config.CleanupTTL,
		NodeID:         config.NodeID,
	}

	readyQueue, err := NewBadgerQueue(storage, readyConfig, logger)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create ready queue",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	pendingQueue, err := NewBadgerQueue(storage, pendingConfig, logger)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create pending queue",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	logger.Info("unified queue initialized",
		"max_size", config.MaxSize,
		"metrics_enabled", config.EnableMetrics,
		"cleanup_enabled", config.CleanupEnabled,
	)

	return &UnifiedQueue{
		readyQueue:   readyQueue,
		pendingQueue: pendingQueue,
		logger:       logger,
	}, nil
}

func (uq *UnifiedQueue) EnqueueReady(ctx context.Context, item ports.QueueItem) error {
	return uq.readyQueue.EnqueueReady(ctx, item)
}

func (uq *UnifiedQueue) EnqueuePending(ctx context.Context, item ports.QueueItem) error {
	return uq.pendingQueue.EnqueuePending(ctx, item)
}

func (uq *UnifiedQueue) DequeueReady(ctx context.Context, opts ...ports.DequeueOption) (*ports.QueueItem, error) {
	return uq.readyQueue.DequeueReady(ctx, opts...)
}

func (uq *UnifiedQueue) GetPendingItems(ctx context.Context) ([]ports.QueueItem, error) {
	return uq.pendingQueue.GetPendingItems(ctx)
}

func (uq *UnifiedQueue) MovePendingToReady(ctx context.Context, itemID string) error {
	return uq.pendingQueue.MovePendingToReady(ctx, itemID)
}

func (uq *UnifiedQueue) RemoveFromPending(ctx context.Context, itemID string) error {
	return uq.pendingQueue.RemoveFromPending(ctx, itemID)
}

func (uq *UnifiedQueue) IsEmpty(ctx context.Context) (bool, error) {
	readyEmpty, err := uq.readyQueue.IsEmpty(ctx)
	if err != nil {
		return false, err
	}

	if !readyEmpty {
		return false, nil
	}

	return uq.pendingQueue.IsEmpty(ctx)
}

func (uq *UnifiedQueue) GetSize(ctx context.Context) (int, error) {
	readySize, err := uq.readyQueue.GetSize(ctx)
	if err != nil {
		return 0, err
	}

	pendingSize, err := uq.pendingQueue.GetSize(ctx)
	if err != nil {
		return 0, err
	}

	return readySize + pendingSize, nil
}

func (uq *UnifiedQueue) VerifyWorkClaim(ctx context.Context, workItemID string, nodeID string) error {
	if err := uq.readyQueue.VerifyWorkClaim(ctx, workItemID, nodeID); err == nil {
		return nil
	}
	return uq.pendingQueue.VerifyWorkClaim(ctx, workItemID, nodeID)
}

func (uq *UnifiedQueue) ReleaseWorkClaim(ctx context.Context, workItemID string, nodeID string) error {
	if err := uq.readyQueue.ReleaseWorkClaim(ctx, workItemID, nodeID); err == nil {
		return nil
	}
	return uq.pendingQueue.ReleaseWorkClaim(ctx, workItemID, nodeID)
}

func (uq *UnifiedQueue) Clear(ctx context.Context) error {
	if err := uq.readyQueue.Clear(ctx); err != nil {
		return err
	}
	return uq.pendingQueue.Clear(ctx)
}
