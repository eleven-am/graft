package ports

import (
	"context"
	"time"
)

type SemaphorePort interface {
	Acquire(ctx context.Context, semaphoreID string, nodeID string, duration time.Duration) error
	Release(ctx context.Context, semaphoreID string, nodeID string) error
	IsAcquired(ctx context.Context, semaphoreID string) (bool, error)
	IsAcquiredByNode(ctx context.Context, semaphoreID string, nodeID string) (bool, error)
	Extend(ctx context.Context, semaphoreID string, nodeID string, newDuration time.Duration) error
}