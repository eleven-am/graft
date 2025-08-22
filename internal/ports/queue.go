package ports

import (
	"context"
	"time"
)

type QueuePort interface {
	EnqueueReady(ctx context.Context, item QueueItem) error
	EnqueuePending(ctx context.Context, item QueueItem) error
	DequeueReady(ctx context.Context, opts ...DequeueOption) (*QueueItem, error)
	GetPendingItems(ctx context.Context) ([]QueueItem, error)
	MovePendingToReady(ctx context.Context, itemID string) error
	RemoveFromPending(ctx context.Context, itemID string) error
	IsEmpty(ctx context.Context) (bool, error)
	VerifyWorkClaim(ctx context.Context, workItemID string, nodeID string) error
	ReleaseWorkClaim(ctx context.Context, workItemID string, nodeID string) error
}

type DequeueOptions struct {
	NodeID        string
	ClaimDuration time.Duration
}

type DequeueOption func(*DequeueOptions)

func WithClaim(nodeID string, duration time.Duration) DequeueOption {
	return func(opts *DequeueOptions) {
		opts.NodeID = nodeID
		opts.ClaimDuration = duration
	}
}

type QueueItem struct {
	ID         string
	WorkflowID string
	NodeName   string
	Config     interface{}
	Priority   int
	EnqueuedAt time.Time
}