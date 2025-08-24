package ports

import (
	"context"
	"time"
)

type QueueType string

const (
	QueueTypeReady      QueueType = "ready"
	QueueTypePending    QueueType = "pending"
	QueueTypeDeadLetter QueueType = "dead_letter"
)

type QueuePort interface {
	// Queue operations
	Enqueue(ctx context.Context, item QueueItem) error
	Dequeue(ctx context.Context, opts ...DequeueOption) (*QueueItem, error)
	GetItems(ctx context.Context) ([]QueueItem, error)
	RemoveItem(ctx context.Context, itemID string) error
	EnqueueBatch(ctx context.Context, items []QueueItem) error
	DequeueBatch(ctx context.Context, maxItems int, opts ...DequeueOption) ([]QueueItem, error)

	// Utility operations
	IsEmpty(ctx context.Context) (bool, error)
	VerifyWorkClaim(ctx context.Context, workItemID string, nodeID string) error
	ReleaseWorkClaim(ctx context.Context, workItemID string, nodeID string) error
	ProcessExpiredClaims(ctx context.Context) error
	GetQueuePartitions(ctx context.Context) ([]QueuePartition, error)
}

type DequeueOptions struct {
	NodeID        string
	ClaimDuration time.Duration
	PartitionKey  string
	MaxRetries    int
}

type DequeueOption func(*DequeueOptions)

func WithClaim(nodeID string, duration time.Duration) DequeueOption {
	return func(opts *DequeueOptions) {
		opts.NodeID = nodeID
		opts.ClaimDuration = duration
	}
}

func WithPartition(partitionKey string) DequeueOption {
	return func(opts *DequeueOptions) {
		opts.PartitionKey = partitionKey
	}
}

func WithMaxRetries(maxRetries int) DequeueOption {
	return func(opts *DequeueOptions) {
		opts.MaxRetries = maxRetries
	}
}

type QueueItem struct {
	ID             string
	WorkflowID     string
	NodeName       string
	Config         interface{}
	Priority       int
	EnqueuedAt     time.Time
	PartitionKey   string
	RetryCount     int
	MaxRetries     int
	Checksum       string
	IdempotencyKey string
}

type DeadLetterItem struct {
	QueueItem
	FailedAt      time.Time
	FailureReason string
	OriginalQueue string
}

type QueuePartition struct {
	Key          string
	ItemCount    int
	LastActivity time.Time
}
