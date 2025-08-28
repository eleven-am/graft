package ports

import (
	"context"
	"time"
)

type QueuePort interface {
	Enqueue(item []byte) error
	Peek() (item []byte, exists bool, err error)
	Claim() (item []byte, claimID string, exists bool, err error)
	Complete(claimID string) error
	WaitForItem(ctx context.Context) <-chan struct{}
	Size() (int, error)
	HasItemsWithPrefix(dataPrefix string) (bool, error)
	GetItemsWithPrefix(dataPrefix string) ([][]byte, error)
	HasClaimedItemsWithPrefix(dataPrefix string) (bool, error)
	GetClaimedItemsWithPrefix(dataPrefix string) ([]ClaimedItem, error)
	Close() error

	SendToDeadLetter(item []byte, reason string) error
	GetDeadLetterItems(limit int) ([]DeadLetterItem, error)
	GetDeadLetterSize() (int, error)
	RetryFromDeadLetter(itemID string) error
}

type DeadLetterItem struct {
	ID         string    `json:"id"`
	Item       []byte    `json:"item"`
	Reason     string    `json:"reason"`
	Timestamp  time.Time `json:"timestamp"`
	RetryCount int       `json:"retry_count"`
}

type ClaimedItem struct {
	Data      []byte    `json:"data"`
	ClaimID   string    `json:"claim_id"`
	ClaimedAt time.Time `json:"claimed_at"`
	Sequence  int64     `json:"sequence"`
}
