package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

type QueueItem struct {
	Data      []byte    `json:"data"`
	Sequence  int64     `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
}

func NewQueueItem(data []byte, sequence int64) *QueueItem {
	return &QueueItem{
		Data:      data,
		Sequence:  sequence,
		Timestamp: time.Now(),
	}
}

func (q *QueueItem) ToBytes() ([]byte, error) {
	return json.Marshal(q)
}

func QueueItemFromBytes(data []byte) (*QueueItem, error) {
	var item QueueItem
	err := json.Unmarshal(data, &item)
	return &item, err
}

type ClaimedItem struct {
	Data      []byte    `json:"data"`
	ClaimID   string    `json:"claim_id"`
	ClaimedAt time.Time `json:"claimed_at"`
	Sequence  int64     `json:"sequence"`
}

func NewClaimedItem(data []byte, claimID string, sequence int64) *ClaimedItem {
	return &ClaimedItem{
		Data:      data,
		ClaimID:   claimID,
		ClaimedAt: time.Now(),
		Sequence:  sequence,
	}
}

func (c *ClaimedItem) ToBytes() ([]byte, error) {
	return json.Marshal(c)
}

func ClaimedItemFromBytes(data []byte) (*ClaimedItem, error) {
	var item ClaimedItem
	err := json.Unmarshal(data, &item)
	return &item, err
}

func QueuePendingKey(name string, sequence int64) string {
	return fmt.Sprintf("queue:%s:pending:%020d", name, sequence)
}

func QueueClaimedKey(name, claimID string) string {
	return fmt.Sprintf("queue:%s:claimed:%s", name, claimID)
}

func QueueSequenceKey(name string) string {
	return fmt.Sprintf("queue:%s:sequence", name)
}

type DeadLetterQueueItem struct {
	ID         string    `json:"id"`
	Data       []byte    `json:"data"`
	Reason     string    `json:"reason"`
	Timestamp  time.Time `json:"timestamp"`
	RetryCount int       `json:"retry_count"`
	Sequence   int64     `json:"sequence"`
}

func NewDeadLetterQueueItem(data []byte, reason string, retryCount int, sequence int64) *DeadLetterQueueItem {
	return &DeadLetterQueueItem{
		ID:         fmt.Sprintf("dlq-%d-%d", sequence, time.Now().UnixNano()),
		Data:       data,
		Reason:     reason,
		Timestamp:  time.Now(),
		RetryCount: retryCount,
		Sequence:   sequence,
	}
}

func (d *DeadLetterQueueItem) ToBytes() ([]byte, error) {
	return json.Marshal(d)
}

func DeadLetterQueueItemFromBytes(data []byte) (*DeadLetterQueueItem, error) {
	var item DeadLetterQueueItem
	err := json.Unmarshal(data, &item)
	return &item, err
}

func QueueDeadLetterKey(name, itemID string) string {
	return fmt.Sprintf("queue:%s:deadletter:%s", name, itemID)
}

func QueueDeadLetterSequenceKey(name string) string {
	return fmt.Sprintf("queue:%s:deadletter:sequence", name)
}