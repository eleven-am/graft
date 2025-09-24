package domain

import (
	json "github.com/goccy/go-json"
	"time"
)

// StateUpdate represents a single state update for batching
type StateUpdate struct {
	WorkflowID   string            `json:"workflow_id"`
	UpdateType   string            `json:"update_type"`
	StateChanges []StateDelta      `json:"state_changes"`
	Timestamp    time.Time         `json:"timestamp"`
	Metadata     map[string]string `json:"metadata"`
}

// BatchedStateUpdate represents a batch of state updates
type BatchedStateUpdate struct {
	WorkflowIDs []string      `json:"workflow_ids"`
	Updates     []StateUpdate `json:"updates"`
	BatchID     string        `json:"batch_id"`
	CreatedAt   time.Time     `json:"created_at"`
	Priority    int           `json:"priority"`
}

// WorkflowStatistics tracks workflow state change patterns
type WorkflowStatistics struct {
	WorkflowID          string    `json:"workflow_id"`
	ChangeFrequency     float64   `json:"change_frequency"`
	StateSize           int64     `json:"state_size"`
	LastChangeTimestamp time.Time `json:"last_change_timestamp"`
}

// StateDelta represents a change in workflow state
type StateDelta struct {
	Operation string          `json:"operation"`
	Path      string          `json:"path"`
	OldValue  json.RawMessage `json:"old_value"`
	NewValue  json.RawMessage `json:"new_value"`
	Timestamp time.Time       `json:"timestamp"`
}

// WorkflowStateSnapshot represents a state snapshot
type WorkflowStateSnapshot struct {
	WorkflowID      string            `json:"workflow_id"`
	SequenceNum     int64             `json:"sequence_num"`
	Timestamp       time.Time         `json:"timestamp"`
	SnapshotType    string            `json:"snapshot_type"` // "full" or "incremental"
	BasedOnSequence *int64            `json:"based_on_sequence,omitempty"`
	RawData         []byte            `json:"raw_data,omitempty"`
	CompressedData  []byte            `json:"compressed_data,omitempty"`
	StateDeltas     []StateDelta      `json:"state_deltas,omitempty"`
	Checksum        string            `json:"checksum,omitempty"`
	OriginalSize    int64             `json:"original_size"`
	CompressedSize  int64             `json:"compressed_size"`
	Metadata        map[string]string `json:"metadata"`
}
