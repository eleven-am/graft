package domain

import (
	"encoding/json"
	"time"
)

// PersistenceStrategy defines how workflow state should be persisted
type PersistenceStrategy string

const (
	// PersistenceImmediate - persist every state change immediately
	PersistenceImmediate PersistenceStrategy = "immediate"

	// PersistenceBatched - batch multiple state changes together
	PersistenceBatched PersistenceStrategy = "batched"

	// PersistenceAdaptive - adapt based on workflow complexity and change frequency
	PersistenceAdaptive PersistenceStrategy = "adaptive"

	// PersistenceIncremental - only persist the delta changes
	PersistenceIncremental PersistenceStrategy = "incremental"
)

// StateOptimizationConfig controls workflow state persistence optimization
type StateOptimizationConfig struct {
	// Strategy determines persistence approach
	Strategy PersistenceStrategy `json:"strategy" yaml:"strategy"`

	// BatchSize controls how many updates to batch together
	BatchSize int `json:"batch_size" yaml:"batch_size"`

	// BatchTimeout controls maximum time to wait before flushing batch
	BatchTimeout time.Duration `json:"batch_timeout" yaml:"batch_timeout"`

	// EnableCompression enables state compression for large workflows
	EnableCompression bool `json:"enable_compression" yaml:"enable_compression"`

	// CompressionThreshold determines minimum size for compression (bytes)
	CompressionThreshold int `json:"compression_threshold" yaml:"compression_threshold"`

	// EnableChecksums enables integrity checking for state data
	EnableChecksums bool `json:"enable_checksums" yaml:"enable_checksums"`

	// IncrementalSnapshots enables incremental state snapshots
	IncrementalSnapshots bool `json:"incremental_snapshots" yaml:"incremental_snapshots"`

	// SnapshotInterval controls frequency of full state snapshots
	SnapshotInterval time.Duration `json:"snapshot_interval" yaml:"snapshot_interval"`

	// MaxIncrementalDeltas before forcing full snapshot
	MaxIncrementalDeltas int `json:"max_incremental_deltas" yaml:"max_incremental_deltas"`

	// AdaptiveThresholds for strategy switching
	AdaptiveThresholds AdaptiveThresholds `json:"adaptive_thresholds" yaml:"adaptive_thresholds"`
}

// AdaptiveThresholds define when to switch persistence strategies
type AdaptiveThresholds struct {
	// HighFrequencyChanges per second to trigger batching
	HighFrequencyChanges float64 `json:"high_frequency_changes" yaml:"high_frequency_changes"`

	// LargeStateSize in bytes to trigger compression
	LargeStateSize int64 `json:"large_state_size" yaml:"large_state_size"`

	// ComplexWorkflowNodes threshold for incremental persistence
	ComplexWorkflowNodes int `json:"complex_workflow_nodes" yaml:"complex_workflow_nodes"`
}

// WorkflowStateSnapshot represents a state snapshot with optimization metadata
type WorkflowStateSnapshot struct {
	WorkflowID   string    `json:"workflow_id"`
	SequenceNum  int64     `json:"sequence_num"`
	Timestamp    time.Time `json:"timestamp"`
	SnapshotType string    `json:"snapshot_type"` // "full", "incremental"

	// Optimized storage fields
	CompressedData []byte `json:"compressed_data,omitempty"`
	RawData        []byte `json:"raw_data,omitempty"`
	Checksum       string `json:"checksum,omitempty"`

	// Incremental snapshot fields
	BasedOnSequence *int64       `json:"based_on_sequence,omitempty"`
	StateDeltas     []StateDelta `json:"state_deltas,omitempty"`

	// Metadata
	OriginalSize   int64             `json:"original_size"`
	CompressedSize int64             `json:"compressed_size,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// StateDelta represents a change in workflow state
type StateDelta struct {
	Operation string          `json:"operation"` // "set", "delete", "merge"
	Path      string          `json:"path"`      // JSON path to the field
	OldValue  json.RawMessage `json:"old_value,omitempty"`
	NewValue  json.RawMessage `json:"new_value,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

// BatchedStateUpdate represents multiple state updates to be persisted together
type BatchedStateUpdate struct {
	WorkflowIDs []string      `json:"workflow_ids"`
	Updates     []StateUpdate `json:"updates"`
	BatchID     string        `json:"batch_id"`
	CreatedAt   time.Time     `json:"created_at"`
	Priority    int           `json:"priority"` // Higher priority = persist sooner
}

// StateUpdate represents a single state update
type StateUpdate struct {
	WorkflowID   string            `json:"workflow_id"`
	UpdateType   string            `json:"update_type"` // "node_started", "node_completed", "state_changed"
	NodeName     string            `json:"node_name,omitempty"`
	StateChanges []StateDelta      `json:"state_changes"`
	Timestamp    time.Time         `json:"timestamp"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// WorkflowStatistics tracks workflow complexity metrics for adaptive persistence
type WorkflowStatistics struct {
	WorkflowID           string        `json:"workflow_id"`
	NodeCount            int           `json:"node_count"`
	ExecutedNodeCount    int           `json:"executed_node_count"`
	StateSize            int64         `json:"state_size"`
	ChangeFrequency      float64       `json:"change_frequency"` // Changes per second
	LastChangeTimestamp  time.Time     `json:"last_change_timestamp"`
	AverageExecutionTime time.Duration `json:"average_execution_time"`
	ErrorRate            float64       `json:"error_rate"`
}

// DefaultStateOptimizationConfig returns default configuration for state optimization
func DefaultStateOptimizationConfig() StateOptimizationConfig {
	return StateOptimizationConfig{
		Strategy:             PersistenceAdaptive,
		BatchSize:            10,
		BatchTimeout:         500 * time.Millisecond,
		EnableCompression:    true,
		CompressionThreshold: 1024,
		EnableChecksums:      true,
		IncrementalSnapshots: true,
		SnapshotInterval:     30 * time.Second,
		MaxIncrementalDeltas: 50,
		AdaptiveThresholds: AdaptiveThresholds{
			HighFrequencyChanges: 5.0,
			LargeStateSize:       10240,
			ComplexWorkflowNodes: 20,
		},
	}
}

// ShouldCompress determines if state should be compressed based on configuration
func (c *StateOptimizationConfig) ShouldCompress(stateSize int64) bool {
	return c.EnableCompression && stateSize >= int64(c.CompressionThreshold)
}

// ShouldUseBatching determines if batching should be used based on statistics
func (c *StateOptimizationConfig) ShouldUseBatching(stats *WorkflowStatistics) bool {
	if c.Strategy == PersistenceImmediate {
		return false
	}
	if c.Strategy == PersistenceBatched {
		return true
	}
	if c.Strategy == PersistenceAdaptive {
		return stats.ChangeFrequency >= c.AdaptiveThresholds.HighFrequencyChanges
	}
	return false
}

// ShouldUseIncremental determines if incremental persistence should be used
func (c *StateOptimizationConfig) ShouldUseIncremental(stats *WorkflowStatistics) bool {
	if !c.IncrementalSnapshots {
		return false
	}
	if c.Strategy == PersistenceAdaptive {
		return stats.NodeCount >= c.AdaptiveThresholds.ComplexWorkflowNodes ||
			stats.StateSize >= c.AdaptiveThresholds.LargeStateSize
	}
	return c.Strategy == PersistenceIncremental
}
