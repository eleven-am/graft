package raftimpl

import (
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type StorageConfig struct {
	DefaultConsistencyLevel ConsistencyLevel `json:"default_consistency_level"`

	QuorumConfig QuorumReadConfig `json:"quorum_config"`

	ForwardingEnabled bool          `json:"forwarding_enabled"`
	ForwardingTimeout time.Duration `json:"forwarding_timeout"`

	CacheSize int64         `json:"cache_size"`
	CacheTTL  time.Duration `json:"cache_ttl"`

	MaxRetries   int           `json:"max_retries"`
	RetryBackoff time.Duration `json:"retry_backoff"`

	BatchSize          int           `json:"batch_size"`
	CompactionInterval time.Duration `json:"compaction_interval"`

	MetricsEnabled      bool          `json:"metrics_enabled"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
}

func DefaultStorageConfig() StorageConfig {
	return StorageConfig{
		DefaultConsistencyLevel: ConsistencyLeader,
		QuorumConfig: QuorumReadConfig{
			MinReplicas:      2,
			ReadTimeout:      3 * time.Second,
			ConsistencyLevel: ConsistencyQuorum,
			RetryAttempts:    2,
		},
		ForwardingEnabled:   true,
		ForwardingTimeout:   5 * time.Second,
		CacheTTL:            30 * time.Second,
		MaxRetries:          3,
		RetryBackoff:        100 * time.Millisecond,
		BatchSize:           100,
		CompactionInterval:  10 * time.Minute,
		MetricsEnabled:      true,
		HealthCheckInterval: 30 * time.Second,
	}
}

type PerformanceConfig struct {
	ReadCacheEnabled bool          `json:"read_cache_enabled"`
	ReadCacheSize    int64         `json:"read_cache_size"`
	ReadCacheTTL     time.Duration `json:"read_cache_ttl"`
	ReadTimeout      time.Duration `json:"read_timeout"`

	WriteBatchSize  int           `json:"write_batch_size"`
	WriteBufferSize int64         `json:"write_buffer_size"`
	WriteTimeout    time.Duration `json:"write_timeout"`
	SyncWrites      bool          `json:"sync_writes"`

	CompactionThreshold float64       `json:"compaction_threshold"`
	CompactionInterval  time.Duration `json:"compaction_interval"`

	MaxConnections    int           `json:"max_connections"`
	ConnectionTimeout time.Duration `json:"connection_timeout"`
	IdleTimeout       time.Duration `json:"idle_timeout"`
}

func DefaultPerformanceConfig() PerformanceConfig {
	return PerformanceConfig{
		ReadCacheEnabled:    true,
		ReadCacheSize:       10 * 1024 * 1024,
		ReadCacheTTL:        5 * time.Minute,
		ReadTimeout:         3 * time.Second,
		WriteBatchSize:      1000,
		WriteBufferSize:     16 * 1024 * 1024,
		WriteTimeout:        5 * time.Second,
		SyncWrites:          true,
		CompactionThreshold: 0.7,
		CompactionInterval:  10 * time.Minute,
		MaxConnections:      50,
		ConnectionTimeout:   10 * time.Second,
		IdleTimeout:         5 * time.Minute,
	}
}

type ConsistencyConfig struct {
	LeaderReadTimeout time.Duration `json:"leader_read_timeout"`
	LeaderCacheTTL    time.Duration `json:"leader_cache_ttl"`

	EventualReadStale time.Duration `json:"eventual_read_stale"`

	QuorumMinReplicas   int           `json:"quorum_min_replicas"`
	QuorumReadTimeout   time.Duration `json:"quorum_read_timeout"`
	QuorumRetryAttempts int           `json:"quorum_retry_attempts"`
}

func DefaultConsistencyConfig() ConsistencyConfig {
	return ConsistencyConfig{
		LeaderReadTimeout:   5 * time.Second,
		LeaderCacheTTL:      30 * time.Second,
		EventualReadStale:   1 * time.Minute,
		QuorumMinReplicas:   2,
		QuorumReadTimeout:   3 * time.Second,
		QuorumRetryAttempts: 2,
	}
}

type MonitoringConfig struct {
	MetricsEnabled  bool          `json:"metrics_enabled"`
	MetricsInterval time.Duration `json:"metrics_interval"`

	HealthCheckEnabled  bool          `json:"health_check_enabled"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	HealthCheckTimeout  time.Duration `json:"health_check_timeout"`

	LogLevel           string        `json:"log_level"`
	LogRequestDetails  bool          `json:"log_request_details"`
	LogSlowQueries     bool          `json:"log_slow_queries"`
	SlowQueryThreshold time.Duration `json:"slow_query_threshold"`

	TracingEnabled    bool    `json:"tracing_enabled"`
	TracingSampleRate float64 `json:"tracing_sample_rate"`
}

func DefaultMonitoringConfig() MonitoringConfig {
	return MonitoringConfig{
		MetricsEnabled:      true,
		MetricsInterval:     30 * time.Second,
		HealthCheckEnabled:  true,
		HealthCheckInterval: 30 * time.Second,
		HealthCheckTimeout:  5 * time.Second,
		LogLevel:            "info",
		LogRequestDetails:   false,
		LogSlowQueries:      true,
		SlowQueryThreshold:  1 * time.Second,
		TracingEnabled:      false,
		TracingSampleRate:   0.1,
	}
}

func (c StorageConfig) Validate() error {
	if c.QuorumConfig.MinReplicas < 1 {
		return domain.NewValidationError("quorum_min_replicas", "must be at least 1")
	}

	if c.QuorumConfig.ReadTimeout <= 0 {
		return domain.NewValidationError("quorum_read_timeout", "must be positive")
	}

	if c.ForwardingTimeout <= 0 {
		return domain.NewValidationError("forwarding_timeout", "must be positive")
	}

	if c.CacheSize < 0 {
		return domain.NewValidationError("cache_size", "cannot be negative")
	}

	if c.MaxRetries < 0 {
		return domain.NewValidationError("max_retries", "cannot be negative")
	}

	if c.BatchSize <= 0 {
		return domain.NewValidationError("batch_size", "must be positive")
	}

	return nil
}
