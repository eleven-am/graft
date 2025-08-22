package ports

import (
	"context"
	"time"
)

type StoragePort interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]KeyValue, error)
	Batch(ctx context.Context, ops []Operation) error
}

type ExtendedStoragePort interface {
	StoragePort
	
	GetWithConsistency(ctx context.Context, key string, level ConsistencyLevel) ([]byte, error)
	SetConsistencyLevel(level ConsistencyLevel)
	
	UpdateConfig(config StorageConfig) error
	GetConfig() StorageConfig
	
	SetQuorumConfig(config QuorumReadConfig)
	SetForwardingEnabled(enabled bool)
	SetCacheSize(size int64)
}

type KeyValue struct {
	Key   string
	Value []byte
}

type Operation struct {
	Type  OperationType
	Key   string
	Value []byte
}

type OperationType int

const (
	OpPut OperationType = iota
	OpDelete
)

type ConsistencyLevel int

const (
	ConsistencyLeader ConsistencyLevel = iota
	ConsistencyEventual
	ConsistencyQuorum
)

type QuorumReadConfig struct {
	MinReplicas      int
	ReadTimeout      time.Duration
	ConsistencyLevel ConsistencyLevel
	RetryAttempts    int
}

type StorageConfig struct {
	DefaultConsistencyLevel ConsistencyLevel
	QuorumConfig           QuorumReadConfig
	ForwardingEnabled      bool
	ForwardingTimeout      time.Duration
	CacheSize              int64
	CacheTTL               time.Duration
	MaxRetries             int
	RetryBackoff           time.Duration
	BatchSize              int
	CompactionInterval     time.Duration
	MetricsEnabled         bool
	HealthCheckInterval    time.Duration
}