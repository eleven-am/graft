package ports

import (
	"time"
)

type StoragePort interface {
	Get(key string) (value []byte, version int64, exists bool, err error)
	Put(key string, value []byte, version int64) error
	PutWithTTL(key string, value []byte, version int64, ttl time.Duration) error
	Delete(key string) error

	Exists(key string) (bool, error)
	GetMetadata(key string) (*KeyMetadata, error)

	BatchWrite(ops []WriteOp) error

	GetNext(prefix string) (key string, value []byte, exists bool, err error)
	GetNextAfter(prefix string, afterKey string) (key string, value []byte, exists bool, err error)
	CountPrefix(prefix string) (count int, err error)
	AtomicIncrement(key string) (newValue int64, err error)

	ListByPrefix(prefix string) ([]KeyValueVersion, error)
	DeleteByPrefix(prefix string) (deletedCount int, err error)

	GetVersion(key string) (int64, error)

	ExpireAt(key string, expireTime time.Time) error
	GetTTL(key string) (time.Duration, error)
	CleanExpired() (cleanedCount int, err error)

	RunInTransaction(fn func(tx Transaction) error) error

	SetRaftNode(node RaftNode)
	SetEventManager(ev EventManager)
	Close() error
}

type Transaction interface {
	Get(key string) (value []byte, version int64, exists bool, err error)
	Put(key string, value []byte, version int64) error
	PutWithTTL(key string, value []byte, version int64, ttl time.Duration) error
	Delete(key string) error
	Exists(key string) (bool, error)
	GetMetadata(key string) (*KeyMetadata, error)
	Commit() error
	Rollback() error
}

type WriteOp struct {
	Type    OpType
	Key     string
	Value   []byte
	Version int64
	TTL     time.Duration
}

type KeyValueVersion struct {
	Key      string
	Value    []byte
	Version  int64
	ExpireAt *time.Time
	Metadata *KeyMetadata
}

type KeyMetadata struct {
	Key      string
	Version  int64
	Size     int64
	ExpireAt *time.Time
	Created  time.Time
	Updated  time.Time
}

type OpType int

const (
	OpPut OpType = iota
	OpDelete
	OpExpire
	OpDeleteIfExists
)
