package ports

import "context"

type StoragePort interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]KeyValue, error)
	Batch(ctx context.Context, ops []Operation) error
	Close() error
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

var ErrKeyNotFound = &KeyNotFoundError{}

type KeyNotFoundError struct{}

func (e *KeyNotFoundError) Error() string {
	return "key not found"
}
