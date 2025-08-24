package semaphore

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type MockStorage struct {
	data map[string][]byte
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		data: make(map[string][]byte),
	}
}

func (m *MockStorage) Put(ctx context.Context, key string, value []byte) error {
	m.data[key] = make([]byte, len(value))
	copy(m.data[key], value)
	return nil
}

func (m *MockStorage) Get(ctx context.Context, key string) ([]byte, error) {
	value, exists := m.data[key]
	if !exists {
		return nil, ports.ErrKeyNotFound
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (m *MockStorage) Delete(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}

func (m *MockStorage) List(ctx context.Context, prefix string) ([]ports.KeyValue, error) {
	var results []ports.KeyValue
	for key, value := range m.data {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result := make([]byte, len(value))
			copy(result, value)
			results = append(results, ports.KeyValue{
				Key:   key,
				Value: result,
			})
		}
	}
	return results, nil
}

func (m *MockStorage) Batch(ctx context.Context, ops []ports.Operation) error {
	for _, op := range ops {
		switch op.Type {
		case ports.OpPut:
			m.Put(ctx, op.Key, op.Value)
		case ports.OpDelete:
			m.Delete(ctx, op.Key)
		}
	}
	return nil
}

func (m *MockStorage) Close() error { return nil }

func createTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError, // Only show errors in tests to reduce noise
	}))
}

func TestRecord_IsActive(t *testing.T) {
	now := time.Now()

	active := &Record{
		Status:    StatusAcquired,
		ExpiresAt: now.Add(5 * time.Minute),
	}

	if !active.IsActive() {
		t.Error("Expected record to be active")
	}

	expired := &Record{
		Status:    StatusAcquired,
		ExpiresAt: now.Add(-5 * time.Minute),
	}

	if expired.IsActive() {
		t.Error("Expected expired record to not be active")
	}

	wrongStatus := &Record{
		Status:    StatusExpired,
		ExpiresAt: now.Add(5 * time.Minute),
	}

	if wrongStatus.IsActive() {
		t.Error("Expected record with expired status to not be active")
	}
}

func TestRecord_Extend(t *testing.T) {
	record := &Record{}

	before := time.Now()
	record.Extend(10 * time.Minute)
	after := time.Now()

	expectedMin := before.Add(10 * time.Minute)
	expectedMax := after.Add(10 * time.Minute)

	if record.ExpiresAt.Before(expectedMin) || record.ExpiresAt.After(expectedMax) {
		t.Errorf("Extended time not in expected range: got %v, expected between %v and %v",
			record.ExpiresAt, expectedMin, expectedMax)
	}
}

func TestAdapter_Acquire_New(t *testing.T) {
	storage := NewMockStorage()
	adapter := NewAdapter(storage, createTestLogger())

	ctx := context.Background()
	err := adapter.Acquire(ctx, "test-semaphore", "node-1", 5*time.Minute)

	if err != nil {
		t.Errorf("Failed to acquire new semaphore: %v", err)
	}

	acquired, err := adapter.IsAcquiredByNode(ctx, "test-semaphore", "node-1")
	if err != nil {
		t.Errorf("Failed to check if acquired by node: %v", err)
	}

	if !acquired {
		t.Error("Expected semaphore to be acquired by node-1")
	}
}

func TestAdapter_Acquire_AlreadyHeld(t *testing.T) {
	storage := NewMockStorage()
	adapter := NewAdapter(storage, createTestLogger())

	ctx := context.Background()

	adapter.Acquire(ctx, "test-semaphore", "node-1", 5*time.Minute)

	err := adapter.Acquire(ctx, "test-semaphore", "node-2", 5*time.Minute)

	if err == nil {
		t.Error("Expected error when trying to acquire already held semaphore")
	}
}

func TestAdapter_Acquire_SameNode(t *testing.T) {
	storage := NewMockStorage()
	adapter := NewAdapter(storage, createTestLogger())

	ctx := context.Background()

	adapter.Acquire(ctx, "test-semaphore", "node-1", 1*time.Second)

	time.Sleep(500 * time.Millisecond)
	err := adapter.Acquire(ctx, "test-semaphore", "node-1", 5*time.Minute)

	if err != nil {
		t.Errorf("Failed to re-acquire with same node: %v", err)
	}

	acquired, err := adapter.IsAcquiredByNode(ctx, "test-semaphore", "node-1")
	if err != nil {
		t.Errorf("Failed to check if still acquired: %v", err)
	}
	if !acquired {
		t.Error("Expected semaphore to still be acquired by node-1 after extension")
	}
}

func TestAdapter_Release_Success(t *testing.T) {
	storage := NewMockStorage()
	adapter := NewAdapter(storage, createTestLogger())

	ctx := context.Background()

	adapter.Acquire(ctx, "test-semaphore", "node-1", 5*time.Minute)

	err := adapter.Release(ctx, "test-semaphore", "node-1")

	if err != nil {
		t.Errorf("Failed to release semaphore: %v", err)
	}

	acquired, err := adapter.IsAcquired(ctx, "test-semaphore")
	if err != nil {
		t.Errorf("Failed to check if acquired: %v", err)
	}

	if acquired {
		t.Error("Expected semaphore to be released")
	}
}

func TestAdapter_Release_WrongNode(t *testing.T) {
	storage := NewMockStorage()
	adapter := NewAdapter(storage, createTestLogger())

	ctx := context.Background()

	adapter.Acquire(ctx, "test-semaphore", "node-1", 5*time.Minute)

	err := adapter.Release(ctx, "test-semaphore", "node-2")

	if err == nil {
		t.Error("Expected error when releasing with wrong node")
	}
}

func TestAdapter_Release_NotExists(t *testing.T) {
	storage := NewMockStorage()
	adapter := NewAdapter(storage, createTestLogger())

	ctx := context.Background()

	err := adapter.Release(ctx, "nonexistent", "node-1")

	if err == nil {
		t.Error("Expected error when releasing non-existent semaphore")
	}
}

func TestAdapter_IsAcquired_NotExists(t *testing.T) {
	storage := NewMockStorage()
	adapter := NewAdapter(storage, createTestLogger())

	ctx := context.Background()

	acquired, err := adapter.IsAcquired(ctx, "nonexistent")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if acquired {
		t.Error("Expected non-existent semaphore to not be acquired")
	}
}

func TestAdapter_Extend_Success(t *testing.T) {
	storage := NewMockStorage()
	adapter := NewAdapter(storage, createTestLogger())

	ctx := context.Background()

	adapter.Acquire(ctx, "test-semaphore", "node-1", 1*time.Second)

	err := adapter.Extend(ctx, "test-semaphore", "node-1", 5*time.Minute)

	if err != nil {
		t.Errorf("Failed to extend semaphore: %v", err)
	}

	acquired, err := adapter.IsAcquired(ctx, "test-semaphore")
	if err != nil {
		t.Errorf("Failed to check if acquired: %v", err)
	}

	if !acquired {
		t.Error("Expected semaphore to still be acquired after extension")
	}
}
