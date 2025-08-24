package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type MockStorage struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		data: make(map[string][]byte),
	}
}

func (m *MockStorage) Put(ctx context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = make([]byte, len(value))
	copy(m.data[key], value)
	return nil
}

func (m *MockStorage) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, exists := m.data[key]
	if !exists {
		return nil, ports.ErrKeyNotFound
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (m *MockStorage) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *MockStorage) List(ctx context.Context, prefix string) ([]ports.KeyValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []ports.KeyValue
	for key, value := range m.data {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, ports.KeyValue{
				Key:   key,
				Value: make([]byte, len(value)),
			})
			copy(result[len(result)-1].Value, value)
		}
	}
	return result, nil
}

func (m *MockStorage) Batch(ctx context.Context, operations []ports.Operation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, op := range operations {
		switch op.Type {
		case ports.OpPut:
			m.data[op.Key] = make([]byte, len(op.Value))
			copy(m.data[op.Key], op.Value)
		case ports.OpDelete:
			delete(m.data, op.Key)
		default:
			return fmt.Errorf("unsupported operation type: %v", op.Type)
		}
	}
	return nil
}

func (m *MockStorage) Close() error {
	return nil
}

func TestQueueAdapter_EnqueueReady(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	item := ports.QueueItem{
		ID:         "test-item-1",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
	}

	ctx := context.Background()
	err := queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	items, err := storage.List(ctx, readyPrefix)
	if err != nil {
		t.Fatalf("Failed to list ready items: %v", err)
	}

	if len(items) != 1 {
		t.Errorf("Expected 1 ready item, got %d", len(items))
	}
}

func TestQueueAdapter_EnqueuePending(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypePending, slog.Default())

	item := ports.QueueItem{
		ID:         "test-item-1",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
	}

	ctx := context.Background()
	err := queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	items, err := storage.List(ctx, pendingPrefix)
	if err != nil {
		t.Fatalf("Failed to list pending items: %v", err)
	}

	if len(items) != 1 {
		t.Errorf("Expected 1 pending item, got %d", len(items))
	}
}

func TestQueueAdapter_DequeueReady(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	item := ports.QueueItem{
		ID:         "test-item-1",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	ctx := context.Background()
	err := queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	dequeued, err := queue.Dequeue(ctx, ports.WithClaim("test-node", 5*time.Minute))
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if dequeued == nil {
		t.Fatal("Expected dequeued item, got nil")
	}

	if dequeued.ID != item.ID {
		t.Errorf("Expected item ID %s, got %s", item.ID, dequeued.ID)
	}

	// Check that item was moved from ready queue to claims
	readyItems, err := storage.List(ctx, readyPrefix)
	if err != nil {
		t.Fatalf("Failed to list ready items: %v", err)
	}

	if len(readyItems) != 0 {
		t.Errorf("Expected 0 ready items after dequeue, got %d", len(readyItems))
	}

	claimItems, err := storage.List(ctx, claimPrefix)
	if err != nil {
		t.Fatalf("Failed to list claim items: %v", err)
	}

	if len(claimItems) != 1 {
		t.Errorf("Expected 1 claim item after dequeue, got %d", len(claimItems))
	}
}

func TestQueueAdapter_DequeuePending(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypePending, slog.Default())

	item := ports.QueueItem{
		ID:         "test-item-1",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	ctx := context.Background()
	err := queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Dequeue from pending queue should fail
	_, err = queue.Dequeue(ctx, ports.WithClaim("test-node", 5*time.Minute))
	if err == nil {
		t.Fatal("Expected dequeue from pending queue to fail, but it succeeded")
	}
}

func TestQueueAdapter_GetItems(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	item := ports.QueueItem{
		ID:         "test-item-1",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
	}

	ctx := context.Background()
	err := queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	items, err := queue.GetItems(ctx)
	if err != nil {
		t.Fatalf("GetItems failed: %v", err)
	}

	if len(items) != 1 {
		t.Errorf("Expected 1 item, got %d", len(items))
	}

	if items[0].ID != item.ID {
		t.Errorf("Expected item ID %s, got %s", item.ID, items[0].ID)
	}
}

func TestQueueAdapter_RemoveItem(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	item := ports.QueueItem{
		ID:         "test-item-1",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
	}

	ctx := context.Background()
	err := queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	err = queue.RemoveItem(ctx, item.ID)
	if err != nil {
		t.Fatalf("RemoveItem failed: %v", err)
	}

	items, err := queue.GetItems(ctx)
	if err != nil {
		t.Fatalf("GetItems failed: %v", err)
	}

	if len(items) != 0 {
		t.Errorf("Expected 0 items after removal, got %d", len(items))
	}
}

func TestQueueAdapter_EnqueueBatch(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	items := []ports.QueueItem{
		{
			ID:         "test-item-1",
			WorkflowID: "workflow-1",
			NodeName:   "node-1",
			Priority:   1,
		},
		{
			ID:         "test-item-2",
			WorkflowID: "workflow-1",
			NodeName:   "node-2",
			Priority:   2,
		},
	}

	ctx := context.Background()
	err := queue.EnqueueBatch(ctx, items)
	if err != nil {
		t.Fatalf("EnqueueBatch failed: %v", err)
	}

	retrievedItems, err := queue.GetItems(ctx)
	if err != nil {
		t.Fatalf("GetItems failed: %v", err)
	}

	if len(retrievedItems) != 2 {
		t.Errorf("Expected 2 items, got %d", len(retrievedItems))
	}
}

func TestQueueAdapter_IsEmpty(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	// Should be empty initially
	empty, err := queue.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty failed: %v", err)
	}
	if !empty {
		t.Error("Expected queue to be empty initially")
	}

	// Add an item
	item := ports.QueueItem{
		ID:         "test-item-1",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
	}

	err = queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Should not be empty now
	empty, err = queue.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty failed: %v", err)
	}
	if empty {
		t.Error("Expected queue to not be empty after adding item")
	}
}
