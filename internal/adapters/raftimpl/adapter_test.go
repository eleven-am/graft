package raftimpl

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/mock"
)


func setupTestAdapter(t *testing.T) (*StorageAdapter, *badger.DB, func()) {
	dir := filepath.Join(os.TempDir(), "graft-adapter-test-"+time.Now().Format("20060102150405"))
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	mockRaft := new(MockRaftForAdapter)
	
	// Mock State() for IsLeader() checks
	mockRaft.On("State").Return(raft.Leader)
	mockRaft.On("LeaderWithID").Return(raft.ServerAddress("127.0.0.1:8300"), raft.ServerID("leader-node"))
	
	mockApplyFuture := new(MockApplyFuture)
	mockApplyFuture.On("Error").Return(nil)
	mockApplyFuture.On("Response").Return(&CommandResult{Success: true})
	mockRaft.On("Apply", mock.Anything, mock.Anything).Return(mockApplyFuture)
	
	mockNode := &RaftNode{
		raft:   mockRaft,
		logger: logger,
	}
	
	mockTransport := NewMockTransportPort()
	
	adapter := NewStorageAdapter(mockNode, db, mockTransport, logger)

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	return adapter, db, cleanup
}

func TestNewStorageAdapter(t *testing.T) {
	adapter, _, cleanup := setupTestAdapter(t)
	defer cleanup()

	if adapter == nil {
		t.Fatal("Expected non-nil adapter")
	}

	if adapter.config.DefaultConsistencyLevel != ConsistencyLeader {
		t.Errorf("Expected default consistency level to be Leader, got %v", adapter.config.DefaultConsistencyLevel)
	}

	if adapter.config.ForwardingTimeout != 5*time.Second {
		t.Errorf("Expected default timeout to be 5s, got %v", adapter.config.ForwardingTimeout)
	}
}

func TestStorageAdapterGet(t *testing.T) {
	adapter, db, cleanup := setupTestAdapter(t)
	defer cleanup()

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("test-key"), []byte("test-value"))
	})
	if err != nil {
		t.Fatalf("Failed to store test value: %v", err)
	}

	ctx := context.Background()
	value, err := adapter.Get(ctx, "test-key")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if string(value) != "test-value" {
		t.Errorf("Expected 'test-value', got '%s'", string(value))
	}
}

func TestStorageAdapterGetNotFound(t *testing.T) {
	adapter, _, cleanup := setupTestAdapter(t)
	defer cleanup()

	ctx := context.Background()
	_, err := adapter.Get(ctx, "non-existent-key")
	if err == nil {
		t.Error("Expected error for non-existent key")
	}
}

func TestStorageAdapterList(t *testing.T) {
	adapter, db, cleanup := setupTestAdapter(t)
	defer cleanup()

	err := db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte("prefix:key1"), []byte("value1"))
		txn.Set([]byte("prefix:key2"), []byte("value2"))
		txn.Set([]byte("different:key"), []byte("other"))
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to store test values: %v", err)
	}

	ctx := context.Background()
	results, err := adapter.List(ctx, "prefix:")
	if err != nil {
		t.Fatalf("Failed to list values: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	foundKeys := make(map[string]bool)
	for _, kv := range results {
		foundKeys[kv.Key] = true
	}

	if !foundKeys["prefix:key1"] {
		t.Error("Expected to find prefix:key1")
	}
	if !foundKeys["prefix:key2"] {
		t.Error("Expected to find prefix:key2")
	}
}

func TestStorageAdapterSetConsistencyLevel(t *testing.T) {
	adapter, _, cleanup := setupTestAdapter(t)
	defer cleanup()

	adapter.SetConsistencyLevel(ConsistencyEventual)
	if adapter.config.DefaultConsistencyLevel != ConsistencyEventual {
		t.Errorf("Expected consistency level to be Eventual, got %v", adapter.config.DefaultConsistencyLevel)
	}

	adapter.SetConsistencyLevel(ConsistencyQuorum)
	if adapter.config.DefaultConsistencyLevel != ConsistencyQuorum {
		t.Errorf("Expected consistency level to be Quorum, got %v", adapter.config.DefaultConsistencyLevel)
	}
}

func TestStorageAdapterForwardToLeader(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-forward-test-"+time.Now().Format("20060102150405"))
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}
	defer db.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	mockNode := &RaftNode{
		logger: logger,
		raft:   &raft.Raft{},
	}

	t.Run("SuccessfulForwarding", func(t *testing.T) {
		mockTransport := NewMockTransportPort()
		adapter := NewStorageAdapter(mockNode, db, mockTransport, logger)

		cmd := NewPutCommand("test-key", []byte("test-value"))
		err := adapter.forwardToLeader(context.Background(), cmd)
		if err != nil {
			t.Errorf("Expected successful forwarding, got error: %v", err)
		}
	})

	t.Run("TransportError", func(t *testing.T) {
		mockTransport := NewMockTransportPortWithError()
		adapter := NewStorageAdapter(mockNode, db, mockTransport, logger)

		cmd := NewPutCommand("test-key", []byte("test-value"))
		err := adapter.forwardToLeader(context.Background(), cmd)
		if err == nil {
			t.Error("Expected error from transport")
		}
	})

	t.Run("FailureResponse", func(t *testing.T) {
		mockTransport := NewMockTransportPortWithFailure()
		adapter := NewStorageAdapter(mockNode, db, mockTransport, logger)

		cmd := NewPutCommand("test-key", []byte("test-value"))
		err := adapter.forwardToLeader(context.Background(), cmd)
		if err == nil {
			t.Error("Expected error from failure response")
		}
	})

	t.Run("NilTransport", func(t *testing.T) {
		adapter := NewStorageAdapter(mockNode, db, nil, logger)

		cmd := NewPutCommand("test-key", []byte("test-value"))
		err := adapter.forwardToLeader(context.Background(), cmd)
		if err == nil {
			t.Error("Expected error for nil transport")
		}
	})
}

func TestStorageAdapterOperationTypeConversion(t *testing.T) {
	adapter, _, cleanup := setupTestAdapter(t)
	defer cleanup()

	tests := []struct {
		opType   ports.OperationType
		expected CommandType
	}{
		{ports.OpPut, CommandPut},
		{ports.OpDelete, CommandDelete},
	}

	for _, test := range tests {
		result := adapter.operationTypeToCommandType(test.opType)
		if result != test.expected {
			t.Errorf("OperationType %v: expected %v, got %v", test.opType, test.expected, result)
		}
	}
}

func TestStorageAdapterPut(t *testing.T) {
	t.Run("PutAsLeader", func(t *testing.T) {
		adapter, _, cleanup := setupTestAdapter(t)
		defer cleanup()

		ctx := context.Background()
		err := adapter.Put(ctx, "test-key", []byte("test-value"))
		if err != nil {
			t.Errorf("Expected successful forwarding, got error: %v", err)
		}
	})

	t.Run("PutWithNilTransport", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), "graft-put-test-"+time.Now().Format("20060102150405"))
		os.MkdirAll(dir, 0755)
		defer os.RemoveAll(dir)

		opts := badger.DefaultOptions(dir)
		opts.Logger = nil
		db, _ := badger.Open(opts)
		defer db.Close()

		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		mockNode := &RaftNode{logger: logger}
		adapter := NewStorageAdapter(mockNode, db, nil, logger)

		ctx := context.Background()
		err := adapter.Put(ctx, "test-key", []byte("test-value"))
		if err == nil {
			t.Error("Expected error for nil transport when not leader")
		}
	})
}

func TestStorageAdapterDelete(t *testing.T) {
	t.Run("DeleteAsNonLeader", func(t *testing.T) {
		adapter, _, cleanup := setupTestAdapter(t)
		defer cleanup()

		ctx := context.Background()
		err := adapter.Delete(ctx, "test-key")
		if err != nil {
			t.Errorf("Expected successful forwarding, got error: %v", err)
		}
	})

	t.Run("DeleteWithTransportError", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), "graft-delete-test-"+time.Now().Format("20060102150405"))
		os.MkdirAll(dir, 0755)
		defer os.RemoveAll(dir)

		opts := badger.DefaultOptions(dir)
		opts.Logger = nil
		db, _ := badger.Open(opts)
		defer db.Close()

		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		mockNode := &RaftNode{logger: logger}
		mockTransport := NewMockTransportPortWithError()
		adapter := NewStorageAdapter(mockNode, db, mockTransport, logger)

		ctx := context.Background()
		err := adapter.Delete(ctx, "test-key")
		if err == nil {
			t.Error("Expected error from transport")
		}
	})
}

func TestStorageAdapterBatch(t *testing.T) {
	t.Run("BatchAsNonLeader", func(t *testing.T) {
		adapter, _, cleanup := setupTestAdapter(t)
		defer cleanup()

		ops := []ports.Operation{
			{Type: ports.OpPut, Key: "key1", Value: []byte("value1")},
			{Type: ports.OpDelete, Key: "key2"},
		}

		ctx := context.Background()
		err := adapter.Batch(ctx, ops)
		if err != nil {
			t.Errorf("Expected successful forwarding, got error: %v", err)
		}
	})

	t.Run("BatchWithFailureResponse", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), "graft-batch-test-"+time.Now().Format("20060102150405"))
		os.MkdirAll(dir, 0755)
		defer os.RemoveAll(dir)

		opts := badger.DefaultOptions(dir)
		opts.Logger = nil
		db, _ := badger.Open(opts)
		defer db.Close()

		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		mockNode := &RaftNode{logger: logger}
		mockTransport := NewMockTransportPortWithFailure()
		adapter := NewStorageAdapter(mockNode, db, mockTransport, logger)

		ops := []ports.Operation{
			{Type: ports.OpPut, Key: "key1", Value: []byte("value1")},
		}

		ctx := context.Background()
		err := adapter.Batch(ctx, ops)
		if err == nil {
			t.Error("Expected error from failure response")
		}
	})

	t.Run("EmptyBatch", func(t *testing.T) {
		adapter, _, cleanup := setupTestAdapter(t)
		defer cleanup()

		ctx := context.Background()
		err := adapter.Batch(ctx, []ports.Operation{})
		if err != nil {
			t.Errorf("Empty batch should not error: %v", err)
		}
	})
}