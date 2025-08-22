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
)

func TestStorageAdapterPutAsLeader(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-put-leader-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("SuccessfulPutAsNonLeader", func(t *testing.T) {
		mockNode := &RaftNode{
			logger: logger,
		}

		mockTransport := NewMockTransportPort()
		adapter := &StorageAdapter{
			node:      mockNode,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		ctx := context.Background()
		err := adapter.Put(ctx, "test-key", []byte("test-value"))
		if err != nil {
			t.Errorf("Expected successful forwarding, got error: %v", err)
		}
	})

	t.Run("PutWithTransportError", func(t *testing.T) {
		mockNode := &RaftNode{
			logger: logger,
		}

		mockTransport := NewMockTransportPortWithError()
		adapter := &StorageAdapter{
			node:      mockNode,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		ctx := context.Background()
		err := adapter.Put(ctx, "test-key", []byte("test-value"))
		if err == nil {
			t.Error("Expected error from transport")
		}
	})
}

func TestStorageAdapterDeleteAsLeader(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-delete-leader-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("SuccessfulDeleteAsNonLeader", func(t *testing.T) {
		mockNode := &RaftNode{
			logger: logger,
		}

		mockTransport := NewMockTransportPort()
		adapter := &StorageAdapter{
			node:      mockNode,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		ctx := context.Background()
		err := adapter.Delete(ctx, "test-key")
		if err != nil {
			t.Errorf("Expected successful forwarding, got error: %v", err)
		}
	})

	t.Run("DeleteWithFailureResponse", func(t *testing.T) {
		mockNode := &RaftNode{
			raft:   nil,
			logger: logger,
		}

		mockTransport := NewMockTransportPortWithFailure()
		adapter := &StorageAdapter{
			node:      mockNode,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		ctx := context.Background()
		err := adapter.Delete(ctx, "test-key")
		if err == nil {
			t.Error("Expected error from failure response")
		}
	})
}

func TestStorageAdapterBatchAsLeader(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-batch-leader-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("SuccessfulBatchAsNonLeader", func(t *testing.T) {
		mockNode := &RaftNode{
			raft:   nil,
			logger: logger,
		}

		mockTransport := NewMockTransportPort()
		adapter := &StorageAdapter{
			node:      mockNode,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

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

	t.Run("BatchWithTransportError", func(t *testing.T) {
		mockNode := &RaftNode{
			raft:   nil,
			logger: logger,
		}

		mockTransport := NewMockTransportPortWithError()
		adapter := &StorageAdapter{
			node:      mockNode,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		ops := []ports.Operation{
			{Type: ports.OpPut, Key: "key1", Value: []byte("value1")},
		}

		ctx := context.Background()
		err := adapter.Batch(ctx, ops)
		if err == nil {
			t.Error("Expected error from transport")
		}
	})
}
