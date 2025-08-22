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
	"github.com/stretchr/testify/assert"
)

func TestOperationTypeToCommandType(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	dir := filepath.Join(os.TempDir(), "batch-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()

	mockRaft := new(MockRaftForAdapter)
	mockRaft.On("State").Return(raft.Follower)
	node := &RaftNode{raft: mockRaft, logger: logger}

	adapter := &StorageAdapter{
		node:      node,
		db:        db,
		transport: nil,
		logger:    logger,
		config:    DefaultStorageConfig(),
	}

	t.Run("Batch operation type conversion", func(t *testing.T) {
		ops := []ports.Operation{
			{Type: ports.OpPut, Key: "key1", Value: []byte("value1")},
			{Type: ports.OpDelete, Key: "key2"},
		}

		err := adapter.Batch(context.Background(), ops)
		assert.Contains(t, err.Error(), "transport not configured")
	})

	mockRaft.AssertExpectations(t)
}

func TestBatchCommandCreation(t *testing.T) {
	ops := []BatchOp{
		{Type: CommandPut, Key: "key1", Value: []byte("value1")},
		{Type: CommandDelete, Key: "key2"},
	}

	cmd := NewBatchCommand(ops)
	assert.Equal(t, CommandBatch, cmd.Type)
	assert.Equal(t, ops, cmd.Batch)
	assert.NotZero(t, cmd.Timestamp)

	data, err := cmd.Marshal()
	assert.NoError(t, err)
	assert.NotEmpty(t, data)
}
