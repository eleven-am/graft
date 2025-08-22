package raftimpl

import (
	"context"
	"errors"
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
	"github.com/stretchr/testify/mock"
)

type MockApplyFuture struct {
	mock.Mock
}

func (m *MockApplyFuture) Error() error {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(error)
}

func (m *MockApplyFuture) Response() interface{} {
	args := m.Called()
	return args.Get(0)
}

func (m *MockApplyFuture) Index() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

type MockRaftForAdapter struct {
	mock.Mock
}

func (m *MockRaftForAdapter) State() raft.RaftState {
	args := m.Called()
	return args.Get(0).(raft.RaftState)
}

func (m *MockRaftForAdapter) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	args := m.Called(cmd, timeout)
	return args.Get(0).(raft.ApplyFuture)
}

func (m *MockRaftForAdapter) LeaderWithID() (raft.ServerAddress, raft.ServerID) {
	args := m.Called()
	return args.Get(0).(raft.ServerAddress), args.Get(1).(raft.ServerID)
}

func (m *MockRaftForAdapter) GetConfiguration() raft.ConfigurationFuture {
	args := m.Called()
	return args.Get(0).(raft.ConfigurationFuture)
}

func (m *MockRaftForAdapter) RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	args := m.Called(id, prevIndex, timeout)
	return args.Get(0).(raft.IndexFuture)
}

func (m *MockRaftForAdapter) AddVoter(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	args := m.Called(id, address, prevIndex, timeout)
	return args.Get(0).(raft.IndexFuture)
}

func (m *MockRaftForAdapter) Stats() map[string]string {
	args := m.Called()
	return args.Get(0).(map[string]string)
}

func (m *MockRaftForAdapter) Shutdown() raft.Future {
	args := m.Called()
	return args.Get(0).(raft.Future)
}

func (m *MockRaftForAdapter) Snapshot() raft.SnapshotFuture {
	args := m.Called()
	return args.Get(0).(raft.SnapshotFuture)
}

func (m *MockRaftForAdapter) BootstrapCluster(configuration raft.Configuration) raft.Future {
	args := m.Called(configuration)
	return args.Get(0).(raft.Future)
}

func TestAdapterPutLeaderPath(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "adapter-put-leader-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("put as leader success", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockFuture := new(MockApplyFuture)
		mockTransport := NewMockTransportPort()

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		adapter := &StorageAdapter{
			node:      node,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		mockRaft.On("State").Return(raft.Leader)
		mockRaft.On("LeaderWithID").Return(raft.ServerAddress("127.0.0.1:8300"), raft.ServerID("leader-node"))
		mockFuture.On("Error").Return(nil)
		mockFuture.On("Response").Return(&CommandResult{Success: true})
		mockRaft.On("Apply", mock.Anything, mock.Anything).Return(mockFuture)

		ctx := context.Background()
		err := adapter.Put(ctx, "key1", []byte("value1"))
		assert.NoError(t, err)

		mockRaft.AssertExpectations(t)
		mockFuture.AssertExpectations(t)
	})

	t.Run("put as leader apply error", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockFuture := new(MockApplyFuture)
		mockTransport := NewMockTransportPort()

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		adapter := &StorageAdapter{
			node:      node,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		mockRaft.On("State").Return(raft.Leader)
		mockRaft.On("LeaderWithID").Return(raft.ServerAddress("127.0.0.1:8300"), raft.ServerID("leader-node"))
		mockFuture.On("Error").Return(errors.New("apply failed"))
		mockRaft.On("Apply", mock.Anything, mock.Anything).Return(mockFuture)

		ctx := context.Background()
		err := adapter.Put(ctx, "key1", []byte("value1"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to apply put command")

		mockRaft.AssertExpectations(t)
		mockFuture.AssertExpectations(t)
	})

	t.Run("put as leader command failed", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockFuture := new(MockApplyFuture)
		mockTransport := NewMockTransportPort()

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		adapter := &StorageAdapter{
			node:      node,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		mockRaft.On("State").Return(raft.Leader)
		mockRaft.On("LeaderWithID").Return(raft.ServerAddress("127.0.0.1:8300"), raft.ServerID("leader-node"))
		mockFuture.On("Error").Return(nil)
		mockFuture.On("Response").Return(&CommandResult{Success: false, Error: "command failed"})
		mockRaft.On("Apply", mock.Anything, mock.Anything).Return(mockFuture)

		ctx := context.Background()
		err := adapter.Put(ctx, "key1", []byte("value1"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to apply put command")

		mockRaft.AssertExpectations(t)
		mockFuture.AssertExpectations(t)
	})
}

func TestAdapterDeleteLeaderPath(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "adapter-delete-leader-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("delete as leader success", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockFuture := new(MockApplyFuture)
		mockTransport := NewMockTransportPort()

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		adapter := &StorageAdapter{
			node:      node,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		mockRaft.On("State").Return(raft.Leader)
		mockFuture.On("Error").Return(nil)
		mockFuture.On("Response").Return(&CommandResult{Success: true})
		mockRaft.On("Apply", mock.Anything, mock.Anything).Return(mockFuture)

		ctx := context.Background()
		err := adapter.Delete(ctx, "key1")
		assert.NoError(t, err)

		mockRaft.AssertExpectations(t)
		mockFuture.AssertExpectations(t)
	})

	t.Run("delete as leader with response error", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockFuture := new(MockApplyFuture)
		mockTransport := NewMockTransportPort()

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		adapter := &StorageAdapter{
			node:      node,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		mockRaft.On("State").Return(raft.Leader)
		mockFuture.On("Error").Return(nil)
		mockFuture.On("Response").Return(&CommandResult{Success: false, Error: "delete failed"})
		mockRaft.On("Apply", mock.Anything, mock.Anything).Return(mockFuture)

		ctx := context.Background()
		err := adapter.Delete(ctx, "key1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to apply delete command")

		mockRaft.AssertExpectations(t)
		mockFuture.AssertExpectations(t)
	})
}

func TestAdapterBatchLeaderPath(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "adapter-batch-leader-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("batch as leader success", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockFuture := new(MockApplyFuture)
		mockTransport := NewMockTransportPort()

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		adapter := &StorageAdapter{
			node:      node,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		mockRaft.On("State").Return(raft.Leader)
		mockFuture.On("Error").Return(nil)
		mockFuture.On("Response").Return(&CommandResult{Success: true})
		mockRaft.On("Apply", mock.Anything, mock.Anything).Return(mockFuture)

		ops := []ports.Operation{
			{Type: ports.OpPut, Key: "key1", Value: []byte("value1")},
			{Type: ports.OpDelete, Key: "key2"},
		}

		ctx := context.Background()
		err := adapter.Batch(ctx, ops)
		assert.NoError(t, err)

		mockRaft.AssertExpectations(t)
		mockFuture.AssertExpectations(t)
	})

	t.Run("batch as leader with apply error", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockFuture := new(MockApplyFuture)
		mockTransport := NewMockTransportPort()

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		adapter := &StorageAdapter{
			node:      node,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		mockRaft.On("State").Return(raft.Leader)
		mockFuture.On("Error").Return(errors.New("batch apply failed"))
		mockRaft.On("Apply", mock.Anything, mock.Anything).Return(mockFuture)

		ops := []ports.Operation{
			{Type: ports.OpPut, Key: "key1", Value: []byte("value1")},
		}

		ctx := context.Background()
		err := adapter.Batch(ctx, ops)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to apply batch command")

		mockRaft.AssertExpectations(t)
		mockFuture.AssertExpectations(t)
	})

	t.Run("batch as leader with response failure", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockFuture := new(MockApplyFuture)
		mockTransport := NewMockTransportPort()

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		adapter := &StorageAdapter{
			node:      node,
			db:        db,
			transport: mockTransport,
			logger:    logger,
			config:    DefaultStorageConfig(),
		}

		mockRaft.On("State").Return(raft.Leader)
		mockFuture.On("Error").Return(nil)
		mockFuture.On("Response").Return(&CommandResult{Success: false, Error: "batch command failed"})
		mockRaft.On("Apply", mock.Anything, mock.Anything).Return(mockFuture)

		ops := []ports.Operation{
			{Type: ports.OpPut, Key: "key1", Value: []byte("value1")},
		}

		ctx := context.Background()
		err := adapter.Batch(ctx, ops)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to apply batch command")

		mockRaft.AssertExpectations(t)
		mockFuture.AssertExpectations(t)
	})
}
