package raftimpl

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRaftNodeCreationPaths(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	dir := filepath.Join(os.TempDir(), "raft-creation-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)
	
	storeConfig := &StoreConfig{
		DataDir:            dir,
		RetainSnapshots:    3,
		ValueLogFileSize:   100 << 20, // 100MB
		NumMemtables:       4,
		NumLevelZeroTables: 4,
		NumCompactors:      2,
	}
	store, err := NewStore(storeConfig, logger)
	assert.NoError(t, err)
	if store != nil {
		defer store.Close()
	}
	
	fsm := NewGraftFSM(store.StateDB(), logger)
	
	config := &RaftConfig{
		NodeID:   "test-node",
		BindAddr: "invalid-address-format",
		DataDir:  dir,
	}
	
	_, err = NewRaftNode(config, store, fsm, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid raft bind address")
}

func TestRaftNodeClusterManagement(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	t.Run("AddVoter success", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockConfigFuture := new(MockConfigurationFuture)
		mockIndexFuture := new(MockIndexFuture)
		
		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}
		
		mockConfigFuture.On("Error").Return(nil)
		mockConfigFuture.On("Configuration").Return(raft.Configuration{Servers: []raft.Server{}})
		mockRaft.On("GetConfiguration").Return(mockConfigFuture)
		
		mockIndexFuture.On("Error").Return(nil)
		mockRaft.On("AddVoter", raft.ServerID("node1"), raft.ServerAddress("127.0.0.1:8301"), uint64(0), time.Duration(0)).Return(mockIndexFuture)
		
		err := node.AddVoter("node1", "127.0.0.1:8301")
		assert.NoError(t, err)
		
		mockRaft.AssertExpectations(t)
		mockConfigFuture.AssertExpectations(t)
		mockIndexFuture.AssertExpectations(t)
	})
	
	t.Run("AddVoter with existing node", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockConfigFuture := new(MockConfigurationFuture)
		
		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}
		
		existingServer := raft.Server{
			ID:      raft.ServerID("node1"),
			Address: raft.ServerAddress("127.0.0.1:8301"),
		}
		mockConfigFuture.On("Error").Return(nil)
		mockConfigFuture.On("Configuration").Return(raft.Configuration{Servers: []raft.Server{existingServer}})
		mockRaft.On("GetConfiguration").Return(mockConfigFuture)
		
		_ = node.AddVoter("node1", "127.0.0.1:8301")
		
		mockRaft.AssertExpectations(t)
		mockConfigFuture.AssertExpectations(t)
	})
	
	t.Run("RemoveServer success", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockIndexFuture := new(MockIndexFuture)
		
		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}
		
		mockIndexFuture.On("Error").Return(nil)
		mockRaft.On("RemoveServer", raft.ServerID("node1"), uint64(0), time.Duration(0)).Return(mockIndexFuture)
		
		err := node.RemoveServer("node1")
		assert.NoError(t, err)
		
		mockRaft.AssertExpectations(t)
		mockIndexFuture.AssertExpectations(t)
	})
}

func TestFSMApplyCommandTypes(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "fsm-apply-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)
	
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()
	
	fsm := &GraftFSM{
		db:     db,
		logger: logger,
	}
	
	t.Run("Apply StateUpdate command", func(t *testing.T) {
		stateData, _ := json.Marshal(map[string]interface{}{
			"key1": "value1",
			"key2": 42,
		})
		cmd := NewStateUpdateCommand("state-key", stateData)
		
		cmdData, _ := cmd.Marshal()
		log := &raft.Log{
			Data:  cmdData,
			Index: 1,
			Term:  1,
		}
		result := fsm.Apply(log)
		
		cmdResult, ok := result.(*CommandResult)
		assert.True(t, ok)
		assert.True(t, cmdResult.Success)
	})
	
	t.Run("Apply QueueOperation command", func(t *testing.T) {
		queueData, _ := json.Marshal(map[string]interface{}{
			"operation": "enqueue",
			"queue":     "test-queue",
			"item":      "test-item",
		})
		cmd := NewQueueOperationCommand("queue-key", queueData)
		
		cmdData, _ := cmd.Marshal()
		log := &raft.Log{
			Data:  cmdData,
			Index: 1,
			Term:  1,
		}
		result := fsm.Apply(log)
		
		cmdResult, ok := result.(*CommandResult)
		assert.True(t, ok)
		assert.True(t, cmdResult.Success)
	})
	
	t.Run("Apply invalid command", func(t *testing.T) {
		log := &raft.Log{
			Data:  []byte("invalid json"),
			Index: 1,
			Term:  1,
		}
		result := fsm.Apply(log)
		
		cmdResult, ok := result.(*CommandResult)
		assert.True(t, ok)
		assert.False(t, cmdResult.Success)
		assert.Contains(t, cmdResult.Error, "failed to unmarshal command")
	})
}

func TestFSMRestoreComprehensive(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "fsm-restore-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)
	
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()
	
	fsm := &GraftFSM{
		db:     db,
		logger: logger,
	}
	
	t.Run("Restore with invalid JSON", func(t *testing.T) {
		reader := strings.NewReader("invalid json data")
		err := fsm.Restore(io.NopCloser(reader))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode snapshot")
	})
	
	t.Run("Restore with valid metadata but invalid data", func(t *testing.T) {
		metadataJSON := `{"item_count":1}
invalid item data`
		
		reader := strings.NewReader(metadataJSON)
		_ = fsm.Restore(io.NopCloser(reader))
	})
}

func TestStorageAdapterErrorPaths(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "adapter-error-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)
	
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()
	
	t.Run("forwardToLeader with nil transport", func(t *testing.T) {
		node := &RaftNode{logger: logger}
		adapter := &StorageAdapter{
			node:      node,
			db:        db,
			logger:    logger,
		}
		
		cmd := NewPutCommand("key", []byte("value"))
		err := adapter.forwardToLeader(context.Background(), cmd)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transport not configured")
	})
	
	t.Run("Get with eventual consistency", func(t *testing.T) {
		err := db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte("test-key"), []byte("test-value"))
		})
		assert.NoError(t, err)
		
		node := &RaftNode{raft: nil, logger: logger}
		
		config := DefaultStorageConfig()
		config.DefaultConsistencyLevel = ConsistencyEventual
		adapter := &StorageAdapter{
			node:             node,
			db:               db,
			logger:           logger,
			config:           config,
		}
		
		value, err := adapter.Get(context.Background(), "test-key")
		assert.NoError(t, err)
		assert.Equal(t, []byte("test-value"), value)
	})
}

type MockConfigurationFuture struct {
	mock.Mock
}

func (m *MockConfigurationFuture) Error() error {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(error)
}

func (m *MockConfigurationFuture) Configuration() raft.Configuration {
	args := m.Called()
	return args.Get(0).(raft.Configuration)
}

func (m *MockConfigurationFuture) Index() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

type MockIndexFuture struct {
	mock.Mock
}

func (m *MockIndexFuture) Error() error {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(error)
}

func (m *MockIndexFuture) Index() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

