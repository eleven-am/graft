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
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

func TestGetWithQuorumConsistency(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	dir := filepath.Join(os.TempDir(), "quorum-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)
	
	storeConfig := &StoreConfig{
		DataDir:            dir,
		RetainSnapshots:    3,
		ValueLogFileSize:   100 << 20,
		NumMemtables:       4,
		NumLevelZeroTables: 4,
		NumCompactors:      2,
	}
	store, err := NewStore(storeConfig, logger)
	assert.NoError(t, err)
	defer store.Close()
	
	err = store.StateDB().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("quorum-key"), []byte("quorum-value"))
	})
	assert.NoError(t, err)
	
	mockRaft := new(MockRaftForAdapter)
	// Mock State() for IsLeader() checks  
	mockRaft.On("State").Return(raft.Leader)
	
	node := &RaftNode{raft: mockRaft, logger: logger}
	
	adapter := &StorageAdapter{
		node:             node,
		db:               store.StateDB(),
		logger:           logger,
	}
	
	value, err := adapter.Get(context.Background(), "quorum-key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("quorum-value"), value)
	
	mockRaft.AssertExpectations(t)
}