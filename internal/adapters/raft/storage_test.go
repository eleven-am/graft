package raft

import (
	"log/slog"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStorage(t *testing.T) {
	tempDir := t.TempDir()
	
	storage, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)
	require.NotNil(t, storage)
	defer storage.Close()
	
	assert.NotNil(t, storage.db)
	assert.NotNil(t, storage.logStore)
	assert.NotNil(t, storage.stableStore)
	assert.NotNil(t, storage.snapStore)
}

func TestStorage_LogStore(t *testing.T) {
	tempDir := t.TempDir()
	
	storage, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)
	defer storage.Close()
	
	logStore := storage.LogStore()
	require.NotNil(t, logStore)
	
	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  []byte("test"),
	}
	
	err = logStore.StoreLog(log)
	assert.NoError(t, err)
	
	retrieved := &raft.Log{}
	err = logStore.GetLog(1, retrieved)
	assert.NoError(t, err)
	assert.Equal(t, log.Data, retrieved.Data)
	assert.Equal(t, log.Index, retrieved.Index)
	assert.Equal(t, log.Term, retrieved.Term)
}

func TestStorage_StableStore(t *testing.T) {
	tempDir := t.TempDir()
	
	storage, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)
	defer storage.Close()
	
	stableStore := storage.StableStore()
	require.NotNil(t, stableStore)
	
	err = stableStore.SetUint64([]byte("test-key"), 42)
	assert.NoError(t, err)
	
	value, err := stableStore.GetUint64([]byte("test-key"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), value)
}

func TestStorage_SnapshotStore(t *testing.T) {
	tempDir := t.TempDir()
	
	storage, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)
	defer storage.Close()
	
	snapStore := storage.SnapshotStore()
	require.NotNil(t, snapStore)
	
	snapshots, err := snapStore.List()
	assert.NoError(t, err)
	assert.Empty(t, snapshots)
}

func TestStorage_StateDB(t *testing.T) {
	tempDir := t.TempDir()
	
	storage, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)
	defer storage.Close()
	
	db := storage.StateDB()
	require.NotNil(t, db)
	
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("test"), []byte("value"))
	})
	assert.NoError(t, err)
	
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("test"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			assert.Equal(t, []byte("value"), val)
			return nil
		})
	})
	assert.NoError(t, err)
}

func TestStorage_Close(t *testing.T) {
	tempDir := t.TempDir()
	
	storage, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)
	require.NotNil(t, storage)
	
	err = storage.Close()
	assert.NoError(t, err)
}

func TestStorage_PersistenceAcrossRestarts(t *testing.T) {
	tempDir := t.TempDir()
	
	storage1, err := NewStorage(tempDir, nil)
	require.NoError(t, err)
	
	err = storage1.StableStore().SetUint64([]byte("term"), 5)
	require.NoError(t, err)
	
	log := &raft.Log{
		Index: 10,
		Term:  5,
		Type:  raft.LogCommand,
		Data:  []byte("persistent"),
	}
	err = storage1.LogStore().StoreLog(log)
	require.NoError(t, err)
	
	err = storage1.Close()
	require.NoError(t, err)
	
	storage2, err := NewStorage(tempDir, nil)
	require.NoError(t, err)
	defer storage2.Close()
	
	term, err := storage2.StableStore().GetUint64([]byte("term"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), term)
	
	retrieved := &raft.Log{}
	err = storage2.LogStore().GetLog(10, retrieved)
	assert.NoError(t, err)
	assert.Equal(t, log.Data, retrieved.Data)
}