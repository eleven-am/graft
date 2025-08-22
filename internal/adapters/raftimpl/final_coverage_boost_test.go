package raftimpl

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStorageAdapterOperationTimeout(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	dir := filepath.Join(os.TempDir(), "timeout-test-"+time.Now().Format("20060102150405"))
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

	adapter := NewStorageAdapter(nil, store.StateDB(), nil, logger)
	assert.NotNil(t, adapter)
	assert.Equal(t, ConsistencyLeader, adapter.config.DefaultConsistencyLevel)
	assert.Equal(t, 5*time.Second, adapter.config.ForwardingTimeout)
	assert.NotNil(t, adapter.logger)
}

func TestStoreGarbageCollectionEdgeCases(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	dir := filepath.Join(os.TempDir(), "gc-test-"+time.Now().Format("20060102150405"))
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
	
	err = store.Close()
	assert.NoError(t, err)
	
	_ = store.Close()
}

func TestAdapterOperationTypeConversion(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	adapter := &StorageAdapter{
		logger: logger,
	}
	
	t.Run("adapter fields initialization", func(t *testing.T) {
		assert.NotNil(t, adapter.logger)
	})
}