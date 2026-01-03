package raft

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v3"
	autoconsensus "github.com/eleven-am/auto-consensus"
	"github.com/hashicorp/raft"
	raftbadger "github.com/rfyiamcool/raft-badger"
)

type StorageFactory struct {
	dataDir  string
	inMemory bool
	logger   *slog.Logger

	mu       sync.Mutex
	storage  *Storage
	logStore interface{ Close() error }
}

func NewStorageFactory(dataDir string, inMemory bool, logger *slog.Logger) *StorageFactory {
	if logger == nil {
		logger = slog.Default()
	}
	return &StorageFactory{
		dataDir:  dataDir,
		inMemory: inMemory,
		logger:   logger,
	}
}

func (f *StorageFactory) Create() (autoconsensus.Storages, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.inMemory {
		return f.createInMemory()
	}
	return f.createPersistent()
}

func (f *StorageFactory) createInMemory() (autoconsensus.Storages, error) {
	inmemStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	stateOpts := badger.DefaultOptions("")
	stateOpts.InMemory = true
	stateOpts.Logger = &badgerAdapter{logger: f.logger.With("component", "raft.badger-state")}

	stateDB, err := badger.Open(stateOpts)
	if err != nil {
		return autoconsensus.Storages{}, newRaftStorageError("failed to open in-memory state database", err)
	}

	f.storage = &Storage{
		logStore:      wrapLogStore(inmemStore),
		stableStore:   wrapStableStore(inmemStore),
		snapshotStore: snapshotStore,
		stateDB:       stateDB,
		inMemory:      true,
	}

	return autoconsensus.Storages{
		LogStore:      f.storage.logStore,
		StableStore:   f.storage.stableStore,
		SnapshotStore: f.storage.snapshotStore,
	}, nil
}

func (f *StorageFactory) createPersistent() (autoconsensus.Storages, error) {
	if f.dataDir == "" {
		return autoconsensus.Storages{}, newRaftStorageError("data directory is required", nil)
	}

	if err := os.MkdirAll(f.dataDir, 0o755); err != nil {
		return autoconsensus.Storages{}, newRaftStorageError("failed to create data directory", err)
	}

	logPath := filepath.Join(f.dataDir, "raft-log")
	statePath := filepath.Join(f.dataDir, "state")

	logOpts := badger.DefaultOptions(logPath)
	logOpts.Logger = &badgerAdapter{logger: f.logger.With("component", "raft.badger-log")}
	logOpts.MemTableSize = 16 << 20
	logOpts.NumMemtables = 2
	logOpts.NumLevelZeroTables = 2
	logOpts.NumLevelZeroTablesStall = 4
	logOpts.BlockCacheSize = 8 << 20
	logOpts.IndexCacheSize = 8 << 20
	logOpts.ValueLogFileSize = 16 << 20

	store, err := raftbadger.New(raftbadger.Config{DataPath: logPath}, &logOpts)
	if err != nil {
		return autoconsensus.Storages{}, newRaftStorageError("failed to open raft log store", err)
	}
	f.logStore = store

	snapshotStore, err := raft.NewFileSnapshotStore(f.dataDir, 2, os.Stderr)
	if err != nil {
		_ = store.Close()
		return autoconsensus.Storages{}, newRaftStorageError("failed to open snapshot store", err)
	}

	stateOpts := badger.DefaultOptions(statePath)
	stateOpts.Logger = &badgerAdapter{logger: f.logger.With("component", "raft.badger-state")}
	stateOpts.MemTableSize = 16 << 20
	stateOpts.NumMemtables = 2
	stateOpts.NumLevelZeroTables = 2
	stateOpts.NumLevelZeroTablesStall = 4
	stateOpts.BlockCacheSize = 8 << 20
	stateOpts.IndexCacheSize = 8 << 20
	stateOpts.ValueLogFileSize = 16 << 20

	stateDB, err := badger.Open(stateOpts)
	if err != nil {
		_ = store.Close()
		return autoconsensus.Storages{}, newRaftStorageError("failed to open state database", err)
	}

	f.storage = &Storage{
		logStore:      wrapLogStore(store),
		stableStore:   wrapStableStore(store),
		snapshotStore: snapshotStore,
		stateDB:       stateDB,
		inMemory:      false,
	}

	return autoconsensus.Storages{
		LogStore:      f.storage.logStore,
		StableStore:   f.storage.stableStore,
		SnapshotStore: f.storage.snapshotStore,
	}, nil
}

func (f *StorageFactory) Reset() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.storage != nil {
		if err := f.storage.Close(); err != nil {
			f.logger.Warn("error closing storage during reset", "error", err)
		}
		f.storage = nil
	}

	if f.logStore != nil {
		if err := f.logStore.Close(); err != nil {
			f.logger.Warn("error closing log store during reset", "error", err)
		}
		f.logStore = nil
	}

	if f.inMemory {
		return nil
	}

	logPath := filepath.Join(f.dataDir, "raft-log")
	if err := os.RemoveAll(logPath); err != nil {
		return newRaftStorageError("failed to remove log store", err)
	}

	statePath := filepath.Join(f.dataDir, "state")
	if err := os.RemoveAll(statePath); err != nil {
		return newRaftStorageError("failed to remove state store", err)
	}

	snapshotPath := filepath.Join(f.dataDir, "snapshots")
	if err := os.RemoveAll(snapshotPath); err != nil {
		return newRaftStorageError("failed to remove snapshots", err)
	}

	return nil
}

func (f *StorageFactory) StateDB() *badger.DB {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.storage == nil {
		return nil
	}
	return f.storage.stateDB
}

func (f *StorageFactory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.storage != nil {
		return f.storage.Close()
	}
	return nil
}

var _ autoconsensus.StorageFactory = (*StorageFactory)(nil)
