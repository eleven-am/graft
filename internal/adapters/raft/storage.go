package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"log/slog"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/hashicorp/raft"
	raftbadger "github.com/rfyiamcool/raft-badger"
)

// Storage encapsulates the on-disk resources backing a raft node.
type Storage struct {
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	stateDB       *badger.DB
}

const storageComponent = "adapters.raft.Storage"

func newRaftStorageError(message string, cause error, opts ...domain.ErrorOption) *domain.DomainError {
	merged := []domain.ErrorOption{domain.WithComponent(storageComponent)}
	if len(opts) > 0 {
		merged = append(merged, opts...)
	}
	return domain.NewStorageError(message, cause, merged...)
}

// StorageConfig captures the directories used by the storage implementation.
type StorageConfig struct {
	DataDir string
}

func NewStorage(cfg StorageConfig, logger *slog.Logger) (*Storage, error) {
	if logger == nil {
		logger = slog.Default()
	}

	if cfg.DataDir == "" {
		return nil, newRaftStorageError("data directory is required", nil)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, newRaftStorageError(
			"failed to create data directory",
			err,
			domain.WithContextDetail("data_dir", cfg.DataDir),
		)
	}

	logPath := filepath.Join(cfg.DataDir, "raft-log")
	snapshotPath := filepath.Join(cfg.DataDir, "snapshots")
	statePath := filepath.Join(cfg.DataDir, "state")

	if err := os.MkdirAll(snapshotPath, 0o755); err != nil {
		return nil, newRaftStorageError(
			"failed to create snapshot directory",
			err,
			domain.WithContextDetail("snapshot_dir", snapshotPath),
		)
	}

	logOpts := badger.DefaultOptions(logPath)
	logOpts.Logger = &badgerAdapter{logger: logger.With("component", "raft.badger-log")}
	logOpts.MemTableSize = 16 << 20
	logOpts.NumMemtables = 2
	logOpts.NumLevelZeroTables = 2
	logOpts.NumLevelZeroTablesStall = 4
	logOpts.BlockCacheSize = 8 << 20
	logOpts.IndexCacheSize = 8 << 20
	logOpts.ValueLogFileSize = 16 << 20

	store, err := raftbadger.New(raftbadger.Config{DataPath: logPath}, &logOpts)
	if err != nil {
		return nil, newRaftStorageError(
			"failed to open raft log store",
			err,
			domain.WithContextDetail("log_path", logPath),
		)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(snapshotPath, 2, os.Stderr)
	if err != nil {
		_ = store.Close()
		return nil, newRaftStorageError(
			"failed to open snapshot store",
			err,
			domain.WithContextDetail("snapshot_dir", snapshotPath),
		)
	}

	stateOpts := badger.DefaultOptions(statePath)
	stateOpts.Logger = &badgerAdapter{logger: logger.With("component", "raft.badger-state")}
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
		return nil, newRaftStorageError(
			"failed to open state database",
			err,
			domain.WithContextDetail("state_path", statePath),
		)
	}

	return &Storage{
		logStore:      wrapLogStore(store),
		stableStore:   wrapStableStore(store),
		snapshotStore: snapshotStore,
		stateDB:       stateDB,
	}, nil
}

func (s *Storage) resources() *StorageResources {
	return &StorageResources{
		LogStore:      s.logStore,
		StableStore:   s.stableStore,
		SnapshotStore: s.snapshotStore,
		StateDB:       s.stateDB,
		Cleanup:       s.Close,
	}
}

// HasExistingState reports whether any durable raft state has been persisted.
// It inspects the log and snapshot stores, tolerating empty stores when the
// node has never bootstrapped before.
func (s *Storage) HasExistingState() bool {
	if s == nil {
		return false
	}

	if s.logStore != nil {
		if lastIndex, err := s.logStore.LastIndex(); err == nil && lastIndex > 0 {
			return true
		}
	}

	if s.snapshotStore != nil {
		if snapshots, err := s.snapshotStore.List(); err == nil && len(snapshots) > 0 {
			return true
		}
	}

	return false
}

// Close releases all resources held by the storage instance.
func (s *Storage) Close() error {
	var firstErr error

	if err := s.closeState(); err != nil {
		firstErr = errors.Join(firstErr, err)
	}

	if closer, ok := s.logStore.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			firstErr = errors.Join(firstErr, err)
		}
	}
	s.logStore = nil

	if s.stableStore != nil {
		if closer, ok := s.stableStore.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				firstErr = errors.Join(firstErr, err)
			}
		}
	}
	s.stableStore = nil

	if closer, ok := s.snapshotStore.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			firstErr = errors.Join(firstErr, err)
		}
	}
	s.snapshotStore = nil

	return firstErr
}

func (s *Storage) StateDB() *badger.DB {
	return s.stateDB
}

func (s *Storage) closeState() error {
	if s.stateDB == nil {
		return nil
	}
	err := s.stateDB.Close()
	if err == nil {
		s.stateDB = nil
	}
	return err
}

func wrapStableStore(store raft.StableStore) raft.StableStore {
	return stableCompat{StableStore: store}
}

func wrapLogStore(store raft.LogStore) raft.LogStore {
	return logCompat{LogStore: store}
}

type stableCompat struct {
	raft.StableStore
}

func (s stableCompat) Get(key []byte) ([]byte, error) {
	value, err := s.StableStore.Get(key)
	if isNotFound(err) {
		return nil, nil
	}
	return value, err
}

func (s stableCompat) Close() error {
	if closer, ok := s.StableStore.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func (s stableCompat) GetUint64(key []byte) (uint64, error) {
	value, err := s.StableStore.GetUint64(key)
	if isNotFound(err) {
		return 0, nil
	}
	return value, err
}

type logCompat struct {
	raft.LogStore
}

func (l logCompat) GetLog(index uint64, out *raft.Log) error {
	if err := l.LogStore.GetLog(index, out); isNotFound(err) {
		return raft.ErrLogNotFound
	} else {
		return err
	}
}

func (l logCompat) Close() error {
	if closer, ok := l.LogStore.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func (l logCompat) FirstIndex() (uint64, error) {
	idx, err := l.LogStore.FirstIndex()
	if isNotFound(err) {
		return 0, nil
	}
	return idx, err
}

func (l logCompat) LastIndex() (uint64, error) {
	idx, err := l.LogStore.LastIndex()
	if isNotFound(err) {
		return 0, nil
	}
	return idx, err
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, badger.ErrKeyNotFound) ||
		strings.Contains(err.Error(), "not found") ||
		strings.Contains(err.Error(), "no such key")
}

type badgerAdapter struct {
	logger *slog.Logger
}

func (b *badgerAdapter) Errorf(format string, args ...interface{}) {
	b.logger.Error(fmt.Sprintf(format, args...))
}

func (b *badgerAdapter) Warningf(format string, args ...interface{}) {
	b.logger.Warn(fmt.Sprintf(format, args...))
}

func (b *badgerAdapter) Infof(format string, args ...interface{}) {
}

func (b *badgerAdapter) Debugf(format string, args ...interface{}) {
}
