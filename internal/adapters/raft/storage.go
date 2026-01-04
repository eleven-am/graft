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

// Storage encapsulates the resources backing a raft node.
type Storage struct {
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	stateDB       *badger.DB
	inMemory      bool
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
	DataDir  string
	InMemory bool
}

func NewInMemoryStorage(logger *slog.Logger) (*Storage, error) {
	if logger == nil {
		logger = slog.Default()
	}

	inmemStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	stateOpts := badger.DefaultOptions("")
	stateOpts.InMemory = true
	stateOpts.Logger = &badgerAdapter{logger: logger.With("component", "raft.badger-state")}

	stateDB, err := badger.Open(stateOpts)
	if err != nil {
		return nil, newRaftStorageError("failed to open in-memory state database", err)
	}

	return &Storage{
		logStore:      wrapLogStore(inmemStore),
		stableStore:   wrapStableStore(inmemStore),
		snapshotStore: snapshotStore,
		stateDB:       stateDB,
		inMemory:      true,
	}, nil
}

func NewStorage(cfg StorageConfig, logger *slog.Logger) (*Storage, error) {
	if cfg.InMemory {
		return NewInMemoryStorage(logger)
	}
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
	statePath := filepath.Join(cfg.DataDir, "state")

	logOpts := badger.DefaultOptions(logPath)
	logOpts.Logger = &badgerAdapter{logger: logger.With("component", "raft.badger-log")}
	logOpts.MemTableSize = 32 << 20
	logOpts.NumMemtables = 2
	logOpts.NumLevelZeroTables = 2
	logOpts.NumLevelZeroTablesStall = 4
	logOpts.BlockCacheSize = 64 << 20
	logOpts.IndexCacheSize = 32 << 20
	logOpts.ValueLogFileSize = 16 << 20

	store, err := raftbadger.New(raftbadger.Config{DataPath: logPath}, &logOpts)
	if err != nil {
		return nil, newRaftStorageError(
			"failed to open raft log store",
			err,
			domain.WithContextDetail("log_path", logPath),
		)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		_ = store.Close()
		return nil, newRaftStorageError(
			"failed to open snapshot store",
			err,
			domain.WithContextDetail("snapshot_dir", cfg.DataDir),
		)
	}

	stateOpts := badger.DefaultOptions(statePath)
	stateOpts.Logger = &badgerAdapter{logger: logger.With("component", "raft.badger-state")}
	stateOpts.MemTableSize = 32 << 20
	stateOpts.NumMemtables = 2
	stateOpts.NumLevelZeroTables = 2
	stateOpts.NumLevelZeroTablesStall = 4
	stateOpts.BlockCacheSize = 64 << 20
	stateOpts.IndexCacheSize = 32 << 20
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

const batchDeleteSize = 5000

func (l logCompat) DeleteRange(min, max uint64) error {
	type badgerStore interface {
		GetDB() *badger.DB
	}

	store, ok := l.LogStore.(badgerStore)
	if !ok {
		return l.LogStore.DeleteRange(min, max)
	}

	db := store.GetDB()
	if db == nil {
		return l.LogStore.DeleteRange(min, max)
	}

	prefixDBLogs := []byte("logs-")

	buildLogsKey := func(idx uint64) []byte {
		bs := append([]byte{}, prefixDBLogs...)
		b := make([]byte, 8)
		b[0] = byte(idx >> 56)
		b[1] = byte(idx >> 48)
		b[2] = byte(idx >> 40)
		b[3] = byte(idx >> 32)
		b[4] = byte(idx >> 24)
		b[5] = byte(idx >> 16)
		b[6] = byte(idx >> 8)
		b[7] = byte(idx)
		return append(bs, b...)
	}

	parseIndexByLogsKey := func(key []byte) uint64 {
		rawkey := key[len(prefixDBLogs):]
		if len(rawkey) < 8 {
			return 0
		}
		return uint64(rawkey[0])<<56 | uint64(rawkey[1])<<48 |
			uint64(rawkey[2])<<40 | uint64(rawkey[3])<<32 |
			uint64(rawkey[4])<<24 | uint64(rawkey[5])<<16 |
			uint64(rawkey[6])<<8 | uint64(rawkey[7])
	}

	for {
		var keysToDelete [][]byte

		err := db.View(func(txn *badger.Txn) error {
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()

			minKey := buildLogsKey(min)
			for iter.Seek(minKey); iter.ValidForPrefix(prefixDBLogs); iter.Next() {
				item := iter.Item()
				idx := parseIndexByLogsKey(item.Key())
				if idx > max {
					break
				}
				keysToDelete = append(keysToDelete, item.KeyCopy(nil))
				if len(keysToDelete) >= batchDeleteSize {
					break
				}
			}
			return nil
		})
		if err != nil {
			return err
		}

		if len(keysToDelete) == 0 {
			break
		}

		err = db.Update(func(txn *badger.Txn) error {
			for _, key := range keysToDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}

		if len(keysToDelete) < batchDeleteSize {
			break
		}
	}

	return nil
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
