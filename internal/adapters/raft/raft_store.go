package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/hashicorp/raft"
	raftbadger "github.com/rfyiamcool/raft-badger"
)

type StoreConfig struct {
	DataDir            string
	RetainSnapshots    int
	SnapshotThreshold  uint64
	Compression        options.CompressionType
	EncryptionKey      []byte
	ValueLogFileSize   int64
	NumMemtables       int
	NumLevelZeroTables int
	NumCompactors      int
}

type Store struct {
	config      *StoreConfig
	logStore    raft.LogStore
	stableStore raft.StableStore
	snapStore   raft.SnapshotStore
	stateDB     *badger.DB
	logger      *slog.Logger
	gcQuit      chan struct{}
	closed      bool
	mu          sync.Mutex
}

type compatStable struct {
	raft.StableStore
}

func (c compatStable) Get(key []byte) ([]byte, error) {
	v, err := c.StableStore.Get(key)
	if isNotFound(err) {
		return nil, nil
	}
	return v, err
}

func (c compatStable) GetUint64(key []byte) (uint64, error) {
	v, err := c.StableStore.GetUint64(key)
	if isNotFound(err) {
		return 0, nil
	}
	return v, err
}

func (c compatStable) Close() error {
	if closer, ok := c.StableStore.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

type compatLog struct {
	raft.LogStore
}

func (c compatLog) GetLog(index uint64, out *raft.Log) error {
	err := c.LogStore.GetLog(index, out)
	if isNotFound(err) {
		return raft.ErrLogNotFound
	}
	return err
}

func (c compatLog) FirstIndex() (uint64, error) {
	idx, err := c.LogStore.FirstIndex()
	if isNotFound(err) {
		return 0, nil
	}
	return idx, err
}

func (c compatLog) LastIndex() (uint64, error) {
	idx, err := c.LogStore.LastIndex()
	if isNotFound(err) {
		return 0, nil
	}
	return idx, err
}

func (c compatLog) Close() error {
	if closer, ok := c.LogStore.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, badger.ErrKeyNotFound) ||
		strings.Contains(err.Error(), "not found") ||
		strings.Contains(err.Error(), "no such key")
}

func NewStore(config *StoreConfig, logger *slog.Logger) (*Store, error) {
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", config.DataDir, err)
	}

	logPath := filepath.Join(config.DataDir, "raft-log")
	stablePath := filepath.Join(config.DataDir, "raft-stable")
	snapPath := filepath.Join(config.DataDir, "snapshots")
	statePath := filepath.Join(config.DataDir, "state")

	if err := os.MkdirAll(snapPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory %s: %w", snapPath, err)
	}

	badgerOpts := badger.DefaultOptions(logPath)
	badgerOpts.Compression = config.Compression
	badgerOpts.ValueLogFileSize = config.ValueLogFileSize
	badgerOpts.NumMemtables = config.NumMemtables
	badgerOpts.NumLevelZeroTables = config.NumLevelZeroTables
	badgerOpts.NumCompactors = config.NumCompactors
	badgerOpts.Logger = &badgerLogger{logger: logger.With("component", "badger-log")}

	if len(config.EncryptionKey) > 0 {
		badgerOpts.EncryptionKey = config.EncryptionKey
	}

	logStore, err := raftbadger.New(raftbadger.Config{
		DataPath: logPath,
	}, &badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create log store at %s: %w", logPath, err)
	}

	stableOpts := badger.DefaultOptions(stablePath)
	stableOpts.Compression = config.Compression
	stableOpts.Logger = &badgerLogger{logger: logger.With("component", "badger-stable")}
	if len(config.EncryptionKey) > 0 {
		stableOpts.EncryptionKey = config.EncryptionKey
	}

	stableStore, err := raftbadger.New(raftbadger.Config{
		DataPath: stablePath,
	}, &stableOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store at %s: %w", stablePath, err)
	}

	snapStore, err := raft.NewFileSnapshotStore(snapPath, config.RetainSnapshots, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store at %s: %w", snapPath, err)
	}

	stateOpts := badger.DefaultOptions(statePath)
	stateOpts.Compression = config.Compression
	stateOpts.Logger = &badgerLogger{logger: logger.With("component", "badger-state")}
	if len(config.EncryptionKey) > 0 {
		stateOpts.EncryptionKey = config.EncryptionKey
	}

	stateDB, err := badger.Open(stateOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open state database at %s: %w", statePath, err)
	}

	gcQuit := make(chan struct{})
	go runGarbageCollection(stateDB, logger, gcQuit)

	return &Store{
		config:      config,
		logStore:    compatLog{logStore},
		stableStore: compatStable{stableStore},
		snapStore:   snapStore,
		stateDB:     stateDB,
		logger:      logger,
		gcQuit:      gcQuit,
	}, nil
}

func (s *Store) LogStore() raft.LogStore {
	return s.logStore
}

func (s *Store) StableStore() raft.StableStore {
	return s.stableStore
}

func (s *Store) SnapshotStore() raft.SnapshotStore {
	return s.snapStore
}

func (s *Store) StateDB() *badger.DB {
	return s.stateDB
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("store already closed")
	}

	if s.gcQuit != nil {
		close(s.gcQuit)
	}

	var firstErr error

	if err := s.stateDB.Close(); err != nil {
		s.logger.Error("failed to close state database", "error", err)
		firstErr = err
	}

	if ls, ok := s.logStore.(interface{ Close() error }); ok {
		if err := ls.Close(); err != nil {
			s.logger.Error("failed to close log store", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if ss, ok := s.stableStore.(interface{ Close() error }); ok {
		if err := ss.Close(); err != nil {
			s.logger.Error("failed to close stable store", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	s.closed = true
	return firstErr
}

func runGarbageCollection(db *badger.DB, logger *slog.Logger, quit chan struct{}) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-quit:
			logger.Debug("stopping garbage collection")
			return
		case <-ticker.C:
			lsm, vlog := db.Size()
			logger.Debug("running garbage collection",
				"lsm_size", lsm,
				"vlog_size", vlog)

			err := db.RunValueLogGC(0.5)
			if err != nil && !errors.Is(err, badger.ErrNoRewrite) {
				logger.Error("garbage collection failed", "error", err)
			}
		}
	}
}

type badgerLogger struct {
	logger *slog.Logger
}

func (l *badgerLogger) Errorf(f string, v ...interface{}) {
	l.logger.Error(fmt.Sprintf(f, v...))
}

func (l *badgerLogger) Warningf(f string, v ...interface{}) {
	l.logger.Warn(fmt.Sprintf(f, v...))
}

func (l *badgerLogger) Infof(f string, v ...interface{}) {
	l.logger.Info(fmt.Sprintf(f, v...))
}

func (l *badgerLogger) Debugf(f string, v ...interface{}) {
	l.logger.Debug(fmt.Sprintf(f, v...))
}
