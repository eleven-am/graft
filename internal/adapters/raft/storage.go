package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	raftbadger "github.com/rfyiamcool/raft-badger"
)

type Storage struct {
	db          *badger.DB
	logStore    raft.LogStore
	stableStore raft.StableStore
	snapStore   raft.SnapshotStore
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

func NewStorage(dataDir string, logger *slog.Logger) (*Storage, error) {
	if logger == nil {
		logger = slog.Default()
	}
	
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", dataDir, err)
	}

	logPath := filepath.Join(dataDir, "raft-log")
	snapPath := filepath.Join(dataDir, "snapshots")
	statePath := filepath.Join(dataDir, "state")

	if err := os.MkdirAll(snapPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory %s: %w", snapPath, err)
	}

	badgerOpts := badger.DefaultOptions(logPath)
	badgerOpts.Logger = &badgerLogger{logger: logger.With("component", "badger-raft")}

	raftStore, err := raftbadger.New(raftbadger.Config{
		DataPath: logPath,
	}, &badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft store: %w", err)
	}

	snapStore, err := raft.NewFileSnapshotStore(snapPath, 3, os.Stderr)
	if err != nil {
		raftStore.Close()
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	stateOpts := badger.DefaultOptions(statePath)
	stateOpts.Logger = &badgerLogger{logger: logger.With("component", "badger-state")}

	stateDB, err := badger.Open(stateOpts)
	if err != nil {
		raftStore.Close()
		return nil, fmt.Errorf("failed to open state database: %w", err)
	}

	return &Storage{
		db:          stateDB,
		logStore:    compatLog{raftStore},
		stableStore: compatStable{raftStore},
		snapStore:   snapStore,
	}, nil
}























func (s *Storage) Close() error {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return fmt.Errorf("failed to close state db: %w", err)
		}
	}

	if closer, ok := s.logStore.(interface{ Close() error }); ok && closer != nil {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("failed to close raft store: %w", err)
		}
	}

	return nil
}


func (s *Storage) LogStore() raft.LogStore {
	return s.logStore
}

func (s *Storage) StableStore() raft.StableStore {
	return s.stableStore
}

func (s *Storage) SnapshotStore() raft.SnapshotStore {
	return s.snapStore
}

func (s *Storage) StateDB() *badger.DB {
	return s.db
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
