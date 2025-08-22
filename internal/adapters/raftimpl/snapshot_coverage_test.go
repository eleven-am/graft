package raftimpl

import (
	"bytes"
	"errors"
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

type errorSink struct {
	failAt int
	count  int
}

func (e *errorSink) Write(p []byte) (n int, err error) {
	e.count++
	if e.count >= e.failAt {
		return 0, errors.New("write error")
	}
	return len(p), nil
}

func (e *errorSink) Close() error {
	return nil
}

func (e *errorSink) ID() string {
	return "error-sink"
}

func (e *errorSink) Cancel() error {
	return nil
}

func TestSnapshotPersistEdgeCases(t *testing.T) {
	t.Run("persist with write error during metadata", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), "snapshot-persist-test-"+time.Now().Format("20060102150405"))
		os.MkdirAll(dir, 0755)
		defer os.RemoveAll(dir)
		
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		
		opts := badger.DefaultOptions(dir)
		opts.Logger = nil
		db, err := badger.Open(opts)
		assert.NoError(t, err)
		defer db.Close()
		
		err = db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte("key1"), []byte("value1"))
		})
		assert.NoError(t, err)
		
		snapshot := &FSMSnapshot{
			db:     db,
			logger: logger,
		}
		
		sink := &errorSink{failAt: 1}
		err = snapshot.Persist(sink)
		assert.Error(t, err)
	})
	
	t.Run("persist with empty database", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), "snapshot-empty-test-"+time.Now().Format("20060102150405"))
		os.MkdirAll(dir, 0755)
		defer os.RemoveAll(dir)
		
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		
		opts := badger.DefaultOptions(dir)
		opts.Logger = nil
		db, err := badger.Open(opts)
		assert.NoError(t, err)
		defer db.Close()
		
		snapshot := &FSMSnapshot{
			db:     db,
			logger: logger,
		}
		
		sink := &BufferSink{}
		err = snapshot.Persist(sink)
		assert.NoError(t, err)
		
		assert.True(t, sink.Len() > 0)
	})
	
	t.Run("persist with large dataset", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), "snapshot-large-test-"+time.Now().Format("20060102150405"))
		os.MkdirAll(dir, 0755)
		defer os.RemoveAll(dir)
		
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		
		opts := badger.DefaultOptions(dir)
		opts.Logger = nil
		db, err := badger.Open(opts)
		assert.NoError(t, err)
		defer db.Close()
		
		batch := db.NewWriteBatch()
		for i := 0; i < 1000; i++ {
			key := []byte(filepath.Join("key", string(rune(i%256)), string(rune(i/256))))
			value := make([]byte, 100)
			err := batch.Set(key, value)
			assert.NoError(t, err)
		}
		err = batch.Flush()
		assert.NoError(t, err)
		
		snapshot := &FSMSnapshot{
			db:     db,
			logger: logger,
		}
		
		sink := &BufferSink{}
		err = snapshot.Persist(sink)
		assert.NoError(t, err)
		
		assert.True(t, sink.Len() > 100)
	})
}

func TestSnapshotRelease(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	snapshot := &FSMSnapshot{
		db:     nil,
		logger: logger,
	}
	
	snapshot.Release()
}

func TestFSMSnapshotWithActiveTransactions(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "fsm-snapshot-txn-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)
	
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()
	
	txn := db.NewTransaction(false)
	defer txn.Discard()
	
	fsm := &GraftFSM{
		db:     db,
		logger: logger,
	}
	
	snapshot, err := fsm.Snapshot()
	assert.NoError(t, err)
	assert.NotNil(t, snapshot)
}

type MockSnapshotSink struct {
	raft.SnapshotSink
	data bytes.Buffer
}

func (m *MockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.data.Write(p)
}

func (m *MockSnapshotSink) Close() error {
	return nil
}

func (m *MockSnapshotSink) ID() string {
	return "mock-snapshot"
}

func (m *MockSnapshotSink) Cancel() error {
	return nil
}

func TestSnapshotPersistWithMockSink(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "snapshot-mock-sink-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)
	
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()
	
	err = db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 10; i++ {
			key := []byte(filepath.Join("test", string(rune('a'+i))))
			value := []byte(filepath.Join("value", string(rune('0'+i))))
			if err := txn.Set(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(t, err)
	
	snapshot := &FSMSnapshot{
		db:     db,
		logger: logger,
	}
	
	sink := &MockSnapshotSink{}
	err = snapshot.Persist(sink)
	assert.NoError(t, err)
	
	assert.True(t, sink.data.Len() > 0)
}