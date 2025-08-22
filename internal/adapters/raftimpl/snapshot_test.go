package raftimpl

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

type mockSnapshotSink struct {
	buf      *bytes.Buffer
	closed   bool
	canceled bool
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	m.closed = true
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snapshot"
}

func (m *mockSnapshotSink) Cancel() error {
	m.canceled = true
	return nil
}


func TestFSMSnapshotPersist(t *testing.T) {
	_, db, cleanup := setupTestFSMWithName(t, "snapshot")
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("key1"), []byte("value1")); err != nil {
			return err
		}
		if err := txn.Set([]byte("key2"), []byte("value2")); err != nil {
			return err
		}
		if err := txn.Set([]byte("key3"), []byte("value3")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to add test data: %v", err)
	}

	snapshot := &FSMSnapshot{
		db:     db,
		logger: logger,
	}

	sink := &mockSnapshotSink{
		buf: &bytes.Buffer{},
	}

	err = snapshot.Persist(sink)
	if err != nil {
		t.Fatalf("Failed to persist snapshot: %v", err)
	}

	if !sink.closed {
		t.Error("Sink was not closed")
	}

	if sink.canceled {
		t.Error("Sink was canceled unexpectedly")
	}

	gzipReader, err := gzip.NewReader(sink.buf)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	var decompressed bytes.Buffer
	_, err = io.Copy(&decompressed, gzipReader)
	if err != nil {
		t.Fatalf("Failed to decompress snapshot: %v", err)
	}

	var snapshotData struct {
		Version int               `json:"version"`
		Data    map[string][]byte `json:"data"`
	}

	err = json.Unmarshal(decompressed.Bytes(), &snapshotData)
	if err != nil {
		t.Fatalf("Failed to unmarshal snapshot: %v", err)
	}

	if snapshotData.Version != 1 {
		t.Errorf("Expected version 1, got %d", snapshotData.Version)
	}

	if len(snapshotData.Data) != 3 {
		t.Errorf("Expected 3 keys in snapshot, got %d", len(snapshotData.Data))
	}

	if string(snapshotData.Data["key1"]) != "value1" {
		t.Errorf("Expected value1 for key1, got %s", string(snapshotData.Data["key1"]))
	}

	if string(snapshotData.Data["key2"]) != "value2" {
		t.Errorf("Expected value2 for key2, got %s", string(snapshotData.Data["key2"]))
	}

	if string(snapshotData.Data["key3"]) != "value3" {
		t.Errorf("Expected value3 for key3, got %s", string(snapshotData.Data["key3"]))
	}
}

func TestFSMSnapshotPersistEmpty(t *testing.T) {
	_, db, cleanup := setupTestFSMWithName(t, "snapshot")
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	snapshot := &FSMSnapshot{
		db:     db,
		logger: logger,
	}

	sink := &mockSnapshotSink{
		buf: &bytes.Buffer{},
	}

	err := snapshot.Persist(sink)
	if err != nil {
		t.Fatalf("Failed to persist empty snapshot: %v", err)
	}

	if !sink.closed {
		t.Error("Sink was not closed")
	}

	gzipReader, err := gzip.NewReader(sink.buf)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	var decompressed bytes.Buffer
	_, err = io.Copy(&decompressed, gzipReader)
	if err != nil {
		t.Fatalf("Failed to decompress snapshot: %v", err)
	}

	var snapshotData struct {
		Version int               `json:"version"`
		Data    map[string][]byte `json:"data"`
	}

	err = json.Unmarshal(decompressed.Bytes(), &snapshotData)
	if err != nil {
		t.Fatalf("Failed to unmarshal snapshot: %v", err)
	}

	if len(snapshotData.Data) != 0 {
		t.Errorf("Expected 0 keys in empty snapshot, got %d", len(snapshotData.Data))
	}
}

func TestFSMSnapshotRelease(t *testing.T) {
	_, db, cleanup := setupTestFSMWithName(t, "snapshot")
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	snapshot := &FSMSnapshot{
		db:     db,
		logger: logger,
	}

	snapshot.Release()
}

type errorSnapshotSink struct {
	*mockSnapshotSink
}

func (e *errorSnapshotSink) Write(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

func TestFSMSnapshotPersistError(t *testing.T) {
	_, db, cleanup := setupTestFSMWithName(t, "snapshot")
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key1"), []byte("value1"))
	})
	if err != nil {
		t.Fatalf("Failed to add test data: %v", err)
	}

	snapshot := &FSMSnapshot{
		db:     db,
		logger: logger,
	}

	sink := &errorSnapshotSink{
		mockSnapshotSink: &mockSnapshotSink{
			buf: &bytes.Buffer{},
		},
	}

	err = snapshot.Persist(sink)
	if err == nil {
		t.Error("Expected error when persisting to error sink")
	}

	if !sink.canceled {
		t.Error("Sink should have been canceled on error")
	}
}

var _ raft.FSMSnapshot = (*FSMSnapshot)(nil)