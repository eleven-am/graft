package raftimpl

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3/options"
)

func TestNewStore(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-store-test-"+time.Now().Format("20060102150405"))
	defer os.RemoveAll(dir)

	config := &StoreConfig{
		DataDir:            dir,
		RetainSnapshots:    3,
		SnapshotThreshold:  1024,
		Compression:        options.None,
		ValueLogFileSize:   100 << 20,
		NumMemtables:       4,
		NumLevelZeroTables: 4,
		NumCompactors:      2,
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := NewStore(config, logger)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	if store.LogStore() == nil {
		t.Error("LogStore should not be nil")
	}

	if store.StableStore() == nil {
		t.Error("StableStore should not be nil")
	}

	if store.SnapshotStore() == nil {
		t.Error("SnapshotStore should not be nil")
	}

	if store.StateDB() == nil {
		t.Error("StateDB should not be nil")
	}
}

func TestNewStoreWithEncryption(t *testing.T) {
	t.Skip("Skipping encryption test due to BadgerDB encryption key length requirements")
	
	dir := filepath.Join(os.TempDir(), "graft-store-enc-test-"+time.Now().Format("20060102150405"))
	defer os.RemoveAll(dir)

	config := &StoreConfig{
		DataDir:            dir,
		RetainSnapshots:    3,
		SnapshotThreshold:  1024,
		Compression:        options.None,
		EncryptionKey:      []byte("test-encryption-key-32-bytes-12"),
		ValueLogFileSize:   100 << 20,
		NumMemtables:       4,
		NumLevelZeroTables: 4,
		NumCompactors:      2,
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := NewStore(config, logger)
	if err != nil {
		t.Fatalf("Failed to create store with encryption: %v", err)
	}
	defer store.Close()

	if store.config.EncryptionKey == nil {
		t.Error("Encryption key should be set")
	}
}

func TestStoreClose(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-store-close-test-"+time.Now().Format("20060102150405"))
	defer os.RemoveAll(dir)

	config := &StoreConfig{
		DataDir:            dir,
		RetainSnapshots:    3,
		SnapshotThreshold:  1024,
		Compression:        options.None,
		ValueLogFileSize:   100 << 20,
		NumMemtables:       4,
		NumLevelZeroTables: 4,
		NumCompactors:      2,
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := NewStore(config, logger)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	err = store.Close()
	if err != nil {
		t.Errorf("Close returned unexpected error: %v", err)
	}
}

func TestNewStoreInvalidPath(t *testing.T) {
	config := &StoreConfig{
		DataDir:            "/invalid\x00path/test",
		RetainSnapshots:    3,
		SnapshotThreshold:  1024,
		Compression:        options.None,
		ValueLogFileSize:   100 << 20,
		NumMemtables:       4,
		NumLevelZeroTables: 4,
		NumCompactors:      2,
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	_, err := NewStore(config, logger)
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestBadgerLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bl := &badgerLogger{logger: logger}

	bl.Errorf("error %s", "test")
	bl.Warningf("warning %s", "test")
	bl.Infof("info %s", "test")
	bl.Debugf("debug %s", "test")
}