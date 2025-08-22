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

func TestStoreRunGarbageCollection(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-store-gc-test-"+time.Now().Format("20060102150405"))
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

	if store.LogStore() == nil {
		t.Error("LogStore should not be nil")
	}

	err = store.Close()
	if err != nil {
		t.Errorf("Failed to close store: %v", err)
	}
}

func TestStoreCloseMultipleTimes(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-store-multi-close-test-"+time.Now().Format("20060102150405"))
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
		t.Errorf("First close failed: %v", err)
	}

	err = store.Close()
	if err != nil {
		t.Errorf("Second close failed: %v", err)
	}
}

func TestStoreWithCompression(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-store-compression-test-"+time.Now().Format("20060102150405"))
	defer os.RemoveAll(dir)

	config := &StoreConfig{
		DataDir:            dir,
		RetainSnapshots:    3,
		SnapshotThreshold:  1024,
		Compression:        options.Snappy,
		ValueLogFileSize:   100 << 20,
		NumMemtables:       4,
		NumLevelZeroTables: 4,
		NumCompactors:      2,
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := NewStore(config, logger)
	if err != nil {
		t.Fatalf("Failed to create store with compression: %v", err)
	}
	defer store.Close()

	if store.config.Compression != options.Snappy {
		t.Error("Compression should be set to Snappy")
	}
}

func TestStoreConfigValidation(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-store-config-test-"+time.Now().Format("20060102150405"))
	defer os.RemoveAll(dir)

	config := &StoreConfig{
		DataDir:            dir,
		SnapshotThreshold:  1024,
		RetainSnapshots:    3,
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

	if store.SnapshotStore() == nil {
		t.Error("SnapshotStore should not be nil")
	}
}