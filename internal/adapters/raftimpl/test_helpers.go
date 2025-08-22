package raftimpl

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
)

func setupTestBadgerDB(t *testing.T) (*badger.DB, func()) {
	dir := filepath.Join(os.TempDir(), "graft-test-"+time.Now().Format("20060102150405"))
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close test database: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Failed to remove test directory: %v", err)
		}
	}

	return db, cleanup
}

func setupTestBadgerDBWithName(t *testing.T, name string) (*badger.DB, func()) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("graft-%s-test-%s", name, time.Now().Format("20060102150405")))
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close test database: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Failed to remove test directory: %v", err)
		}
	}

	return db, cleanup
}

func setupTestFSM(t *testing.T) (*GraftFSM, *badger.DB, func()) {
	db, dbCleanup := setupTestBadgerDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	fsm := NewGraftFSM(db, logger)
	
	cleanup := func() {
		dbCleanup()
	}

	return fsm, db, cleanup
}

func setupTestFSMWithName(t *testing.T, name string) (*GraftFSM, *badger.DB, func()) {
	db, dbCleanup := setupTestBadgerDBWithName(t, name)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	fsm := NewGraftFSM(db, logger)
	
	cleanup := func() {
		dbCleanup()
	}

	return fsm, db, cleanup
}