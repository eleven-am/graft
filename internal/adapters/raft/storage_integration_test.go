package raft

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func TestStorage_CreatesDirectoriesCorrectly(t *testing.T) {
	tmpDir := t.TempDir()
	logger := slog.Default()

	storage, err := NewStorage(StorageConfig{DataDir: tmpDir}, logger)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	defer storage.Close()

	snapshotsDir := filepath.Join(tmpDir, "snapshots")
	if _, err := os.Stat(snapshotsDir); os.IsNotExist(err) {
		t.Errorf("expected snapshots directory to exist at %s", snapshotsDir)
	}

	raftLogDir := filepath.Join(tmpDir, "raft-log")
	if _, err := os.Stat(raftLogDir); os.IsNotExist(err) {
		t.Errorf("expected raft-log directory to exist at %s", raftLogDir)
	}

	stateDir := filepath.Join(tmpDir, "state")
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		t.Errorf("expected state directory to exist at %s", stateDir)
	}
}

func TestStorage_WorksAfterClearExistingState(t *testing.T) {
	tmpDir := t.TempDir()
	logger := slog.Default()

	raftDir := filepath.Join(tmpDir, "raft")
	storage1, err := NewStorage(StorageConfig{DataDir: raftDir}, logger)
	if err != nil {
		t.Fatalf("first NewStorage failed: %v", err)
	}
	if err := storage1.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}

	rt := &Runtime{deps: RuntimeDeps{Logger: logger}}
	if err := rt.clearExistingState(tmpDir); err != nil {
		t.Fatalf("clearExistingState failed: %v", err)
	}

	storage2, err := NewStorage(StorageConfig{DataDir: raftDir}, logger)
	if err != nil {
		t.Fatalf("NewStorage after clear failed: %v", err)
	}
	defer storage2.Close()

	hasState, err := raft.HasExistingState(storage2.logStore, storage2.stableStore, storage2.snapshotStore)
	if err != nil {
		t.Fatalf("HasExistingState failed: %v", err)
	}
	if hasState {
		t.Error("expected no existing state after clear")
	}
}

func TestStorage_HasExistingStateWorks(t *testing.T) {
	tmpDir := t.TempDir()
	logger := slog.Default()

	storage, err := NewStorage(StorageConfig{DataDir: tmpDir}, logger)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	defer storage.Close()

	hasState, err := raft.HasExistingState(storage.logStore, storage.stableStore, storage.snapshotStore)
	if err != nil {
		t.Fatalf("HasExistingState failed: %v", err)
	}
	if hasState {
		t.Error("expected no existing state on fresh storage")
	}
}
