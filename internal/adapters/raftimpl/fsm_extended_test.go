package raftimpl

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

func TestFSMApplyPutError(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-fsm-put-error-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsm := NewGraftFSM(db, logger)

	db.Close()

	cmd := NewPutCommand("test-key", []byte("test-value"))
	cmdData, _ := json.Marshal(cmd)

	log := &raft.Log{
		Type: raft.LogCommand,
		Data: cmdData,
	}

	result := fsm.Apply(log)
	if cmdResult, ok := result.(*CommandResult); ok {
		if cmdResult.Success {
			t.Error("Expected failure when database is closed")
		}
		if cmdResult.Error == "" {
			t.Error("Expected error message")
		}
	} else {
		t.Error("Expected CommandResult response")
	}
}

func TestFSMApplyDeleteError(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-fsm-delete-error-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsm := NewGraftFSM(db, logger)

	db.Close()

	cmd := NewDeleteCommand("test-key")
	cmdData, _ := json.Marshal(cmd)

	log := &raft.Log{
		Type: raft.LogCommand,
		Data: cmdData,
	}

	result := fsm.Apply(log)
	if cmdResult, ok := result.(*CommandResult); ok {
		if cmdResult.Success {
			t.Error("Expected failure when database is closed")
		}
		if cmdResult.Error == "" {
			t.Error("Expected error message")
		}
	} else {
		t.Error("Expected CommandResult response")
	}
}

func TestFSMApplyBatchWithErrors(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-fsm-batch-error-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsm := NewGraftFSM(db, logger)

	ops := []BatchOp{
		{Type: CommandType(99), Key: "key1", Value: []byte("value1")},
	}

	cmd := NewBatchCommand(ops)
	cmdData, _ := json.Marshal(cmd)

	log := &raft.Log{
		Type: raft.LogCommand,
		Data: cmdData,
	}

	result := fsm.Apply(log)
	if cmdResult, ok := result.(*CommandResult); ok {
		if cmdResult.Success {
			t.Error("Expected failure for unknown operation type")
		}
	}

	db.Close()

	ops = []BatchOp{
		{Type: CommandPut, Key: "key1", Value: []byte("value1")},
	}

	cmd = NewBatchCommand(ops)
	cmdData, _ = json.Marshal(cmd)

	log = &raft.Log{
		Type: raft.LogCommand,
		Data: cmdData,
	}

	result = fsm.Apply(log)
	if cmdResult, ok := result.(*CommandResult); ok {
		if cmdResult.Success {
			t.Error("Expected failure when database is closed")
		}
	}
}

func TestFSMApplyStateUpdateError(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-fsm-state-error-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsm := NewGraftFSM(db, logger)

	db.Close()

	cmd := NewStateUpdateCommand("workflow-123", []byte(`{"status":"running"}`))
	cmdData, _ := json.Marshal(cmd)

	log := &raft.Log{
		Type: raft.LogCommand,
		Data: cmdData,
	}

	result := fsm.Apply(log)
	if cmdResult, ok := result.(*CommandResult); ok {
		if cmdResult.Success {
			t.Error("Expected failure when database is closed")
		}
		if cmdResult.Error == "" {
			t.Error("Expected error message")
		}
	}
}

func TestFSMApplyQueueOperationError(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-fsm-queue-error-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsm := NewGraftFSM(db, logger)

	db.Close()

	cmd := NewQueueOperationCommand("task-456", []byte(`{"action":"process"}`))
	cmdData, _ := json.Marshal(cmd)

	log := &raft.Log{
		Type: raft.LogCommand,
		Data: cmdData,
	}

	result := fsm.Apply(log)
	if cmdResult, ok := result.(*CommandResult); ok {
		if cmdResult.Success {
			t.Error("Expected failure when database is closed")
		}
		if cmdResult.Error == "" {
			t.Error("Expected error message")
		}
	}
}

func TestFSMApplyUnknownCommandType(t *testing.T) {
	_, db, cleanup := setupTestFSM(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsm := NewGraftFSM(db, logger)

	cmd := &Command{
		Type: CommandType(99),
		Key:  "test-key",
	}
	cmdData, _ := json.Marshal(cmd)

	log := &raft.Log{
		Type: raft.LogCommand,
		Data: cmdData,
	}

	result := fsm.Apply(log)
	if cmdResult, ok := result.(*CommandResult); ok {
		if cmdResult.Success {
			t.Error("Expected failure for unknown command type")
		}
		if cmdResult.Error == "" {
			t.Error("Expected error message for unknown command type")
		}
	}
}

func TestFSMRestoreInvalidJSON(t *testing.T) {
	_, db, cleanup := setupTestFSM(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsm := NewGraftFSM(db, logger)

	reader := io.NopCloser(bytes.NewReader([]byte("invalid json")))
	err := fsm.Restore(reader)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestFSMRestoreWithDBError(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-fsm-restore-error-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fsm := NewGraftFSM(db, logger)

	db.Close()

	snapshotData := struct {
		Version int               `json:"version"`
		Data    map[string][]byte `json:"data"`
	}{
		Version: 1,
		Data: map[string][]byte{
			"key1": []byte("value1"),
		},
	}

	jsonData, _ := json.Marshal(snapshotData)
	reader := io.NopCloser(bytes.NewReader(jsonData))

	err := fsm.Restore(reader)
	if err == nil {
		t.Error("Expected error when database is closed")
	}
}