package raftimpl

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)


func TestNewGraftFSM(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	if fsm == nil {
		t.Fatal("Expected non-nil FSM")
	}
	if fsm.db != db {
		t.Error("FSM database not set correctly")
	}
	if fsm.logger == nil {
		t.Error("FSM logger not set correctly")
	}
}

func TestFSMApplyPut(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	cmd := NewPutCommand("test-key", []byte("test-value"))
	cmdData, _ := cmd.Marshal()

	log := &raft.Log{
		Data:  cmdData,
		Index: 1,
		Term:  1,
	}

	result := fsm.Apply(log)
	cmdResult, ok := result.(*CommandResult)
	if !ok {
		t.Fatal("Expected CommandResult type")
	}

	if !cmdResult.Success {
		t.Errorf("Expected success, got error: %s", cmdResult.Error)
	}

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("test-key"))
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if string(value) != "test-value" {
			t.Errorf("Expected value 'test-value', got '%s'", string(value))
		}
		return nil
	})

	if err != nil {
		t.Errorf("Failed to verify stored value: %v", err)
	}
}

func TestFSMApplyDelete(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("delete-key"), []byte("value"))
	})

	cmd := NewDeleteCommand("delete-key")
	cmdData, _ := cmd.Marshal()

	log := &raft.Log{
		Data:  cmdData,
		Index: 2,
		Term:  1,
	}

	result := fsm.Apply(log)
	cmdResult, ok := result.(*CommandResult)
	if !ok {
		t.Fatal("Expected CommandResult type")
	}

	if !cmdResult.Success {
		t.Errorf("Expected success, got error: %s", cmdResult.Error)
	}

	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("delete-key"))
		if err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	})

	if err != nil {
		t.Errorf("Key should have been deleted: %v", err)
	}
}

func TestFSMApplyBatch(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	ops := []BatchOp{
		{Type: CommandPut, Key: "batch-key1", Value: []byte("value1")},
		{Type: CommandPut, Key: "batch-key2", Value: []byte("value2")},
		{Type: CommandDelete, Key: "batch-key3"},
	}

	cmd := NewBatchCommand(ops)
	cmdData, _ := cmd.Marshal()

	log := &raft.Log{
		Data:  cmdData,
		Index: 3,
		Term:  1,
	}

	result := fsm.Apply(log)
	cmdResult, ok := result.(*CommandResult)
	if !ok {
		t.Fatal("Expected CommandResult type")
	}

	if !cmdResult.Success {
		t.Errorf("Expected success, got error: %s", cmdResult.Error)
	}

	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("batch-key1"))
		if err != nil {
			t.Errorf("Failed to get batch-key1: %v", err)
			return err
		}
		value, _ := item.ValueCopy(nil)
		if string(value) != "value1" {
			t.Errorf("Expected value1, got %s", string(value))
		}

		item, err = txn.Get([]byte("batch-key2"))
		if err != nil {
			t.Errorf("Failed to get batch-key2: %v", err)
			return err
		}
		value, _ = item.ValueCopy(nil)
		if string(value) != "value2" {
			t.Errorf("Expected value2, got %s", string(value))
		}

		return nil
	})
}

func TestFSMApplyStateUpdate(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	cmd := NewStateUpdateCommand("workflow-123", []byte(`{"status":"running"}`))
	cmdData, _ := cmd.Marshal()

	log := &raft.Log{
		Data:  cmdData,
		Index: 4,
		Term:  1,
	}

	result := fsm.Apply(log)
	cmdResult, ok := result.(*CommandResult)
	if !ok {
		t.Fatal("Expected CommandResult type")
	}

	if !cmdResult.Success {
		t.Errorf("Expected success, got error: %s", cmdResult.Error)
	}

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("state:workflow-123"))
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if string(value) != `{"status":"running"}` {
			t.Errorf("Expected state value, got '%s'", string(value))
		}
		return nil
	})

	if err != nil {
		t.Errorf("Failed to verify state update: %v", err)
	}
}

func TestFSMApplyQueueOperation(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	cmd := NewQueueOperationCommand("task-456", []byte(`{"action":"process"}`))
	cmdData, _ := cmd.Marshal()

	log := &raft.Log{
		Data:  cmdData,
		Index: 5,
		Term:  1,
	}

	result := fsm.Apply(log)
	cmdResult, ok := result.(*CommandResult)
	if !ok {
		t.Fatal("Expected CommandResult type")
	}

	if !cmdResult.Success {
		t.Errorf("Expected success, got error: %s", cmdResult.Error)
	}

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("queue:task-456"))
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if string(value) != `{"action":"process"}` {
			t.Errorf("Expected queue value, got '%s'", string(value))
		}
		return nil
	})

	if err != nil {
		t.Errorf("Failed to verify queue operation: %v", err)
	}
}

func TestFSMApplyInvalidCommand(t *testing.T) {
	fsm, _, cleanup := setupTestFSM(t)
	defer cleanup()

	log := &raft.Log{
		Data:  []byte("{invalid json}"),
		Index: 6,
		Term:  1,
	}

	result := fsm.Apply(log)
	cmdResult, ok := result.(*CommandResult)
	if !ok {
		t.Fatal("Expected CommandResult type")
	}

	if cmdResult.Success {
		t.Error("Expected failure for invalid command")
	}
	if cmdResult.Error == "" {
		t.Error("Expected error message for invalid command")
	}
}

func TestFSMSnapshot(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte("key1"), []byte("value1"))
		txn.Set([]byte("key2"), []byte("value2"))
		return nil
	})

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	if snapshot == nil {
		t.Fatal("Expected non-nil snapshot")
	}
}

func TestFSMRestore(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	snapshotData := struct {
		Version int               `json:"version"`
		Data    map[string][]byte `json:"data"`
	}{
		Version: 1,
		Data: map[string][]byte{
			"restored-key1": []byte("restored-value1"),
			"restored-key2": []byte("restored-value2"),
		},
	}

	jsonData, _ := json.Marshal(snapshotData)
	reader := io.NopCloser(bytes.NewReader(jsonData))

	err := fsm.Restore(reader)
	if err != nil {
		t.Fatalf("Failed to restore: %v", err)
	}

	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("restored-key1"))
		if err != nil {
			t.Errorf("Failed to get restored-key1: %v", err)
			return err
		}
		value, _ := item.ValueCopy(nil)
		if string(value) != "restored-value1" {
			t.Errorf("Expected restored-value1, got %s", string(value))
		}

		item, err = txn.Get([]byte("restored-key2"))
		if err != nil {
			t.Errorf("Failed to get restored-key2: %v", err)
			return err
		}
		value, _ = item.ValueCopy(nil)
		if string(value) != "restored-value2" {
			t.Errorf("Expected restored-value2, got %s", string(value))
		}

		return nil
	})
}

func TestFSMRestoreInvalidVersion(t *testing.T) {
	fsm, _, cleanup := setupTestFSM(t)
	defer cleanup()


	snapshotData := struct {
		Version int               `json:"version"`
		Data    map[string][]byte `json:"data"`
	}{
		Version: 999,
		Data:    map[string][]byte{},
	}

	jsonData, _ := json.Marshal(snapshotData)
	reader := io.NopCloser(bytes.NewReader(jsonData))

	err := fsm.Restore(reader)
	if err == nil {
		t.Error("Expected error for invalid version")
	}
}