package raftimpl

import (
	"testing"
	"time"
)

func TestNewPutCommand(t *testing.T) {
	key := "test-key"
	value := []byte("test-value")
	
	cmd := NewPutCommand(key, value)
	
	if cmd.Type != CommandPut {
		t.Errorf("Expected command type %v, got %v", CommandPut, cmd.Type)
	}
	if cmd.Key != key {
		t.Errorf("Expected key %s, got %s", key, cmd.Key)
	}
	if string(cmd.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(cmd.Value))
	}
	if cmd.Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}
}

func TestNewDeleteCommand(t *testing.T) {
	key := "test-key"
	
	cmd := NewDeleteCommand(key)
	
	if cmd.Type != CommandDelete {
		t.Errorf("Expected command type %v, got %v", CommandDelete, cmd.Type)
	}
	if cmd.Key != key {
		t.Errorf("Expected key %s, got %s", key, cmd.Key)
	}
	if cmd.Value != nil {
		t.Errorf("Expected nil value, got %v", cmd.Value)
	}
}

func TestNewBatchCommand(t *testing.T) {
	ops := []BatchOp{
		{Type: CommandPut, Key: "key1", Value: []byte("value1")},
		{Type: CommandDelete, Key: "key2"},
	}
	
	cmd := NewBatchCommand(ops)
	
	if cmd.Type != CommandBatch {
		t.Errorf("Expected command type %v, got %v", CommandBatch, cmd.Type)
	}
	if len(cmd.Batch) != len(ops) {
		t.Errorf("Expected %d operations, got %d", len(ops), len(cmd.Batch))
	}
	for i, op := range cmd.Batch {
		if op.Type != ops[i].Type {
			t.Errorf("Operation %d: expected type %v, got %v", i, ops[i].Type, op.Type)
		}
		if op.Key != ops[i].Key {
			t.Errorf("Operation %d: expected key %s, got %s", i, ops[i].Key, op.Key)
		}
		if string(op.Value) != string(ops[i].Value) {
			t.Errorf("Operation %d: expected value %s, got %s", i, string(ops[i].Value), string(op.Value))
		}
	}
}

func TestNewStateUpdateCommand(t *testing.T) {
	key := "workflow-123"
	value := []byte(`{"state":"running"}`)
	
	cmd := NewStateUpdateCommand(key, value)
	
	if cmd.Type != CommandStateUpdate {
		t.Errorf("Expected command type %v, got %v", CommandStateUpdate, cmd.Type)
	}
	if cmd.Key != key {
		t.Errorf("Expected key %s, got %s", key, cmd.Key)
	}
	if string(cmd.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(cmd.Value))
	}
}

func TestNewQueueOperationCommand(t *testing.T) {
	key := "queue-item-456"
	value := []byte(`{"task":"process"}`)
	
	cmd := NewQueueOperationCommand(key, value)
	
	if cmd.Type != CommandQueueOperation {
		t.Errorf("Expected command type %v, got %v", CommandQueueOperation, cmd.Type)
	}
	if cmd.Key != key {
		t.Errorf("Expected key %s, got %s", key, cmd.Key)
	}
}

func TestCommandMarshalUnmarshal(t *testing.T) {
	original := &Command{
		Type:      CommandPut,
		Key:       "test-key",
		Value:     []byte("test-value"),
		Timestamp: time.Now(),
	}
	
	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}
	
	unmarshaled, err := UnmarshalCommand(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal command: %v", err)
	}
	
	if unmarshaled.Type != original.Type {
		t.Errorf("Expected type %v, got %v", original.Type, unmarshaled.Type)
	}
	if unmarshaled.Key != original.Key {
		t.Errorf("Expected key %s, got %s", original.Key, unmarshaled.Key)
	}
	if string(unmarshaled.Value) != string(original.Value) {
		t.Errorf("Expected value %s, got %s", string(original.Value), string(unmarshaled.Value))
	}
}

func TestCommandBatchMarshalUnmarshal(t *testing.T) {
	ops := []BatchOp{
		{Type: CommandPut, Key: "key1", Value: []byte("value1")},
		{Type: CommandDelete, Key: "key2"},
		{Type: CommandPut, Key: "key3", Value: []byte("value3")},
	}
	
	original := NewBatchCommand(ops)
	
	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal batch command: %v", err)
	}
	
	unmarshaled, err := UnmarshalCommand(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal batch command: %v", err)
	}
	
	if unmarshaled.Type != CommandBatch {
		t.Errorf("Expected type %v, got %v", CommandBatch, unmarshaled.Type)
	}
	if len(unmarshaled.Batch) != len(ops) {
		t.Fatalf("Expected %d operations, got %d", len(ops), len(unmarshaled.Batch))
	}
	
	for i, op := range unmarshaled.Batch {
		if op.Type != ops[i].Type {
			t.Errorf("Operation %d: type mismatch", i)
		}
		if op.Key != ops[i].Key {
			t.Errorf("Operation %d: key mismatch", i)
		}
		if string(op.Value) != string(ops[i].Value) {
			t.Errorf("Operation %d: value mismatch", i)
		}
	}
}

func TestCommandTypeString(t *testing.T) {
	tests := []struct {
		cmdType  CommandType
		expected string
	}{
		{CommandPut, "PUT"},
		{CommandDelete, "DELETE"},
		{CommandBatch, "BATCH"},
		{CommandStateUpdate, "STATE_UPDATE"},
		{CommandQueueOperation, "QUEUE_OPERATION"},
		{CommandType(99), "UNKNOWN"},
	}
	
	for _, test := range tests {
		result := test.cmdType.String()
		if result != test.expected {
			t.Errorf("CommandType %v: expected %s, got %s", test.cmdType, test.expected, result)
		}
	}
}

func TestUnmarshalCommandError(t *testing.T) {
	invalidJSON := []byte("{invalid json}")
	
	_, err := UnmarshalCommand(invalidJSON)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}