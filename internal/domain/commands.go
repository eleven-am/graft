package domain

import (
	"encoding/json"
	"time"
)

type CommandType uint8

const (
	CommandPut CommandType = iota
	CommandDelete
	CommandBatch
	CommandAtomicBatch
	CommandStateUpdate
	CommandQueueOperation
	CommandCleanupWorkflow
	CommandCompleteWorkflowPurge
	CommandResourceLock
	CommandResourceUnlock
)

type Command struct {
	Type           CommandType            `json:"type"`
	Key            string                 `json:"key,omitempty"`
	Value          []byte                 `json:"value,omitempty"`
	Batch          []BatchOp              `json:"batch,omitempty"`
	IdempotencyKey *string                `json:"idempotency_key,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
	Timestamp      time.Time              `json:"timestamp"`
}

type BatchOp struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value []byte      `json:"value,omitempty"`
}

type CommandResult struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func NewPutCommand(key string, value []byte) *Command {
	return &Command{
		Type:      CommandPut,
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}
}

func NewDeleteCommand(key string) *Command {
	return &Command{
		Type:      CommandDelete,
		Key:       key,
		Timestamp: time.Now(),
	}
}

func NewBatchCommand(ops []BatchOp) *Command {
	return &Command{
		Type:      CommandBatch,
		Batch:     ops,
		Timestamp: time.Now(),
	}
}

func NewAtomicBatchCommand(ops []BatchOp, idempotencyKey string) *Command {
	return &Command{
		Type:           CommandAtomicBatch,
		Batch:          ops,
		IdempotencyKey: &idempotencyKey,
		Timestamp:      time.Now(),
	}
}

func NewStateUpdateCommand(key string, value []byte) *Command {
	return &Command{
		Type:      CommandStateUpdate,
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}
}

func NewQueueOperationCommand(key string, value []byte) *Command {
	return &Command{
		Type:      CommandQueueOperation,
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}
}

func NewCleanupWorkflowCommand(key string) *Command {
	return &Command{
		Type:      CommandCleanupWorkflow,
		Key:       key,
		Timestamp: time.Now(),
	}
}

func NewCompleteWorkflowPurgeCommand(workflowID string) *Command {
	return &Command{
		Type:      CommandCompleteWorkflowPurge,
		Key:       workflowID,
		Timestamp: time.Now(),
	}
}

func NewResourceLockCommand(key string, value []byte) *Command {
	return &Command{
		Type:      CommandResourceLock,
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}
}

func NewResourceUnlockCommand(key string) *Command {
	return &Command{
		Type:      CommandResourceUnlock,
		Key:       key,
		Timestamp: time.Now(),
	}
}

func (c *Command) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func UnmarshalCommand(data []byte) (*Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return &cmd, err
}

func (c CommandType) String() string {
	switch c {
	case CommandPut:
		return "PUT"
	case CommandDelete:
		return "DELETE"
	case CommandBatch:
		return "BATCH"
	case CommandAtomicBatch:
		return "ATOMIC_BATCH"
	case CommandStateUpdate:
		return "STATE_UPDATE"
	case CommandQueueOperation:
		return "QUEUE_OPERATION"
	case CommandCleanupWorkflow:
		return "CLEANUP_WORKFLOW"
	case CommandCompleteWorkflowPurge:
		return "COMPLETE_WORKFLOW_PURGE"
	case CommandResourceLock:
		return "RESOURCE_LOCK"
	case CommandResourceUnlock:
		return "RESOURCE_UNLOCK"
	default:
		return "UNKNOWN"
	}
}
