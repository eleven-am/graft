package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type CommandType uint8

const (
	CommandPut CommandType = iota
	CommandDelete
	CommandCAS
	CommandBatch
	CommandTypeAtomicIncrement
	CommandTypeDev
	CommandTypeNoop
)

type Command struct {
	Type       CommandType `json:"type"`
	Key        string      `json:"key,omitempty"`
	Value      []byte      `json:"value,omitempty"`
	Expected   []byte      `json:"expected,omitempty"`
	Version    int64       `json:"version,omitempty"`
	Batch      []BatchOp   `json:"batch,omitempty"`
	RequestID  string      `json:"request_id,omitempty"`
	Timestamp  time.Time   `json:"timestamp"`
	TTLSeconds int64       `json:"ttl_seconds,omitempty"`
}

type BatchOp struct {
	Type       CommandType `json:"type"`
	Key        string      `json:"key"`
	Value      []byte      `json:"value,omitempty"`
	Expected   []byte      `json:"expected,omitempty"`
	Version    int64       `json:"version,omitempty"`
	TTLSeconds int64       `json:"ttl_seconds,omitempty"`
}

type CommandResult struct {
	Success      bool     `json:"success"`
	Error        string   `json:"error,omitempty"`
	Version      int64    `json:"version,omitempty"`
	PrevVersion  int64    `json:"prev_version,omitempty"`
	Events       []Event  `json:"events,omitempty"`
	BatchResults []Result `json:"batch_results,omitempty"`
}

type Result struct {
	Key     string `json:"key"`
	Success bool   `json:"success"`
	Version int64  `json:"version,omitempty"`
	Error   string `json:"error,omitempty"`
}

type EventType uint8

const (
	EventPut EventType = iota
	EventDelete
	EventCAS
	EventExpire
)

type Event struct {
	Type      EventType `json:"type"`
	Key       string    `json:"key"`
	Version   int64     `json:"version"`
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
	RequestID string    `json:"request_id"`
}

func NewPutCommand(key string, value []byte, version int64) *Command {
	return &Command{
		Type:      CommandPut,
		Key:       key,
		Value:     value,
		Version:   version,
		Timestamp: time.Now(),
		RequestID: generateRequestID(),
	}
}

func NewPutWithTTLCommand(key string, value []byte, version int64, ttl time.Duration) *Command {
	return &Command{
		Type:       CommandPut,
		Key:        key,
		Value:      value,
		Version:    version,
		TTLSeconds: int64(ttl.Seconds()),
		Timestamp:  time.Now(),
		RequestID:  generateRequestID(),
	}
}

func NewDeleteCommand(key string) *Command {
	return &Command{
		Type:      CommandDelete,
		Key:       key,
		Timestamp: time.Now(),
		RequestID: generateRequestID(),
	}
}

func NewCASCommand(key string, expected, newValue []byte) *Command {
	return &Command{
		Type:      CommandCAS,
		Key:       key,
		Expected:  expected,
		Value:     newValue,
		Timestamp: time.Now(),
		RequestID: generateRequestID(),
	}
}

func NewVersionedCASCommand(key string, version int64, newValue []byte) *Command {
	return &Command{
		Type:      CommandCAS,
		Key:       key,
		Version:   version,
		Value:     newValue,
		Timestamp: time.Now(),
		RequestID: generateRequestID(),
	}
}

func NewBatchCommand(ops []BatchOp) *Command {
	return &Command{
		Type:      CommandBatch,
		Batch:     ops,
		Timestamp: time.Now(),
		RequestID: generateRequestID(),
	}
}

func NewAtomicIncrementCommand(key string, incrementBy int64) *Command {
	return &Command{
		Type:      CommandTypeAtomicIncrement,
		Key:       key,
		Value:     []byte(fmt.Sprintf("%d", incrementBy)),
		Timestamp: time.Now(),
		RequestID: generateRequestID(),
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
	case CommandCAS:
		return "CAS"
	case CommandBatch:
		return "BATCH"
	case CommandTypeAtomicIncrement:
		return "ATOMIC_INCREMENT"
	case CommandTypeDev:
		return "DEV_COMMAND"
	case CommandTypeNoop:
		return "NOOP"
	default:
		return "UNKNOWN"
	}
}

type DevCommand struct {
	Command string      `json:"command"`
	Params  interface{} `json:"params"`
}

type CommandHandler func(ctx context.Context, from string, params interface{}) error

func (dc *DevCommand) ToInternalCommand() (*Command, error) {
	payload, err := json.Marshal(dc)
	if err != nil {
		return nil, fmt.Errorf("marshal dev command %s: %w", dc.Command, err)
	}
	return &Command{
		Type:      CommandTypeDev,
		Key:       "dev-cmd:" + dc.Command,
		Value:     payload,
		RequestID: generateRequestID(),
		Timestamp: time.Now(),
	}, nil
}

func generateRequestID() string {
	return time.Now().Format("20060102150405.999999999")
}
