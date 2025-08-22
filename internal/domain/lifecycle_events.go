package domain

import (
	"runtime"
	"time"
)

type WorkflowCompletedEvent struct {
	WorkflowID    string                 `json:"workflow_id"`
	FinalState    interface{}            `json:"final_state"`
	CompletedAt   time.Time              `json:"completed_at"`
	ExecutedNodes []string               `json:"executed_nodes"`
	Duration      time.Duration          `json:"duration"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

type WorkflowErrorEvent struct {
	WorkflowID   string                 `json:"workflow_id"`
	CurrentState interface{}            `json:"current_state"`
	Error        error                  `json:"error"`
	FailedNode   string                 `json:"failed_node"`
	FailedAt     time.Time              `json:"failed_at"`
	ErrorType    string                 `json:"error_type"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

type WorkflowProgressEvent struct {
	WorkflowID    string                 `json:"workflow_id"`
	NodeCompleted string                 `json:"node_completed"`
	CurrentState  interface{}            `json:"current_state"`
	CompletedAt   time.Time              `json:"completed_at"`
	ExecutionTime time.Duration          `json:"execution_time"`
	NextNodes     []string               `json:"next_nodes,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

type WorkflowPanicError struct {
	WorkflowID  string      `json:"workflow_id"`
	NodeID      string      `json:"node_id"`
	PanicValue  interface{} `json:"panic_value"`
	StackTrace  string      `json:"stack_trace"`
	Timestamp   time.Time   `json:"timestamp"`
	RecoveredAt string      `json:"recovered_at"`
}

func (wpe *WorkflowPanicError) Error() string {
	return "node execution panicked: " + wpe.NodeID
}

func NewPanicError(workflowID, nodeID string, panicValue interface{}) *WorkflowPanicError {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)

	pc, file, line, ok := runtime.Caller(2)
	recoveredAt := "unknown"
	if ok {
		fn := runtime.FuncForPC(pc)
		if fn != nil {
			recoveredAt = fn.Name() + " at " + file + ":" + string(rune(line))
		}
	}

	return &WorkflowPanicError{
		WorkflowID:  workflowID,
		NodeID:      nodeID,
		PanicValue:  panicValue,
		StackTrace:  string(buf[:n]),
		Timestamp:   time.Now(),
		RecoveredAt: recoveredAt,
	}
}

type HandlerExecutionResult struct {
	HandlerType string        `json:"handler_type"`
	WorkflowID  string        `json:"workflow_id"`
	ExecutedAt  time.Time     `json:"executed_at"`
	Duration    time.Duration `json:"duration"`
	Success     bool          `json:"success"`
	Error       string        `json:"error,omitempty"`
	Retries     int           `json:"retries"`
}

type WorkflowCompletionData struct {
	WorkflowID  string            `json:"workflow_id"`
	FinalState  interface{}       `json:"final_state"`
	Status      string            `json:"status"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt time.Time         `json:"completed_at"`
	Duration    time.Duration     `json:"duration"`
	Metadata    map[string]string `json:"metadata,omitempty"`

	ExecutedNodes   []ExecutedNodeData   `json:"executed_nodes,omitempty"`
	Checkpoints     []CheckpointData     `json:"checkpoints,omitempty"`
	QueueItems      []QueueItemData      `json:"queue_items,omitempty"`
	IdempotencyKeys []IdempotencyKeyData `json:"idempotency_keys,omitempty"`
	ClaimsData      []ClaimData          `json:"claims,omitempty"`
}

type ExecutedNodeData struct {
	NodeName   string        `json:"node_name"`
	ExecutedAt time.Time     `json:"executed_at"`
	Duration   time.Duration `json:"duration"`
	Status     string        `json:"status"`
	Config     interface{}   `json:"config,omitempty"`
	Results    interface{}   `json:"results,omitempty"`
	Error      *string       `json:"error,omitempty"`
}

type CheckpointData struct {
	Timestamp  time.Time   `json:"timestamp"`
	State      interface{} `json:"state"`
	StorageKey string      `json:"storage_key"`
}

type QueueItemData struct {
	ID         string      `json:"id"`
	NodeName   string      `json:"node_name"`
	Config     interface{} `json:"config"`
	Priority   int         `json:"priority"`
	EnqueuedAt time.Time   `json:"enqueued_at"`
	QueueType  string      `json:"queue_type"`
}

type IdempotencyKeyData struct {
	Key        string    `json:"key"`
	ClaimedAt  time.Time `json:"claimed_at"`
	StorageKey string    `json:"storage_key"`
}

type ClaimData struct {
	NodeName  string      `json:"node_name"`
	ClaimedAt time.Time   `json:"claimed_at"`
	Data      interface{} `json:"data,omitempty"`
}
