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
	WorkflowID    string                 `json:"workflow_id"`
	CurrentState  interface{}            `json:"current_state"`
	Error         error                  `json:"error"`
	FailedNode    string                 `json:"failed_node"`
	FailedAt      time.Time              `json:"failed_at"`
	ErrorType     string                 `json:"error_type"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

type WorkflowProgressEvent struct {
	WorkflowID      string                 `json:"workflow_id"`
	NodeCompleted   string                 `json:"node_completed"`
	CurrentState    interface{}            `json:"current_state"`
	CompletedAt     time.Time              `json:"completed_at"`
	ExecutionTime   time.Duration          `json:"execution_time"`
	NextNodes       []string               `json:"next_nodes,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

type WorkflowPanicError struct {
	WorkflowID   string    `json:"workflow_id"`
	NodeID       string    `json:"node_id"`
	PanicValue   interface{} `json:"panic_value"`
	StackTrace   string    `json:"stack_trace"`
	Timestamp    time.Time `json:"timestamp"`
	RecoveredAt  string    `json:"recovered_at"`
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
	HandlerType   string        `json:"handler_type"`
	WorkflowID    string        `json:"workflow_id"`
	ExecutedAt    time.Time     `json:"executed_at"`
	Duration      time.Duration `json:"duration"`
	Success       bool          `json:"success"`
	Error         string        `json:"error,omitempty"`
	Retries       int           `json:"retries"`
}