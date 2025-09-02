package domain

import (
	"runtime"
	"strconv"
	"time"
)

type WorkflowStartedEvent struct {
	WorkflowID   string                 `json:"workflow_id"`
	Trigger      WorkflowTrigger        `json:"trigger"`
	StartedAt    time.Time              `json:"started_at"`
	InitialNodes []string               `json:"initial_nodes"`
	NodeID       string                 `json:"node_id"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

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

type WorkflowPausedEvent struct {
	WorkflowID   string      `json:"workflow_id"`
	PausedAt     time.Time   `json:"paused_at"`
	CurrentState interface{} `json:"current_state"`
	Reason       string      `json:"reason,omitempty"`
	NodeID       string      `json:"node_id"`
}

type WorkflowResumedEvent struct {
	WorkflowID   string      `json:"workflow_id"`
	ResumedAt    time.Time   `json:"resumed_at"`
	CurrentState interface{} `json:"current_state"`
	NodeID       string      `json:"node_id"`
}

type NodeStartedEvent struct {
	WorkflowID  string      `json:"workflow_id"`
	NodeName    string      `json:"node_name"`
	Config      interface{} `json:"config"`
	StartedAt   time.Time   `json:"started_at"`
	ExecutionID string      `json:"execution_id"`
	NodeID      string      `json:"node_id"`
}

type NodeCompletedEvent struct {
	WorkflowID  string        `json:"workflow_id"`
	NodeName    string        `json:"node_name"`
	Result      interface{}   `json:"result"`
	CompletedAt time.Time     `json:"completed_at"`
	Duration    time.Duration `json:"duration"`
	ExecutionID string        `json:"execution_id"`
	NextNodes   []string      `json:"next_nodes,omitempty"`
}

type NodeErrorEvent struct {
	WorkflowID   string        `json:"workflow_id"`
	NodeName     string        `json:"node_name"`
	Error        error         `json:"error"`
	FailedAt     time.Time     `json:"failed_at"`
	Duration     time.Duration `json:"duration"`
	ExecutionID  string        `json:"execution_id"`
	Config       interface{}   `json:"config"`
	CurrentState interface{}   `json:"current_state"`
	Recoverable  bool          `json:"recoverable"`
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

type NodeJoinedEvent struct {
	NodeID   string                 `json:"node_id"`
	Address  string                 `json:"address"`
	JoinedAt time.Time              `json:"joined_at"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type NodeLeftEvent struct {
	NodeID   string                 `json:"node_id"`
	Address  string                 `json:"address"`
	LeftAt   time.Time              `json:"left_at"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type LeaderChangedEvent struct {
	NewLeaderID   string                 `json:"new_leader_id"`
	NewLeaderAddr string                 `json:"new_leader_addr"`
	PreviousID    string                 `json:"previous_id,omitempty"`
	ChangedAt     time.Time              `json:"changed_at"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

func NewPanicError(workflowID, nodeID string, panicValue interface{}) *WorkflowPanicError {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)

	pc, file, line, ok := runtime.Caller(2)
	recoveredAt := "unknown"
	if ok {
		fn := runtime.FuncForPC(pc)
		if fn != nil {
			recoveredAt = fn.Name() + " at " + file + ":" + strconv.Itoa(line)
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
