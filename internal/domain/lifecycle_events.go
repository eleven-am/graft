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
	PendingNodes    []PendingNodeData    `json:"pending_nodes,omitempty"`
	ReadyNodes      []ReadyNodeData      `json:"ready_nodes,omitempty"`
	Checkpoints     []CheckpointData     `json:"checkpoints,omitempty"`
	QueueItems      []QueueItemData      `json:"queue_items,omitempty"`
	IdempotencyKeys []IdempotencyKeyData `json:"idempotency_keys,omitempty"`
	ClaimsData      []ClaimData          `json:"claims,omitempty"`
	ResourceStates  []ResourceStateData  `json:"resource_states,omitempty"`
	Dependencies    []DependencyData     `json:"dependencies,omitempty"`

	ExecutionMetrics ExecutionMetricsData `json:"execution_metrics"`
	SystemContext    SystemContextData    `json:"system_context"`
}

type ExecutedNodeData struct {
	NodeName    string        `json:"node_name"`
	ExecutedAt  time.Time     `json:"executed_at"`
	Duration    time.Duration `json:"duration"`
	Status      string        `json:"status"`
	Config      interface{}   `json:"config,omitempty"`
	Results     interface{}   `json:"results,omitempty"`
	Error       *string       `json:"error,omitempty"`
	TriggeredBy string        `json:"triggered_by,omitempty"`
}

type CheckpointData struct {
	ID         string                 `json:"id"`
	WorkflowID string                 `json:"workflow_id"`
	NodeID     string                 `json:"node_id"`
	State      interface{}            `json:"state"`
	CreatedAt  time.Time              `json:"created_at"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type QueueItemData struct {
	ID             string                 `json:"id"`
	WorkflowID     string                 `json:"workflow_id"`
	NodeName       string                 `json:"node_name"`
	Config         interface{}            `json:"config"`
	Priority       int                    `json:"priority"`
	EnqueuedAt     time.Time              `json:"enqueued_at"`
	PartitionKey   string                 `json:"partition_key"`
	RetryCount     int                    `json:"retry_count"`
	MaxRetries     int                    `json:"max_retries"`
	Checksum       string                 `json:"checksum"`
	IdempotencyKey string                 `json:"idempotency_key"`
	QueueType      string                 `json:"queue_type"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

type IdempotencyKeyData struct {
	Key        string                 `json:"key"`
	WorkflowID string                 `json:"workflow_id"`
	NodeID     string                 `json:"node_id"`
	CreatedAt  time.Time              `json:"created_at"`
	ExpiresAt  time.Time              `json:"expires_at"`
	Result     interface{}            `json:"result,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type ClaimData struct {
	ID         string                 `json:"id"`
	WorkflowID string                 `json:"workflow_id"`
	NodeID     string                 `json:"node_id"`
	ResourceID string                 `json:"resource_id"`
	ClaimedAt  time.Time              `json:"claimed_at"`
	ExpiresAt  time.Time              `json:"expires_at"`
	ClaimedBy  string                 `json:"claimed_by"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type WorkflowErrorData struct {
	WorkflowID   string            `json:"workflow_id"`
	CurrentState interface{}       `json:"current_state"`
	Status       string            `json:"status"`
	StartedAt    time.Time         `json:"started_at"`
	FailedAt     time.Time         `json:"failed_at"`
	Duration     time.Duration     `json:"duration"`
	Metadata     map[string]string `json:"metadata,omitempty"`

	Error         error    `json:"error"`
	ErrorType     string   `json:"error_type"`
	FailedNode    string   `json:"failed_node"`
	StackTrace    string   `json:"stack_trace,omitempty"`
	RecoveryHints []string `json:"recovery_hints,omitempty"`

	ExecutedNodes   []ExecutedNodeData   `json:"executed_nodes,omitempty"`
	PendingNodes    []PendingNodeData    `json:"pending_nodes,omitempty"`
	ReadyNodes      []ReadyNodeData      `json:"ready_nodes,omitempty"`
	Checkpoints     []CheckpointData     `json:"checkpoints,omitempty"`
	QueueItems      []QueueItemData      `json:"queue_items,omitempty"`
	IdempotencyKeys []IdempotencyKeyData `json:"idempotency_keys,omitempty"`
	ClaimsData      []ClaimData          `json:"claims,omitempty"`
	ResourceStates  []ResourceStateData  `json:"resource_states,omitempty"`
	Dependencies    []DependencyData     `json:"dependencies,omitempty"`

	ExecutionMetrics ExecutionMetricsData `json:"execution_metrics"`
	SystemContext    SystemContextData    `json:"system_context"`
}

type PendingNodeData struct {
	NodeName    string      `json:"node_name"`
	Config      interface{} `json:"config"`
	QueuedAt    time.Time   `json:"queued_at"`
	Priority    int         `json:"priority"`
	Reason      string      `json:"reason,omitempty"`
	TriggeredBy string      `json:"triggered_by,omitempty"`
}

type ReadyNodeData struct {
	NodeName       string      `json:"node_name"`
	Config         interface{} `json:"config"`
	QueuedAt       time.Time   `json:"queued_at"`
	Priority       int         `json:"priority"`
	WorkflowID     string      `json:"workflow_id"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	TriggeredBy    string      `json:"triggered_by,omitempty"`
}

type ResourceStateData struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	State     interface{}            `json:"state"`
	ClaimedBy string                 `json:"claimed_by,omitempty"`
	ClaimedAt *time.Time             `json:"claimed_at,omitempty"`
	ExpiresAt *time.Time             `json:"expires_at,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type DependencyData struct {
	SourceNode     string                 `json:"source_node"`
	TargetNode     string                 `json:"target_node"`
	DependencyType string                 `json:"dependency_type"`
	Data           interface{}            `json:"data,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

type ExecutionMetricsData struct {
	NodesExecuted       int                        `json:"nodes_executed"`
	TotalExecutionTime  time.Duration              `json:"total_execution_time"`
	AverageNodeTime     time.Duration              `json:"average_node_time"`
	ResourceUtilization map[string]interface{}     `json:"resource_utilization,omitempty"`
	QueueMetrics        QueueMetricsData           `json:"queue_metrics"`
	MemoryMetrics       MemoryMetricsData          `json:"memory_metrics"`
	NodeMetrics         map[string]NodeMetricsData `json:"node_metrics,omitempty"`
}

type SystemContextData struct {
	EngineVersion  string                 `json:"engine_version"`
	NodeID         string                 `json:"node_id"`
	ClusterState   string                 `json:"cluster_state"`
	RaftTerm       uint64                 `json:"raft_term,omitempty"`
	RaftIndex      uint64                 `json:"raft_index,omitempty"`
	Timestamp      time.Time              `json:"timestamp"`
	RuntimeInfo    RuntimeInfoData        `json:"runtime_info"`
	ConfigSnapshot map[string]interface{} `json:"config_snapshot,omitempty"`
}

type QueueMetricsData struct {
	ReadyQueueSize   int           `json:"ready_queue_size"`
	PendingQueueSize int           `json:"pending_queue_size"`
	MaxQueueSize     int           `json:"max_queue_size"`
	AverageQueueTime time.Duration `json:"average_queue_time"`
	QueueThroughput  float64       `json:"queue_throughput"`
}

type MemoryMetricsData struct {
	HeapSize       uint64 `json:"heap_size"`
	HeapInUse      uint64 `json:"heap_in_use"`
	StackInUse     uint64 `json:"stack_in_use"`
	GoroutineCount int    `json:"goroutine_count"`
	GCPauseTotal   uint64 `json:"gc_pause_total"`
	NextGC         uint64 `json:"next_gc"`
}

type NodeMetricsData struct {
	ExecutionCount int           `json:"execution_count"`
	TotalTime      time.Duration `json:"total_time"`
	AverageTime    time.Duration `json:"average_time"`
	SuccessRate    float64       `json:"success_rate"`
	LastExecutedAt time.Time     `json:"last_executed_at"`
	MemoryUsage    uint64        `json:"memory_usage,omitempty"`
}

type RuntimeInfoData struct {
	GoVersion     string `json:"go_version"`
	GOOS          string `json:"goos"`
	GOARCH        string `json:"goarch"`
	NumCPU        int    `json:"num_cpu"`
	NumGoroutines int    `json:"num_goroutines"`
}
