package domain

import (
	"sync/atomic"
	"time"
)

type ExecutionMetrics struct {
	WorkflowsStarted    int64 `json:"workflows_started"`
	WorkflowsCompleted  int64 `json:"workflows_completed"`
	WorkflowsFailed     int64 `json:"workflows_failed"`
	WorkflowsPaused     int64 `json:"workflows_paused"`
	WorkflowsResumed    int64 `json:"workflows_resumed"`
	
	NodesExecuted       int64 `json:"nodes_executed"`
	NodesSucceeded      int64 `json:"nodes_succeeded"`
	NodesFailed         int64 `json:"nodes_failed"`
	NodesTimedOut       int64 `json:"nodes_timed_out"`
	NodesRetried        int64 `json:"nodes_retried"`
	
  
	ItemsEnqueued       int64 `json:"items_enqueued"`
	ItemsProcessed      int64 `json:"items_processed"`
	ItemsSentToDeadLetter int64 `json:"items_sent_to_dead_letter"`
	ItemsRetriedFromDeadLetter int64 `json:"items_retried_from_dead_letter"`
	
	TotalExecutionTimeNs int64 `json:"total_execution_time_ns"`
	NodeExecutionCount   int64 `json:"node_execution_count"`
}

func NewExecutionMetrics() *ExecutionMetrics {
	return &ExecutionMetrics{}
}

func (m *ExecutionMetrics) IncrementWorkflowsStarted() {
	atomic.AddInt64(&m.WorkflowsStarted, 1)
}

func (m *ExecutionMetrics) IncrementWorkflowsCompleted() {
	atomic.AddInt64(&m.WorkflowsCompleted, 1)
}

func (m *ExecutionMetrics) IncrementWorkflowsFailed() {
	atomic.AddInt64(&m.WorkflowsFailed, 1)
}

func (m *ExecutionMetrics) IncrementWorkflowsPaused() {
	atomic.AddInt64(&m.WorkflowsPaused, 1)
}

func (m *ExecutionMetrics) IncrementWorkflowsResumed() {
	atomic.AddInt64(&m.WorkflowsResumed, 1)
}

func (m *ExecutionMetrics) IncrementNodesExecuted() {
	atomic.AddInt64(&m.NodesExecuted, 1)
}

func (m *ExecutionMetrics) IncrementNodesSucceeded() {
	atomic.AddInt64(&m.NodesSucceeded, 1)
}

func (m *ExecutionMetrics) IncrementNodesFailed() {
	atomic.AddInt64(&m.NodesFailed, 1)
}

func (m *ExecutionMetrics) IncrementNodesTimedOut() {
	atomic.AddInt64(&m.NodesTimedOut, 1)
}

func (m *ExecutionMetrics) IncrementNodesRetried() {
	atomic.AddInt64(&m.NodesRetried, 1)
}

func (m *ExecutionMetrics) IncrementItemsEnqueued() {
	atomic.AddInt64(&m.ItemsEnqueued, 1)
}

func (m *ExecutionMetrics) IncrementItemsProcessed() {
	atomic.AddInt64(&m.ItemsProcessed, 1)
}

func (m *ExecutionMetrics) IncrementItemsSentToDeadLetter() {
	atomic.AddInt64(&m.ItemsSentToDeadLetter, 1)
}

func (m *ExecutionMetrics) IncrementItemsRetriedFromDeadLetter() {
	atomic.AddInt64(&m.ItemsRetriedFromDeadLetter, 1)
}

func (m *ExecutionMetrics) AddExecutionTime(duration time.Duration) {
	atomic.AddInt64(&m.TotalExecutionTimeNs, int64(duration))
	atomic.AddInt64(&m.NodeExecutionCount, 1)
}

func (m *ExecutionMetrics) GetSnapshot() ExecutionMetrics {
	return ExecutionMetrics{
		WorkflowsStarted:               atomic.LoadInt64(&m.WorkflowsStarted),
		WorkflowsCompleted:             atomic.LoadInt64(&m.WorkflowsCompleted),
		WorkflowsFailed:                atomic.LoadInt64(&m.WorkflowsFailed),
		WorkflowsPaused:                atomic.LoadInt64(&m.WorkflowsPaused),
		WorkflowsResumed:               atomic.LoadInt64(&m.WorkflowsResumed),
		NodesExecuted:                  atomic.LoadInt64(&m.NodesExecuted),
		NodesSucceeded:                 atomic.LoadInt64(&m.NodesSucceeded),
		NodesFailed:                    atomic.LoadInt64(&m.NodesFailed),
		NodesTimedOut:                  atomic.LoadInt64(&m.NodesTimedOut),
		NodesRetried:                   atomic.LoadInt64(&m.NodesRetried),
		ItemsEnqueued:                  atomic.LoadInt64(&m.ItemsEnqueued),
		ItemsProcessed:                 atomic.LoadInt64(&m.ItemsProcessed),
		ItemsSentToDeadLetter:          atomic.LoadInt64(&m.ItemsSentToDeadLetter),
		ItemsRetriedFromDeadLetter:     atomic.LoadInt64(&m.ItemsRetriedFromDeadLetter),
		TotalExecutionTimeNs:           atomic.LoadInt64(&m.TotalExecutionTimeNs),
		NodeExecutionCount:             atomic.LoadInt64(&m.NodeExecutionCount),
	}
}

func (m *ExecutionMetrics) GetAverageExecutionTime() time.Duration {
	totalNs := atomic.LoadInt64(&m.TotalExecutionTimeNs)
	count := atomic.LoadInt64(&m.NodeExecutionCount)
	
	if count == 0 {
		return 0
	}
	
	return time.Duration(totalNs / count)
}

func (m *ExecutionMetrics) Reset() {
	atomic.StoreInt64(&m.WorkflowsStarted, 0)
	atomic.StoreInt64(&m.WorkflowsCompleted, 0)
	atomic.StoreInt64(&m.WorkflowsFailed, 0)
	atomic.StoreInt64(&m.WorkflowsPaused, 0)
	atomic.StoreInt64(&m.WorkflowsResumed, 0)
	atomic.StoreInt64(&m.NodesExecuted, 0)
	atomic.StoreInt64(&m.NodesSucceeded, 0)
	atomic.StoreInt64(&m.NodesFailed, 0)
	atomic.StoreInt64(&m.NodesTimedOut, 0)
	atomic.StoreInt64(&m.NodesRetried, 0)
	atomic.StoreInt64(&m.ItemsEnqueued, 0)
	atomic.StoreInt64(&m.ItemsProcessed, 0)
	atomic.StoreInt64(&m.ItemsSentToDeadLetter, 0)
	atomic.StoreInt64(&m.ItemsRetriedFromDeadLetter, 0)
	atomic.StoreInt64(&m.TotalExecutionTimeNs, 0)
	atomic.StoreInt64(&m.NodeExecutionCount, 0)
}