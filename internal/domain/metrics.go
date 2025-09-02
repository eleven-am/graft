package domain

import (
	"sync/atomic"
	"time"
)

type ExecutionMetrics struct {
	WorkflowsStarted   int64 `json:"workflows_started"`
	WorkflowsCompleted int64 `json:"workflows_completed"`
	WorkflowsFailed    int64 `json:"workflows_failed"`
	WorkflowsPaused    int64 `json:"workflows_paused"`
	WorkflowsResumed   int64 `json:"workflows_resumed"`
	WorkflowsActive    int64 `json:"workflows_active"`

	NodesExecuted  int64 `json:"nodes_executed"`
	NodesSucceeded int64 `json:"nodes_succeeded"`
	NodesFailed    int64 `json:"nodes_failed"`
	NodesTimedOut  int64 `json:"nodes_timed_out"`
	NodesRetried   int64 `json:"nodes_retried"`
	NodesActive    int64 `json:"nodes_active"`

	ItemsEnqueued              int64 `json:"items_enqueued"`
	ItemsProcessed             int64 `json:"items_processed"`
	ItemsSentToDeadLetter      int64 `json:"items_sent_to_dead_letter"`
	ItemsRetriedFromDeadLetter int64 `json:"items_retried_from_dead_letter"`
	ItemsPending               int64 `json:"items_pending"`
	ItemsClaimed               int64 `json:"items_claimed"`

	TotalExecutionTimeNs int64 `json:"total_execution_time_ns"`
	NodeExecutionCount   int64 `json:"node_execution_count"`
	MinExecutionTimeNs   int64 `json:"min_execution_time_ns"`
	MaxExecutionTimeNs   int64 `json:"max_execution_time_ns"`

	ErrorRate           float64 `json:"error_rate"`
	ThroughputPerSecond float64 `json:"throughput_per_second"`
	AverageLatencyMs    float64 `json:"average_latency_ms"`

	LastUpdated int64 `json:"last_updated_unix"`
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
	nanos := int64(duration)
	atomic.AddInt64(&m.TotalExecutionTimeNs, nanos)
	atomic.AddInt64(&m.NodeExecutionCount, 1)

	for {
		currentMin := atomic.LoadInt64(&m.MinExecutionTimeNs)
		if currentMin == 0 || nanos < currentMin {
			if atomic.CompareAndSwapInt64(&m.MinExecutionTimeNs, currentMin, nanos) {
				break
			}
		} else {
			break
		}
	}

	for {
		currentMax := atomic.LoadInt64(&m.MaxExecutionTimeNs)
		if nanos > currentMax {
			if atomic.CompareAndSwapInt64(&m.MaxExecutionTimeNs, currentMax, nanos) {
				break
			}
		} else {
			break
		}
	}

	atomic.StoreInt64(&m.LastUpdated, time.Now().Unix())
}

func (m *ExecutionMetrics) IncrementWorkflowsActive() {
	atomic.AddInt64(&m.WorkflowsActive, 1)
}

func (m *ExecutionMetrics) DecrementWorkflowsActive() {
	atomic.AddInt64(&m.WorkflowsActive, -1)
}

func (m *ExecutionMetrics) IncrementNodesActive() {
	atomic.AddInt64(&m.NodesActive, 1)
}

func (m *ExecutionMetrics) DecrementNodesActive() {
	atomic.AddInt64(&m.NodesActive, -1)
}

func (m *ExecutionMetrics) SetItemsPending(count int64) {
	atomic.StoreInt64(&m.ItemsPending, count)
}

func (m *ExecutionMetrics) SetItemsClaimed(count int64) {
	atomic.StoreInt64(&m.ItemsClaimed, count)
}

func (m *ExecutionMetrics) GetSnapshot() ExecutionMetrics {
	snapshot := ExecutionMetrics{
		WorkflowsStarted:           atomic.LoadInt64(&m.WorkflowsStarted),
		WorkflowsCompleted:         atomic.LoadInt64(&m.WorkflowsCompleted),
		WorkflowsFailed:            atomic.LoadInt64(&m.WorkflowsFailed),
		WorkflowsPaused:            atomic.LoadInt64(&m.WorkflowsPaused),
		WorkflowsResumed:           atomic.LoadInt64(&m.WorkflowsResumed),
		WorkflowsActive:            atomic.LoadInt64(&m.WorkflowsActive),
		NodesExecuted:              atomic.LoadInt64(&m.NodesExecuted),
		NodesSucceeded:             atomic.LoadInt64(&m.NodesSucceeded),
		NodesFailed:                atomic.LoadInt64(&m.NodesFailed),
		NodesTimedOut:              atomic.LoadInt64(&m.NodesTimedOut),
		NodesRetried:               atomic.LoadInt64(&m.NodesRetried),
		NodesActive:                atomic.LoadInt64(&m.NodesActive),
		ItemsEnqueued:              atomic.LoadInt64(&m.ItemsEnqueued),
		ItemsProcessed:             atomic.LoadInt64(&m.ItemsProcessed),
		ItemsSentToDeadLetter:      atomic.LoadInt64(&m.ItemsSentToDeadLetter),
		ItemsRetriedFromDeadLetter: atomic.LoadInt64(&m.ItemsRetriedFromDeadLetter),
		ItemsPending:               atomic.LoadInt64(&m.ItemsPending),
		ItemsClaimed:               atomic.LoadInt64(&m.ItemsClaimed),
		TotalExecutionTimeNs:       atomic.LoadInt64(&m.TotalExecutionTimeNs),
		NodeExecutionCount:         atomic.LoadInt64(&m.NodeExecutionCount),
		MinExecutionTimeNs:         atomic.LoadInt64(&m.MinExecutionTimeNs),
		MaxExecutionTimeNs:         atomic.LoadInt64(&m.MaxExecutionTimeNs),
		LastUpdated:                atomic.LoadInt64(&m.LastUpdated),
	}

	if snapshot.NodesExecuted > 0 {
		snapshot.ErrorRate = float64(snapshot.NodesFailed) / float64(snapshot.NodesExecuted)
	}

	if snapshot.NodeExecutionCount > 0 {
		avgNs := float64(snapshot.TotalExecutionTimeNs) / float64(snapshot.NodeExecutionCount)
		snapshot.AverageLatencyMs = avgNs / 1e6
	}

	if snapshot.LastUpdated > 0 {
		elapsed := time.Since(time.Unix(snapshot.LastUpdated, 0))
		if elapsed.Seconds() > 0 {
			snapshot.ThroughputPerSecond = float64(snapshot.NodesExecuted) / elapsed.Seconds()
		}
	}

	return snapshot
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
	atomic.StoreInt64(&m.WorkflowsActive, 0)
	atomic.StoreInt64(&m.NodesExecuted, 0)
	atomic.StoreInt64(&m.NodesSucceeded, 0)
	atomic.StoreInt64(&m.NodesFailed, 0)
	atomic.StoreInt64(&m.NodesTimedOut, 0)
	atomic.StoreInt64(&m.NodesRetried, 0)
	atomic.StoreInt64(&m.NodesActive, 0)
	atomic.StoreInt64(&m.ItemsEnqueued, 0)
	atomic.StoreInt64(&m.ItemsProcessed, 0)
	atomic.StoreInt64(&m.ItemsSentToDeadLetter, 0)
	atomic.StoreInt64(&m.ItemsRetriedFromDeadLetter, 0)
	atomic.StoreInt64(&m.ItemsPending, 0)
	atomic.StoreInt64(&m.ItemsClaimed, 0)
	atomic.StoreInt64(&m.TotalExecutionTimeNs, 0)
	atomic.StoreInt64(&m.NodeExecutionCount, 0)
	atomic.StoreInt64(&m.MinExecutionTimeNs, 0)
	atomic.StoreInt64(&m.MaxExecutionTimeNs, 0)
	atomic.StoreInt64(&m.LastUpdated, 0)
}
