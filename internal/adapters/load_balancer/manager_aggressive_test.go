package load_balancer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestLoadBalancer_ConcurrentEventStorm(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	storage.On("Put", mock.AnythingOfType("string"), mock.MatchedBy(func(data []byte) bool { return true }), int64(0)).Return(nil)
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "stress-node", nil, &Config{FailurePolicy: "fail-open"}, nil)
	ctx := context.Background()
	err := manager.Start(ctx)
	assert.NoError(t, err)
	defer manager.Stop()

	const numGoroutines = 100
	const eventsPerGoroutine = 1000
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				workflowID := fmt.Sprintf("stress-workflow-%d-%d", goroutineID, j)

				startedEvent := &domain.NodeStartedEvent{
					WorkflowID: workflowID,
					NodeName:   "ml-training-heavy",
					NodeID:     "stress-node",
					StartedAt:  time.Now(),
				}
				manager.onNodeStarted(startedEvent)

				completedEvent := &domain.NodeCompletedEvent{
					WorkflowID: workflowID,
					Duration:   time.Duration(j%1000) * time.Millisecond,
				}
				manager.onNodeCompleted(completedEvent)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	t.Logf("Concurrent storm completed in %v", elapsed)
	t.Logf("Final state: Weight=%f, Units=%d", manager.totalWeight, len(manager.executionUnits))

	if manager.totalWeight != 0 {
		t.Errorf("Expected weight to be 0 after all events, got %f", manager.totalWeight)
	}
	if len(manager.executionUnits) != 0 {
		t.Errorf("Expected no execution units after cleanup, got %d", len(manager.executionUnits))
	}
}

func TestLoadBalancer_RaceConditionDetection(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	storage.On("Put", mock.AnythingOfType("string"), mock.MatchedBy(func(data []byte) bool { return true }), int64(0)).Return(nil)
	storage.On("ListByPrefix", "cluster:load:").Return([]ports.KeyValueVersion{}, nil)
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "race-node", nil, &Config{FailurePolicy: "fail-open"}, nil)
	ctx := context.Background()
	err := manager.Start(ctx)
	assert.NoError(t, err)
	defer manager.Stop()

	var wg sync.WaitGroup
	const numWorkers = 50

	for i := 0; i < numWorkers; i++ {
		wg.Add(3)

		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				workflowID := fmt.Sprintf("race-workflow-%d-%d", id, j)
				manager.onNodeStarted(&domain.NodeStartedEvent{
					WorkflowID: workflowID,
					NodeName:   "processor-node",
					NodeID:     "race-node",
				})
			}
		}(i)

		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = manager.ShouldExecuteNode("race-node", fmt.Sprintf("decision-%d-%d", id, j), "test")
			}
		}(i)

		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				workflowID := fmt.Sprintf("race-workflow-%d-%d", id, j)
				manager.onNodeCompleted(&domain.NodeCompletedEvent{
					WorkflowID: workflowID,
					Duration:   10 * time.Millisecond,
				})
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Race condition test completed without panics")
}

func TestLoadBalancer_MemoryLeakDetection(t *testing.T) {

	manager := &Manager{
		storage:        &memoryEfficientStorage{},
		events:         &memoryEfficientEvents{},
		nodeID:         "leak-node",
		logger:         slog.Default(),
		executionUnits: make(map[string]float64),
		errorWindow:    NewRollingWindow(100),
		nodeCapacities: make(map[string]float64),
		scoreCache:     make(map[string]scoreCacheEntry),
		scoreCacheTTL:  1 * time.Second,
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	const cycles = 1000
	for i := 0; i < cycles; i++ {
		workflowID := fmt.Sprintf("leak-test-%d", i)

		manager.mu.Lock()
		weight := ports.GetNodeWeight(nil, "heavy-ml-training")
		manager.executionUnits[workflowID] = weight
		manager.totalWeight += weight

		load := &ports.NodeLoad{
			NodeID:          manager.nodeID,
			TotalWeight:     manager.totalWeight,
			ExecutionUnits:  manager.executionUnits,
			RecentLatencyMs: manager.recentLatencyMs,
			RecentErrorRate: manager.errorWindow.GetErrorRate(),
			LastUpdated:     time.Now().Unix(),
		}
		manager.mu.Unlock()

		err := manager.updateNodeLoad(load)
		assert.NoError(t, err)

		manager.mu.Lock()
		if weight, exists := manager.executionUnits[workflowID]; exists {
			manager.totalWeight -= weight
			delete(manager.executionUnits, workflowID)
		}

		latencyMs := float64(i % 100)
		if latencyMs > 0 {
			manager.recentLatencyMs = UpdateEWMA(manager.recentLatencyMs, latencyMs, 0.2)
		}
		manager.errorWindow.Record(true)
		manager.mu.Unlock()

		if i%100 == 0 {
			runtime.GC()
		}
	}

	runtime.GC()
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	var allocIncrease int64
	if m2.Alloc >= m1.Alloc {
		allocIncrease = int64(m2.Alloc - m1.Alloc)
	} else {
		allocIncrease = -int64(m1.Alloc - m2.Alloc)
	}

	t.Logf("Memory before: %d bytes, after: %d bytes, change: %d bytes",
		m1.Alloc, m2.Alloc, allocIncrease)

	if allocIncrease > 100*1024 {
		t.Errorf("Potential memory leak detected: %d bytes leaked", allocIncrease)
	}
}

// Memory-efficient stub implementations that don't retain data
type memoryEfficientStorage struct{}

func (s *memoryEfficientStorage) Put(key string, value []byte, ttl int64) error { return nil }
func (s *memoryEfficientStorage) Get(key string) ([]byte, int64, bool, error) {
	return nil, 0, false, nil
}
func (s *memoryEfficientStorage) Delete(key string) error { return nil }
func (s *memoryEfficientStorage) ListByPrefix(prefix string) ([]ports.KeyValueVersion, error) {
	return nil, nil
}
func (s *memoryEfficientStorage) Close() error                                       { return nil }
func (s *memoryEfficientStorage) Exists(key string) (bool, error)                    { return false, nil }
func (s *memoryEfficientStorage) GetMetadata(key string) (*ports.KeyMetadata, error) { return nil, nil }
func (s *memoryEfficientStorage) BatchWrite(ops []ports.WriteOp) error               { return nil }
func (s *memoryEfficientStorage) GetNext(prefix string) (string, []byte, bool, error) {
	return "", nil, false, nil
}
func (s *memoryEfficientStorage) GetNextAfter(prefix string, afterKey string) (string, []byte, bool, error) {
	return "", nil, false, nil
}
func (s *memoryEfficientStorage) CountPrefix(prefix string) (int, error)                  { return 0, nil }
func (s *memoryEfficientStorage) AtomicIncrement(key string) (int64, error)               { return 0, nil }
func (s *memoryEfficientStorage) DeleteByPrefix(prefix string) (int, error)               { return 0, nil }
func (s *memoryEfficientStorage) GetVersion(key string) (int64, error)                    { return 0, nil }
func (s *memoryEfficientStorage) IncrementVersion(key string) (int64, error)              { return 0, nil }
func (s *memoryEfficientStorage) ExpireAt(key string, expireTime time.Time) error         { return nil }
func (s *memoryEfficientStorage) GetTTL(key string) (time.Duration, error)                { return 0, nil }
func (s *memoryEfficientStorage) CleanExpired() (int, error)                              { return 0, nil }
func (s *memoryEfficientStorage) RunInTransaction(fn func(ports.Transaction) error) error { return nil }
func (s *memoryEfficientStorage) CreateSnapshot() (io.ReadCloser, error)                  { return nil, nil }
func (s *memoryEfficientStorage) CreateCompressedSnapshot() (io.ReadCloser, error)        { return nil, nil }
func (s *memoryEfficientStorage) RestoreSnapshot(snapshot io.Reader) error                { return nil }
func (s *memoryEfficientStorage) RestoreCompressedSnapshot(snapshot io.Reader) error      { return nil }
func (s *memoryEfficientStorage) SetRaftNode(node ports.RaftNode)                         {}
func (s *memoryEfficientStorage) PutWithTTL(key string, value []byte, version int64, ttl time.Duration) error {
	return nil
}
func (s *memoryEfficientStorage) Subscribe(prefix string) (<-chan ports.StorageEvent, func(), error) {
	return nil, func() {}, nil
}

type memoryEfficientEvents struct{}

func (e *memoryEfficientEvents) Start(ctx context.Context) error    { return nil }
func (e *memoryEfficientEvents) Stop() error                        { return nil }
func (e *memoryEfficientEvents) Broadcast(event domain.Event) error { return nil }
func (e *memoryEfficientEvents) OnWorkflowStarted(handler func(*domain.WorkflowStartedEvent)) error {
	return nil
}
func (e *memoryEfficientEvents) OnWorkflowCompleted(handler func(*domain.WorkflowCompletedEvent)) error {
	return nil
}
func (e *memoryEfficientEvents) OnWorkflowFailed(handler func(*domain.WorkflowErrorEvent)) error {
	return nil
}
func (e *memoryEfficientEvents) OnWorkflowPaused(handler func(*domain.WorkflowPausedEvent)) error {
	return nil
}
func (e *memoryEfficientEvents) OnWorkflowResumed(handler func(*domain.WorkflowResumedEvent)) error {
	return nil
}
func (e *memoryEfficientEvents) SubscribeToChannel(prefix string) (<-chan ports.StorageEvent, func(), error) {
	return nil, func() {}, nil
}
func (e *memoryEfficientEvents) Subscribe(pattern string, handler func(string, interface{})) error {
	return nil
}
func (e *memoryEfficientEvents) Unsubscribe(pattern string) error { return nil }
func (e *memoryEfficientEvents) BroadcastCommand(ctx context.Context, devCmd *domain.DevCommand) error {
	return nil
}
func (e *memoryEfficientEvents) RegisterCommandHandler(cmdName string, handler domain.CommandHandler) error {
	return nil
}
func (e *memoryEfficientEvents) OnNodeJoined(handler func(*domain.NodeJoinedEvent)) error { return nil }
func (e *memoryEfficientEvents) OnNodeLeft(handler func(*domain.NodeLeftEvent)) error     { return nil }
func (e *memoryEfficientEvents) OnLeaderChanged(handler func(*domain.LeaderChangedEvent)) error {
	return nil
}
func (e *memoryEfficientEvents) OnNodeStarted(handler func(*domain.NodeStartedEvent)) error {
	return nil
}
func (e *memoryEfficientEvents) OnNodeCompleted(handler func(*domain.NodeCompletedEvent)) error {
	return nil
}
func (e *memoryEfficientEvents) OnNodeError(handler func(*domain.NodeErrorEvent)) error { return nil }
func (e *memoryEfficientEvents) BroadcastWorkflowStarted(event *domain.WorkflowStartedEvent) error {
	return nil
}
func (e *memoryEfficientEvents) BroadcastWorkflowCompleted(event *domain.WorkflowCompletedEvent) error {
	return nil
}
func (e *memoryEfficientEvents) BroadcastWorkflowError(event *domain.WorkflowErrorEvent) error {
	return nil
}
func (e *memoryEfficientEvents) BroadcastWorkflowPaused(event *domain.WorkflowPausedEvent) error {
	return nil
}
func (e *memoryEfficientEvents) BroadcastWorkflowResumed(event *domain.WorkflowResumedEvent) error {
	return nil
}
func (e *memoryEfficientEvents) BroadcastNodeStarted(event *domain.NodeStartedEvent) error {
	return nil
}
func (e *memoryEfficientEvents) BroadcastNodeCompleted(event *domain.NodeCompletedEvent) error {
	return nil
}
func (e *memoryEfficientEvents) BroadcastNodeError(event *domain.NodeErrorEvent) error { return nil }

func TestLoadBalancer_CorruptedDataHandling(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	corruptedData := []ports.KeyValueVersion{
		{Key: "cluster:load:corrupt1", Value: []byte(`{"invalid":"json"malformed`)},
		{Key: "cluster:load:corrupt2", Value: []byte(`{"node_id":"","total_weight":"not_a_number"}`)},
		{Key: "cluster:load:corrupt3", Value: []byte(`null`)},
		{Key: "cluster:load:corrupt4", Value: []byte(`{"execution_units":null}`)},
		{Key: "cluster:load:corrupt5", Value: []byte(`{"recent_latency_ms":-999999,"recent_error_rate":2.5}`)},
	}

	storage.On("ListByPrefix", "cluster:load:").Return(corruptedData, nil)
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "corrupt-node", nil, &Config{FailurePolicy: "fail-open"}, nil)
	_ = manager

	clusterLoad, err := manager.GetClusterLoad()
	assert.NoError(t, err)

	t.Logf("Cluster load from corrupted data: %+v", clusterLoad)

	shouldExecute, err := manager.ShouldExecuteNode("corrupt-node", "test-workflow", "test-node")
	assert.NoError(t, err, "Should handle corrupted data gracefully")
	assert.True(t, shouldExecute, "Should default to execute on corrupted data")
}

func TestLoadBalancer_ExtremeValues(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "extreme-node", nil, &Config{FailurePolicy: "fail-open"}, nil)
	_ = manager

	testCases := []struct {
		name     string
		load     *ports.NodeLoad
		capacity float64
	}{
		{
			name: "zero_values",
			load: &ports.NodeLoad{
				NodeID:          "zero-node",
				TotalWeight:     0,
				RecentLatencyMs: 0,
				RecentErrorRate: 0,
			},
			capacity: 0,
		},
		{
			name: "negative_values",
			load: &ports.NodeLoad{
				NodeID:          "negative-node",
				TotalWeight:     -100,
				RecentLatencyMs: -1000,
				RecentErrorRate: -0.5,
			},
			capacity: -10,
		},
		{
			name: "infinity_values",
			load: &ports.NodeLoad{
				NodeID:          "inf-node",
				TotalWeight:     math.Inf(1),
				RecentLatencyMs: math.Inf(1),
				RecentErrorRate: math.Inf(1),
			},
			capacity: math.Inf(1),
		},
		{
			name: "nan_values",
			load: &ports.NodeLoad{
				NodeID:          "nan-node",
				TotalWeight:     math.NaN(),
				RecentLatencyMs: math.NaN(),
				RecentErrorRate: math.NaN(),
			},
			capacity: math.NaN(),
		},
		{
			name: "max_float_values",
			load: &ports.NodeLoad{
				NodeID:          "max-node",
				TotalWeight:     math.MaxFloat64,
				RecentLatencyMs: math.MaxFloat64,
				RecentErrorRate: math.MaxFloat64,
			},
			capacity: math.MaxFloat64,
		},
	}

	scorer := DefaultScorer
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			score := scorer.CalculateScore(tc.load, tc.capacity)
			t.Logf("Score for %s: %f", tc.name, score)

			if math.IsNaN(score) {
				t.Logf("WARNING: Score is NaN for %s", tc.name)
			}
			if math.IsInf(score, 0) {
				t.Logf("WARNING: Score is infinite for %s", tc.name)
			}
		})
	}
}

func TestLoadBalancer_DeadlockPrevention(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	storage.On("Put", mock.AnythingOfType("string"), mock.MatchedBy(func(data []byte) bool { return true }), int64(0)).Return(nil).Maybe()
	storage.On("ListByPrefix", "cluster:load:").Return([]ports.KeyValueVersion{}, nil).Maybe()
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "deadlock-node", nil, &Config{FailurePolicy: "fail-open"}, nil)
	ctx := context.Background()
	err := manager.Start(ctx)
	assert.NoError(t, err)
	defer manager.Stop()

	done := make(chan bool, 1)
	timeout := time.After(30 * time.Second)

	go func() {
		var wg sync.WaitGroup
		const workers = 20

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					workflowID := fmt.Sprintf("deadlock-%d-%d", id, j)

					manager.onNodeStarted(&domain.NodeStartedEvent{
						WorkflowID: workflowID,
						NodeName:   "test-node",
						NodeID:     "deadlock-node",
					})

					_, _ = manager.GetClusterLoad()
					_, _ = manager.ShouldExecuteNode("deadlock-node", workflowID, "test")

					manager.onNodeCompleted(&domain.NodeCompletedEvent{
						WorkflowID: workflowID,
						Duration:   1 * time.Millisecond,
					})
				}
			}(i)
		}

		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		t.Log("Deadlock prevention test completed successfully")
	case <-timeout:
		t.Fatal("Test timed out - possible deadlock detected")
	}
}

func TestLoadBalancer_FloatOverflowHandling(t *testing.T) {
	window := NewRollingWindow(100)

	for i := 0; i < 10000; i++ {
		window.Record(i%2 == 0)
	}

	errorRate := window.GetErrorRate()
	assert.False(t, math.IsNaN(errorRate), "Error rate should not be NaN")
	assert.False(t, math.IsInf(errorRate, 0), "Error rate should not be infinite")
	assert.True(t, errorRate >= 0 && errorRate <= 1, "Error rate should be between 0 and 1")

	var ewma float64 = 100
	for i := 0; i < 10000; i++ {
		value := float64(i) * 1000000
		ewma = UpdateEWMA(ewma, value, 0.2)

		if math.IsNaN(ewma) || math.IsInf(ewma, 0) {
			t.Fatalf("EWMA became invalid at iteration %d: %f", i, ewma)
		}
	}

	t.Logf("Final EWMA after overflow test: %f", ewma)
}

func TestLoadBalancer_PerformanceDegradation(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	nodeLoads := make([]ports.KeyValueVersion, 1000)
	for i := 0; i < 1000; i++ {
		load := ports.NodeLoad{
			NodeID:          fmt.Sprintf("perf-node-%d", i),
			TotalWeight:     float64(i % 50),
			RecentLatencyMs: float64(i % 200),
			RecentErrorRate: float64(i%10) / 100.0,
			LastUpdated:     time.Now().Unix(),
			ExecutionUnits:  make(map[string]float64),
		}

		for j := 0; j < i%20; j++ {
			load.ExecutionUnits[fmt.Sprintf("wf-%d-%d", i, j)] = float64(j % 5)
		}

		data, _ := json.Marshal(load)
		nodeLoads[i] = ports.KeyValueVersion{
			Key:   fmt.Sprintf("cluster:load:perf-node-%d", i),
			Value: data,
		}
	}

	storage.On("ListByPrefix", "cluster:load:").Return(nodeLoads, nil)
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "perf-test-node", nil, &Config{FailurePolicy: "fail-open"}, nil)

	iterations := 1000
	totalDuration := time.Duration(0)

	for i := 0; i < iterations; i++ {
		start := time.Now()
		_, err := manager.ShouldExecuteNode("perf-test-node", fmt.Sprintf("wf-%d", i), "test-node")
		elapsed := time.Since(start)
		totalDuration += elapsed

		assert.NoError(t, err)

		if elapsed > 100*time.Millisecond {
			t.Errorf("Decision took too long: %v on iteration %d", elapsed, i)
		}
	}

	avgDuration := totalDuration / time.Duration(iterations)
	t.Logf("Average decision time with 1000 nodes: %v", avgDuration)

	if avgDuration > 10*time.Millisecond {
		t.Errorf("Performance degradation detected: average %v exceeds 10ms threshold", avgDuration)
	}
}

func TestLoadBalancer_EventLeakage(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	storage.On("Put", mock.AnythingOfType("string"), mock.MatchedBy(func(data []byte) bool { return true }), int64(0)).Return(nil)
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "leak-test", nil, &Config{FailurePolicy: "fail-open"}, nil)
	ctx := context.Background()
	err := manager.Start(ctx)
	assert.NoError(t, err)
	defer manager.Stop()

	workflowIDs := make(map[string]bool)

	for i := 0; i < 10000; i++ {
		workflowID := fmt.Sprintf("leak-workflow-%d", i)
		workflowIDs[workflowID] = true

		manager.onNodeStarted(&domain.NodeStartedEvent{
			WorkflowID: workflowID,
			NodeName:   "test-node",
			NodeID:     "leak-test",
		})

		if i%2 == 0 {
			manager.onNodeCompleted(&domain.NodeCompletedEvent{
				WorkflowID: workflowID,
				Duration:   time.Millisecond,
			})
			delete(workflowIDs, workflowID)
		}
	}

	expectedRemaining := len(workflowIDs)
	actualRemaining := len(manager.executionUnits)

	t.Logf("Expected remaining workflows: %d, Actual: %d", expectedRemaining, actualRemaining)

	if actualRemaining != expectedRemaining {
		t.Errorf("Event leakage detected: expected %d workflows, found %d", expectedRemaining, actualRemaining)

		for id := range workflowIDs {
			if _, exists := manager.executionUnits[id]; !exists {
				t.Errorf("Missing workflow: %s", id)
			}
		}

		for id := range manager.executionUnits {
			if !workflowIDs[id] {
				t.Errorf("Unexpected workflow found: %s", id)
			}
		}
	}
}
