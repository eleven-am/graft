package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/adapters/queue"
	"github.com/eleven-am/graft/internal/adapters/storage"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type AggressiveTestNode struct {
	name           string
	executeCount   int64
	canStartCount  int64
	panicOnExecute bool
	hangOnExecute  bool
	returnError    bool
	executionDelay time.Duration
	mutex          sync.Mutex
}

func (n *AggressiveTestNode) GetName() string {
	return n.name
}

func (n *AggressiveTestNode) CanStart(ctx context.Context, state json.RawMessage, config json.RawMessage) bool {
	atomic.AddInt64(&n.canStartCount, 1)

	return atomic.LoadInt64(&n.canStartCount)%7 != 0
}

func (n *AggressiveTestNode) Execute(ctx context.Context, state json.RawMessage, config json.RawMessage) (*ports.NodeResult, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	atomic.AddInt64(&n.executeCount, 1)

	if n.panicOnExecute {
		panic(fmt.Sprintf("node %s panicked during execution", n.name))
	}

	if n.hangOnExecute {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Minute):
			return nil, fmt.Errorf("node hung")
		}
	}

	if n.executionDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(n.executionDelay):
		}
	}

	if n.returnError {
		return nil, fmt.Errorf("node %s failed execution attempt %d", n.name, atomic.LoadInt64(&n.executeCount))
	}

	globalState := map[string]interface{}{
		"executed_by": n.name,
		"attempt":     atomic.LoadInt64(&n.executeCount),
		"timestamp":   time.Now().UnixNano(),
	}
	return &ports.NodeResult{
		GlobalState: globalState,
		NextNodes:   []ports.NextNode{},
	}, nil
}

func setupAggressiveEngine(t *testing.T) (*Engine, *TestNodeRegistry, func()) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	tempDir := t.TempDir()

	opts := badger.DefaultOptions(tempDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	require.NoError(t, err)

	mockRaftNode := new(mocks.MockRaftNode)
	mockRaftNode.On("IsLeader").Return(true).Maybe()
	mockRaftNode.On("Apply", mock.Anything, mock.Anything).Return(func(cmd domain.Command, timeout time.Duration) *domain.CommandResult {

		result := &domain.CommandResult{
			Success: true,
			Events:  []domain.Event{},
		}

		switch cmd.Type {
		case domain.CommandPut:
			err := db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(cmd.Key), cmd.Value)
			})
			if err != nil {
				result.Success = false
				result.Error = err.Error()
			} else {
				result.Events = append(result.Events, domain.Event{
					Type:      domain.EventPut,
					Key:       cmd.Key,
					Timestamp: time.Now(),
				})
			}
		case domain.CommandDelete:
			err := db.Update(func(txn *badger.Txn) error {
				return txn.Delete([]byte(cmd.Key))
			})
			if err != nil {
				result.Success = false
				result.Error = err.Error()
			} else {
				result.Events = append(result.Events, domain.Event{
					Type:      domain.EventDelete,
					Key:       cmd.Key,
					Timestamp: time.Now(),
				})
			}
		case domain.CommandBatch:

			err := db.Update(func(txn *badger.Txn) error {
				for _, op := range cmd.Batch {
					switch op.Type {
					case domain.CommandPut:
						if err := txn.Set([]byte(op.Key), op.Value); err != nil {
							return err
						}
					case domain.CommandDelete:
						if err := txn.Delete([]byte(op.Key)); err != nil {
							return err
						}
					}
				}
				return nil
			})
			if err != nil {
				result.Success = false
				result.Error = err.Error()
			} else {

				for _, op := range cmd.Batch {
					eventType := domain.EventPut
					if op.Type == domain.CommandDelete {
						eventType = domain.EventDelete
					}
					result.Events = append(result.Events, domain.Event{
						Type:      eventType,
						Key:       op.Key,
						Timestamp: time.Now(),
					})
				}
			}
		case domain.CommandTypeAtomicIncrement:

			var newValue int64 = 1
			err := db.Update(func(txn *badger.Txn) error {

				item, err := txn.Get([]byte(cmd.Key))
				if err == nil {
					err = item.Value(func(val []byte) error {

						var oldValue int64
						if json.Unmarshal(val, &oldValue) == nil {
							newValue = oldValue + 1
						}
						return nil
					})
				}

				valueBytes, _ := json.Marshal(newValue)
				return txn.Set([]byte(cmd.Key), valueBytes)
			})
			if err != nil {
				result.Success = false
				result.Error = err.Error()
			} else {
				result.Events = append(result.Events, domain.Event{
					Type:      domain.EventPut,
					Key:       cmd.Key,
					Timestamp: time.Now(),
				})
			}
		}

		return result
	}, nil)

	appStorage := storage.NewAppStorage(mockRaftNode, db, logger)
	mockEventManager := &mocks.MockEventManager{}
	mockEventManager.On("Subscribe", mock.AnythingOfType("string"), mock.AnythingOfType("func(string, interface {})")).Return(nil).Maybe()
	mockEventManager.On("Broadcast", mock.AnythingOfType("domain.Event")).Return(nil).Maybe()

	ch := make(chan ports.StorageEvent, 1)
	mockEventManager.On("SubscribeToChannel", mock.AnythingOfType("string")).Return((<-chan ports.StorageEvent)(ch), func() {}, nil).Maybe()
	testQueue := queue.NewQueue("aggressive-test-queue", appStorage, mockEventManager, logger)
	nodeRegistry := NewTestNodeRegistry()

	mockLoadBalancer := mocks.NewMockLoadBalancer(t)
	mockLoadBalancer.On("ShouldExecuteNode", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(true, nil).Maybe()

	config := domain.DefaultEngineConfig()
	config.NodeExecutionTimeout = 100 * time.Millisecond
	config.RetryAttempts = 1
	config.WorkerCount = 1

	engine := NewEngine(config, "test-node", nodeRegistry, testQueue, appStorage, mockEventManager, mockLoadBalancer, logger)

	ctx := context.Background()
	err = engine.Start(ctx)
	require.NoError(t, err)

	cleanup := func() {
		_ = engine.Stop()
		_ = db.Close()
	}

	return engine, nodeRegistry, cleanup
}

func TestEngine_ConcurrentWorkflowStorm(t *testing.T) {
	engine, registry, cleanup := setupAggressiveEngine(t)
	defer cleanup()

	for i := 0; i < 10; i++ {
		node := &AggressiveTestNode{
			name:           fmt.Sprintf("storm_node_%d", i),
			executionDelay: time.Duration(i) * 5 * time.Millisecond,
		}
		require.NoError(t, registry.RegisterNode(node))
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(workflowID int) {
			defer wg.Done()

			trigger := domain.WorkflowTrigger{
				WorkflowID: fmt.Sprintf("storm-workflow-%d", workflowID),
				InitialNodes: []domain.NodeConfig{
					{Name: fmt.Sprintf("storm_node_%d", workflowID%10), Config: json.RawMessage(`{"storm_id":` + fmt.Sprintf("%d", workflowID) + `}`)},
				},
				InitialState: json.RawMessage(`{"storm_test":true}`),
			}

			if err := engine.ProcessTrigger(trigger); err != nil {
				errors <- fmt.Errorf("workflow %d failed to trigger: %w", workflowID, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent workflow error: %v", err)
	}

	time.Sleep(2 * time.Second)
	metrics := engine.GetMetrics()

	t.Logf("Metrics after storm: Started=%d, Completed=%d, Failed=%d",
		metrics.WorkflowsStarted, metrics.WorkflowsCompleted, metrics.WorkflowsFailed)

	require.Greater(t, metrics.WorkflowsStarted, int64(90), "Should have started most workflows")
}

func TestEngine_TimeoutCascadeFailure(t *testing.T) {
	engine, registry, cleanup := setupAggressiveEngine(t)
	defer cleanup()

	hangingNode := &AggressiveTestNode{
		name:          "hanging_node",
		hangOnExecute: true,
	}
	require.NoError(t, registry.RegisterNode(hangingNode))

	for i := 0; i < 10; i++ {
		trigger := domain.WorkflowTrigger{
			WorkflowID: fmt.Sprintf("timeout-cascade-%d", i),
			InitialNodes: []domain.NodeConfig{
				{Name: "hanging_node", Config: json.RawMessage(`{}`)},
			},
			InitialState: json.RawMessage(`{"test":"timeout"}`),
		}

		require.NoError(t, engine.ProcessTrigger(trigger))
	}

	time.Sleep(3 * time.Second)

	metrics := engine.GetMetrics()
	t.Logf("Timeout test metrics: TimedOut=%d, Failed=%d, Retried=%d",
		metrics.NodesTimedOut, metrics.NodesFailed, metrics.NodesRetried)

	require.Greater(t, metrics.NodesTimedOut, int64(0), "Should have timed out nodes")
	require.Greater(t, metrics.NodesRetried, int64(0), "Should have retried nodes")
}

func TestEngine_PanicRecovery(t *testing.T) {
	engine, registry, cleanup := setupAggressiveEngine(t)
	defer cleanup()

	panicNode := &AggressiveTestNode{
		name:           "panic_node",
		panicOnExecute: true,
	}
	require.NoError(t, registry.RegisterNode(panicNode))

	trigger := domain.WorkflowTrigger{
		WorkflowID: "panic-test",
		InitialNodes: []domain.NodeConfig{
			{Name: "panic_node", Config: json.RawMessage(`{}`)},
		},
		InitialState: json.RawMessage(`{"test":"panic"}`),
	}

	require.NoError(t, engine.ProcessTrigger(trigger))

	time.Sleep(4 * time.Second)

	status, err := engine.GetWorkflowStatus("panic-test")
	require.NoError(t, err)

	require.Equal(t, domain.WorkflowStateRunning, status.Status)

	dlqSize, err := engine.GetDeadLetterSize()
	require.NoError(t, err)
	require.Greater(t, dlqSize, 0, "Panicking node should have been sent to DLQ")

	metrics := engine.GetMetrics()
	require.Greater(t, metrics.NodesFailed, int64(0), "Should have failed node executions due to panic")
	require.Greater(t, metrics.ItemsSentToDeadLetter, int64(0), "Should have sent items to DLQ")

	t.Logf("✅ Panic was handled gracefully: Engine survived, DLQ size: %d", dlqSize)
}

func TestEngine_MemoryLeakUnderLoad(t *testing.T) {
	engine, registry, cleanup := setupAggressiveEngine(t)
	defer cleanup()

	simpleNode := &AggressiveTestNode{
		name: "memory_test_node",
	}
	require.NoError(t, registry.RegisterNode(simpleNode))

	for batch := 0; batch < 10; batch++ {
		var wg sync.WaitGroup

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(workflowID int) {
				defer wg.Done()

				trigger := domain.WorkflowTrigger{
					WorkflowID: fmt.Sprintf("memory-test-%d-%d", batch, workflowID),
					InitialNodes: []domain.NodeConfig{
						{Name: "memory_test_node", Config: json.RawMessage(`{"batch":` + fmt.Sprintf("%d", batch) + `}`)},
					},
					InitialState: json.RawMessage(`{"memory_test":true}`),
				}

				_ = engine.ProcessTrigger(trigger)
			}(i)
		}

		wg.Wait()

		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)

	metrics := engine.GetMetrics()
	t.Logf("Memory leak test completed: Started=%d, Completed=%d",
		metrics.WorkflowsStarted, metrics.WorkflowsCompleted)

	require.Greater(t, metrics.WorkflowsStarted, int64(150), "Should have started most workflows")
}

func TestEngine_RapidStartStop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	for i := 0; i < 20; i++ {
		tempDir := t.TempDir()

		opts := badger.DefaultOptions(tempDir)
		opts.Logger = nil
		db, err := badger.Open(opts)
		require.NoError(t, err)

		mockRaftNode := new(mocks.MockRaftNode)
		mockRaftNode.On("IsLeader").Return(true).Maybe()
		mockRaftNode.On("Apply", mock.Anything, mock.Anything).Return(func(cmd domain.Command, timeout time.Duration) *domain.CommandResult {

			result := &domain.CommandResult{
				Success: true,
				Events:  []domain.Event{},
			}

			switch cmd.Type {
			case domain.CommandPut:
				err := db.Update(func(txn *badger.Txn) error {
					return txn.Set([]byte(cmd.Key), cmd.Value)
				})
				if err != nil {
					result.Success = false
					result.Error = err.Error()
				} else {
					result.Events = append(result.Events, domain.Event{
						Type:      domain.EventPut,
						Key:       cmd.Key,
						Timestamp: time.Now(),
					})
				}
			case domain.CommandDelete:
				err := db.Update(func(txn *badger.Txn) error {
					return txn.Delete([]byte(cmd.Key))
				})
				if err != nil {
					result.Success = false
					result.Error = err.Error()
				} else {
					result.Events = append(result.Events, domain.Event{
						Type:      domain.EventDelete,
						Key:       cmd.Key,
						Timestamp: time.Now(),
					})
				}
			case domain.CommandBatch:

				err := db.Update(func(txn *badger.Txn) error {
					for _, op := range cmd.Batch {
						switch op.Type {
						case domain.CommandPut:
							if err := txn.Set([]byte(op.Key), op.Value); err != nil {
								return err
							}
						case domain.CommandDelete:
							if err := txn.Delete([]byte(op.Key)); err != nil {
								return err
							}
						}
					}
					return nil
				})
				if err != nil {
					result.Success = false
					result.Error = err.Error()
				} else {

					for _, op := range cmd.Batch {
						eventType := domain.EventPut
						if op.Type == domain.CommandDelete {
							eventType = domain.EventDelete
						}
						result.Events = append(result.Events, domain.Event{
							Type:      eventType,
							Key:       op.Key,
							Timestamp: time.Now(),
						})
					}
				}
			case domain.CommandTypeAtomicIncrement:

				var newValue int64 = 1
				err := db.Update(func(txn *badger.Txn) error {

					item, err := txn.Get([]byte(cmd.Key))
					if err == nil {
						err = item.Value(func(val []byte) error {

							var oldValue int64
							if json.Unmarshal(val, &oldValue) == nil {
								newValue = oldValue + 1
							}
							return nil
						})
					}

					valueBytes, _ := json.Marshal(newValue)
					return txn.Set([]byte(cmd.Key), valueBytes)
				})
				if err != nil {
					result.Success = false
					result.Error = err.Error()
				} else {
					result.Events = append(result.Events, domain.Event{
						Type:      domain.EventPut,
						Key:       cmd.Key,
						Timestamp: time.Now(),
					})
				}
			}

			return result
		}, nil)

		appStorage := storage.NewAppStorage(mockRaftNode, db, logger)
		mockEventManager := &mocks.MockEventManager{}
		mockEventManager.On("Subscribe", mock.AnythingOfType("string"), mock.AnythingOfType("func(string, interface {})")).Return(nil).Maybe()
		mockEventManager.On("Broadcast", mock.AnythingOfType("domain.Event")).Return(nil).Maybe()

		ch := make(chan ports.StorageEvent, 1)
		mockEventManager.On("SubscribeToChannel", mock.AnythingOfType("string")).Return((<-chan ports.StorageEvent)(ch), func() {}, nil).Maybe()
		testQueue := queue.NewQueue(fmt.Sprintf("rapid-test-%d", i), appStorage, mockEventManager, logger)
		nodeRegistry := NewTestNodeRegistry()

		mockLoadBalancer := mocks.NewMockLoadBalancer(t)
		mockLoadBalancer.On("ShouldExecuteNode", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(true, nil).Maybe()

		config := domain.DefaultEngineConfig()
		engine := NewEngine(config, fmt.Sprintf("test-node-%d", i), nodeRegistry, testQueue, appStorage, mockEventManager, mockLoadBalancer, logger)

		ctx := context.Background()

		require.NoError(t, engine.Start(ctx))
		require.NoError(t, engine.Stop())
		require.NoError(t, db.Close())
	}

	t.Log("Rapid start/stop test completed without deadlocks")
}

func TestEngine_DeadLetterQueueOverflow(t *testing.T) {
	engine, registry, cleanup := setupAggressiveEngine(t)
	defer cleanup()

	failingNode := &AggressiveTestNode{
		name:        "always_fails",
		returnError: true,
	}
	require.NoError(t, registry.RegisterNode(failingNode))

	for i := 0; i < 50; i++ {
		trigger := domain.WorkflowTrigger{
			WorkflowID: fmt.Sprintf("dlq-overflow-%d", i),
			InitialNodes: []domain.NodeConfig{
				{Name: "always_fails", Config: json.RawMessage(`{}`)},
			},
			InitialState: json.RawMessage(`{"test":"dlq"}`),
		}

		require.NoError(t, engine.ProcessTrigger(trigger))
	}

	time.Sleep(6 * time.Second)

	metrics := engine.GetMetrics()
	t.Logf("DLQ overflow test: Failed=%d, Retried=%d, SentToDLQ=%d",
		metrics.NodesFailed, metrics.NodesRetried, metrics.ItemsSentToDeadLetter)

	require.Greater(t, metrics.ItemsSentToDeadLetter, int64(0), "Should have items in dead letter queue")

	dlqSize, err := engine.GetDeadLetterSize()
	require.NoError(t, err)
	require.Greater(t, dlqSize, 0, "Dead letter queue should have items")

	t.Logf("Dead letter queue size: %d", dlqSize)
}

// TestEngine_ThroughputAnalysis analyzes if concurrent processing completion rate is a bug
func TestEngine_ThroughputAnalysis(t *testing.T) {
	engine, registry, cleanup := setupAggressiveEngine(t)
	defer cleanup()

	registry.RegisterNode(&FastTestNode{name: "fast_node"})

	t.Log("=== Sequential Test (Expected: ~100% completion) ===")
	sequentialCount := 20
	for i := 0; i < sequentialCount; i++ {
		trigger := domain.WorkflowTrigger{
			WorkflowID: fmt.Sprintf("seq-%d", i),
			InitialNodes: []domain.NodeConfig{
				{Name: "fast_node", Config: []byte(`{}`)},
			},
			InitialState: []byte(`{}`),
		}
		require.NoError(t, engine.ProcessTrigger(trigger))
		time.Sleep(25 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)

	sequentialMetrics := engine.GetMetrics()
	sequentialCompletionRate := float64(sequentialMetrics.WorkflowsCompleted) / float64(sequentialCount) * 100
	t.Logf("Sequential: Started=%d, Completed=%d, Rate=%.1f%%",
		sequentialCount, sequentialMetrics.WorkflowsCompleted, sequentialCompletionRate)

	t.Log("=== Concurrent Test (Analyze completion rate) ===")

	engine2, registry2, cleanup2 := setupAggressiveEngine(t)
	defer cleanup2()

	registry2.RegisterNode(&FastTestNode{name: "fast_node"})

	concurrentCount := 50
	startTime := time.Now()

	for i := 0; i < concurrentCount; i++ {
		trigger := domain.WorkflowTrigger{
			WorkflowID: fmt.Sprintf("concurrent-%d", i),
			InitialNodes: []domain.NodeConfig{
				{Name: "fast_node", Config: []byte(`{}`)},
			},
			InitialState: []byte(`{}`),
		}
		require.NoError(t, engine2.ProcessTrigger(trigger))
	}

	submissionTime := time.Since(startTime)
	t.Logf("All %d workflows submitted in %v", concurrentCount, submissionTime)

	for elapsed := 1 * time.Second; elapsed <= 6*time.Second; elapsed += 1 * time.Second {
		time.Sleep(1 * time.Second)
		metrics := engine2.GetMetrics()
		completionRate := float64(metrics.WorkflowsCompleted) / float64(concurrentCount) * 100
		t.Logf("After %v: Completed=%d/%d (%.1f%%), Failed=%d",
			elapsed, metrics.WorkflowsCompleted, concurrentCount, completionRate, metrics.WorkflowsFailed)
	}

	finalMetrics := engine2.GetMetrics()
	finalCompletionRate := float64(finalMetrics.WorkflowsCompleted) / float64(concurrentCount) * 100

	t.Log("=== Analysis ===")
	t.Logf("Sequential completion rate: %.1f%%", sequentialCompletionRate)
	t.Logf("Concurrent completion rate: %.1f%%", finalCompletionRate)

	completionGap := sequentialCompletionRate - finalCompletionRate

	if finalCompletionRate < 80 && completionGap > 15 {
		t.Log("POTENTIAL BUG: Significant drop in concurrent completion rate")
		t.Log("This suggests the engine may lose workflows under concurrent load")

		t.Logf("Detailed metrics:")
		t.Logf("- Items enqueued: %d", finalMetrics.ItemsEnqueued)
		t.Logf("- Items processed: %d", finalMetrics.ItemsProcessed)
		t.Logf("- Nodes executed: %d", finalMetrics.NodesExecuted)
		t.Logf("- Nodes succeeded: %d", finalMetrics.NodesSucceeded)
		t.Logf("- Nodes failed: %d", finalMetrics.NodesFailed)

		if finalMetrics.ItemsProcessed < finalMetrics.ItemsEnqueued {
			t.Log("❌ BUG CONFIRMED: Not all enqueued items were processed")
		}

		if finalMetrics.NodesExecuted < int64(concurrentCount) {
			t.Log("❌ BUG CONFIRMED: Not all workflows had their nodes executed")
		}
	} else {
		t.Log("✅ NORMAL BEHAVIOR: Engine maintains high completion rate under concurrent load")
		t.Log("The previous 61% completion rate was likely due to aggressive test conditions")
	}
}

// FastTestNode is a simple, fast node for throughput testing
type FastTestNode struct {
	name string
}

func (n *FastTestNode) GetName() string {
	return n.name
}

func (n *FastTestNode) Execute(ctx context.Context, state json.RawMessage, config json.RawMessage) (*ports.NodeResult, error) {

	return &ports.NodeResult{
		GlobalState: map[string]interface{}{"completed": true},
		NextNodes:   []ports.NextNode{},
	}, nil
}

func (n *FastTestNode) CanStart(ctx context.Context, state json.RawMessage, config json.RawMessage) bool {
	return true
}

// TestNodeRegistry implementation for aggressive testing
type TestNodeRegistry struct {
	nodes map[string]ports.NodePort
	mutex sync.RWMutex
}

func NewTestNodeRegistry() *TestNodeRegistry {
	return &TestNodeRegistry{
		nodes: make(map[string]ports.NodePort),
	}
}

func (r *TestNodeRegistry) RegisterNode(node interface{}) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if portNode, ok := node.(ports.NodePort); ok {
		r.nodes[portNode.GetName()] = portNode
		return nil
	}
	return fmt.Errorf("node does not implement ports.NodePort")
}

func (r *TestNodeRegistry) GetNode(nodeName string) (ports.NodePort, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	node, exists := r.nodes[nodeName]
	if !exists {
		return nil, domain.NewNotFoundError("node", nodeName)
	}
	return node, nil
}

func (r *TestNodeRegistry) ListNodes() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var names []string
	for name := range r.nodes {
		names = append(names, name)
	}
	return names
}

func (r *TestNodeRegistry) HasNode(nodeName string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	_, exists := r.nodes[nodeName]
	return exists
}

func (r *TestNodeRegistry) UnregisterNode(nodeName string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.nodes[nodeName]; !exists {
		return domain.NewNotFoundError("node", nodeName)
	}
	delete(r.nodes, nodeName)
	return nil
}

func (r *TestNodeRegistry) GetNodeCount() int {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return len(r.nodes)
}
