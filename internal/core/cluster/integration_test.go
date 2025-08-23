package cluster

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testPortCounter int64

func TestCluster_Integration_BasicWorkflow(t *testing.T) {
	ctx := context.Background()

	config := createTestConfig(t)
	logger := slog.Default()

	cluster, err := New(config, logger)
	require.NoError(t, err)

	err = cluster.Start(ctx)
	require.NoError(t, err)
	defer cluster.Stop()

	testNode := &TestNode{
		name: "test-node",
		executeFunc: func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
			time.Sleep(500 * time.Millisecond)
			return map[string]interface{}{
				"result":          "success",
				"processed_count": 42,
			}, []ports.NextNode{}, nil
		},
	}

	err = cluster.RegisterNode(testNode)
	require.NoError(t, err)

	trigger := ports.WorkflowTrigger{
		WorkflowID:   "test-workflow-001",
		InitialState: map[string]interface{}{"input": "test-data"},
		InitialNodes: []ports.NodeConfig{
			{
				Name: "test-node",
				Config: map[string]interface{}{
					"timeout": 30,
					"retries": 3,
				},
			},
		},
		Metadata: map[string]string{
			"source":      "integration-test",
			"environment": "test",
		},
	}

	err = cluster.ProcessTrigger(trigger)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	status, err := cluster.GetWorkflowStatus("test-workflow-001")
	require.NoError(t, err)
	assert.Equal(t, "test-workflow-001", status.WorkflowID)
	assert.Contains(t, []ports.WorkflowState{
		ports.WorkflowStateRunning,
		ports.WorkflowStateCompleted,
	}, status.Status)
}

func TestCluster_Integration_MultiNodeWorkflow(t *testing.T) {
	ctx := context.Background()

	config := createTestConfig(t)
	logger := slog.Default()

	cluster, err := New(config, logger)
	require.NoError(t, err)

	err = cluster.Start(ctx)
	require.NoError(t, err)
	defer cluster.Stop()

	dataNode := &TestNode{
		name: "data-processor",
		executeFunc: func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
			time.Sleep(800 * time.Millisecond)
			return map[string]interface{}{
				"processed_data": []string{"item1", "item2", "item3"},
				"count":          3,
			}, []ports.NextNode{}, nil
		},
	}

	validatorNode := &TestNode{
		name: "validator",
		executeFunc: func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
			time.Sleep(800 * time.Millisecond)
			return map[string]interface{}{
				"validation_result": "passed",
				"validated_count":   3,
			}, []ports.NextNode{}, nil
		},
	}

	reportNode := &TestNode{
		name: "report-generator",
		executeFunc: func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
			time.Sleep(800 * time.Millisecond)
			return map[string]interface{}{
				"report_id": "report-12345",
				"status":    "generated",
			}, []ports.NextNode{}, nil
		},
	}

	require.NoError(t, cluster.RegisterNode(dataNode))
	require.NoError(t, cluster.RegisterNode(validatorNode))
	require.NoError(t, cluster.RegisterNode(reportNode))

	trigger := ports.WorkflowTrigger{
		WorkflowID:   "multi-node-workflow-001",
		InitialState: map[string]interface{}{"dataset": "test-dataset"},
		InitialNodes: []ports.NodeConfig{
			{Name: "data-processor", Config: map[string]interface{}{"batch_size": 100}},
			{Name: "validator", Config: map[string]interface{}{"strict_mode": true}},
			{Name: "report-generator", Config: map[string]interface{}{"format": "json"}},
		},
		Metadata: map[string]string{
			"workflow_type": "data-pipeline",
			"priority":      "high",
		},
	}

	err = cluster.ProcessTrigger(trigger)
	require.NoError(t, err)

	time.Sleep(400 * time.Millisecond)

	status, err := cluster.GetWorkflowStatus("multi-node-workflow-001")
	require.NoError(t, err)
	assert.Equal(t, "multi-node-workflow-001", status.WorkflowID)
}

func TestCluster_Integration_ClusterInfo(t *testing.T) {
	ctx := context.Background()

	config := createTestConfig(t)
	logger := slog.Default()

	cluster, err := New(config, logger)
	require.NoError(t, err)

	err = cluster.Start(ctx)
	require.NoError(t, err)
	defer cluster.Stop()

	testNode := &TestNode{name: "info-test-node"}
	err = cluster.RegisterNode(testNode)
	require.NoError(t, err)

	info := cluster.GetClusterInfo()

	assert.Equal(t, config.NodeID, info.NodeID)
	assert.Contains(t, info.RegisteredNodes, "info-test-node")
	assert.Equal(t, config.Resources.MaxConcurrentTotal, info.ResourceLimits.MaxConcurrentTotal)
	assert.Equal(t, config.Resources.MaxConcurrentTotal, info.ExecutionStats.TotalCapacity)
	assert.Equal(t, config.Resources.MaxConcurrentTotal, info.ExecutionStats.AvailableSlots)
}

func TestCluster_Integration_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	config := createTestConfig(t)
	logger := slog.Default()

	cluster, err := New(config, logger)
	require.NoError(t, err)

	err = cluster.Start(ctx)
	require.NoError(t, err)
	defer cluster.Stop()

	errorNode := &TestNode{
		name: "error-node",
		executeFunc: func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
			time.Sleep(500 * time.Millisecond)
			return nil, []ports.NextNode{}, fmt.Errorf("simulated node execution error")
		},
	}

	err = cluster.RegisterNode(errorNode)
	require.NoError(t, err)

	trigger := ports.WorkflowTrigger{
		WorkflowID:   "error-workflow-001",
		InitialState: map[string]interface{}{"test": "error-handling"},
		InitialNodes: []ports.NodeConfig{
			{Name: "error-node", Config: map[string]interface{}{}},
		},
	}

	err = cluster.ProcessTrigger(trigger)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	status, err := cluster.GetWorkflowStatus("error-workflow-001")
	require.NoError(t, err)
	assert.Equal(t, "error-workflow-001", status.WorkflowID)
}

func TestCluster_Integration_ResourceLimits(t *testing.T) {
	ctx := context.Background()

	config := createTestConfig(t)
	config.Resources.MaxConcurrentTotal = 2
	config.Resources.MaxConcurrentPerType = map[string]int{
		"limited-node": 1,
	}
	logger := slog.Default()

	cluster, err := New(config, logger)
	require.NoError(t, err)

	err = cluster.Start(ctx)
	require.NoError(t, err)
	defer cluster.Stop()

	limitedNode := &TestNode{
		name: "limited-node",
		executeFunc: func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
			time.Sleep(1 * time.Second)
			return map[string]interface{}{"result": "completed"}, []ports.NextNode{}, nil
		},
	}

	err = cluster.RegisterNode(limitedNode)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		trigger := ports.WorkflowTrigger{
			WorkflowID:   fmt.Sprintf("resource-test-workflow-%d", i+1),
			InitialState: map[string]interface{}{"index": i + 1},
			InitialNodes: []ports.NodeConfig{
				{Name: "limited-node", Config: map[string]interface{}{}},
			},
		}

		err = cluster.ProcessTrigger(trigger)
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	info := cluster.GetClusterInfo()
	assert.LessOrEqual(t, info.ExecutionStats.TotalExecuting, config.Resources.MaxConcurrentTotal)
}

func TestCluster_Integration_ComponentFailure(t *testing.T) {
	ctx := context.Background()

	config := createTestConfig(t)
	logger := slog.Default()

	cluster, err := New(config, logger)
	require.NoError(t, err)

	err = cluster.Start(ctx)
	require.NoError(t, err)

	err = cluster.Stop()
	require.NoError(t, err)

	err = cluster.Start(ctx)
	assert.Error(t, err, "should not be able to start already stopped cluster without re-initialization")
}

func TestCluster_Integration_ConcurrentWorkflows(t *testing.T) {
	ctx := context.Background()

	config := createTestConfig(t)
	config.Engine.MaxConcurrentWorkflows = 10
	logger := slog.Default()

	cluster, err := New(config, logger)
	require.NoError(t, err)

	err = cluster.Start(ctx)
	require.NoError(t, err)
	defer cluster.Stop()

	testNode := &TestNode{
		name: "concurrent-node",
		executeFunc: func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
			time.Sleep(500 * time.Millisecond)
			return map[string]interface{}{
				"result":    "processed",
				"timestamp": time.Now().Unix(),
			}, []ports.NextNode{}, nil
		},
	}

	err = cluster.RegisterNode(testNode)
	require.NoError(t, err)

	for i := 0; i < 15; i++ {
		trigger := ports.WorkflowTrigger{
			WorkflowID:   fmt.Sprintf("concurrent-workflow-%d", i+1),
			InitialState: map[string]interface{}{"batch_id": i + 1},
			InitialNodes: []ports.NodeConfig{
				{Name: "concurrent-node", Config: map[string]interface{}{"id": i + 1}},
			},
		}

		err = cluster.ProcessTrigger(trigger)
		if i < config.Engine.MaxConcurrentWorkflows {
			require.NoError(t, err)
		} else {
			assert.Error(t, err, "should reject workflows beyond max concurrent limit")
		}
	}

	info := cluster.GetClusterInfo()
	assert.LessOrEqual(t, info.EngineMetrics.ActiveWorkflows, int64(config.Engine.MaxConcurrentWorkflows))
}

func TestCluster_Integration_WithMocks(t *testing.T) {
	mockWorkflowEngine := mocks.NewMockWorkflowEnginePort(t)
	mockResourceManager := mocks.NewMockResourceManagerPort(t)
	mockNodeRegistry := mocks.NewMockNodeRegistryPort(t)

	testTrigger := ports.WorkflowTrigger{
		WorkflowID:   "mock-workflow-001",
		InitialState: map[string]interface{}{"input": "mock-data"},
		InitialNodes: []ports.NodeConfig{
			{
				Name:   "mock-node",
				Config: map[string]interface{}{"timeout": 30},
			},
		},
		Metadata: map[string]string{"source": "mock-test"},
	}

	mockWorkflowEngine.On("ProcessTrigger", testTrigger).Return(nil)

	completedTime := time.Now()
	mockWorkflowEngine.On("GetWorkflowStatus", "mock-workflow-001").Return(&ports.WorkflowStatus{
		WorkflowID:   "mock-workflow-001",
		Status:       ports.WorkflowStateCompleted,
		CurrentState: map[string]interface{}{"result": "mock-success"},
		StartedAt:    time.Now().Add(-time.Minute),
		CompletedAt:  &completedTime,
		ExecutedNodes: []ports.ExecutedNode{{
			NodeName:   "mock-node",
			Status:     ports.NodeExecutionStatusCompleted,
			ExecutedAt: time.Now().Add(-30 * time.Second),
			Duration:   time.Second,
			Config:     map[string]interface{}{},
			Results:    map[string]interface{}{"result": "mock-success"},
		}},
		PendingNodes: []ports.PendingNode{},
		ReadyNodes:   []ports.ReadyNode{},
	}, nil)

	mockWorkflowEngine.On("GetExecutionMetrics").Return(ports.EngineMetrics{
		TotalWorkflows:       1,
		ActiveWorkflows:      0,
		CompletedWorkflows:   1,
		FailedWorkflows:      0,
		NodesExecuted:        1,
		AverageExecutionTime: 100 * time.Millisecond,
		WorkerPoolSize:       10,
		QueueSizes:           ports.QueueSizes{},
	})

	mockResourceManager.On("GetExecutionStats").Return(ports.ExecutionStats{
		TotalExecuting:   0,
		PerTypeExecuting: map[string]int{},
		TotalCapacity:    50,
		PerTypeCapacity:  map[string]int{},
		AvailableSlots:   50,
	})

	mockNodeRegistry.On("ListNodes").Return([]string{"mock-node"})

	err := mockWorkflowEngine.ProcessTrigger(testTrigger)
	require.NoError(t, err)

	status, err := mockWorkflowEngine.GetWorkflowStatus("mock-workflow-001")
	require.NoError(t, err)
	assert.Equal(t, "mock-workflow-001", status.WorkflowID)
	assert.Equal(t, ports.WorkflowStateCompleted, status.Status)
	assert.Len(t, status.ExecutedNodes, 1)
	assert.Equal(t, "mock-node", status.ExecutedNodes[0].NodeName)

	metrics := mockWorkflowEngine.GetExecutionMetrics()
	assert.Equal(t, int64(1), metrics.CompletedWorkflows)
	assert.Equal(t, int64(0), metrics.ActiveWorkflows)

	stats := mockResourceManager.GetExecutionStats()
	assert.Equal(t, 50, stats.TotalCapacity)
	assert.Equal(t, 0, stats.TotalExecuting)

	nodes := mockNodeRegistry.ListNodes()
	assert.Contains(t, nodes, "mock-node")

	mockWorkflowEngine.AssertExpectations(t)
	mockResourceManager.AssertExpectations(t)
	mockNodeRegistry.AssertExpectations(t)
}

type TestNode struct {
	name        string
	executeFunc func(context.Context, interface{}, interface{}) (interface{}, []ports.NextNode, error)
}

func (n *TestNode) Execute(ctx context.Context, args ...interface{}) (*ports.NodeResult, error) {
	if n.executeFunc != nil {
		globalState := args[0]
		config := args[1]
		result, nextNodes, err := n.executeFunc(ctx, globalState, config)
		if err != nil {
			return nil, err
		}
		return &ports.NodeResult{GlobalState: result, NextNodes: nextNodes}, nil
	}
	return &ports.NodeResult{GlobalState: map[string]interface{}{"result": "default"}}, nil
}

func (n *TestNode) GetName() string {
	return n.name
}

func (n *TestNode) CanStart(ctx context.Context, args ...interface{}) bool {
	return true
}

func (n *TestNode) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"input": map[string]interface{}{"type": "string"},
		},
	}
}

func (n *TestNode) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"result": map[string]interface{}{"type": "string"},
		},
	}
}

func createTestConfig(t *testing.T) ClusterConfig {
	tempDir, err := os.MkdirTemp("", "cluster-test-*")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	portOffset := atomic.AddInt64(&testPortCounter, 1)

	randBytes := make([]byte, 4)
	_, err = rand.Read(randBytes)
	require.NoError(t, err)
	randValue := int(randBytes[0])<<8 | int(randBytes[1])

	config := DefaultClusterConfig()
	config.NodeID = fmt.Sprintf("test-node-%d", portOffset)
	config.ServiceName = "test-cluster"
	config.ServicePort = 8080 + int(portOffset*10) + (randValue % 100)
	config.Discovery.Strategy = "static"
	config.Transport.ListenPort = 9090 + int(portOffset*10) + (randValue % 100)
	config.Storage.ListenPort = 7000 + int(portOffset*10) + (randValue % 100)
	config.Storage.DataDir = filepath.Join(tempDir, "raft")
	config.Queue.DataDir = filepath.Join(tempDir, "queue")
	config.Resources.MaxConcurrentTotal = 10
	config.Engine.MaxConcurrentWorkflows = 20

	return config
}
