package engine

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/eleven-am/graft/internal/testutil/workflow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEngine(t *testing.T) {
	testConfig := workflow.DefaultTestConfig()
	testConfig.MaxConcurrentWorkflows = 10
	testConfig.NodeExecutionTimeout = 30 * time.Second

	config := Config{
		MaxConcurrentWorkflows: testConfig.MaxConcurrentWorkflows,
		NodeExecutionTimeout:   testConfig.NodeExecutionTimeout,
		StateUpdateInterval:    testConfig.StateUpdateInterval,
		RetryAttempts:          testConfig.RetryAttempts,
		RetryBackoff:           testConfig.RetryBackoff,
	}

	engine := NewEngine(config, slog.Default())

	assert.NotNil(t, engine)
	assert.Equal(t, config, engine.config)
	assert.NotNil(t, engine.activeWorkflows)
	assert.NotNil(t, engine.coordinator)
	assert.NotNil(t, engine.stateManager)
}

func TestEngineSetters(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetResourceManager(mockComponents.ResourceManager)
	engine.SetStorage(mockComponents.Storage)
	engine.SetQueue(mockComponents.Queue)
	engine.SetTransport(mockComponents.Transport)
	engine.SetDiscovery(mockComponents.Discovery)

	assert.Equal(t, mockComponents.NodeRegistry, engine.nodeRegistry)
	assert.Equal(t, mockComponents.ResourceManager, engine.resourceManager)
	assert.Equal(t, mockComponents.Storage, engine.storage)
	assert.Equal(t, mockComponents.Queue, engine.queue)
	assert.Equal(t, mockComponents.Transport, engine.transport)
	assert.Equal(t, mockComponents.Discovery, engine.discovery)
}

func TestEngineStart_MissingDependencies(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())

	err := engine.Start(context.Background())

	assert.Error(t, err)
	domainErr, ok := err.(domain.Error)
	require.True(t, ok)
	assert.Equal(t, domain.ErrorTypeValidation, domainErr.Type)
	assert.Contains(t, domainErr.Message, "missing required dependencies")
}

func TestEngineStart_Success(t *testing.T) {
	engine := NewEngine(Config{
		StateUpdateInterval: 10 * time.Millisecond,
	}, slog.Default())

	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetResourceManager(mockComponents.ResourceManager)
	engine.SetStorage(mockComponents.Storage)
	engine.SetQueue(mockComponents.Queue)

	workflow.SetupEmptyStorageMock(mockComponents.Storage)
	workflow.SetupEmptyQueueMock(mockComponents.Queue)

	err := engine.Start(context.Background())
	assert.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	err = engine.Stop()
	assert.NoError(t, err)
}

func TestEngineStop(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())

	engine.activeWorkflows["test-workflow"] = &WorkflowInstance{
		ID:     "test-workflow",
		Status: ports.WorkflowStateRunning,
	}

	err := engine.Stop()
	assert.NoError(t, err)

	workflowInstance := engine.activeWorkflows["test-workflow"]
	assert.Equal(t, ports.WorkflowStatePaused, workflowInstance.Status)
}

func TestEngineProcessTrigger_MaxConcurrentReached(t *testing.T) {
	engine := NewEngine(Config{
		MaxConcurrentWorkflows: 1,
	}, slog.Default())

	engine.activeWorkflows["existing-workflow"] = &WorkflowInstance{
		ID: "existing-workflow",
	}

	trigger := ports.WorkflowTrigger{
		WorkflowID:   "new-workflow",
		InitialNodes: []ports.NodeConfig{},
		InitialState: map[string]interface{}{},
	}

	err := engine.ProcessTrigger(trigger)

	assert.Error(t, err)
	domainErr, ok := err.(domain.Error)
	require.True(t, ok)
	assert.Equal(t, domain.ErrorTypeRateLimit, domainErr.Type)
}

func TestEngineProcessTrigger_Success(t *testing.T) {
	engine := NewEngine(Config{
		MaxConcurrentWorkflows: 10,
	}, slog.Default())

	mockQueue := mocks.NewMockQueuePort(t)
	engine.SetQueue(mockQueue)

	trigger := workflow.CreateTestWorkflowTriggerWithNodes("test-workflow", []string{"node1"})
	workflow.SetupWorkflowTriggerQueueMock(mockQueue, "test-workflow", []string{"node1"})

	err := engine.ProcessTrigger(trigger)

	assert.NoError(t, err)
	assert.Contains(t, engine.activeWorkflows, "test-workflow")

	workflowInstance := engine.activeWorkflows["test-workflow"]
	assert.Equal(t, "test-workflow", workflowInstance.ID)
	assert.Equal(t, ports.WorkflowStateRunning, workflowInstance.Status)
	assert.Equal(t, trigger.InitialState, workflowInstance.CurrentState)
	assert.Equal(t, trigger.Metadata, workflowInstance.Metadata)
}

func TestEngineGetWorkflowStatus_NotFound(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())

	status, err := engine.GetWorkflowStatus("nonexistent")

	assert.Nil(t, status)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestEngineGetWorkflowStatus_Success(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())

	startTime := time.Now()
	engine.activeWorkflows["test-workflow"] = &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"key": "value"},
		StartedAt:    startTime,
		CompletedAt:  nil,
		Metadata:     map[string]string{"env": "test"},
		LastError:    nil,
	}

	status, err := engine.GetWorkflowStatus("test-workflow")

	assert.NoError(t, err)
	assert.NotNil(t, status)
	assert.Equal(t, "test-workflow", status.WorkflowID)
	assert.Equal(t, ports.WorkflowStateRunning, status.Status)
	assert.Equal(t, map[string]interface{}{"key": "value"}, status.CurrentState)
	assert.Equal(t, startTime, status.StartedAt)
	assert.Nil(t, status.CompletedAt)
	assert.Nil(t, status.LastError)
}

func TestEngineGetExecutionMetrics(t *testing.T) {
	engine := NewEngine(Config{
		MaxConcurrentWorkflows: 10,
	}, slog.Default())

	mockQueue := mocks.NewMockQueuePort(t)
	engine.SetQueue(mockQueue)

	engine.activeWorkflows["running"] = &WorkflowInstance{Status: ports.WorkflowStateRunning}
	engine.activeWorkflows["completed"] = &WorkflowInstance{Status: ports.WorkflowStateCompleted}
	engine.activeWorkflows["failed"] = &WorkflowInstance{Status: ports.WorkflowStateFailed}

	workflow.SetupMetricsQueueMock(mockQueue, 2)

	metrics := engine.GetExecutionMetrics()

	assert.Equal(t, int64(3), metrics.TotalWorkflows)
	assert.Equal(t, int64(1), metrics.ActiveWorkflows)
	assert.Equal(t, int64(1), metrics.CompletedWorkflows)
	assert.Equal(t, int64(1), metrics.FailedWorkflows)
	assert.Equal(t, 10, metrics.WorkerPoolSize)
	assert.Equal(t, 1, metrics.QueueSizes.Ready)
	assert.Equal(t, 2, metrics.QueueSizes.Pending)
}

func TestEngineQueueNodeForExecution(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())

	mockQueue := mocks.NewMockQueuePort(t)
	engine.SetQueue(mockQueue)

	nodeConfig := workflow.CreateTestNodeConfig("test-node")
	if configMap, ok := nodeConfig.Config.(map[string]interface{}); ok {
		configMap["param"] = "value"
	}

	workflow.SetupNodeExecutionQueueMock(mockQueue, "test-workflow", "test-node", map[string]interface{}{
		"param":   "value",
		"timeout": 30,
	})

	err := engine.queueNodeForExecution("test-workflow", nodeConfig, "test")

	assert.NoError(t, err)
}

func TestGenerateItemID(t *testing.T) {
	id := generateItemID()

	assert.NotEmpty(t, id)
	assert.Contains(t, id, "_")

	id2 := generateItemID()
	assert.NotEqual(t, id, id2)
}

func TestGenerateUUID(t *testing.T) {
	uuidStr := uuid.New().String()

	assert.NotEmpty(t, uuidStr)
	assert.Len(t, uuidStr, 36)
	assert.Contains(t, uuidStr, "-")

	uuid2Str := uuid.New().String()
	assert.NotEqual(t, uuidStr, uuid2Str)
}

func TestWorkflowInstanceMutex(t *testing.T) {
	workflowInstance := &WorkflowInstance{
		ID:           "test",
		Status:       ports.WorkflowStateRunning,
		CurrentState: make(map[string]interface{}),
	}

	go func() {
		workflowInstance.mu.Lock()
		workflowInstance.Status = ports.WorkflowStateCompleted
		time.Sleep(10 * time.Millisecond)
		workflowInstance.mu.Unlock()
	}()

	time.Sleep(5 * time.Millisecond)

	workflowInstance.mu.RLock()
	status := workflowInstance.Status
	workflowInstance.mu.RUnlock()

	assert.Equal(t, ports.WorkflowStateCompleted, status)
}

func TestEngineProcessTrigger_WithArrayInitialState(t *testing.T) {
	engine := NewEngine(Config{
		MaxConcurrentWorkflows: 10,
	}, slog.Default())

	mockQueue := mocks.NewMockQueuePort(t)
	engine.SetQueue(mockQueue)

	trigger := ports.WorkflowTrigger{
		WorkflowID:   "array-test-workflow",
		InitialNodes: []ports.NodeConfig{{Name: "node1", Config: []string{"a", "b", "c"}}},
		InitialState: []int{1, 2, 3, 4, 5},
		Metadata:     map[string]string{"type": "array"},
	}

	workflow.SetupWorkflowTriggerQueueMock(mockQueue, "array-test-workflow", []string{"node1"})

	err := engine.ProcessTrigger(trigger)

	assert.NoError(t, err)
	assert.Contains(t, engine.activeWorkflows, "array-test-workflow")

	workflowInstance := engine.activeWorkflows["array-test-workflow"]
	assert.Equal(t, "array-test-workflow", workflowInstance.ID)
	assert.Equal(t, ports.WorkflowStateRunning, workflowInstance.Status)
	assert.Equal(t, []int{1, 2, 3, 4, 5}, workflowInstance.CurrentState)
	assert.Equal(t, trigger.Metadata, workflowInstance.Metadata)
}

func TestEngineProcessTrigger_WithPrimitiveInitialState(t *testing.T) {
	engine := NewEngine(Config{
		MaxConcurrentWorkflows: 10,
	}, slog.Default())

	mockQueue := mocks.NewMockQueuePort(t)
	engine.SetQueue(mockQueue)

	trigger := ports.WorkflowTrigger{
		WorkflowID:   "primitive-test-workflow",
		InitialNodes: []ports.NodeConfig{{Name: "node1", Config: "simple-string-config"}},
		InitialState: "initial-state-string",
		Metadata:     map[string]string{"type": "primitive"},
	}

	workflow.SetupWorkflowTriggerQueueMock(mockQueue, "primitive-test-workflow", []string{"node1"})

	err := engine.ProcessTrigger(trigger)

	assert.NoError(t, err)
	assert.Contains(t, engine.activeWorkflows, "primitive-test-workflow")

	workflowInstance := engine.activeWorkflows["primitive-test-workflow"]
	assert.Equal(t, "primitive-test-workflow", workflowInstance.ID)
	assert.Equal(t, ports.WorkflowStateRunning, workflowInstance.Status)
	assert.Equal(t, "initial-state-string", workflowInstance.CurrentState)
	assert.Equal(t, trigger.Metadata, workflowInstance.Metadata)
}
