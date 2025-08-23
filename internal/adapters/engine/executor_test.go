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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewNodeExecutor(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	executor := NewNodeExecutor(engine)

	assert.NotNil(t, executor)
	assert.Equal(t, engine, executor.engine)
}

func TestNodeExecutor_ExecuteNode_WorkflowNotFound(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetQueue(mockComponents.Queue)

	executor := NewNodeExecutor(engine)

	item := workflow.CreateTestExecutionItem("nonexistent", "test-node")

	// Mock the ReleaseWorkClaim call that happens in defer
	mockComponents.Queue.EXPECT().ReleaseWorkClaim(mock.Anything, item.ID, engine.nodeID).Return(nil).Maybe()

	err := executor.ExecuteNode(context.Background(), item)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestNodeExecutor_ExecuteNode_NodeNotFound(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetQueue(mockComponents.Queue)

	executor := NewNodeExecutor(engine)

	workflowInstance := workflow.CreateTestWorkflowForExecution("test-workflow")
	engine.activeWorkflows["test-workflow"] = &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: workflowInstance["CurrentState"].(map[string]interface{}),
	}

	item := workflow.CreateTestExecutionItem("test-workflow", "nonexistent-node")
	mockComponents.NodeRegistry.EXPECT().GetNode("nonexistent-node").Return(nil, domain.NewNotFoundError("node", "nonexistent-node"))

	// Mock the ReleaseWorkClaim call that happens in defer
	mockComponents.Queue.EXPECT().ReleaseWorkClaim(mock.Anything, item.ID, engine.nodeID).Return(nil).Maybe()

	err := executor.ExecuteNode(context.Background(), item)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestNodeExecutor_ExecuteNode_InsufficientResources(t *testing.T) {
	engine := NewEngine(Config{RetryBackoff: time.Millisecond}, slog.Default())
	mockComponents, workflowInstance := workflow.SetupNodeExecutionTest(t, "test-workflow", "test-node")

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetResourceManager(mockComponents.ResourceManager)
	engine.SetQueue(mockComponents.Queue)

	executor := NewNodeExecutor(engine)

	engine.activeWorkflows["test-workflow"] = &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: workflowInstance.(map[string]interface{})["CurrentState"].(map[string]interface{}),
	}

	item := workflow.CreateTestExecutionItem("test-workflow", "test-node")
	testNode := workflow.CreateSuccessfulTestNode("test-node")

	mockComponents.NodeRegistry.EXPECT().GetNode("test-node").Return(testNode, nil)
	mockComponents.ResourceManager.EXPECT().CanExecuteNode("test-node").Return(false)
	mockComponents.Queue.EXPECT().EnqueueReady(mock.Anything, *item).Return(nil)

	// Mock the ReleaseWorkClaim call that happens in defer
	mockComponents.Queue.EXPECT().ReleaseWorkClaim(mock.Anything, item.ID, engine.nodeID).Return(nil).Maybe()

	err := executor.ExecuteNode(context.Background(), item)

	assert.NoError(t, err)
}

func TestNodeExecutor_ExecuteNode_CannotStart(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetResourceManager(mockComponents.ResourceManager)
	engine.SetQueue(mockComponents.Queue)

	executor := NewNodeExecutor(engine)

	engine.activeWorkflows["test-workflow"] = &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"ready": false},
	}

	item := workflow.CreateTestExecutionItem("test-workflow", "test-node")
	testNode := workflow.CreateTestNode("test-node", func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
		return nil, nil, nil
	})
	testNode.CanStartFunc = func(ctx context.Context, args ...interface{}) bool {
		return false
	}

	mockComponents.NodeRegistry.EXPECT().GetNode("test-node").Return(testNode, nil)
	mockComponents.ResourceManager.EXPECT().CanExecuteNode("test-node").Return(true)
	mockComponents.ResourceManager.EXPECT().AcquireNode("test-node").Return(nil)
	mockComponents.ResourceManager.EXPECT().ReleaseNode("test-node").Return(nil)
	mockComponents.Queue.EXPECT().EnqueuePending(mock.Anything, *item).Return(nil)

	// Mock the ReleaseWorkClaim call that happens in defer
	mockComponents.Queue.EXPECT().ReleaseWorkClaim(mock.Anything, item.ID, engine.nodeID).Return(nil).Maybe()

	err := executor.ExecuteNode(context.Background(), item)

	assert.NoError(t, err)
}

func TestNodeExecutor_ExecuteNode_ExecutionFailure(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetResourceManager(mockComponents.ResourceManager)
	engine.SetStorage(mockComponents.Storage)
	engine.SetQueue(mockComponents.Queue)

	executor := NewNodeExecutor(engine)

	workflowInstance := workflow.CreateTestWorkflowForExecution("test-workflow")
	workflowObj := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: workflowInstance["CurrentState"].(map[string]interface{}),
	}
	engine.activeWorkflows["test-workflow"] = workflowObj

	item := workflow.CreateTestExecutionItem("test-workflow", "test-node")

	execError := domain.Error{Type: domain.ErrorTypeInternal, Message: "execution failed"}
	testNode := workflow.CreateFailingTestNode("test-node", execError)

	mockComponents.NodeRegistry.EXPECT().GetNode("test-node").Return(testNode, nil)
	mockComponents.ResourceManager.EXPECT().CanExecuteNode("test-node").Return(true)
	mockComponents.ResourceManager.EXPECT().AcquireNode("test-node").Return(nil)
	mockComponents.ResourceManager.EXPECT().ReleaseNode("test-node").Return(nil)
	mockComponents.Storage.EXPECT().Put(mock.Anything, "workflow:state:test-workflow", mock.AnythingOfType("[]uint8")).Return(nil)

	// Mock the ReleaseWorkClaim call that happens in defer
	mockComponents.Queue.EXPECT().ReleaseWorkClaim(mock.Anything, item.ID, engine.nodeID).Return(nil).Maybe()

	err := executor.ExecuteNode(context.Background(), item)

	assert.NoError(t, err)
	assert.Equal(t, ports.WorkflowStateFailed, workflowObj.Status)
	assert.NotNil(t, workflowObj.LastError)
	assert.NotNil(t, workflowObj.CompletedAt)
}

func TestNodeExecutor_ExecuteNode_Success(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	mockSemaphore := mocks.NewMockSemaphorePort(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetResourceManager(mockComponents.ResourceManager)
	engine.SetStorage(mockComponents.Storage)
	engine.SetQueue(mockComponents.Queue)
	engine.SetSemaphore(mockSemaphore)

	executor := NewNodeExecutor(engine)

	workflowObj := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"key": "value"},
	}
	engine.activeWorkflows["test-workflow"] = workflowObj

	item := workflow.CreateTestExecutionItem("test-workflow", "test-node")

	results := map[string]interface{}{"result": "success", "count": 42}
	nextNodes := workflow.CreateTestNextNodes([]string{"next-node"})

	testNode := workflow.CreateTestNode("test-node", func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
		return results, nextNodes, nil
	})
	nextTestNode := workflow.CreateSuccessfulTestNode("next-node")

	mockComponents.NodeRegistry.EXPECT().GetNode("test-node").Return(testNode, nil)
	mockComponents.NodeRegistry.EXPECT().GetNode("next-node").Return(nextTestNode, nil)
	mockComponents.ResourceManager.EXPECT().CanExecuteNode("test-node").Return(true)
	mockComponents.ResourceManager.EXPECT().AcquireNode("test-node").Return(nil)
	mockComponents.ResourceManager.EXPECT().ReleaseNode("test-node").Return(nil)
	mockComponents.Storage.EXPECT().Put(mock.Anything, "workflow:state:test-workflow", mock.AnythingOfType("[]uint8")).Return(nil)
	// Add a catch-all for any other storage operations (execution data, etc.)
	mockComponents.Storage.EXPECT().Put(mock.Anything, mock.Anything, mock.AnythingOfType("[]uint8")).Return(nil).Maybe()
	mockComponents.Queue.EXPECT().EnqueueReady(mock.Anything, mock.MatchedBy(func(item ports.QueueItem) bool {
		return item.WorkflowID == "test-workflow" && item.NodeName == "next-node"
	})).Return(nil)

	// Mock the ReleaseWorkClaim call that happens in defer
	mockComponents.Queue.EXPECT().ReleaseWorkClaim(mock.Anything, item.ID, engine.nodeID).Return(nil).Maybe()

	// Mock semaphore operations for workflow state locking
	mockSemaphore.EXPECT().Acquire(mock.Anything, "test-workflow", mock.Anything, mock.Anything).Return(nil)
	mockSemaphore.EXPECT().Release(mock.Anything, "test-workflow", mock.Anything).Return(nil)

	err := executor.ExecuteNode(context.Background(), item)

	assert.NoError(t, err)
	assert.Equal(t, ports.WorkflowStateRunning, workflowObj.Status)

	if stateMap, ok := workflowObj.CurrentState.(map[string]interface{}); ok {
		assert.Equal(t, "success", stateMap["result"])
		assert.Equal(t, 42, stateMap["count"])
		assert.Equal(t, "value", stateMap["key"])
	} else {
		t.Error("Expected CurrentState to be a map[string]interface{}")
	}
}

func TestNodeExecutor_ExecuteNode_AcquireResourcesFails(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents, workflowInstance := workflow.SetupNodeExecutionTest(t, "test-workflow", "test-node")

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetResourceManager(mockComponents.ResourceManager)
	engine.SetQueue(mockComponents.Queue)

	executor := NewNodeExecutor(engine)

	engine.activeWorkflows["test-workflow"] = &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: workflowInstance.(map[string]interface{})["CurrentState"].(map[string]interface{}),
	}

	item := workflow.CreateTestExecutionItem("test-workflow", "test-node")
	testNode := workflow.CreateSuccessfulTestNode("test-node")

	mockComponents.NodeRegistry.EXPECT().GetNode("test-node").Return(testNode, nil)
	mockComponents.ResourceManager.EXPECT().CanExecuteNode("test-node").Return(true)
	mockComponents.ResourceManager.EXPECT().AcquireNode("test-node").Return(assert.AnError)

	// Mock the ReleaseWorkClaim call that happens in defer
	mockComponents.Queue.EXPECT().ReleaseWorkClaim(mock.Anything, item.ID, engine.nodeID).Return(nil).Maybe()

	err := executor.ExecuteNode(context.Background(), item)

	assert.Error(t, err)
}

func TestNodeExecutor_QueueNextNode_NodeNotFound(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetNodeRegistry(mockComponents.NodeRegistry)

	executor := NewNodeExecutor(engine)

	nextNode := ports.NextNode{
		NodeName: "nonexistent-node",
		Config:   map[string]interface{}{},
	}

	mockComponents.NodeRegistry.EXPECT().GetNode("nonexistent-node").Return(nil, domain.NewNotFoundError("node", "nonexistent-node"))

	err := executor.queueNextNode(context.Background(), "test-workflow", nextNode)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestNodeExecutor_QueueNextNode_CanStart(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetQueue(mockComponents.Queue)

	executor := NewNodeExecutor(engine)

	engine.activeWorkflows["test-workflow"] = &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"ready": true},
	}

	nextNode := ports.NextNode{
		NodeName: "next-node",
		Config:   map[string]interface{}{"param": "value"},
	}

	testNode := workflow.CreateSuccessfulTestNode("next-node")
	mockComponents.NodeRegistry.EXPECT().GetNode("next-node").Return(testNode, nil)
	mockComponents.Queue.EXPECT().EnqueueReady(mock.Anything, mock.MatchedBy(func(item ports.QueueItem) bool {
		return item.WorkflowID == "test-workflow" && item.NodeName == "next-node"
	})).Return(nil)

	err := executor.queueNextNode(context.Background(), "test-workflow", nextNode)

	assert.NoError(t, err)
}

func TestNodeExecutor_QueueNextNode_CannotStart(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetQueue(mockComponents.Queue)

	executor := NewNodeExecutor(engine)

	engine.activeWorkflows["test-workflow"] = &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"ready": false},
	}

	nextNode := ports.NextNode{
		NodeName: "next-node",
		Config:   map[string]interface{}{"param": "value"},
	}

	testNode := workflow.CreateTestNode("next-node", func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
		return nil, nil, nil
	})
	testNode.CanStartFunc = func(ctx context.Context, args ...interface{}) bool {
		return false
	}
	mockComponents.NodeRegistry.EXPECT().GetNode("next-node").Return(testNode, nil)
	mockComponents.Queue.EXPECT().EnqueuePending(mock.Anything, mock.MatchedBy(func(item ports.QueueItem) bool {
		return item.WorkflowID == "test-workflow" && item.NodeName == "next-node"
	})).Return(nil)

	err := executor.queueNextNode(context.Background(), "test-workflow", nextNode)

	assert.NoError(t, err)
}

func TestNodeExecutor_HandleExecutionFailure(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetStorage(mockComponents.Storage)

	executor := NewNodeExecutor(engine)

	workflowInstance := workflow.CreateTestWorkflowForExecution("test-workflow")
	workflowObj := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: workflowInstance["CurrentState"].(map[string]interface{}),
	}

	item := workflow.CreateTestExecutionItem("test-workflow", "failed-node")

	execError := domain.Error{Type: domain.ErrorTypeInternal, Message: "execution failed"}

	mockComponents.Storage.EXPECT().Put(mock.Anything, "workflow:state:test-workflow", mock.AnythingOfType("[]uint8")).Return(nil)

	err := executor.handleExecutionFailure(context.Background(), workflowObj, item, execError)

	assert.NoError(t, err)
	assert.Equal(t, ports.WorkflowStateFailed, workflowObj.Status)
	assert.NotNil(t, workflowObj.LastError)
	assert.Contains(t, *workflowObj.LastError, "execution failed")
	assert.NotNil(t, workflowObj.CompletedAt)
}

func TestNodeExecutor_UpdateWorkflowState(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	mockSemaphore := mocks.NewMockSemaphorePort(t)
	engine.SetStorage(mockComponents.Storage)
	engine.SetSemaphore(mockSemaphore)

	executor := NewNodeExecutor(engine)

	workflowObj := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"existing": "value"},
	}

	results := map[string]interface{}{
		"new_key":  "new_value",
		"existing": "updated_value",
	}

	executedNode := ports.ExecutedNode{
		NodeName:   "test-node",
		ExecutedAt: time.Now(),
		Duration:   time.Second,
		Status:     ports.NodeExecutionStatusCompleted,
	}

	mockComponents.Storage.EXPECT().Put(mock.Anything, "workflow:state:test-workflow", mock.AnythingOfType("[]uint8")).Return(nil)
	// Add a catch-all for any other storage operations (execution data, etc.)
	mockComponents.Storage.EXPECT().Put(mock.Anything, mock.Anything, mock.AnythingOfType("[]uint8")).Return(nil).Maybe()

	// Mock semaphore operations for workflow state locking
	mockSemaphore.EXPECT().Acquire(mock.Anything, "test-workflow", mock.Anything, mock.Anything).Return(nil)
	mockSemaphore.EXPECT().Release(mock.Anything, "test-workflow", mock.Anything).Return(nil)

	err := executor.updateWorkflowState(context.Background(), workflowObj, results, &executedNode)

	assert.NoError(t, err)

	if stateMap, ok := workflowObj.CurrentState.(map[string]interface{}); ok {
		assert.Equal(t, "new_value", stateMap["new_key"])
		assert.Equal(t, "updated_value", stateMap["existing"])
	} else {
		t.Error("Expected CurrentState to be a map[string]interface{}")
	}
}

func TestNodeExecutor_UpdateWorkflowState_NonMapResults(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	mockSemaphore := mocks.NewMockSemaphorePort(t)
	engine.SetStorage(mockComponents.Storage)
	engine.SetSemaphore(mockSemaphore)

	executor := NewNodeExecutor(engine)

	workflowObj := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"existing": "value"},
	}

	results := "string result"

	executedNode := ports.ExecutedNode{
		NodeName: "test-node",
		Status:   ports.NodeExecutionStatusCompleted,
	}

	// No storage expectations since merge will fail and error before persist
	// Mock semaphore operations for workflow state locking
	mockSemaphore.EXPECT().Acquire(mock.Anything, "test-workflow", mock.Anything, mock.Anything).Return(nil)
	mockSemaphore.EXPECT().Release(mock.Anything, "test-workflow", mock.Anything).Return(nil)

	err := executor.updateWorkflowState(context.Background(), workflowObj, results, &executedNode)

	// With strict mergo-only policy, incompatible types should error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to merge workflow state using mergo")
}

func TestSerializeWorkflowData(t *testing.T) {
	data := map[string]interface{}{
		"id":     "test-workflow",
		"status": "running",
		"state":  map[string]interface{}{"key": "value"},
	}

	serialized, err := serializeWorkflowData(data)

	assert.NoError(t, err)
	assert.NotEmpty(t, serialized)
	assert.Contains(t, string(serialized), "test-workflow")
	assert.Contains(t, string(serialized), "running")
}

func TestNodeExecutor_CheckAndClaimIdempotencyKey_FirstTime(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetStorage(mockComponents.Storage)

	executor := NewNodeExecutor(engine)

	mockComponents.Storage.EXPECT().Get(mock.Anything, "workflow:idempotency:test-workflow:test-key").Return(nil, domain.NewNotFoundError("key", "not found"))
	mockComponents.Storage.EXPECT().Put(mock.Anything, "workflow:idempotency:test-workflow:test-key", mock.AnythingOfType("[]uint8")).Return(nil)

	err := executor.checkAndClaimIdempotencyKey(context.Background(), "test-workflow", "test-key")

	assert.NoError(t, err)
}

func TestNodeExecutor_CheckAndClaimIdempotencyKey_AlreadyClaimed(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetStorage(mockComponents.Storage)

	executor := NewNodeExecutor(engine)

	existingData := []byte(`{"workflow_id":"test-workflow","idempotency_key":"test-key","claimed_at":"2023-01-01T00:00:00Z"}`)
	mockComponents.Storage.EXPECT().Get(mock.Anything, "workflow:idempotency:test-workflow:test-key").Return(existingData, nil)

	err := executor.checkAndClaimIdempotencyKey(context.Background(), "test-workflow", "test-key")

	assert.Error(t, err)
	assert.IsType(t, &IdempotencyKeyClaimedError{}, err)
}

func TestNodeExecutor_CheckAndClaimIdempotencyKey_NoStorage(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	executor := NewNodeExecutor(engine)

	err := executor.checkAndClaimIdempotencyKey(context.Background(), "test-workflow", "test-key")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storage not available")
}

func TestNodeExecutor_QueueNextNode_WithIdempotencyKey_FirstTime(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetStorage(mockComponents.Storage)
	engine.SetQueue(mockComponents.Queue)

	workflowInstance := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"test": "data"},
	}
	engine.activeWorkflows["test-workflow"] = workflowInstance

	executor := NewNodeExecutor(engine)
	mockNode := workflow.CreateSuccessfulTestNode("test-node")

	idempotencyKey := "test-key"
	nextNode := ports.NextNode{
		NodeName:       "test-node",
		Config:         map[string]interface{}{"test": "config"},
		IdempotencyKey: &idempotencyKey,
	}

	mockComponents.NodeRegistry.EXPECT().GetNode("test-node").Return(mockNode, nil)
	mockComponents.Storage.EXPECT().Get(mock.Anything, "workflow:idempotency:test-workflow:test-key").Return(nil, domain.NewNotFoundError("key", "not found"))
	mockComponents.Storage.EXPECT().Put(mock.Anything, "workflow:idempotency:test-workflow:test-key", mock.AnythingOfType("[]uint8")).Return(nil)
	mockComponents.Queue.EXPECT().EnqueueReady(mock.Anything, mock.AnythingOfType("ports.QueueItem")).Return(nil)

	err := executor.queueNextNode(context.Background(), "test-workflow", nextNode)

	assert.NoError(t, err)
}

func TestNodeExecutor_QueueNextNode_WithIdempotencyKey_AlreadyClaimed(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetStorage(mockComponents.Storage)

	workflowInstance := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"test": "data"},
	}
	engine.activeWorkflows["test-workflow"] = workflowInstance

	executor := NewNodeExecutor(engine)
	mockNode := workflow.CreateSuccessfulTestNode("test-node")

	idempotencyKey := "test-key"
	nextNode := ports.NextNode{
		NodeName:       "test-node",
		Config:         map[string]interface{}{"test": "config"},
		IdempotencyKey: &idempotencyKey,
	}

	existingData := []byte(`{"workflow_id":"test-workflow","idempotency_key":"test-key","claimed_at":"2023-01-01T00:00:00Z"}`)
	mockComponents.NodeRegistry.EXPECT().GetNode("test-node").Return(mockNode, nil)
	mockComponents.Storage.EXPECT().Get(mock.Anything, "workflow:idempotency:test-workflow:test-key").Return(existingData, nil)

	err := executor.queueNextNode(context.Background(), "test-workflow", nextNode)

	assert.NoError(t, err)
}

func TestNodeExecutor_QueueNextNode_WithoutIdempotencyKey(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetQueue(mockComponents.Queue)

	workflowInstance := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"test": "data"},
	}
	engine.activeWorkflows["test-workflow"] = workflowInstance

	executor := NewNodeExecutor(engine)
	mockNode := workflow.CreateSuccessfulTestNode("test-node")

	nextNode := ports.NextNode{
		NodeName: "test-node",
		Config:   map[string]interface{}{"test": "config"},
	}

	mockComponents.NodeRegistry.EXPECT().GetNode("test-node").Return(mockNode, nil)
	mockComponents.Queue.EXPECT().EnqueueReady(mock.Anything, mock.AnythingOfType("ports.QueueItem")).Return(nil)

	err := executor.queueNextNode(context.Background(), "test-workflow", nextNode)

	assert.NoError(t, err)
}
