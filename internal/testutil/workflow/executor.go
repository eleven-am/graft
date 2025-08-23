package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func CreateTestExecutionItem(workflowID, nodeName string) *ports.QueueItem {
	return &ports.QueueItem{
		ID:         "test-item-id",
		WorkflowID: workflowID,
		NodeName:   nodeName,
		Config:     map[string]interface{}{"test": "config"},
		Priority:   1,
		EnqueuedAt: time.Now(),
	}
}

func CreateTestNode(name string, executeFunc func(context.Context, interface{}, interface{}) (interface{}, []ports.NextNode, error)) *TestNode {
	return &TestNode{
		name:        name,
		executeFunc: executeFunc,
	}
}

type TestNode struct {
	name         string
	executeFunc  func(context.Context, interface{}, interface{}) (interface{}, []ports.NextNode, error)
	CanStartFunc func(context.Context, ...interface{}) bool
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
	if n.CanStartFunc != nil {
		return n.CanStartFunc(ctx, args...)
	}
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

func CreateSuccessfulTestNode(name string) *TestNode {
	return CreateTestNode(name, func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
		return map[string]interface{}{
			"result":    "success",
			"processed": true,
		}, []ports.NextNode{}, nil
	})
}

func CreateFailingTestNode(name string, err error) *TestNode {
	return CreateTestNode(name, func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
		return nil, []ports.NextNode{}, err
	})
}

func SetupResourceManagerMock(mockRM *mocks.MockResourceManagerPort, nodeName string, canExecute bool) {
	if canExecute {
		mockRM.On("TryAcquire", nodeName).Return(true, nil).Maybe()
		mockRM.On("Release", nodeName).Return().Maybe()
	} else {
		mockRM.On("TryAcquire", nodeName).Return(false, nil).Maybe()
	}
}

func SetupStorageMock(t *testing.T, mockStorage *mocks.MockStoragePort, workflowID string) {
	t.Helper()

	mockStorage.On("Put", mock.Anything, mock.MatchedBy(func(key string) bool {
		return key == "workflow:state:"+workflowID
	}), mock.AnythingOfType("[]uint8")).Return(nil).Maybe()

	mockStorage.On("Get", mock.Anything, mock.MatchedBy(func(key string) bool {
		return key == "workflow:state:"+workflowID
	})).Return([]byte(`{"id":"`+workflowID+`","status":"running","current_state":{},"started_at":"2023-01-01T00:00:00Z","version":1,"updated_at":"2023-01-01T00:00:00Z"}`), nil).Maybe()
}

func ValidateExecutionResult(t *testing.T, result interface{}, expectedKeys ...string) {
	t.Helper()

	assert.NotNil(t, result)

	if resultMap, ok := result.(map[string]interface{}); ok {
		for _, key := range expectedKeys {
			assert.Contains(t, resultMap, key, "Expected key %s in execution result", key)
		}
	} else {
		t.Errorf("Expected result to be map[string]interface{}, got %T", result)
	}
}

func SetupNodeExecutorTest(t *testing.T, nodeName string) (*mocks.MockNodePort, *mocks.MockResourceManagerPort, *mocks.MockStoragePort, *mocks.MockQueuePort) {
	t.Helper()

	mockNode := mocks.NewMockNodePort(t)
	mockResourceManager := mocks.NewMockResourceManagerPort(t)
	mockStorage := mocks.NewMockStoragePort(t)
	mockQueue := mocks.NewMockQueuePort(t)

	return mockNode, mockResourceManager, mockStorage, mockQueue
}

func SetupExecutorMockExpectations(mockRM *mocks.MockResourceManagerPort, mockStorage *mocks.MockStoragePort, workflowID, nodeName string, canExecute bool) {
	if canExecute {
		mockRM.EXPECT().CanExecuteNode(nodeName).Return(true)
		mockRM.EXPECT().AcquireNode(nodeName).Return(nil)
		mockRM.EXPECT().ReleaseNode(nodeName).Return(nil)
	} else {
		mockRM.EXPECT().CanExecuteNode(nodeName).Return(false)
	}

	mockStorage.EXPECT().Put(mock.Anything, "workflow:state:"+workflowID, mock.AnythingOfType("[]uint8")).Return(nil).Maybe()
}

func CreateTestWorkflowForExecution(workflowID string) map[string]interface{} {
	return map[string]interface{}{
		"ID":           workflowID,
		"Status":       "running",
		"CurrentState": map[string]interface{}{"existing": "value"},
		"StartedAt":    time.Now(),
	}
}

func ConfigureAllMocks(mockComponents *MockComponents, workflowID, nodeName string, canExecute bool, shouldSucceed bool) {
	testNode := CreateSuccessfulTestNode(nodeName)
	if !shouldSucceed {
		testNode = CreateFailingTestNode(nodeName, assert.AnError)
	}

	mockComponents.NodeRegistry.EXPECT().GetNode(nodeName).Return(testNode, nil).Maybe()
	mockComponents.ResourceManager.EXPECT().CanExecuteNode(nodeName).Return(canExecute).Maybe()
	if canExecute {
		mockComponents.ResourceManager.EXPECT().AcquireNode(nodeName).Return(nil).Maybe()
		mockComponents.ResourceManager.EXPECT().ReleaseNode(nodeName).Return(nil).Maybe()
	}
	mockComponents.Storage.EXPECT().Put(mock.Anything, "workflow:state:"+workflowID, mock.AnythingOfType("[]uint8")).Return(nil).Maybe()
	mockComponents.Queue.EXPECT().EnqueueReady(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockComponents.Queue.EXPECT().EnqueuePending(mock.Anything, mock.Anything).Return(nil).Maybe()
}

func SetupNodeExecutionTest(t *testing.T, workflowID, nodeName string) (*MockComponents, interface{}) {
	t.Helper()

	mockComponents := SetupMockComponents(t)
	workflowInstance := CreateTestWorkflowForExecution(workflowID)

	return mockComponents, workflowInstance
}

func CreateStandardExecutorTestSetup(t *testing.T, workflowID, nodeName string) (*MockComponents, interface{}, interface{}) {
	t.Helper()

	mockComponents := SetupMockComponents(t)
	workflowInstance := CreateTestWorkflowForExecution(workflowID)

	workflowObj := map[string]interface{}{
		"ID":           workflowID,
		"Status":       "running",
		"CurrentState": workflowInstance["CurrentState"],
		"StartedAt":    workflowInstance["StartedAt"],
	}

	return mockComponents, workflowInstance, workflowObj
}

func SetupExecutorMocksForSuccess(mockComponents *MockComponents, nodeName string, withNextNode bool) {
	testNode := CreateSuccessfulTestNode(nodeName)
	mockComponents.NodeRegistry.EXPECT().GetNode(nodeName).Return(testNode, nil)
	mockComponents.ResourceManager.EXPECT().CanExecuteNode(nodeName).Return(true)
	mockComponents.ResourceManager.EXPECT().AcquireNode(nodeName).Return(nil)
	mockComponents.ResourceManager.EXPECT().ReleaseNode(nodeName).Return(nil)
	mockComponents.Storage.EXPECT().Put(mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Return(nil)

	if withNextNode {
		mockComponents.Queue.EXPECT().EnqueueReady(mock.Anything, mock.AnythingOfType("ports.QueueItem")).Return(nil)
	}
}

func SetupExecutorMocksForFailure(mockComponents *MockComponents, nodeName string, execError error) {
	testNode := CreateFailingTestNode(nodeName, execError)
	mockComponents.NodeRegistry.EXPECT().GetNode(nodeName).Return(testNode, nil)
	mockComponents.ResourceManager.EXPECT().CanExecuteNode(nodeName).Return(true)
	mockComponents.ResourceManager.EXPECT().AcquireNode(nodeName).Return(nil)
	mockComponents.ResourceManager.EXPECT().ReleaseNode(nodeName).Return(nil)
	mockComponents.Storage.EXPECT().Put(mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Return(nil)
}

func SetupReleaseWorkClaimMock(mockComponents *MockComponents, itemID, nodeID string) {
	mockComponents.Queue.EXPECT().ReleaseWorkClaim(mock.Anything, itemID, nodeID).Return(nil).Maybe()
}

func SetupStandardExecutorMocks(mockComponents *MockComponents, nodeName string) {
	mockComponents.NodeRegistry.EXPECT().GetNode(nodeName).Return(CreateSuccessfulTestNode(nodeName), nil)
	mockComponents.ResourceManager.EXPECT().CanExecuteNode(nodeName).Return(true)
	mockComponents.ResourceManager.EXPECT().AcquireNode(nodeName).Return(nil)
	mockComponents.ResourceManager.EXPECT().ReleaseNode(nodeName).Return(nil)
}

func CreateExecutorTestWithStandardWorkflow(t *testing.T, workflowID, nodeName string, canExecute bool) (*MockComponents, interface{}, interface{}) {
	t.Helper()

	mockComponents, workflowInstance, _ := CreateStandardExecutorTestSetup(t, workflowID, nodeName)

	workflowObj := map[string]interface{}{
		"ID":           workflowID,
		"Status":       "running",
		"CurrentState": workflowInstance.(map[string]interface{})["CurrentState"],
	}

	testNode := CreateSuccessfulTestNode(nodeName)
	mockComponents.NodeRegistry.EXPECT().GetNode(nodeName).Return(testNode, nil)
	mockComponents.ResourceManager.EXPECT().CanExecuteNode(nodeName).Return(canExecute)

	return mockComponents, workflowInstance, workflowObj
}
