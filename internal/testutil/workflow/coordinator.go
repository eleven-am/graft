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


func SetupQueueMockExpectations(mockQueue *mocks.MockQueuePort, workflowID string) {
	mockQueue.On("EnqueueReady", mock.Anything, mock.AnythingOfType("*ports.QueueItem")).Return(nil).Maybe()
	mockQueue.On("EnqueuePending", mock.Anything, mock.AnythingOfType("*ports.QueueItem")).Return(nil).Maybe()
	mockQueue.On("DequeueReady", mock.Anything).Return(&ports.QueueItem{
		ID:         "test-item",
		WorkflowID: workflowID,
		NodeName:   "test-node",
		Config:     map[string]interface{}{},
		Priority:   1,
		EnqueuedAt:   time.Now(),
	}, nil).Maybe()
	mockQueue.On("GetPendingItems", mock.Anything, mock.AnythingOfType("int")).Return([]*ports.QueueItem{}, nil).Maybe()
	mockQueue.On("IsEmpty", mock.Anything).Return(true, nil).Maybe()
}

func SetupNodeRegistryMockExpectations(mockNodeRegistry *mocks.MockNodeRegistryPort, nodeName string, node ports.NodePort) {
	mockNodeRegistry.On("GetNode", nodeName).Return(node, nil).Maybe()
	mockNodeRegistry.On("ListNodes").Return([]string{nodeName}).Maybe()
}

func CreateTestQueueItem(workflowID, nodeName string, priority int) *ports.QueueItem {
	return &ports.QueueItem{
		ID:         "test-queue-item-" + nodeName,
		WorkflowID: workflowID,
		NodeName:   nodeName,
		Config:     map[string]interface{}{"test": "config"},
		Priority:   priority,
		EnqueuedAt:   time.Now(),
	}
}

func ValidateNodeQueuing(t *testing.T, mockQueue *mocks.MockQueuePort, expectedNodeName string) {
	t.Helper()
	
	mockQueue.AssertCalled(t, "EnqueueReady", mock.Anything, mock.MatchedBy(func(item *ports.QueueItem) bool {
		return item.NodeName == expectedNodeName
	}))
}

func SetupWorkflowTriggerTest(t *testing.T, mocks *MockComponents, workflowID string, nodeNames []string) {
	t.Helper()
	
	SetupQueueMockExpectations(mocks.Queue, workflowID)
	
	for _, nodeName := range nodeNames {
		testNode := CreateSuccessfulTestNode(nodeName)
		SetupNodeRegistryMockExpectations(mocks.NodeRegistry, nodeName, testNode)
	}
	
	mocks.ResourceManager.On("TryAcquire", mock.AnythingOfType("string")).Return(true, nil).Maybe()
	mocks.ResourceManager.On("Release", mock.AnythingOfType("string")).Return().Maybe()
	
	mocks.Storage.On("Put", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Return(nil).Maybe()
}


func CreateTestNextNodes(nodeNames []string) []ports.NextNode {
	nextNodes := make([]ports.NextNode, len(nodeNames))
	for i, nodeName := range nodeNames {
		nextNodes[i] = ports.NextNode{
			NodeName: nodeName,
			Config:   map[string]interface{}{"sequence": i + 1},
		}
	}
	return nextNodes
}

func CreateChainedTestNode(name string, nextNodeNames []string) *TestNode {
	return CreateTestNode(name, func(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
		result := map[string]interface{}{
			"result":     "success",
			"processed":  true,
			"chain_step": name,
		}
		
		nextNodes := CreateTestNextNodes(nextNodeNames)
		return result, nextNodes, nil
	})
}

func ValidateWorkflowChaining(t *testing.T, mocks *MockComponents, parentNode string, expectedNextNodes []string) {
	t.Helper()
	
	for _, nodeName := range expectedNextNodes {
		mockQueue := mocks.Queue
		mockQueue.AssertCalled(t, "EnqueueReady", mock.Anything, mock.MatchedBy(func(item *ports.QueueItem) bool {
			return item.NodeName == nodeName
		}))
	}
}

func SetupCoordinatorTestWorkflow(workflowID string, status string) map[string]interface{} {
	return map[string]interface{}{
		"ID":           workflowID,
		"Status":       status,
		"CurrentState": map[string]interface{}{},
		"StartedAt":    time.Now(),
	}
}

func SetupCoordinatorMockExpectations(mocks *MockComponents, workflowID, nodeName string) {
	mocks.Queue.On("DequeueReady", mock.Anything).Return(&ports.QueueItem{
		ID:         "test-item",
		WorkflowID: workflowID,
		NodeName:   nodeName,
		Config:     map[string]interface{}{},
		Priority:   1,
		EnqueuedAt: time.Now(),
	}, nil).Maybe()
	
	mocks.Queue.On("GetPendingItems", mock.Anything).Return([]ports.QueueItem{}, nil).Maybe()
	mocks.Queue.On("IsEmpty", mock.Anything).Return(true, nil).Maybe()
	
	testNode := CreateSuccessfulTestNode(nodeName)
	SetupNodeRegistryMockExpectations(mocks.NodeRegistry, nodeName, testNode)
	
	mocks.ResourceManager.On("TryAcquire", mock.AnythingOfType("string")).Return(true, nil).Maybe()
	mocks.ResourceManager.On("Release", mock.AnythingOfType("string")).Return().Maybe()
	
	mocks.Storage.On("Put", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Return(nil).Maybe()
}

func SetupEmptyQueueMocks(mockQueue *mocks.MockQueuePort) {
	mockQueue.On("DequeueReady", mock.Anything).Return(nil, assert.AnError).Maybe()
	mockQueue.On("GetPendingItems", mock.Anything).Return([]ports.QueueItem{}, nil).Maybe()
	mockQueue.On("IsEmpty", mock.Anything).Return(true, nil).Maybe()
}

func SetupWorkflowTriggerQueueMocks(mockQueue *mocks.MockQueuePort, workflowID string, nodeNames []string) {
	for _, nodeName := range nodeNames {
		mockQueue.On("EnqueueReady", mock.Anything, mock.MatchedBy(func(item ports.QueueItem) bool {
			return item.WorkflowID == workflowID && item.NodeName == nodeName
		})).Return(nil)
	}
}

func SetupMetricsQueueMocks(mockQueue *mocks.MockQueuePort, pendingCount int) {
	mockQueue.On("IsEmpty", mock.Anything).Return(false, nil)
	pendingItems := make([]ports.QueueItem, pendingCount)
	for i := 0; i < pendingCount; i++ {
		pendingItems[i] = ports.QueueItem{
			ID:         "pending-item",
			WorkflowID: "test-workflow",
			NodeName:   "test-node",
		}
	}
	mockQueue.On("GetPendingItems", mock.Anything).Return(pendingItems, nil)
}