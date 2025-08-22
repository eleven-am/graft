package workflow

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type TestConfig struct {
	MaxConcurrentWorkflows int
	NodeExecutionTimeout   time.Duration
	StateUpdateInterval    time.Duration
	RetryAttempts          int
	RetryBackoff           time.Duration
}

func DefaultTestConfig() TestConfig {
	return TestConfig{
		MaxConcurrentWorkflows: 10,
		NodeExecutionTimeout:   30 * time.Second,
		StateUpdateInterval:    time.Second,
		RetryAttempts:          3,
		RetryBackoff:           time.Millisecond,
	}
}


type MockComponents struct {
	NodeRegistry    *mocks.MockNodeRegistryPort
	ResourceManager *mocks.MockResourceManagerPort
	Storage         *mocks.MockStoragePort
	Queue           *mocks.MockQueuePort
	Transport       *mocks.MockTransportPort
	Discovery       *mocks.MockDiscoveryPort
}

func SetupMockComponents(t *testing.T) *MockComponents {
	t.Helper()
	
	return &MockComponents{
		NodeRegistry:    mocks.NewMockNodeRegistryPort(t),
		ResourceManager: mocks.NewMockResourceManagerPort(t),
		Storage:         mocks.NewMockStoragePort(t),
		Queue:           mocks.NewMockQueuePort(t),
		Transport:       mocks.NewMockTransportPort(t),
		Discovery:       mocks.NewMockDiscoveryPort(t),
	}
}

func CreateTestWorkflowTrigger(workflowID string, nodeConfigs []ports.NodeConfig) ports.WorkflowTrigger {
	return ports.WorkflowTrigger{
		WorkflowID:   workflowID,
		InitialNodes: nodeConfigs,
		InitialState: map[string]interface{}{"test": "data"},
		Metadata:     map[string]string{"source": "test"},
	}
}

func CreateTestNodeConfig(nodeName string) ports.NodeConfig {
	return ports.NodeConfig{
		Name:   nodeName,
		Config: map[string]interface{}{"timeout": 30},
	}
}



func AssertWorkflowState(t *testing.T, status *ports.WorkflowStatus, expectedID string, expectedStatus ports.WorkflowState) {
	t.Helper()
	
	assert.Equal(t, expectedID, status.WorkflowID)
	assert.Equal(t, expectedStatus, status.Status)
	assert.NotNil(t, status.CurrentState)
	assert.NotZero(t, status.StartedAt)
}

func SetupEmptyQueueMock(mockQueue *mocks.MockQueuePort) {
	mockQueue.EXPECT().DequeueReady(mock.Anything).Return(nil, domain.NewNotFoundError("queue_item", "ready queue is empty")).Maybe()
	mockQueue.EXPECT().GetPendingItems(mock.Anything).Return([]ports.QueueItem{}, nil).Maybe()
	mockQueue.EXPECT().IsEmpty(mock.Anything).Return(true, nil).Maybe()
}

func SetupEmptyStorageMock(mockStorage *mocks.MockStoragePort) {
	mockStorage.EXPECT().List(mock.Anything, "workflow:state:").Return([]ports.KeyValue{}, nil)
}

func CreateTestWorkflowTriggerWithNodes(workflowID string, nodeNames []string) ports.WorkflowTrigger {
	nodes := make([]ports.NodeConfig, len(nodeNames))
	for i, nodeName := range nodeNames {
		nodes[i] = CreateTestNodeConfig(nodeName)
	}
	return CreateTestWorkflowTrigger(workflowID, nodes)
}

func SetupWorkflowTriggerQueueMock(mockQueue *mocks.MockQueuePort, workflowID string, nodeNames []string) {
	for _, nodeName := range nodeNames {
		mockQueue.EXPECT().EnqueueReady(mock.Anything, mock.MatchedBy(func(item ports.QueueItem) bool {
			return item.WorkflowID == workflowID && item.NodeName == nodeName
		})).Return(nil)
	}
}

func SetupNodeExecutionQueueMock(mockQueue *mocks.MockQueuePort, workflowID, nodeName string, expectedConfig interface{}) {
	mockQueue.EXPECT().EnqueueReady(mock.Anything, mock.MatchedBy(func(item ports.QueueItem) bool {
		if item.WorkflowID != workflowID || item.NodeName != nodeName {
			return false
		}
		
		return reflect.DeepEqual(item.Config, expectedConfig)
	})).Return(nil)
}

type WorkflowInstanceCreator struct{}

func CreateTestWorkflowStates() map[string]interface{} {
	return map[string]interface{}{
		"running":   map[string]interface{}{"Status": "running"},
		"completed": map[string]interface{}{"Status": "completed"},
		"failed":    map[string]interface{}{"Status": "failed"},
	}
}

func SetupMetricsQueueMock(mockQueue *mocks.MockQueuePort, pendingCount int) {
	mockQueue.EXPECT().IsEmpty(mock.Anything).Return(false, nil)
	
	pendingItems := make([]ports.QueueItem, pendingCount)
	for i := 0; i < pendingCount; i++ {
		pendingItems[i] = ports.QueueItem{ID: fmt.Sprintf("item%d", i+1)}
	}
	mockQueue.EXPECT().GetPendingItems(mock.Anything).Return(pendingItems, nil)
}

func ValidateExecutionMetrics(t *testing.T, metrics interface{}, expectedCounts map[string]int64) {
	t.Helper()
}