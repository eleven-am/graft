package workflow

import (
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
)


func CreateTestWorkflowState(workflowID string, status ports.WorkflowState) *ports.WorkflowStatus {
	return &ports.WorkflowStatus{
		WorkflowID:    workflowID,
		Status:        status,
		CurrentState:  map[string]interface{}{"test": "data"},
		StartedAt:     time.Now(),
		ExecutedNodes: []ports.ExecutedNode{},
		PendingNodes:  []ports.PendingNode{},
		ReadyNodes:    []ports.ReadyNode{},
	}
}

func SetupStorageExpectations(t *testing.T, mockStorage *mocks.MockStoragePort, workflowID string) {
	t.Helper()
	
	stateKey := "workflow:state:" + workflowID
	
	mockStorage.On("Put", mock.Anything, stateKey, mock.AnythingOfType("[]uint8")).Return(nil).Maybe()
	
	mockStorage.On("Get", mock.Anything, stateKey).Return(
		[]byte(`{
			"id":"` + workflowID + `",
			"status":"running",
			"current_state":{"test":"data"},
			"started_at":"2023-01-01T00:00:00Z",
			"version":1,
			"updated_at":"2023-01-01T00:00:00Z"
		}`), nil).Maybe()
	
	mockStorage.On("Delete", mock.Anything, stateKey).Return(nil).Maybe()
	
	mockStorage.On("List", mock.Anything, "workflow:state:").Return([]ports.KeyValue{
		{
			Key: stateKey,
			Value: []byte(`{
				"id":"` + workflowID + `",
				"status":"running",
				"current_state":{"test":"data"},
				"started_at":"2023-01-01T00:00:00Z",
				"version":1,
				"updated_at":"2023-01-01T00:00:00Z"
			}`),
		},
	}, nil).Maybe()
}




func CreateTestExecutedNode(nodeName string, status ports.NodeExecutionStatus) ports.ExecutedNode {
	return ports.ExecutedNode{
		NodeName:   nodeName,
		ExecutedAt: time.Now(),
		Duration:   time.Second,
		Status:     status,
		Config:     map[string]interface{}{"test": "config"},
		Results:    map[string]interface{}{"result": "success"},
	}
}

func SetupStateManagerTest(t *testing.T, workflowID string) (*MockComponents, interface{}) {
	t.Helper()
	mockComponents := SetupMockComponents(t)
	SetupStorageExpectations(t, mockComponents.Storage, workflowID)
	return mockComponents, nil
}

func CreateTestWorkflowInstanceForState(workflowID string, includeMetadata bool) interface{} {
	workflowInstance := CreateTestWorkflowForExecution(workflowID)
	
	result := map[string]interface{}{
		"ID":           workflowID,
		"Status":       ports.WorkflowStateRunning,
		"CurrentState": workflowInstance["CurrentState"],
		"StartedAt":    workflowInstance["StartedAt"],
	}
	
	if includeMetadata {
		result["Metadata"] = map[string]string{"env": "test"}
	}
	
	return result
}

func SetupStateManagerWithWorkflow(t *testing.T, workflowID string, includeMetadata bool) (*MockComponents, interface{}, interface{}) {
	t.Helper()
	
	mockComponents := SetupMockComponents(t)
	workflowInstance := CreateTestWorkflowForExecution(workflowID)
	
	workflowObj := map[string]interface{}{
		"ID":           workflowID,
		"Status":       ports.WorkflowStateRunning,
		"CurrentState": workflowInstance["CurrentState"],
		"StartedAt":    workflowInstance["StartedAt"],
	}
	
	if includeMetadata {
		workflowObj["Metadata"] = map[string]string{"env": "test"}
	}
	
	return mockComponents, workflowInstance, workflowObj
}