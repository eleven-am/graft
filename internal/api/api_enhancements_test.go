package api

import (
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
)

func TestAPIEnhancements_ExecutedNodeStatus(t *testing.T) {
	completedStatus := ports.WorkflowStatus{
		WorkflowID:   "test-workflow",
		Status:       ports.WorkflowStateCompleted,
		CurrentState: map[string]interface{}{},
		StartedAt:    time.Now(),
		ExecutedNodes: []ports.ExecutedNode{{
			NodeName:   "test-node",
			ExecutedAt: time.Now(),
			Duration:   time.Second,
			Status:     ports.NodeExecutionStatusCompleted,
			Config:     map[string]interface{}{},
			Results:    map[string]interface{}{"result": "success"},
		}},
		PendingNodes: []ports.PendingNode{},
		ReadyNodes:   []ports.ReadyNode{},
	}

	converted := ConvertWorkflowStatus(completedStatus)
	convertedMap := converted.(map[string]interface{})
	executedNodes := convertedMap["executed_nodes"].([]interface{})
	firstNode := executedNodes[0].(map[string]interface{})

	assert.Equal(t, "completed", firstNode["status"])
	assert.Equal(t, "test-node", firstNode["node_name"])

	failedStatus := ports.WorkflowStatus{
		WorkflowID:   "test-workflow-failed",
		Status:       ports.WorkflowStateFailed,
		CurrentState: map[string]interface{}{},
		StartedAt:    time.Now(),
		ExecutedNodes: []ports.ExecutedNode{{
			NodeName:   "failed-node",
			ExecutedAt: time.Now(),
			Duration:   time.Second,
			Status:     ports.NodeExecutionStatusFailed,
			Config:     map[string]interface{}{},
			Results:    map[string]interface{}{},
			Error:      &[]string{"execution error"}[0],
		}},
		PendingNodes: []ports.PendingNode{},
		ReadyNodes:   []ports.ReadyNode{},
	}

	convertedFailed := ConvertWorkflowStatus(failedStatus)
	convertedFailedMap := convertedFailed.(map[string]interface{})
	failedExecutedNodes := convertedFailedMap["executed_nodes"].([]interface{})
	firstFailedNode := failedExecutedNodes[0].(map[string]interface{})

	assert.Equal(t, "failed", firstFailedNode["status"])
	assert.Equal(t, "failed-node", firstFailedNode["node_name"])
	assert.NotNil(t, firstFailedNode["error"])
}

func TestAPIEnhancements_PendingNodePriority(t *testing.T) {
	status := ports.WorkflowStatus{
		WorkflowID:    "test-workflow",
		Status:        ports.WorkflowStateRunning,
		CurrentState:  map[string]interface{}{},
		StartedAt:     time.Now(),
		ExecutedNodes: []ports.ExecutedNode{},
		PendingNodes: []ports.PendingNode{{
			NodeName: "pending-node",
			Config:   map[string]interface{}{"param": "value"},
			QueuedAt: time.Now(),
			Reason:   "waiting for dependency",
			Priority: 5,
		}},
		ReadyNodes: []ports.ReadyNode{},
	}

	converted := ConvertWorkflowStatus(status)
	convertedMap := converted.(map[string]interface{})
	pendingNodes := convertedMap["pending_nodes"].([]interface{})
	firstPendingNode := pendingNodes[0].(map[string]interface{})

	assert.Equal(t, "pending-node", firstPendingNode["node_name"])
	assert.Equal(t, 5, firstPendingNode["priority"])
	assert.Equal(t, []string{"waiting for dependency"}, firstPendingNode["dependencies"])
}

func TestAPIEnhancements_ReadyNodePriority(t *testing.T) {
	status := ports.WorkflowStatus{
		WorkflowID:    "test-workflow",
		Status:        ports.WorkflowStateRunning,
		CurrentState:  map[string]interface{}{},
		StartedAt:     time.Now(),
		ExecutedNodes: []ports.ExecutedNode{},
		PendingNodes:  []ports.PendingNode{},
		ReadyNodes: []ports.ReadyNode{{
			NodeName: "ready-node",
			Config:   map[string]interface{}{"ready": true},
			QueuedAt: time.Now(),
			Priority: 10,
		}},
	}

	converted := ConvertWorkflowStatus(status)
	convertedMap := converted.(map[string]interface{})
	readyNodes := convertedMap["ready_nodes"].([]interface{})
	firstReadyNode := readyNodes[0].(map[string]interface{})

	assert.Equal(t, "ready-node", firstReadyNode["node_name"])
	assert.Equal(t, 10, firstReadyNode["priority"])
}

func TestAPIEnhancements_NodeExecutionStatusValues(t *testing.T) {
	assert.Equal(t, "completed", string(ports.NodeExecutionStatusCompleted))
	assert.Equal(t, "failed", string(ports.NodeExecutionStatusFailed))
	assert.Equal(t, "cancelled", string(ports.NodeExecutionStatusCancelled))
}

func TestAPIEnhancements_MultipleNodeTypes(t *testing.T) {
	now := time.Now()
	status := ports.WorkflowStatus{
		WorkflowID:   "comprehensive-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"count": 42},
		StartedAt:    now.Add(-2 * time.Minute),
		ExecutedNodes: []ports.ExecutedNode{{
			NodeName:   "completed-node",
			ExecutedAt: now.Add(-time.Minute),
			Duration:   30 * time.Second,
			Status:     ports.NodeExecutionStatusCompleted,
			Config:     map[string]interface{}{"input": "data"},
			Results:    map[string]interface{}{"output": "processed"},
		}},
		PendingNodes: []ports.PendingNode{{
			NodeName: "waiting-node",
			Config:   map[string]interface{}{"wait_for": "signal"},
			QueuedAt: now.Add(-30 * time.Second),
			Reason:   "waiting for external signal",
			Priority: 3,
		}},
		ReadyNodes: []ports.ReadyNode{{
			NodeName: "next-node",
			Config:   map[string]interface{}{"process": "next"},
			QueuedAt: now.Add(-10 * time.Second),
			Priority: 8,
		}},
	}

	converted := ConvertWorkflowStatus(status)
	convertedMap := converted.(map[string]interface{})

	executedNodes := convertedMap["executed_nodes"].([]interface{})
	assert.Len(t, executedNodes, 1)
	firstExecutedNode := executedNodes[0].(map[string]interface{})
	assert.Equal(t, "completed", firstExecutedNode["status"])
	assert.Equal(t, "completed-node", firstExecutedNode["node_name"])

	pendingNodes := convertedMap["pending_nodes"].([]interface{})
	assert.Len(t, pendingNodes, 1)
	firstPendingNode := pendingNodes[0].(map[string]interface{})
	assert.Equal(t, 3, firstPendingNode["priority"])
	assert.Equal(t, "waiting-node", firstPendingNode["node_name"])

	readyNodes := convertedMap["ready_nodes"].([]interface{})
	assert.Len(t, readyNodes, 1)
	firstReadyNode := readyNodes[0].(map[string]interface{})
	assert.Equal(t, 8, firstReadyNode["priority"])
	assert.Equal(t, "next-node", firstReadyNode["node_name"])
}
