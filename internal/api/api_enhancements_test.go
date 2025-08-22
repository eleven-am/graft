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

	converted := convertWorkflowStatus(completedStatus)

	assert.Equal(t, "completed", converted.ExecutedNodes[0].Status)
	assert.Equal(t, "test-node", converted.ExecutedNodes[0].NodeName)

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

	convertedFailed := convertWorkflowStatus(failedStatus)

	assert.Equal(t, "failed", convertedFailed.ExecutedNodes[0].Status)
	assert.Equal(t, "failed-node", convertedFailed.ExecutedNodes[0].NodeName)
	assert.NotNil(t, convertedFailed.ExecutedNodes[0].Error)
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

	converted := convertWorkflowStatus(status)

	assert.Equal(t, "pending-node", converted.PendingNodes[0].NodeName)
	assert.Equal(t, 5, converted.PendingNodes[0].Priority)
	assert.Equal(t, []string{"waiting for dependency"}, converted.PendingNodes[0].Dependencies)
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

	converted := convertWorkflowStatus(status)

	assert.Equal(t, "ready-node", converted.ReadyNodes[0].NodeName)
	assert.Equal(t, 10, converted.ReadyNodes[0].Priority)
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

	converted := convertWorkflowStatus(status)

	assert.Len(t, converted.ExecutedNodes, 1)
	assert.Equal(t, "completed", converted.ExecutedNodes[0].Status)
	assert.Equal(t, "completed-node", converted.ExecutedNodes[0].NodeName)

	assert.Len(t, converted.PendingNodes, 1)
	assert.Equal(t, 3, converted.PendingNodes[0].Priority)
	assert.Equal(t, "waiting-node", converted.PendingNodes[0].NodeName)

	assert.Len(t, converted.ReadyNodes, 1)
	assert.Equal(t, 8, converted.ReadyNodes[0].Priority)
	assert.Equal(t, "next-node", converted.ReadyNodes[0].NodeName)
}
