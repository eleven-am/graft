package engine

import (
	"context"
	"log/slog"
	"testing"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWorkflowInstance_Mutex(t *testing.T) {
	workflow := &WorkflowInstance{
		ID:           "test",
		CurrentState: make(map[string]interface{}),
	}

	workflow.mu.Lock()
	workflow.ID = "updated"
	workflow.mu.Unlock()

	workflow.mu.RLock()
	id := workflow.ID
	workflow.mu.RUnlock()

	assert.Equal(t, "updated", id)
}

func TestEngineConfig_Fields(t *testing.T) {
	config := Config{
		MaxConcurrentWorkflows: 100,
		RetryAttempts:          5,
	}

	assert.Equal(t, 100, config.MaxConcurrentWorkflows)
	assert.Equal(t, 5, config.RetryAttempts)
}

func TestWorkflowStateData_Fields(t *testing.T) {
	data := WorkflowStateData{
		ID:           "test",
		Status:       "running",
		CurrentState: map[string]interface{}{"key": "value"},
		Version:      1,
	}

	assert.Equal(t, "test", data.ID)
	assert.Equal(t, "running", data.Status)
	assert.Equal(t, map[string]interface{}{"key": "value"}, data.CurrentState)
	assert.Equal(t, 1, data.Version)
}

func TestHasNodesForWorkflow_EmptyQueues(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())

	// Need to set up mock storage since hasActiveClaimsForWorkflow checks storage
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)

	coordinator := NewWorkflowCoordinator(engine)

	isEmpty := true
	pendingItems := []ports.QueueItem{}

	// Mock the List call to return empty claims
	mockStorage.EXPECT().List(mock.Anything, "claims:").Return([]ports.KeyValue{}, nil)

	result := coordinator.hasNodesForWorkflow(context.TODO(), "test-workflow", isEmpty, pendingItems)

	assert.False(t, result)
}

func TestHasNodesForWorkflow_ReadyNotEmpty(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	coordinator := NewWorkflowCoordinator(engine)

	isEmpty := false
	pendingItems := []ports.QueueItem{}

	result := coordinator.hasNodesForWorkflow(context.TODO(), "test-workflow", isEmpty, pendingItems)

	assert.True(t, result)
}

func TestHasNodesForWorkflow_PendingHasWorkflowItem(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	coordinator := NewWorkflowCoordinator(engine)

	isEmpty := true
	pendingItems := []ports.QueueItem{
		{ID: "item1", WorkflowID: "other-workflow"},
		{ID: "item2", WorkflowID: "test-workflow"},
	}

	result := coordinator.hasNodesForWorkflow(context.TODO(), "test-workflow", isEmpty, pendingItems)

	assert.True(t, result)
}

func TestNodeExecutor_GetWorkflow_Found(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	executor := NewNodeExecutor(engine)

	workflow := &WorkflowInstance{
		ID:     "test-workflow",
		Status: ports.WorkflowStateRunning,
	}
	engine.activeWorkflows["test-workflow"] = workflow

	result, err := executor.getWorkflow("test-workflow")

	assert.NoError(t, err)
	assert.Equal(t, workflow, result)
}
