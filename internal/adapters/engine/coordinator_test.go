package engine

import (
	"context"
	"log/slog"
	"testing"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/eleven-am/graft/internal/testutil/workflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewWorkflowCoordinator(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	coordinator := NewWorkflowCoordinator(engine)

	assert.NotNil(t, coordinator)
	assert.Equal(t, engine, coordinator.engine)
	assert.NotNil(t, coordinator.executor)
}

func TestWorkflowCoordinator_ProcessNextReadyNode_EmptyQueue(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetQueue(mockComponents.Queue)
	
	coordinator := NewWorkflowCoordinator(engine)

	mockComponents.Queue.EXPECT().DequeueReady(mock.Anything).Return(nil, domain.NewNotFoundError("queue_item", "ready queue is empty"))

	err := coordinator.processNextReadyNode(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestWorkflowCoordinator_ProcessNextReadyNode_Success(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	
	engine.SetQueue(mockComponents.Queue)
	
	coordinator := NewWorkflowCoordinator(engine)

	item := workflow.CreateTestQueueItem("workflow-123", "test-node", 1)

	mockComponents.Queue.EXPECT().DequeueReady(mock.Anything).Return(item, nil)

	err := coordinator.processNextReadyNode(context.Background())

	assert.NoError(t, err)
}


func TestWorkflowCoordinator_CheckWorkflowCompletion_NotRunning(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	coordinator := NewWorkflowCoordinator(engine)

	workflow := &WorkflowInstance{
		ID:     "completed-workflow",
		Status: ports.WorkflowStateCompleted,
	}

	err := coordinator.CheckWorkflowCompletion(context.Background(), workflow)

	assert.NoError(t, err)
}

func TestWorkflowCoordinator_CheckWorkflowCompletion_HasNodes(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockQueue := mocks.NewMockQueuePort(t)
	engine.SetQueue(mockQueue)
	
	coordinator := NewWorkflowCoordinator(engine)

	workflow := &WorkflowInstance{
		ID:     "running-workflow",
		Status: ports.WorkflowStateRunning,
	}

	mockQueue.EXPECT().IsEmpty(mock.Anything).Return(false, nil)
	mockQueue.EXPECT().GetPendingItems(mock.Anything).Return([]ports.QueueItem{}, nil)

	err := coordinator.CheckWorkflowCompletion(context.Background(), workflow)

	assert.NoError(t, err)
	assert.Equal(t, ports.WorkflowStateRunning, workflow.Status)
}

func TestWorkflowCoordinator_CheckWorkflowCompletion_NoNodes(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockQueue := mocks.NewMockQueuePort(t)
	mockStorage := mocks.NewMockStoragePort(t)
	
	engine.SetQueue(mockQueue)
	engine.SetStorage(mockStorage)
	
	coordinator := NewWorkflowCoordinator(engine)

	workflow := &WorkflowInstance{
		ID:           "running-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{},
	}

	engine.activeWorkflows["running-workflow"] = workflow

	mockQueue.EXPECT().IsEmpty(mock.Anything).Return(true, nil)
	mockQueue.EXPECT().GetPendingItems(mock.Anything).Return([]ports.QueueItem{}, nil)
	mockStorage.EXPECT().Put(mock.Anything, "workflow:state:running-workflow", mock.AnythingOfType("[]uint8")).Return(nil)

	err := coordinator.CheckWorkflowCompletion(context.Background(), workflow)

	assert.NoError(t, err)
	assert.Equal(t, ports.WorkflowStateCompleted, workflow.Status)
	assert.NotNil(t, workflow.CompletedAt)
	assert.NotContains(t, engine.activeWorkflows, "running-workflow")
}

func TestWorkflowCoordinator_PauseWorkflow_NotFound(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	coordinator := NewWorkflowCoordinator(engine)

	err := coordinator.PauseWorkflow(context.Background(), "nonexistent")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestWorkflowCoordinator_PauseWorkflow_NotRunning(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	coordinator := NewWorkflowCoordinator(engine)

	engine.activeWorkflows["paused-workflow"] = &WorkflowInstance{
		ID:     "paused-workflow",
		Status: ports.WorkflowStatePaused,
	}

	err := coordinator.PauseWorkflow(context.Background(), "paused-workflow")

	assert.Error(t, err)
	domainErr, ok := err.(domain.Error)
	require.True(t, ok)
	assert.Equal(t, domain.ErrorTypeConflict, domainErr.Type)
}

func TestWorkflowCoordinator_PauseWorkflow_Success(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	coordinator := NewWorkflowCoordinator(engine)

	workflow := &WorkflowInstance{
		ID:           "running-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{},
	}
	engine.activeWorkflows["running-workflow"] = workflow

	mockStorage.EXPECT().Put(mock.Anything, "workflow:state:running-workflow", mock.AnythingOfType("[]uint8")).Return(nil)

	err := coordinator.PauseWorkflow(context.Background(), "running-workflow")

	assert.NoError(t, err)
	assert.Equal(t, ports.WorkflowStatePaused, workflow.Status)
}

func TestWorkflowCoordinator_ResumeWorkflow_Success(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	coordinator := NewWorkflowCoordinator(engine)

	workflow := &WorkflowInstance{
		ID:           "paused-workflow",
		Status:       ports.WorkflowStatePaused,
		CurrentState: map[string]interface{}{},
	}
	engine.activeWorkflows["paused-workflow"] = workflow

	mockStorage.EXPECT().Put(mock.Anything, "workflow:state:paused-workflow", mock.AnythingOfType("[]uint8")).Return(nil)

	err := coordinator.ResumeWorkflow(context.Background(), "paused-workflow")

	assert.NoError(t, err)
	assert.Equal(t, ports.WorkflowStateRunning, workflow.Status)
}

func TestWorkflowCoordinator_StopWorkflow_Success(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	coordinator := NewWorkflowCoordinator(engine)

	workflow := &WorkflowInstance{
		ID:           "running-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{},
	}
	engine.activeWorkflows["running-workflow"] = workflow

	mockStorage.EXPECT().Put(mock.Anything, "workflow:state:running-workflow", mock.AnythingOfType("[]uint8")).Return(nil)

	err := coordinator.StopWorkflow(context.Background(), "running-workflow")

	assert.NoError(t, err)
	assert.Equal(t, ports.WorkflowStateFailed, workflow.Status)
	assert.NotNil(t, workflow.CompletedAt)
	assert.NotNil(t, workflow.LastError)
	assert.NotContains(t, engine.activeWorkflows, "running-workflow")
}

func TestIsNotFoundError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"not found error", domain.NewNotFoundError("resource", "id"), true},
		{"empty error", domain.NewNotFoundError("queue", "empty"), true},
		{"other error", domain.NewValidationError("field", "reason"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNotFoundError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}