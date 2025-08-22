package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/eleven-am/graft/internal/testutil/workflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewStateManager(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	stateManager := NewStateManager(engine)

	assert.NotNil(t, stateManager)
	assert.Equal(t, engine, stateManager.engine)
}

func TestStateManager_SaveWorkflowState(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetStorage(mockComponents.Storage)
	
	stateManager := NewStateManager(engine)

	workflowInstance := workflow.CreateTestWorkflowForExecution("test-workflow")
	workflowObj := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: workflowInstance["CurrentState"].(map[string]interface{}),
		StartedAt:    workflowInstance["StartedAt"].(time.Time),
		Metadata:     map[string]string{"env": "test"},
	}

	mockComponents.Storage.EXPECT().Put(mock.Anything, "workflow:state:test-workflow", mock.AnythingOfType("[]uint8")).Return(nil)

	err := stateManager.SaveWorkflowState(context.Background(), workflowObj)

	assert.NoError(t, err)
}

func TestStateManager_SaveWorkflowState_StorageError(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetStorage(mockComponents.Storage)
	
	stateManager := NewStateManager(engine)

	workflowInstance := workflow.CreateTestWorkflowForExecution("test-workflow")
	workflowObj := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: workflowInstance["CurrentState"].(map[string]interface{}),
		StartedAt:    workflowInstance["StartedAt"].(time.Time),
	}

	mockComponents.Storage.EXPECT().Put(mock.Anything, "workflow:state:test-workflow", mock.AnythingOfType("[]uint8")).
		Return(assert.AnError)

	err := stateManager.SaveWorkflowState(context.Background(), workflowObj)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to persist workflow state")
}

func TestStateManager_LoadWorkflowState_Success(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)
	engine.SetStorage(mockComponents.Storage)
	
	stateManager := NewStateManager(engine)

	startTime := time.Now()
	stateData := WorkflowStateData{
		ID:           "test-workflow",
		Status:       string(ports.WorkflowStateRunning),
		CurrentState: map[string]interface{}{"key": "value"},
		StartedAt:    startTime,
		Metadata:     map[string]string{"env": "test"},
		Version:      1,
		UpdatedAt:    time.Now(),
	}

	serializedData, err := json.Marshal(stateData)
	require.NoError(t, err)

	mockComponents.Storage.EXPECT().Get(mock.Anything, "workflow:state:test-workflow").
		Return(serializedData, nil)

	workflow, err := stateManager.LoadWorkflowState(context.Background(), "test-workflow")

	assert.NoError(t, err)
	assert.NotNil(t, workflow)
	assert.Equal(t, "test-workflow", workflow.ID)
	assert.Equal(t, ports.WorkflowStateRunning, workflow.Status)
	assert.Equal(t, map[string]interface{}{"key": "value"}, workflow.CurrentState)
	assert.Equal(t, startTime.Unix(), workflow.StartedAt.Unix())
	assert.Equal(t, map[string]string{"env": "test"}, workflow.Metadata)
}

func TestStateManager_LoadWorkflowState_StorageError(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	stateManager := NewStateManager(engine)

	mockStorage.EXPECT().Get(mock.Anything, "workflow:state:nonexistent").
		Return(nil, assert.AnError)

	workflow, err := stateManager.LoadWorkflowState(context.Background(), "nonexistent")

	assert.Nil(t, workflow)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load workflow state")
}

func TestStateManager_LoadWorkflowState_InvalidJSON(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	stateManager := NewStateManager(engine)

	mockStorage.EXPECT().Get(mock.Anything, "workflow:state:invalid").
		Return([]byte("invalid json"), nil)

	workflow, err := stateManager.LoadWorkflowState(context.Background(), "invalid")

	assert.Nil(t, workflow)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to deserialize workflow state")
}

func TestStateManager_DeleteWorkflowState(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	stateManager := NewStateManager(engine)

	mockStorage.EXPECT().Delete(mock.Anything, "workflow:state:test-workflow").Return(nil)

	err := stateManager.DeleteWorkflowState(context.Background(), "test-workflow")

	assert.NoError(t, err)
}

func TestStateManager_ListWorkflowStates(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	stateManager := NewStateManager(engine)

	startTime := time.Now()
	stateData1 := WorkflowStateData{
		ID:           "workflow-1",
		Status:       string(ports.WorkflowStateRunning),
		CurrentState: map[string]interface{}{"key": "value1"},
		StartedAt:    startTime,
		Version:      1,
		UpdatedAt:    time.Now(),
	}

	stateData2 := WorkflowStateData{
		ID:           "workflow-2",
		Status:       string(ports.WorkflowStateCompleted),
		CurrentState: map[string]interface{}{"key": "value2"},
		StartedAt:    startTime,
		Version:      1,
		UpdatedAt:    time.Now(),
	}

	serialized1, _ := json.Marshal(stateData1)
	serialized2, _ := json.Marshal(stateData2)

	keyValues := []ports.KeyValue{
		{Key: "workflow:state:workflow-1", Value: serialized1},
		{Key: "workflow:state:workflow-2", Value: serialized2},
	}

	mockStorage.EXPECT().List(mock.Anything, "workflow:state:").Return(keyValues, nil)

	workflows, err := stateManager.ListWorkflowStates(context.Background())

	assert.NoError(t, err)
	assert.Len(t, workflows, 2)
	assert.Equal(t, "workflow-1", workflows[0].ID)
	assert.Equal(t, ports.WorkflowStateRunning, workflows[0].Status)
	assert.Equal(t, "workflow-2", workflows[1].ID)
	assert.Equal(t, ports.WorkflowStateCompleted, workflows[1].Status)
}

func TestStateManager_RecoverActiveWorkflows(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	stateManager := NewStateManager(engine)

	startTime := time.Now()
	runningState := WorkflowStateData{
		ID:           "running-workflow",
		Status:       string(ports.WorkflowStateRunning),
		CurrentState: map[string]interface{}{"key": "value"},
		StartedAt:    startTime,
		Version:      1,
		UpdatedAt:    time.Now(),
	}

	pausedState := WorkflowStateData{
		ID:           "paused-workflow",
		Status:       string(ports.WorkflowStatePaused),
		CurrentState: map[string]interface{}{"key": "value"},
		StartedAt:    startTime,
		Version:      1,
		UpdatedAt:    time.Now(),
	}

	completedState := WorkflowStateData{
		ID:           "completed-workflow",
		Status:       string(ports.WorkflowStateCompleted),
		CurrentState: map[string]interface{}{"key": "value"},
		StartedAt:    startTime,
		Version:      1,
		UpdatedAt:    time.Now(),
	}

	serializedRunning, _ := json.Marshal(runningState)
	serializedPaused, _ := json.Marshal(pausedState)
	serializedCompleted, _ := json.Marshal(completedState)

	keyValues := []ports.KeyValue{
		{Key: "workflow:state:running-workflow", Value: serializedRunning},
		{Key: "workflow:state:paused-workflow", Value: serializedPaused},
		{Key: "workflow:state:completed-workflow", Value: serializedCompleted},
	}

	mockStorage.EXPECT().List(mock.Anything, "workflow:state:").Return(keyValues, nil)

	err := stateManager.RecoverActiveWorkflows(context.Background())

	assert.NoError(t, err)
	assert.Len(t, engine.activeWorkflows, 2)
	assert.Contains(t, engine.activeWorkflows, "running-workflow")
	assert.Contains(t, engine.activeWorkflows, "paused-workflow")
	assert.NotContains(t, engine.activeWorkflows, "completed-workflow")
}

func TestStateManager_UpdateWorkflowState_NotFound(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	stateManager := NewStateManager(engine)

	err := stateManager.UpdateWorkflowState(context.Background(), "nonexistent", map[string]interface{}{"key": "value"})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}


func TestStateManager_GetWorkflowState_FromMemory(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	stateManager := NewStateManager(engine)

	workflow := &WorkflowInstance{
		ID:           "test-workflow",
		CurrentState: map[string]interface{}{"key": "value"},
	}
	engine.activeWorkflows["test-workflow"] = workflow

	state, err := stateManager.GetWorkflowState(context.Background(), "test-workflow")

	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"key": "value"}, state)
}

func TestStateManager_GetWorkflowState_FromStorage(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	stateManager := NewStateManager(engine)

	stateData := WorkflowStateData{
		ID:           "test-workflow",
		Status:       string(ports.WorkflowStateRunning),
		CurrentState: map[string]interface{}{"key": "value"},
		StartedAt:    time.Now(),
		Version:      1,
		UpdatedAt:    time.Now(),
	}

	serializedData, _ := json.Marshal(stateData)

	mockStorage.EXPECT().Get(mock.Anything, "workflow:state:test-workflow").
		Return(serializedData, nil)

	state, err := stateManager.GetWorkflowState(context.Background(), "test-workflow")

	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"key": "value"}, state)
}

func TestStateManager_CleanupCompletedWorkflows(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	stateManager := NewStateManager(engine)

	oldTime := time.Now().Add(-2 * time.Hour)
	recentTime := time.Now().Add(-30 * time.Minute)

	oldCompleted := WorkflowStateData{
		ID:           "old-completed",
		Status:       string(ports.WorkflowStateCompleted),
		CurrentState: map[string]interface{}{},
		StartedAt:    oldTime,
		CompletedAt:  &oldTime,
		Version:      1,
		UpdatedAt:    time.Now(),
	}

	recentCompleted := WorkflowStateData{
		ID:           "recent-completed",
		Status:       string(ports.WorkflowStateCompleted),
		CurrentState: map[string]interface{}{},
		StartedAt:    recentTime,
		CompletedAt:  &recentTime,
		Version:      1,
		UpdatedAt:    time.Now(),
	}

	serializedOld, _ := json.Marshal(oldCompleted)
	serializedRecent, _ := json.Marshal(recentCompleted)

	keyValues := []ports.KeyValue{
		{Key: "workflow:state:old-completed", Value: serializedOld},
		{Key: "workflow:state:recent-completed", Value: serializedRecent},
	}

	mockStorage.EXPECT().List(mock.Anything, "workflow:state:").Return(keyValues, nil)
	mockStorage.EXPECT().Delete(mock.Anything, "workflow:state:old-completed").Return(nil)

	err := stateManager.CleanupCompletedWorkflows(context.Background(), time.Hour)

	assert.NoError(t, err)
}

func TestStateManager_CreateCheckpoint(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockStorage := mocks.NewMockStoragePort(t)
	engine.SetStorage(mockStorage)
	
	stateManager := NewStateManager(engine)

	workflow := &WorkflowInstance{
		ID:           "test-workflow",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"key": "value"},
		StartedAt:    time.Now(),
	}
	engine.activeWorkflows["test-workflow"] = workflow

	mockStorage.EXPECT().Put(mock.Anything, mock.MatchedBy(func(key string) bool {
		return key[:20] == "workflow:checkpoint:"
	}), mock.AnythingOfType("[]uint8")).Return(nil)

	err := stateManager.CreateCheckpoint(context.Background(), "test-workflow")

	assert.NoError(t, err)
}

func TestStateManager_GenerateStateKey(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	stateManager := NewStateManager(engine)

	key := stateManager.generateStateKey("test-workflow")

	assert.Equal(t, "workflow:state:test-workflow", key)
}