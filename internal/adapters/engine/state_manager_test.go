package engine

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestOptimizedStateManager_SaveWorkflowState_Immediate(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sm := NewStateManager(storage, logger)
	defer sm.Stop()

	workflow := &domain.WorkflowInstance{
		ID:      "workflow-1",
		Status:  domain.WorkflowStateRunning,
		Version: 1,
	}

	storage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), int64(1)).Return(nil)

	ctx := context.Background()
	err := sm.SaveWorkflowState(ctx, workflow)

	assert.NoError(t, err)
	storage.AssertExpectations(t)
}

func TestOptimizedStateManager_SaveWorkflowState_Batched(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sm := NewStateManager(storage, logger)
	defer sm.Stop()

	workflow1 := &domain.WorkflowInstance{
		ID:      "workflow-1",
		Status:  domain.WorkflowStateRunning,
		Version: 2,
	}

	storage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), mock.MatchedBy(func(v int64) bool {
		return v == 2 || v == 0
	})).Return(nil).Maybe()

	ctx := context.Background()

	err := sm.SaveWorkflowState(ctx, workflow1)
	assert.NoError(t, err)

	time.Sleep(600 * time.Millisecond)

}

func TestOptimizedStateManager_LoadWorkflowState(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sm := NewStateManager(storage, logger)
	defer sm.Stop()

	workflowData := []byte(`{"id":"workflow-1","status":"running","version":1}`)
	storage.On("Get", domain.WorkflowStateKey("workflow-1")).Return(workflowData, int64(1), true, nil)

	ctx := context.Background()
	workflow, err := sm.LoadWorkflowState(ctx, "workflow-1")

	assert.NoError(t, err)
	assert.NotNil(t, workflow)
	assert.Equal(t, "workflow-1", workflow.ID)
	assert.Equal(t, domain.WorkflowStateRunning, workflow.Status)

	storage.AssertExpectations(t)
}

func TestOptimizedStateManager_UpdateWorkflowState(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sm := NewStateManager(storage, logger)
	defer sm.Stop()

	workflowData := []byte(`{"id":"workflow-1","status":"running","version":1}`)
	storage.On("Get", domain.WorkflowStateKey("workflow-1")).Return(workflowData, int64(1), true, nil)

	storage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), mock.MatchedBy(func(v int64) bool {
		return v == 2 || v == 0
	})).Return(nil).Maybe()

	ctx := context.Background()
	err := sm.UpdateWorkflowState(ctx, "workflow-1", func(workflow *domain.WorkflowInstance) error {
		workflow.Status = domain.WorkflowStateCompleted
		return nil
	})

	assert.NoError(t, err)

	time.Sleep(600 * time.Millisecond)
}

func TestOptimizedStateManager_Compression(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sm := NewStateManager(storage, logger)
	defer sm.Stop()

	workflow := &domain.WorkflowInstance{
		ID:      "workflow-1",
		Status:  domain.WorkflowStateRunning,
		Version: 1,
		Metadata: map[string]string{
			"large_data": "This is a large piece of data that should trigger compression when the threshold is set low enough for testing purposes",
		},
	}

	storage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), int64(1)).Return(nil)

	ctx := context.Background()
	err := sm.SaveWorkflowState(ctx, workflow)

	assert.NoError(t, err)
	storage.AssertExpectations(t)
}

func TestOptimizedStateManager_AdaptiveStrategy(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sm := NewStateManager(storage, logger)
	defer sm.Stop()

	workflow := &domain.WorkflowInstance{
		ID:      "workflow-1",
		Status:  domain.WorkflowStateRunning,
		Version: 1,
	}

	storage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), mock.MatchedBy(func(v int64) bool {
		return v == 1 || v == 0
	})).Return(nil).Maybe()

	ctx := context.Background()
	err := sm.SaveWorkflowState(ctx, workflow)
	assert.NoError(t, err)

	workflow2 := &domain.WorkflowInstance{
		ID:      "workflow-2",
		Status:  domain.WorkflowStateRunning,
		Version: 2,
	}

	workflow2.Metadata = map[string]string{"trigger": "batch"}

	storage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), mock.MatchedBy(func(v int64) bool {
		return v == 2 || v == 0
	})).Return(nil).Maybe()
	err = sm.SaveWorkflowState(ctx, workflow2)
	assert.NoError(t, err)

	time.Sleep(600 * time.Millisecond)
}

func TestOptimizedStateManager_StatisticsTracking(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sm := NewStateManager(storage, logger)
	defer sm.Stop()

	workflow := &domain.WorkflowInstance{
		ID:       "workflow-1",
		Status:   domain.WorkflowStateRunning,
		Version:  1,
		Metadata: map[string]string{"test": "data"},
	}

	storage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), int64(1)).Return(nil)
	ctx := context.Background()
	err := sm.SaveWorkflowState(ctx, workflow)
	assert.NoError(t, err)

	stats := sm.getStatistics("workflow-1")
	assert.NotNil(t, stats)
	assert.Equal(t, "workflow-1", stats.WorkflowID)
	assert.True(t, stats.StateSize > 0)
	assert.False(t, stats.LastChangeTimestamp.IsZero())
}

func TestOptimizedStateManager_BatchTimeout(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sm := NewStateManager(storage, logger)
	defer sm.Stop()

	workflow := &domain.WorkflowInstance{
		ID:      "workflow-1",
		Status:  domain.WorkflowStateRunning,
		Version: 2,
	}

	storage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), mock.MatchedBy(func(v int64) bool {
		return v == 2 || v == 0
	})).Return(nil).Maybe()

	ctx := context.Background()
	err := sm.SaveWorkflowState(ctx, workflow)
	assert.NoError(t, err)

	time.Sleep(600 * time.Millisecond)
}

func TestOptimizedStateManager_Stop(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sm := NewStateManager(storage, logger)

	workflow := &domain.WorkflowInstance{
		ID:      "workflow-1",
		Status:  domain.WorkflowStateRunning,
		Version: 2,
	}

	storage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), mock.MatchedBy(func(v int64) bool {
		return v == 2 || v == 0
	})).Return(nil)

	ctx := context.Background()
	err := sm.SaveWorkflowState(ctx, workflow)
	assert.NoError(t, err)

	err = sm.Stop()
	assert.NoError(t, err)

	storage.AssertExpectations(t)
}
