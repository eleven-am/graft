package engine

import (
	"context"
	"github.com/eleven-am/graft/internal/adapters/raftimpl"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"log/slog"
	"os"
)

type MockStoragePort struct {
	mock.Mock
}

func (m *MockStoragePort) Get(ctx context.Context, key string) ([]byte, error) {
	args := m.Called(ctx, key)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockStoragePort) Put(ctx context.Context, key string, value []byte) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

func (m *MockStoragePort) Delete(ctx context.Context, key string) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

func (m *MockStoragePort) List(ctx context.Context, prefix string) ([]ports.KeyValue, error) {
	args := m.Called(ctx, prefix)
	return args.Get(0).([]ports.KeyValue), args.Error(1)
}

func (m *MockStoragePort) Batch(ctx context.Context, ops []ports.Operation) error {
	args := m.Called(ctx, ops)
	return args.Error(0)
}

type MockQueuePort struct {
	mock.Mock
}

func (m *MockQueuePort) EnqueueReady(ctx context.Context, item ports.QueueItem) error {
	args := m.Called(ctx, item)
	return args.Error(0)
}

func (m *MockQueuePort) DequeueReady(ctx context.Context, opts ...ports.DequeueOption) (*ports.QueueItem, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(*ports.QueueItem), args.Error(1)
}

func (m *MockQueuePort) EnqueuePending(ctx context.Context, item ports.QueueItem) error {
	args := m.Called(ctx, item)
	return args.Error(0)
}

func (m *MockQueuePort) GetPendingItems(ctx context.Context) ([]ports.QueueItem, error) {
	args := m.Called(ctx)
	return args.Get(0).([]ports.QueueItem), args.Error(1)
}

func (m *MockQueuePort) IsEmpty(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}

func (m *MockQueuePort) MovePendingToReady(ctx context.Context, itemID string) error {
	args := m.Called(ctx, itemID)
	return args.Error(0)
}

func (m *MockQueuePort) RemoveFromPending(ctx context.Context, itemID string) error {
	args := m.Called(ctx, itemID)
	return args.Error(0)
}

func (m *MockQueuePort) VerifyWorkClaim(ctx context.Context, workItemID string, nodeID string) error {
	args := m.Called(ctx, workItemID, nodeID)
	return args.Error(0)
}

func (m *MockQueuePort) ReleaseWorkClaim(ctx context.Context, workItemID string, nodeID string) error {
	args := m.Called(ctx, workItemID, nodeID)
	return args.Error(0)
}

type MockQueueCleaner struct {
	mock.Mock
}

func (m *MockQueueCleaner) RemoveWorkflowItems(ctx context.Context, workflowID string) error {
	args := m.Called(ctx, workflowID)
	return args.Error(0)
}

func (m *MockQueueCleaner) RemovePendingItems(ctx context.Context, workflowID string) error {
	args := m.Called(ctx, workflowID)
	return args.Error(0)
}

func (m *MockQueueCleaner) ReleaseWorkflowClaims(ctx context.Context, workflowID string) error {
	args := m.Called(ctx, workflowID)
	return args.Error(0)
}

func (m *MockQueueCleaner) GetWorkflowItemCount(ctx context.Context, workflowID string) int {
	args := m.Called(ctx, workflowID)
	return args.Int(0)
}

func (m *MockQueueCleaner) RemoveAllWorkflowData(ctx context.Context, workflowID string) error {
	args := m.Called(ctx, workflowID)
	return args.Error(0)
}

func (m *MockQueueCleaner) BatchRemoveWorkflows(ctx context.Context, workflowIDs []string) error {
	args := m.Called(ctx, workflowIDs)
	return args.Error(0)
}

func (m *MockQueueCleaner) GetOrphanedItems(ctx context.Context) ([]ports.QueueItem, error) {
	args := m.Called(ctx)
	return args.Get(0).([]ports.QueueItem), args.Error(1)
}

func TestCleanupOrchestrator_CleanupWorkflow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockStorage := new(MockStoragePort)
	mockQueue := new(MockQueuePort)
	mockCleaner := new(MockQueueCleaner)

	orchestrator := NewCleanupOrchestrator(mockStorage, mockQueue, mockCleaner, nil, logger)

	workflowID := "test-workflow-123"
	options := CleanupOptions{
		PreserveState:   false,
		PreserveAudit:   true,
		RetentionPeriod: time.Hour * 24,
		Force:           true,
	}

	mockCleaner.On("RemoveAllWorkflowData", mock.Anything, workflowID).Return(nil)
	mockStorage.On("Batch", mock.Anything, mock.Anything).Return(nil)

	err := orchestrator.CleanupWorkflow(context.Background(), workflowID, options)
	assert.NoError(t, err)

	mockCleaner.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}

func TestCleanupOrchestrator_CleanupBatch(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockStorage := new(MockStoragePort)
	mockQueue := new(MockQueuePort)
	mockCleaner := new(MockQueueCleaner)

	orchestrator := NewCleanupOrchestrator(mockStorage, mockQueue, mockCleaner, nil, logger)

	workflowIDs := []string{"workflow-1", "workflow-2", "workflow-3"}
	options := CleanupOptions{
		PreserveState:   false,
		PreserveAudit:   true,
		RetentionPeriod: time.Hour * 24,
		Force:           true,
	}

	for _, workflowID := range workflowIDs {
		mockCleaner.On("RemoveAllWorkflowData", mock.Anything, workflowID).Return(nil)
	}
	mockStorage.On("Batch", mock.Anything, mock.Anything).Return(nil).Times(len(workflowIDs) * 2)

	err := orchestrator.CleanupBatch(context.Background(), workflowIDs, options)
	assert.NoError(t, err)

	mockCleaner.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}

func TestCleanupOrchestrator_ScheduleCleanup(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockStorage := new(MockStoragePort)
	mockQueue := new(MockQueuePort)
	mockCleaner := new(MockQueueCleaner)

	orchestrator := NewCleanupOrchestrator(mockStorage, mockQueue, mockCleaner, nil, logger)

	workflowID := "test-workflow-scheduled"
	options := CleanupOptions{
		PreserveState:   false,
		PreserveAudit:   true,
		RetentionPeriod: time.Hour * 24,
		Force:           true,
	}

	err := orchestrator.ScheduleCleanup(workflowID, time.Millisecond*100, options)
	assert.NoError(t, err)

	mockCleaner.On("RemoveAllWorkflowData", mock.Anything, workflowID).Return(nil)
	mockStorage.On("Batch", mock.Anything, mock.Anything).Return(nil)

	time.Sleep(time.Millisecond * 200)

	mockCleaner.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}

func TestCleanupOrchestrator_ValidateCleanupSafety(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockStorage := new(MockStoragePort)
	mockQueue := new(MockQueuePort)
	mockCleaner := new(MockQueueCleaner)

	orchestrator := NewCleanupOrchestrator(mockStorage, mockQueue, mockCleaner, nil, logger)

	workflowID := "test-workflow-safety"

	t.Run("Running workflow should fail safety check", func(t *testing.T) {
		workflowData := `{"status": "running", "id": "` + workflowID + `"}`
		mockStorage.On("Get", mock.Anything, "workflow:state:"+workflowID).Return([]byte(workflowData), nil)

		err := orchestrator.validateCleanupSafety(context.Background(), workflowID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot cleanup active workflow")

		mockStorage.AssertExpectations(t)
	})

	t.Run("Completed workflow should pass safety check", func(t *testing.T) {
		mockStorage.ExpectedCalls = nil
		mockCleaner.ExpectedCalls = nil

		workflowData := `{"status": "completed", "id": "` + workflowID + `"}`
		mockStorage.On("Get", mock.Anything, "workflow:state:"+workflowID).Return([]byte(workflowData), nil)
		mockCleaner.On("GetWorkflowItemCount", mock.Anything, workflowID).Return(0)

		err := orchestrator.validateCleanupSafety(context.Background(), workflowID)
		assert.NoError(t, err)

		mockStorage.AssertExpectations(t)
		mockCleaner.AssertExpectations(t)
	})
}

func TestCleanupOrchestrator_BuildOperations(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	mockStorage := new(MockStoragePort)
	mockQueue := new(MockQueuePort)
	mockCleaner := new(MockQueueCleaner)

	orchestrator := NewCleanupOrchestrator(mockStorage, mockQueue, mockCleaner, nil, logger)

	t.Run("Default cleanup operations", func(t *testing.T) {
		options := CleanupOptions{
			PreserveState: false,
			PreserveAudit: false,
		}

		operations := orchestrator.buildCleanupOperations(options)
		assert.Len(t, operations, 4)

		expectedTargets := map[string]bool{"state": false, "queue": false, "claims": false, "audit": false}
		for _, op := range operations {
			expectedTargets[op.Target] = true
			assert.Equal(t, "delete", op.Action)
		}

		for target, found := range expectedTargets {
			assert.True(t, found, "Missing operation for target: %s", target)
		}
	})

	t.Run("Archive operations with location", func(t *testing.T) {
		options := CleanupOptions{
			PreserveState:   false,
			PreserveAudit:   false,
			ArchiveLocation: "s3://my-bucket/archives",
		}

		operations := orchestrator.buildCleanupOperations(options)
		assert.Len(t, operations, 4)

		stateAndAuditArchived := 0
		for _, op := range operations {
			if op.Target == "state" || op.Target == "audit" {
				assert.Equal(t, "archive", op.Action)
				stateAndAuditArchived++
			} else {
				assert.Equal(t, "delete", op.Action)
			}
		}
		assert.Equal(t, 2, stateAndAuditArchived)
	})

	t.Run("Preserve state and audit", func(t *testing.T) {
		options := CleanupOptions{
			PreserveState: true,
			PreserveAudit: true,
		}

		operations := orchestrator.buildCleanupOperations(options)
		assert.Len(t, operations, 2)

		expectedTargets := map[string]bool{"queue": false, "claims": false}
		for _, op := range operations {
			expectedTargets[op.Target] = true
			assert.Equal(t, "delete", op.Action)
		}

		for target, found := range expectedTargets {
			assert.True(t, found, "Missing operation for target: %s", target)
		}
	})
}

func TestCleanupMetrics(t *testing.T) {
	tracker := NewCleanupMetricsTracker()

	t.Run("Record cleanup success", func(t *testing.T) {
		tracker.RecordCleanupStart()

		results := map[string]string{
			"state":  "deleted 5 items",
			"queue":  "deleted 10 items",
			"claims": "deleted 2 items",
		}

		tracker.RecordCleanupSuccess(time.Second*2, results)

		metrics := tracker.GetMetrics()
		assert.Equal(t, int64(1), metrics.TotalCleanups)
		assert.Equal(t, int64(1), metrics.SuccessfulCleanups)
		assert.Equal(t, int64(0), metrics.FailedCleanups)
		assert.Equal(t, int64(5), metrics.StateItemsDeleted)
		assert.Equal(t, int64(10), metrics.QueueItemsDeleted)
		assert.Equal(t, int64(2), metrics.ClaimsDeleted)
		assert.NotNil(t, metrics.LastCleanupTime)
	})

	t.Run("Record cleanup failure", func(t *testing.T) {
		tracker.RecordCleanupStart()
		tracker.RecordCleanupFailure(time.Second * 1)

		metrics := tracker.GetMetrics()
		assert.Equal(t, int64(2), metrics.TotalCleanups)
		assert.Equal(t, int64(1), metrics.SuccessfulCleanups)
		assert.Equal(t, int64(1), metrics.FailedCleanups)
	})

	t.Run("Reset metrics", func(t *testing.T) {
		tracker.Reset()

		metrics := tracker.GetMetrics()
		assert.Equal(t, int64(0), metrics.TotalCleanups)
		assert.Equal(t, int64(0), metrics.SuccessfulCleanups)
		assert.Equal(t, int64(0), metrics.FailedCleanups)
		assert.Nil(t, metrics.LastCleanupTime)
	})
}

func TestWorkflowCleanupCommand_Validation(t *testing.T) {
	t.Run("Valid command", func(t *testing.T) {
		operations := []raftimpl.CleanupOp{
			{Target: "state", Action: "delete"},
			{Target: "queue", Action: "delete"},
		}

		cmd := raftimpl.NewWorkflowCleanupCommand("test-workflow", operations)
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("Empty workflow ID", func(t *testing.T) {
		operations := []raftimpl.CleanupOp{
			{Target: "state", Action: "delete"},
		}

		cmd := raftimpl.NewWorkflowCleanupCommand("", operations)
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "workflow ID cannot be empty")
	})

	t.Run("Empty operations", func(t *testing.T) {
		cmd := raftimpl.NewWorkflowCleanupCommand("test-workflow", []raftimpl.CleanupOp{})
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cleanup operations cannot be empty")
	})

	t.Run("Invalid target", func(t *testing.T) {
		operations := []raftimpl.CleanupOp{
			{Target: "invalid", Action: "delete"},
		}

		cmd := raftimpl.NewWorkflowCleanupCommand("test-workflow", operations)
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cleanup target")
	})

	t.Run("Invalid action", func(t *testing.T) {
		operations := []raftimpl.CleanupOp{
			{Target: "state", Action: "invalid"},
		}

		cmd := raftimpl.NewWorkflowCleanupCommand("test-workflow", operations)
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cleanup action")
	})
}
