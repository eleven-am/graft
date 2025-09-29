package core

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/helpers/metadata"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/readiness"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestManagerBootstrapHandoff(t *testing.T) {
	t.Run("provisional leader detects senior peer and demotes", func(t *testing.T) {
		manager, _, _ := createBootstrapTestManager(t)
		defer manager.Stop()

		olderTimestamp := time.Now().Add(-5 * time.Minute).UnixNano()
		seniorPeer := ports.Peer{
			ID:      "senior-node",
			Address: "127.0.0.1",
			Port:    8080,
			Metadata: map[string]string{
				metadata.BootIDKey:          "senior-boot-id",
				metadata.LaunchTimestampKey: time.Unix(0, olderTimestamp).Format(time.RFC3339Nano),
			},
		}

		manager.initiateDemotion(seniorPeer)

		assert.Equal(t, readiness.StateReady, manager.readinessManager.GetState())
		assert.True(t, manager.isWorkflowIntakeAllowed())
	})

	t.Run("non-provisional node skips demotion", func(t *testing.T) {
		manager, mockRaft, _ := createBootstrapTestManager(t)
		defer manager.Stop()

		manager.readinessManager.SetState(readiness.StateDetecting)

		assert.Equal(t, readiness.StateDetecting, manager.readinessManager.GetState())

		mockRaft.ExpectedCalls = nil
		mockRaft.EXPECT().IsProvisional().Return(false).Times(1)

		seniorPeer := ports.Peer{
			ID:      "senior-node",
			Address: "127.0.0.1",
			Port:    8080,
			Metadata: map[string]string{
				metadata.BootIDKey:          "senior-boot-id",
				metadata.LaunchTimestampKey: time.Now().Format(time.RFC3339Nano),
			},
		}

		manager.initiateDemotion(seniorPeer)

		assert.Equal(t, readiness.StateDetecting, manager.readinessManager.GetState())
	})

	t.Run("workflow intake blocked during demotion", func(t *testing.T) {
		manager, _, _ := createBootstrapTestManager(t)
		defer manager.Stop()

		manager.pauseWorkflowIntake()

		trigger := WorkflowTrigger{
			WorkflowID: "test-workflow",
		}

		err := manager.StartWorkflow(trigger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "workflow intake paused")

		manager.resumeWorkflowIntake()

		err = manager.StartWorkflow(trigger)
		assert.NoError(t, err)
	})

	t.Run("readiness state transitions", func(t *testing.T) {
		manager, _, _ := createBootstrapTestManager(t)
		defer manager.Stop()

		assert.Equal(t, readiness.StateProvisional, manager.readinessManager.GetState())
		assert.False(t, manager.IsReady())

		manager.readinessManager.SetState(readiness.StateDetecting)
		assert.Equal(t, readiness.StateDetecting, manager.readinessManager.GetState())
		assert.False(t, manager.IsReady())

		manager.readinessManager.SetState(readiness.StateReady)
		assert.Equal(t, readiness.StateReady, manager.readinessManager.GetState())
		assert.True(t, manager.IsReady())
	})

	t.Run("wait until ready functionality", func(t *testing.T) {
		manager, _, _ := createBootstrapTestManager(t)
		defer manager.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := manager.WaitUntilReady(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)

		manager.readinessManager.SetState(readiness.StateReady)

		ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel2()

		err = manager.WaitUntilReady(ctx2)
		assert.NoError(t, err)
	})

	t.Run("readiness callback logic from manager start", func(t *testing.T) {
		manager, _, _ := createBootstrapTestManager(t)
		defer manager.Stop()

		readinessCallback := func(ready bool) {
			if ready {
				manager.logger.Info("raft reported ready - transitioning to ready state")
				manager.readinessManager.SetState(readiness.StateReady)
				manager.resumeWorkflowIntake()
			} else {
				manager.logger.Info("raft reported not ready - pausing workflow intake")
				manager.pauseWorkflowIntake()
			}
		}

		manager.raftAdapter.SetReadinessCallback(readinessCallback)

		manager.readinessManager.SetState(readiness.StateProvisional)
		manager.resumeWorkflowIntake()

		assert.True(t, manager.isWorkflowIntakeAllowed())
		readinessCallback(false)
		assert.False(t, manager.isWorkflowIntakeAllowed())

		readinessCallback(true)
		assert.Equal(t, readiness.StateReady, manager.readinessManager.GetState())
		assert.True(t, manager.isWorkflowIntakeAllowed())
	})

	t.Run("raft readiness callback drives state and workflow intake", func(t *testing.T) {
		manager, _, _ := createBootstrapTestManager(t)
		defer manager.Stop()

		manager.readinessManager.SetState(readiness.StateProvisional)
		manager.resumeWorkflowIntake()

		callback := func(ready bool) {
			if ready {
				manager.readinessManager.SetState(readiness.StateReady)
				manager.resumeWorkflowIntake()
			} else {
				manager.pauseWorkflowIntake()
			}
		}

		assert.True(t, manager.isWorkflowIntakeAllowed())
		callback(false)
		assert.False(t, manager.isWorkflowIntakeAllowed())

		callback(true)
		assert.Equal(t, readiness.StateReady, manager.readinessManager.GetState())
		assert.True(t, manager.isWorkflowIntakeAllowed())
	})
}

func TestManagerHealth(t *testing.T) {
	manager, _, _ := createBootstrapTestManager(t)
	defer manager.Stop()

	health := manager.GetHealth()

	assert.True(t, health.Healthy)
	assert.Contains(t, health.Details, "readiness")
	assert.Contains(t, health.Details, "bootstrap")

	readinessDetails := health.Details["readiness"].(map[string]interface{})
	assert.Equal(t, "provisional", readinessDetails["state"])
	assert.False(t, readinessDetails["ready"].(bool))
	assert.True(t, readinessDetails["intake"].(bool))

	bootstrapDetails := health.Details["bootstrap"].(map[string]interface{})
	assert.True(t, bootstrapDetails["provisional"].(bool))
	assert.NotEmpty(t, bootstrapDetails["boot_id"])
	assert.Greater(t, bootstrapDetails["timestamp"], int64(0))
}

func createBootstrapTestManager(t *testing.T) (*Manager, *mocks.MockRaftNode, *mocks.MockEnginePort) {
	t.Helper()

	mockRaft := mocks.NewMockRaftNode(t)
	mockEngine := mocks.NewMockEnginePort(t)

	mockRaft.EXPECT().GetBootMetadata().Return("test-boot-id", time.Now().UnixNano()).Maybe()
	mockRaft.EXPECT().GetHealth().Return(ports.HealthStatus{Healthy: true}).Maybe()
	mockRaft.EXPECT().GetClusterInfo().Return(ports.ClusterInfo{}).Maybe()
	mockRaft.EXPECT().Stop().Return(nil).Maybe()
	mockRaft.EXPECT().SetReadinessCallback(mock.AnythingOfType("func(bool)")).Maybe()

	mockRaft.EXPECT().IsProvisional().Return(true).Maybe()
	mockRaft.EXPECT().DemoteAndJoin(mock.Anything, mock.Anything).Return(nil).Maybe()

	mockEngine.EXPECT().ProcessTrigger(mock.Anything).Return(nil).Maybe()
	mockEngine.EXPECT().Stop().Return(nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())

	manager := &Manager{
		nodeID:           "test-node",
		raftAdapter:      mockRaft,
		engine:           mockEngine,
		readinessManager: readiness.NewManager(),
		workflowIntakeOk: true,
		logger:           slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		ctx:              ctx,
		cancel:           cancel,
	}

	return manager, mockRaft, mockEngine
}
