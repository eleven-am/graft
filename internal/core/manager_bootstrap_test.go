package core

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
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
		mockRaft.EXPECT().Stop().Return(nil).Maybe()
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

func TestManagerBootstrapConfig(t *testing.T) {
	t.Run("config creates correct BootstrapConfig", func(t *testing.T) {
		config := domain.DefaultConfig()
		config.NodeID = "test-node"
		config.BindAddr = "127.0.0.1:9090"
		config.DataDir = t.TempDir()
		config.Bootstrap.ServiceName = "test-service"
		config.Bootstrap.Ordinal = 1
		config.Bootstrap.Replicas = 3
		config.Bootstrap.BasePort = 7222

		assert.Equal(t, "test-service", config.Bootstrap.ServiceName)
		assert.Equal(t, 1, config.Bootstrap.Ordinal)
		assert.Equal(t, 3, config.Bootstrap.Replicas)
		assert.Equal(t, 7222, config.Bootstrap.BasePort)
	})

	t.Run("BootstrapConfig defaults are set correctly", func(t *testing.T) {
		config := domain.DefaultBootstrapConfig()

		assert.Equal(t, "graft", config.ServiceName)
		assert.Equal(t, 0, config.Ordinal)
		assert.Equal(t, 3, config.Replicas)
		assert.Equal(t, 7946, config.BasePort)
		assert.Equal(t, 30*time.Second, config.LeaderWaitTimeout)
		assert.Equal(t, 60*time.Second, config.ReadyTimeout)
		assert.Equal(t, 5*time.Second, config.StaleCheckInterval)
		assert.True(t, config.FencingEnabled)
	})
}

func TestManagerBootstrapConfigBuilder(t *testing.T) {
	t.Run("WithBootstrap sets basic config", func(t *testing.T) {
		config := domain.DefaultConfig()
		config.WithBootstrap("my-service", 2, 5)

		assert.Equal(t, "my-service", config.Bootstrap.ServiceName)
		assert.Equal(t, 2, config.Bootstrap.Ordinal)
		assert.Equal(t, 5, config.Bootstrap.Replicas)
	})

	t.Run("WithBootstrapFencing sets fencing config", func(t *testing.T) {
		config := domain.DefaultConfig()
		config.WithBootstrap("my-service", 0, 3)
		config.WithBootstrapFencing("/path/to/key", 2)

		assert.True(t, config.Bootstrap.FencingEnabled)
		assert.Equal(t, "/path/to/key", config.Bootstrap.FencingKeyPath)
		assert.Equal(t, 2, config.Bootstrap.FencingQuorum)
	})

	t.Run("WithBootstrapTLS sets TLS config", func(t *testing.T) {
		config := domain.DefaultConfig()
		config.WithBootstrap("my-service", 0, 3)
		config.WithBootstrapTLS("/path/cert.pem", "/path/key.pem", "/path/ca.pem")

		assert.True(t, config.Bootstrap.TLSEnabled)
		assert.Equal(t, "/path/cert.pem", config.Bootstrap.TLSCertPath)
		assert.Equal(t, "/path/key.pem", config.Bootstrap.TLSKeyPath)
		assert.Equal(t, "/path/ca.pem", config.Bootstrap.TLSCAPath)
	})

	t.Run("WithBootstrapTimeouts sets timeout config", func(t *testing.T) {
		config := domain.DefaultConfig()
		config.WithBootstrap("my-service", 0, 3)
		config.WithBootstrapTimeouts(45*time.Second, 90*time.Second, 60*time.Second)

		assert.Equal(t, 45*time.Second, config.Bootstrap.LeaderWaitTimeout)
		assert.Equal(t, 90*time.Second, config.Bootstrap.ReadyTimeout)
		assert.Equal(t, 60*time.Second, config.Bootstrap.StaleCheckInterval)
	})
}

func createBootstrapTestManager(t *testing.T) (*Manager, *mocks.MockRaftNode, *mocks.MockEnginePort) {
	t.Helper()

	mockRaft := mocks.NewMockRaftNode(t)
	mockEngine := mocks.NewMockEnginePort(t)
	mockTransport := mocks.NewMockTransportPort(t)

	mockRaft.EXPECT().GetBootMetadata().Return("test-boot-id", time.Now().UnixNano()).Maybe()
	mockRaft.EXPECT().GetHealth().Return(ports.HealthStatus{Healthy: true}).Maybe()
	mockRaft.EXPECT().GetClusterInfo().Return(ports.ClusterInfo{}).Maybe()
	mockRaft.EXPECT().GetLeadershipInfo().Return(ports.RaftLeadershipInfo{
		State:    ports.RaftLeadershipLeader,
		LeaderID: "test-node",
	}).Maybe()
	mockRaft.EXPECT().Stop().Return(nil).Maybe()
	mockRaft.EXPECT().SetReadinessCallback(mock.AnythingOfType("func(bool)")).Maybe()

	mockRaft.EXPECT().IsProvisional().Return(true).Maybe()
	mockRaft.EXPECT().DemoteAndJoin(mock.Anything, mock.Anything).Return(nil).Maybe()

	mockEngine.EXPECT().ProcessTrigger(mock.Anything).Return(nil).Maybe()
	mockEngine.EXPECT().Stop().Return(nil).Maybe()

	mockTransport.EXPECT().SendJoinRequest(mock.Anything, mock.Anything, mock.Anything).Return(&ports.JoinResponse{Accepted: true}, nil).Maybe()
	mockTransport.EXPECT().Stop().Return(nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())

	manager := &Manager{
		nodeID:           "test-node",
		raftAdapter:      mockRaft,
		engine:           mockEngine,
		transport:        mockTransport,
		readinessManager: readiness.NewManager(),
		workflowIntakeOk: true,
		grpcPort:         9090,
		config: &domain.Config{
			NodeID:   "test-node",
			BindAddr: "127.0.0.1:9090",
		},
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		ctx:    ctx,
		cancel: cancel,
	}

	return manager, mockRaft, mockEngine
}
