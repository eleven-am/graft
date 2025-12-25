package core

import (
	"testing"

	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/readiness"
)

func TestHealthHealthyWhenLeader(t *testing.T) {
	t.Parallel()
	mgr := newTestManagerWithRaft(t, ports.HealthStatus{Healthy: true}, ports.RaftLeadershipInfo{
		State:    ports.RaftLeadershipLeader,
		LeaderID: "node-1",
	})

	health := mgr.GetHealth()
	if !health.Healthy {
		t.Fatalf("expected healthy=true for leader, got false (error=%s)", health.Error)
	}
}

func TestHealthHealthyWhenFollowerWithLeader(t *testing.T) {
	t.Parallel()
	mgr := newTestManagerWithRaft(t, ports.HealthStatus{Healthy: true}, ports.RaftLeadershipInfo{
		State:    ports.RaftLeadershipFollower,
		LeaderID: "node-2",
	})

	health := mgr.GetHealth()
	if !health.Healthy {
		t.Fatalf("expected healthy=true for follower with leader present, got false (error=%s)", health.Error)
	}
}

func TestHealthUnhealthyWhenNoLeader(t *testing.T) {
	t.Parallel()
	mgr := newTestManagerWithRaft(t, ports.HealthStatus{Healthy: true}, ports.RaftLeadershipInfo{
		State:    ports.RaftLeadershipFollower,
		LeaderID: "",
	})

	health := mgr.GetHealth()
	if health.Healthy {
		t.Fatalf("expected healthy=false when no leader is present")
	}
}

func TestHealthUnhealthyWhenRaftUnhealthy(t *testing.T) {
	t.Parallel()
	mgr := newTestManagerWithRaft(t, ports.HealthStatus{Healthy: false, Error: "raft down"}, ports.RaftLeadershipInfo{
		State:    ports.RaftLeadershipLeader,
		LeaderID: "node-1",
	})

	health := mgr.GetHealth()
	if health.Healthy {
		t.Fatalf("expected healthy=false when raft is unhealthy")
	}
}

func TestHealthReadinessNotReadyWhenNoLeaderEvenIfReadinessManagerReady(t *testing.T) {
	t.Parallel()

	mgr := newTestManagerWithRaft(t, ports.HealthStatus{Healthy: true}, ports.RaftLeadershipInfo{
		State:    ports.RaftLeadershipFollower,
		LeaderID: "",
	})

	// Force readiness manager to Ready and intake to true to simulate a drifted state.
	mgr.readinessManager.SetState(readiness.StateReady)
	mgr.workflowIntakeOk = true

	health := mgr.GetHealth()
	if health.Healthy {
		t.Fatalf("expected healthy=false when no leader is present")
	}
	if readinessDetails, ok := health.Details["readiness"].(map[string]interface{}); ok {
		if ready, ok := readinessDetails["ready"].(bool); ok && ready {
			t.Fatalf("expected readiness.ready=false when no leader is present")
		}
		if intake, ok := readinessDetails["intake"].(bool); ok && intake {
			t.Fatalf("expected readiness.intake=false when no leader is present")
		}
	}
}

// ---------------------------------------------------------------------------
// Startup guard / persisted config validation (expected to fail until implemented)
// ---------------------------------------------------------------------------

// In-memory stub to simulate persisted raft config returned from GetClusterInfo.
type stubClusterInfo struct {
	ports.ClusterInfo
}

func TestStartupFailsOnStaleSingleNodeWhenPeersExpected(t *testing.T) {
	t.Parallel()

	mockRaft := mocks.NewMockRaftNode(t)
	// Single-node persisted config (only self).
	mockRaft.On("GetClusterInfo").Return(ports.ClusterInfo{
		NodeID: "node-1",
		Members: []ports.RaftNodeInfo{
			{ID: "node-1", Address: "node-1:9191"},
		},
	})
	mockRaft.On("GetLeadershipInfo").Return(ports.RaftLeadershipInfo{
		State:    ports.RaftLeadershipFollower,
		LeaderID: "",
	}).Maybe()
	mockRaft.On("GetHealth").Return(ports.HealthStatus{Healthy: true}).Maybe()
	mockRaft.On("GetBootMetadata").Return("boot-id", int64(1)).Maybe()
	mockRaft.On("IsProvisional").Return(false).Maybe()

	mockEngine := mocks.NewMockEnginePort(t)
	mockEngine.On("GetMetrics").Return(nil).Maybe()

	mgr := &Manager{
		nodeID:           "node-1",
		raftAdapter:      mockRaft,
		engine:           mockEngine,
		readinessManager: readiness.NewManager(),
		workflowIntakeOk: true,
		// Simulate expected static peers > 1 (e.g., from auto-generated peers or config).
		expectedStaticPeers: 2,
	}

	health := mgr.GetHealth()
	if health.Healthy {
		t.Fatalf("expected unhealthy when persisted config is single-node but peers are expected")
	}
	if health.Error == "" {
		t.Fatalf("expected an error describing stale single-node state")
	}
}

func newTestManagerWithRaft(t *testing.T, raftHealth ports.HealthStatus, leadership ports.RaftLeadershipInfo) *Manager {
	t.Helper()

	mockRaft := mocks.NewMockRaftNode(t)
	mockRaft.On("GetHealth").Return(raftHealth)
	mockRaft.On("GetClusterInfo").Return(ports.ClusterInfo{})
	mockRaft.On("GetLeadershipInfo").Return(leadership)
	mockRaft.On("GetBootMetadata").Return("boot-id", int64(1)).Maybe()
	mockRaft.On("IsProvisional").Return(false).Maybe()
	mockRaft.On("GetMetrics").Return(ports.SystemMetrics{}).Maybe()
	mockRaft.On("GetRaftStatus").Return(ports.RaftStatus{}).Maybe()

	mockEngine := mocks.NewMockEnginePort(t)
	mockEngine.On("GetMetrics").Return(nil).Maybe()

	return &Manager{
		nodeID:           "node-1",
		raftAdapter:      mockRaft,
		engine:           mockEngine,
		readinessManager: readiness.NewManager(),
		workflowIntakeOk: true,
	}
}
