package core

import (
	"testing"

	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/readiness"
)

func TestGetClusterInfoMarksNodeAsLeaderWithoutStateCheck(t *testing.T) {
	t.Parallel()

	mockRaft := mocks.NewMockRaftNode(t)
	mockRaft.On("GetClusterInfo").Return(ports.ClusterInfo{
		NodeID: "node-1",
		Leader: &ports.RaftNodeInfo{
			ID:      "node-1",
			Address: "127.0.0.1:9000",
			State:   ports.NodeFollower,
		},
		Members: []ports.RaftNodeInfo{
			{ID: "node-1", Address: "127.0.0.1:9000", State: ports.NodeFollower},
		},
	})
	mockRaft.On("GetLeadershipInfo").Return(ports.RaftLeadershipInfo{
		State:    ports.RaftLeadershipFollower,
		LeaderID: "node-1",
	})

	mgr := &Manager{
		nodeID:      "node-1",
		raftAdapter: mockRaft,
	}

	info := mgr.GetClusterInfo()
	if info.IsLeader {
		t.Fatalf("expected is_leader to be false when leadership state is not leader")
	}
}

func TestGetHealthUnhealthyWhenNoLeader(t *testing.T) {
	t.Parallel()

	mockRaft := mocks.NewMockRaftNode(t)
	mockRaft.On("GetHealth").Return(ports.HealthStatus{Healthy: true})
	mockRaft.On("GetClusterInfo").Return(ports.ClusterInfo{})
	mockRaft.On("GetLeadershipInfo").Return(ports.RaftLeadershipInfo{
		State:    ports.RaftLeadershipFollower,
		LeaderID: "",
	})
	mockRaft.On("GetBootMetadata").Return("boot-id", int64(1)).Maybe()
	mockRaft.On("IsProvisional").Return(false).Maybe()

	engine := mocks.NewMockEnginePort(t)

	mgr := &Manager{
		nodeID:           "node-1",
		raftAdapter:      mockRaft,
		engine:           engine,
		readinessManager: readiness.NewManager(),
		workflowIntakeOk: true,
	}

	health := mgr.GetHealth()
	if health.Healthy {
		t.Fatalf("expected health to be unhealthy when no leader is present")
	}
}
