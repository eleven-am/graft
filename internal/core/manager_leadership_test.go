package core

import (
	"testing"

	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
)

func TestGetClusterInfoMarksNodeAsLeaderWithoutStateCheck(t *testing.T) {
	t.Parallel()

	mockRaft := mocks.NewMockRaftNode(t)
	mockRaft.On("GetClusterInfo").Return(ports.ClusterInfo{
		NodeID: "node-1",
		Leader: &ports.RaftNodeInfo{
			ID:      "node-1",
			Address: "127.0.0.1:9000",
			State:   ports.NodeFollower, // represents a provisional/follower view despite matching ID.
		},
		Members: []ports.RaftNodeInfo{
			{ID: "node-1", Address: "127.0.0.1:9000", State: ports.NodeFollower},
		},
	})
	mockRaft.On("GetLeadershipInfo").Return(ports.RaftLeadershipInfo{State: ports.RaftLeadershipFollower})

	mgr := &Manager{
		nodeID:      "node-1",
		raftAdapter: mockRaft,
	}

	info := mgr.GetClusterInfo()
	if info.IsLeader {
		t.Fatalf("expected is_leader to be false when leadership state is not leader")
	}
}
