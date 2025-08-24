package raft

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestClusterJoinLogic(t *testing.T) {
	leader := setupSingleNode(t, 0)
	ctx := context.Background()
	err := leader.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer cleanupAdapter(leader)

	waitForLeadership(leader)
	require.True(t, leader.IsLeader(), "Node-0 should be leader")

	follower := setupSingleNode(t, 1)
	err = follower.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer cleanupAdapter(follower)

	err = leader.AddNode("node-1", "127.0.0.1:19001")
	require.NoError(t, err, "Leader should be able to add follower")

	time.Sleep(1 * time.Second)

	clusterInfo := leader.GetClusterInfo()
	require.Len(t, clusterInfo.Members, 2, "Cluster should have 2 members")

	memberIDs := make([]string, len(clusterInfo.Members))
	for i, member := range clusterInfo.Members {
		memberIDs[i] = member.ID
	}
	require.Contains(t, memberIDs, "node-0")
	require.Contains(t, memberIDs, "node-1")

	require.True(t, leader.IsLeader(), "Node-0 should still be leader")
	require.False(t, follower.IsLeader(), "Node-1 should be follower")
}
