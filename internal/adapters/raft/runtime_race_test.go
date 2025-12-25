package raft

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
)

func TestRuntimeLeaderObservationNotOverwrittenByInitialSnapshot(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := NewRuntime(RuntimeDeps{})
	rt.options = domain.RaftControllerOptions{NodeID: "node-1"}
	rt.observerCh = make(chan raft.Observation, 1)
	rt.runDone = make(chan struct{})

	go rt.observe(ctx)

	// Simulate a fast LeaderObservation arriving before the initial snapshot logic completes.
	rt.observerCh <- raft.Observation{
		Data: raft.LeaderObservation{
			LeaderID:   raft.ServerID("node-1"),
			LeaderAddr: raft.ServerAddress("127.0.0.1:9000"),
		},
	}

	waitForLeadershipState(t, rt, ports.RaftLeadershipLeader)

	// Mirror the initialization snapshot that currently runs after observe().
	rt.updateLeadership(ports.RaftLeadershipInfo{
		State:         ports.RaftLeadershipProvisional,
		LeaderID:      "node-1",
		LeaderAddress: "127.0.0.1:9000",
		Term:          1,
	})

	if got := rt.LeadershipInfo().State; got != ports.RaftLeadershipLeader {
		t.Fatalf("expected leader observation to persist, got state %s", got)
	}
}

func waitForLeadershipState(t *testing.T, rt *Runtime, expected ports.RaftLeadershipState) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rt.LeadershipInfo().State == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leadership state %s, last state %s", expected, rt.LeadershipInfo().State)
}

func TestUpdateLeadershipBlocksLeaderToProvisionalButAllowsFollower(t *testing.T) {
	t.Parallel()

	rt := NewRuntime(RuntimeDeps{})
	rt.leadership = ports.RaftLeadershipInfo{State: ports.RaftLeadershipLeader}

	rt.updateLeadership(ports.RaftLeadershipInfo{State: ports.RaftLeadershipProvisional})
	if rt.LeadershipInfo().State != ports.RaftLeadershipLeader {
		t.Fatalf("expected leader state to persist when downgraded to provisional; got %s", rt.LeadershipInfo().State)
	}

	rt.updateLeadership(ports.RaftLeadershipInfo{State: ports.RaftLeadershipFollower})
	if rt.LeadershipInfo().State != ports.RaftLeadershipFollower {
		t.Fatalf("expected follower state to be applied; got %s", rt.LeadershipInfo().State)
	}
}

func TestMapLeadershipStateToNodeState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in       ports.RaftLeadershipState
		expected ports.NodeState
	}{
		{ports.RaftLeadershipLeader, ports.NodeLeader},
		{ports.RaftLeadershipFollower, ports.NodeFollower},
		{ports.RaftLeadershipJoining, ports.NodeCandidate},
		{ports.RaftLeadershipProvisional, ports.NodeFollower},
		{ports.RaftLeadershipUnknown, ports.NodeFollower},
	}

	for _, tc := range cases {
		if got := mapLeadershipStateToNodeState(tc.in); got != tc.expected {
			t.Fatalf("state %s mapped to %v, expected %v", tc.in, got, tc.expected)
		}
	}
}
