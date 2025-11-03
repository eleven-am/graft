package core

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/readiness"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestPrepareBootstrapPlan_UsesRecoveredPlanWhenPersistedState(t *testing.T) {
	m := &Manager{
		nodeID:                "node-1",
		logger:                testLogger(),
		hasPersistedRaftState: true,
	}

	existingPeers := []ports.Peer{{ID: "node-2"}}
	plan, err := m.prepareBootstrapPlan(context.Background(), time.Second, existingPeers)
	require.NoError(t, err)
	require.Equal(t, "node-1", plan.CandidateID)
	require.False(t, plan.BootstrapMultiNode)
	require.False(t, plan.JoinRequired())
	require.Empty(t, plan.JoinTargets)
	require.Equal(t, readiness.StateDetecting, plan.InitialReadinessState)
	require.False(t, plan.AwaitLeadership)
}

func TestPrepareBootstrapPlan_RecoveredPlanAwaitsLeadershipWithNoPeers(t *testing.T) {
	m := &Manager{
		nodeID:                "solo",
		logger:                testLogger(),
		hasPersistedRaftState: true,
	}

	plan, err := m.prepareBootstrapPlan(context.Background(), time.Second, nil)
	require.NoError(t, err)
	require.Equal(t, "solo", plan.CandidateID)
	require.True(t, plan.AwaitLeadership)
	require.Equal(t, readiness.StateProvisional, plan.InitialReadinessState)
	require.False(t, plan.JoinRequired())
}

func TestNewFollowerBootstrapPlan_JoinRequired(t *testing.T) {
	peers := []ports.Peer{{ID: "node-2"}}
	joinTargets := []ports.Peer{{ID: "node-1"}}
	plan := newFollowerBootstrapPlan("node-1", peers, joinTargets)
	require.True(t, plan.JoinRequired())
	require.Equal(t, readiness.StateDetecting, plan.InitialReadinessState)
	require.False(t, plan.AwaitLeadership)
}
