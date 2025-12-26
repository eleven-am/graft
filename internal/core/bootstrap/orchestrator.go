package bootstrap

import (
	"context"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/readiness"
)

// Orchestrator coordinates bootstrap (discovery + raft start) and readiness wiring.
// It wraps the existing Coordinator and delegates to Manager for side-effects.
type Orchestrator struct {
	Coordinator      *Coordinator
	Raft             ports.RaftNode
	Logger           *slog.Logger
	NodeID           string
	DiscoveryTimeout func() time.Duration

	// callbacks injected from manager
	SetReadiness func(state readiness.State)
	PauseIntake  func()
	ResumeIntake func()
}

// Start runs discovery/bootstrap and wires readiness based on raft callbacks.
func (o *Orchestrator) Start(ctx context.Context, grpcPort int) (*Result, error) {
	if o.Coordinator == nil || o.Raft == nil {
		return nil, nil
	}

	bootstrapResult, err := o.Coordinator.Start(ctx, grpcPort, o.DiscoveryTimeout)
	if err != nil {
		return nil, err
	}

	o.Raft.SetReadinessCallback(func(ready bool) {
		leader := o.Raft.GetLeadershipInfo()
		h := o.Raft.GetHealth()
		effectiveReady := ready && h.Healthy && leader.LeaderID != ""

		if effectiveReady {
			if o.Logger != nil {
				o.Logger.Info("raft reported ready - transitioning to ready state")
			}
			if o.SetReadiness != nil {
				o.SetReadiness(readiness.StateReady)
			}
			if o.ResumeIntake != nil {
				o.ResumeIntake()
			}
		} else {
			if o.Logger != nil {
				o.Logger.Info("raft not ready (health/leader missing) - pausing workflow intake",
					"raft_ready", ready,
					"health", h.Healthy,
					"leader_present", leader.LeaderID != "")
			}
			if o.SetReadiness != nil {
				o.SetReadiness(readiness.StateDetecting)
			}
			if o.PauseIntake != nil {
				o.PauseIntake()
			}
		}
	})

	if o.PauseIntake != nil {
		o.PauseIntake()
	}
	if o.SetReadiness != nil {
		o.SetReadiness(readiness.StateDetecting)
	}

	go func() {
		leaderCtx, cancel := context.WithTimeout(ctx, o.DiscoveryTimeout())
		defer cancel()
		if err := o.Raft.WaitForLeader(leaderCtx); err != nil {
			if o.Logger != nil {
				o.Logger.Warn("leader not established during bootstrap", "error", err)
			}
			if o.SetReadiness != nil {
				o.SetReadiness(readiness.StateDetecting)
			}
			if o.PauseIntake != nil {
				o.PauseIntake()
			}
		}
	}()

	return bootstrapResult, nil
}

// Stop is a no-op placeholder for API completeness.
func (o *Orchestrator) Stop() {}
