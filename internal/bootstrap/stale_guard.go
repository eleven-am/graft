package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type StaleGuardDeps struct {
	Config           *BootstrapConfig
	MetaStore        MetaStore
	FencingManager   *FencingManager
	UnfenceTracker   *UnfenceTracker
	Prober           PeerProber
	RaftConfigFunc   func() ([]VoterInfo, error)
	ExpectedPeersFunc func() []VoterInfo
	Logger           *slog.Logger
	Clock            func() time.Time
}

type StaleSingleNodeGuard struct {
	config            *BootstrapConfig
	metaStore         MetaStore
	fencingManager    *FencingManager
	unfenceTracker    *UnfenceTracker
	prober            PeerProber
	raftConfigFunc    func() ([]VoterInfo, error)
	expectedPeersFunc func() []VoterInfo
	logger            *slog.Logger
	clock             func() time.Time
}

func NewStaleSingleNodeGuard(deps StaleGuardDeps) *StaleSingleNodeGuard {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	return &StaleSingleNodeGuard{
		config:            deps.Config,
		metaStore:         deps.MetaStore,
		fencingManager:    deps.FencingManager,
		unfenceTracker:    deps.UnfenceTracker,
		prober:            deps.Prober,
		raftConfigFunc:    deps.RaftConfigFunc,
		expectedPeersFunc: deps.ExpectedPeersFunc,
		logger:            logger,
		clock:             clock,
	}
}

func (g *StaleSingleNodeGuard) Check(ctx context.Context) (StaleStatus, *StaleCheckDetails, error) {
	meta, err := g.metaStore.LoadMeta()
	if err != nil {
		if errors.Is(err, ErrMetaNotFound) {
			g.logger.Debug("stale check: safe (no metadata, first boot)")
			return StaleStatusSafe, &StaleCheckDetails{
				Reason:    "no cluster metadata (first boot)",
				CheckTime: g.clock(),
			}, nil
		}
		return StaleStatusUnknown, nil, fmt.Errorf("load cluster meta: %w", err)
	}

	voters, err := g.raftConfigFunc()
	if err != nil {
		return StaleStatusUnknown, nil, fmt.Errorf("get raft config: %w", err)
	}

	persistedVoters := len(voters)
	expectedVoters := g.getExpectedVoters()

	details := &StaleCheckDetails{
		PersistedVoters:   persistedVoters,
		ExpectedVoters:    expectedVoters,
		LocalFencingEpoch: g.fencingManager.CurrentEpoch(),
		LocalClusterUUID:  meta.ClusterUUID,
		BootstrapTime:     meta.BootstrapTime,
		CheckTime:         g.clock(),
	}

	if persistedVoters != 1 || expectedVoters <= 1 {
		details.Reason = "not a single-node-multi-expected scenario"
		g.logger.Debug("stale check: safe (not single-node)",
			"persisted_voters", persistedVoters,
			"expected_voters", expectedVoters,
		)
		return StaleStatusSafe, details, nil
	}

	expectedPeers := g.getExpectedPeers(voters)
	probeResults := g.prober.ProbeAll(ctx, expectedPeers)
	details.PeerResults = probeResults
	details.ReachablePeers = details.CountReachable()

	if details.ReachablePeers == 0 {
		return g.handleNoPeersReachable(details)
	}

	return g.handlePeersReachable(details)
}

func (g *StaleSingleNodeGuard) handleNoPeersReachable(details *StaleCheckDetails) (StaleStatus, *StaleCheckDetails, error) {
	timeSinceBootstrap := g.clock().Sub(details.BootstrapTime)

	if timeSinceBootstrap < DefaultSafetyWindowDuration {
		details.Reason = fmt.Sprintf("within safety window (%v since bootstrap)", timeSinceBootstrap.Round(time.Second))
		g.logger.Debug("stale check: safe (within safety window)",
			"time_since_bootstrap", timeSinceBootstrap,
			"safety_window", DefaultSafetyWindowDuration,
		)
		return StaleStatusSafe, details, nil
	}

	if g.unfenceTracker.CanAutoUnfence() {
		details.Reason = "sustained isolation detected, safe to unfence"
		g.logger.Info("stale check: safe to unfence (sustained isolation)",
			"probes_in_window", g.unfenceTracker.ProbesInWindow(),
		)
		return StaleStatusSafeToUnfence, details, nil
	}

	details.Reason = fmt.Sprintf("no peers reachable, awaiting sufficient probes (%d/%d)",
		g.unfenceTracker.ProbesInWindow(), DefaultUnfenceQuorumThreshold)
	g.logger.Debug("stale check: questionable (awaiting probes)",
		"probes_in_window", g.unfenceTracker.ProbesInWindow(),
		"threshold", DefaultUnfenceQuorumThreshold,
	)
	return StaleStatusQuestionable, details, nil
}

func (g *StaleSingleNodeGuard) handlePeersReachable(details *StaleCheckDetails) (StaleStatus, *StaleCheckDetails, error) {
	if hasSplitBrain, peer := details.HasSplitBrain(); hasSplitBrain {
		details.Reason = fmt.Sprintf("cluster UUID mismatch with peer %s (local=%s, peer=%s)",
			peer.ServerID, details.LocalClusterUUID, peer.ClusterUUID)
		g.logger.Error("stale check: split-brain detected",
			"local_uuid", details.LocalClusterUUID,
			"peer_uuid", peer.ClusterUUID,
			"peer_id", peer.ServerID,
		)
		return StaleStatusSplitBrain, details, nil
	}

	if hasHigherEpoch, peer := details.HasHigherEpoch(); hasHigherEpoch {
		details.Reason = fmt.Sprintf("peer %s has higher fencing epoch (local=%d, peer=%d)",
			peer.ServerID, details.LocalFencingEpoch, peer.FencingEpoch)
		g.logger.Warn("stale check: stale (higher epoch detected)",
			"local_epoch", details.LocalFencingEpoch,
			"peer_epoch", peer.FencingEpoch,
			"peer_id", peer.ServerID,
		)
		return StaleStatusStale, details, nil
	}

	quorum := (details.ExpectedVoters / 2) + 1
	if details.ReachablePeers+1 >= quorum {
		details.Reason = fmt.Sprintf("quorum reachable (reachable+self=%d, quorum=%d)",
			details.ReachablePeers+1, quorum)
		g.logger.Debug("stale check: safe (quorum reachable)",
			"reachable", details.ReachablePeers,
			"quorum", quorum,
		)
		return StaleStatusSafe, details, nil
	}

	if details.ReachablePeers >= details.ExpectedVoters-1 {
		details.Reason = fmt.Sprintf("all other nodes reachable but no quorum (likely we are stale)")
		g.logger.Warn("stale check: stale (complete isolation from cluster)",
			"reachable", details.ReachablePeers,
			"expected", details.ExpectedVoters,
		)
		return StaleStatusStale, details, nil
	}

	details.Reason = fmt.Sprintf("partial reachability (reachable=%d, expected=%d)",
		details.ReachablePeers, details.ExpectedVoters)
	g.logger.Debug("stale check: questionable (partial reachability)",
		"reachable", details.ReachablePeers,
		"expected", details.ExpectedVoters,
	)
	return StaleStatusQuestionable, details, nil
}

func (g *StaleSingleNodeGuard) getExpectedVoters() int {
	if g.config != nil && g.config.ExpectedNodes > 0 {
		return g.config.ExpectedNodes
	}
	return 1
}

func (g *StaleSingleNodeGuard) getExpectedPeers(currentVoters []VoterInfo) []VoterInfo {
	if g.config == nil || g.config.ExpectedNodes <= 1 {
		return nil
	}

	meta, err := g.metaStore.LoadMeta()
	if err != nil {
		return nil
	}

	if g.expectedPeersFunc != nil {
		allPeers := g.expectedPeersFunc()
		var peers []VoterInfo
		for _, v := range allPeers {
			if v.ServerID != meta.ServerID {
				peers = append(peers, v)
			}
		}
		if len(peers) > 0 {
			return peers
		}
	}

	var peers []VoterInfo
	for _, v := range currentVoters {
		if v.ServerID != meta.ServerID {
			peers = append(peers, v)
		}
	}

	return peers
}
