package core

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/readiness"
)

type bootstrapPlan struct {
	CandidateID           string
	ExistingPeers         []ports.Peer
	JoinTargets           []ports.Peer
	BootstrapMultiNode    bool
	AwaitLeadership       bool
	LeadershipRole        string
	InitialReadinessState readiness.State
}

func (p *bootstrapPlan) JoinRequired() bool {
	return !p.BootstrapMultiNode && len(p.JoinTargets) > 0
}

func (m *Manager) prepareBootstrapPlan(ctx context.Context, discoveryTimeout time.Duration, initialPeers []ports.Peer) (*bootstrapPlan, error) {
	existingPeers := initialPeers
	var candidateID string

	if m.hasPersistedRaftState {
		m.logger.Info("persisted raft state detected - using recovery bootstrap plan",
			"existing_peer_count", len(existingPeers))
		m.updateBootstrapMetadata(bootstrapStateCandidate, m.nodeID, false)
		readiness.LogPeerMetadata(existingPeers, m.logger)
		return newRecoveredBootstrapPlan(m.nodeID, existingPeers), nil
	}

	for {
		peersView := filterPeers(m.discovery.GetPeers(), m.nodeID)
		if len(peersView) == 0 && len(existingPeers) > 0 {
			peersView = existingPeers
		}

		candidateID = m.selectBootstrapCandidate(peersView, nil)
		if candidateID == "" {
			candidateID = m.nodeID
		}
		m.bootstrapCandidateID = candidateID

		m.logger.Info("bootstrap candidate evaluated",
			"candidate_id", candidateID,
			"peer_view_count", len(peersView),
			"existing_peer_count", len(existingPeers))

		if candidateID == m.nodeID {
			m.updateBootstrapMetadata(bootstrapStateCandidate, candidateID, false)
			if len(peersView) > 0 {
				existingPeers = peersView
			}
			readiness.LogPeerMetadata(existingPeers, m.logger)
			return newCandidateBootstrapPlan(m.nodeID, existingPeers), nil
		}

		m.updateBootstrapMetadata(bootstrapStateAwaiting, candidateID, false)

		currentCandidate, err := m.waitForBootstrapReady(ctx, candidateID, discoveryTimeout)
		if err == nil {
			if currentCandidate != "" && currentCandidate != candidateID {
				candidateID = currentCandidate
				m.bootstrapCandidateID = currentCandidate
			}
			refreshedPeers := filterPeers(m.discovery.GetPeers(), m.nodeID)
			if len(refreshedPeers) > 0 {
				existingPeers = refreshedPeers
			} else if len(peersView) > 0 {
				existingPeers = peersView
			}
			readiness.LogPeerMetadata(existingPeers, m.logger)
			return newFollowerBootstrapPlan(candidateID, existingPeers, m.joinTargets(existingPeers, candidateID)), nil
		}

		switch {
		case errors.Is(err, errBootstrapCandidateChanged):
			m.logger.Warn("bootstrap candidate changed while waiting",
				"previous_candidate", candidateID,
				"new_candidate", currentCandidate)
			if currentCandidate != "" && currentCandidate != candidateID {
				candidateID = currentCandidate
				m.bootstrapCandidateID = currentCandidate
			}
			continue
		case errors.Is(err, context.DeadlineExceeded):
			m.logger.Debug("bootstrap candidate timed out waiting for readiness",
				"candidate", candidateID,
				"timeout", discoveryTimeout)
			if len(peersView) > 0 {
				existingPeers = peersView
			}
			readiness.LogPeerMetadata(existingPeers, m.logger)
			return newFollowerBootstrapPlan(candidateID, existingPeers, m.joinTargets(existingPeers, candidateID)), nil
		case errors.Is(err, ErrDiscoveryStopped):
			m.logger.Warn("discovery stopped before bootstrap candidate became ready",
				"candidate", candidateID)
			if len(peersView) > 0 {
				existingPeers = peersView
			}
			readiness.LogPeerMetadata(existingPeers, m.logger)
			return newFollowerBootstrapPlan(candidateID, existingPeers, m.joinTargets(existingPeers, candidateID)), nil
		default:
			return nil, domain.NewDomainErrorWithCategory(
				domain.CategoryDiscovery,
				"bootstrap leader wait failed",
				err,
				domain.WithComponent("core.Manager.Start"),
				domain.WithContextDetail("candidate", candidateID),
			)
		}

		refreshedPeers := filterPeers(m.discovery.GetPeers(), m.nodeID)
		if len(refreshedPeers) > 0 {
			existingPeers = refreshedPeers
		}
	}
}

func newCandidateBootstrapPlan(nodeID string, peers []ports.Peer) *bootstrapPlan {
	multiNode := len(peers) > 0
	role := "provisional node"
	if multiNode {
		role = "bootstrap leader"
	}

	return &bootstrapPlan{
		CandidateID:           nodeID,
		ExistingPeers:         peers,
		JoinTargets:           nil,
		BootstrapMultiNode:    multiNode,
		AwaitLeadership:       true,
		LeadershipRole:        role,
		InitialReadinessState: readiness.StateProvisional,
	}
}

func newFollowerBootstrapPlan(candidateID string, peers []ports.Peer, joinTargets []ports.Peer) *bootstrapPlan {
	state := readiness.StateDetecting
	await := false
	role := ""

	if len(peers) == 0 {
		state = readiness.StateProvisional
		await = true
		role = "provisional node"
	}

	return &bootstrapPlan{
		CandidateID:           candidateID,
		ExistingPeers:         peers,
		JoinTargets:           joinTargets,
		BootstrapMultiNode:    false,
		AwaitLeadership:       await,
		LeadershipRole:        role,
		InitialReadinessState: state,
	}
}

func newRecoveredBootstrapPlan(nodeID string, peers []ports.Peer) *bootstrapPlan {
	state := readiness.StateDetecting
	await := false
	role := ""

	if len(peers) == 0 {
		state = readiness.StateProvisional
		await = true
		role = "provisional node"
	}

	return &bootstrapPlan{
		CandidateID:           nodeID,
		ExistingPeers:         peers,
		JoinTargets:           nil,
		BootstrapMultiNode:    false,
		AwaitLeadership:       await,
		LeadershipRole:        role,
		InitialReadinessState: state,
	}
}

func filterPeers(peers []ports.Peer, selfID string) []ports.Peer {
	if len(peers) == 0 {
		return nil
	}

	result := make([]ports.Peer, 0, len(peers))
	for _, peer := range peers {
		if peer.ID == "" || peer.ID == selfID {
			continue
		}
		result = append(result, peer)
	}
	return result
}

func (m *Manager) hasSmallerPeer(peers []ports.Peer) bool {
	for _, peer := range peers {
		if peer.ID != "" && peer.ID < m.nodeID {
			return true
		}
	}
	return false
}

func (m *Manager) selectBootstrapCandidate(peers []ports.Peer, exclude map[string]struct{}) string {
	actual := make(map[string]struct{}, len(peers)+1)
	hints := make(map[string]struct{})

	actual[m.nodeID] = struct{}{}

	for _, peer := range peers {
		if peer.ID != "" {
			actual[peer.ID] = struct{}{}
		}
		if peer.Metadata != nil {
			if cand := peer.Metadata[bootstrapCandidateKey]; cand != "" {
				hints[cand] = struct{}{}
			}
		}
	}

	orderedActual := orderedIDs(actual, exclude)
	if len(orderedActual) > 0 {
		sort.Strings(orderedActual)
		return orderedActual[0]
	}

	orderedHints := orderedIDs(hints, exclude)
	if len(orderedHints) > 0 {
		sort.Strings(orderedHints)
		return orderedHints[0]
	}

	return m.nodeID
}

func orderedIDs(source map[string]struct{}, exclude map[string]struct{}) []string {
	if len(source) == 0 {
		return nil
	}

	result := make([]string, 0, len(source))
	for id := range source {
		if id == "" {
			continue
		}
		if exclude != nil {
			if _, skip := exclude[id]; skip {
				continue
			}
		}
		result = append(result, id)
	}
	return result
}

func (m *Manager) extractBootstrapStatus(peers []ports.Peer) (string, bool) {
	candidate := m.bootstrapCandidateID
	ready := false

	for _, peer := range peers {
		md := peer.Metadata
		if md == nil {
			continue
		}

		state := md[bootstrapStateKey]
		cand := md[bootstrapCandidateKey]
		if cand == "" {
			cand = peer.ID
		}

		switch state {
		case bootstrapStateCandidate, bootstrapStateReady:
			candidate = cand
			if md[bootstrapReadyKey] == "true" || state == bootstrapStateReady {
				ready = true
			}
		case bootstrapStateAwaiting:
			if candidate == "" {
				candidate = cand
			}
		}
	}

	if candidate == "" {
		candidate = m.bootstrapCandidateID
	}

	return candidate, ready
}

func (m *Manager) waitForBootstrapReady(parent context.Context, candidate string, timeout time.Duration) (string, error) {
	check := func() (string, bool) {
		peers := m.discovery.GetPeers()
		return m.extractBootstrapStatus(peers)
	}

	lastCandidate := candidate

	current, ready := check()
	if ready {
		m.logger.Info("bootstrap candidate reported ready",
			"candidate", current,
			"previous_candidate", lastCandidate)
	}
	if ready {
		return current, nil
	}
	if current != "" && current != candidate {
		m.logger.Info("bootstrap candidate changed before ready",
			"previous_candidate", candidate,
			"new_candidate", current)
		return current, errBootstrapCandidateChanged
	}

	events, unsubscribe := m.discovery.Subscribe()
	defer unsubscribe()

	waitCtx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			current, ready := check()
			if ready {
				m.logger.Info("bootstrap candidate reported ready before timeout",
					"candidate", current,
					"previous_candidate", lastCandidate)
				return current, nil
			}
			return current, waitCtx.Err()
		case _, ok := <-events:
			if !ok {
				current, ready := check()
				if ready {
					m.logger.Info("bootstrap candidate reported ready via event before channel close",
						"candidate", current,
						"previous_candidate", lastCandidate)
					return current, nil
				}
				return current, ErrDiscoveryStopped
			}
			current, ready := check()
			if ready {
				if current != "" && current != lastCandidate {
					m.logger.Info("bootstrap candidate updated via event",
						"previous_candidate", lastCandidate,
						"new_candidate", current)
					lastCandidate = current
				}
				m.logger.Info("bootstrap candidate reported ready via event",
					"candidate", current)
				return current, nil
			}
			if current != "" && current != candidate {
				m.logger.Info("bootstrap candidate changed via event",
					"previous_candidate", candidate,
					"new_candidate", current)
				return current, errBootstrapCandidateChanged
			}
		case <-ticker.C:
			current, ready := check()
			if ready {
				if current != "" && current != lastCandidate {
					m.logger.Info("bootstrap candidate updated via poll",
						"previous_candidate", lastCandidate,
						"new_candidate", current)
					lastCandidate = current
				}
				m.logger.Info("bootstrap candidate reported ready via poll",
					"candidate", current)
				return current, nil
			}
			if current != "" && current != candidate {
				m.logger.Info("bootstrap candidate changed via poll",
					"previous_candidate", candidate,
					"new_candidate", current)
				return current, errBootstrapCandidateChanged
			}
		case <-parent.Done():
			return candidate, parent.Err()
		}
	}
}

func (m *Manager) updateBootstrapMetadata(state, candidate string, ready bool) {
	if m == nil || m.discovery == nil {
		return
	}
	if candidate == "" {
		candidate = m.nodeID
	}
	m.bootstrapCandidateID = candidate
	readyValue := "false"
	if ready {
		readyValue = "true"
	}
	epoch := m.bootEpoch
	if epoch == "" {
		epoch = m.nodeID
	}
	if err := m.discovery.UpdateMetadata(func(md map[string]string) map[string]string {
		if md == nil {
			md = make(map[string]string)
		}
		md[bootstrapEpochKey] = epoch
		md[bootstrapCandidateKey] = candidate
		if state != "" {
			md[bootstrapStateKey] = state
		}
		md[bootstrapReadyKey] = readyValue
		return md
	}); err != nil {
		m.logger.Warn("failed to update bootstrap metadata", "error", err)
	}
}

func (m *Manager) joinTargets(peers []ports.Peer, candidate string) []ports.Peer {
	if candidate == "" {
		return peers
	}
	for _, peer := range peers {
		if peer.ID == candidate {
			return []ports.Peer{peer}
		}
	}
	return peers
}
