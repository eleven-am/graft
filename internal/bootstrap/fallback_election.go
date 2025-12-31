package bootstrap

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type FallbackElectionDeps struct {
	Config          *BootstrapConfig
	Transport       BootstrapTransport
	Fencing         *FencingManager
	Meta            *ClusterMeta
	StateMachine    *StateMachine
	MembershipStore MembershipStore
	Prober          PeerProber
	Secrets         *SecretsManager
	Logger          *slog.Logger
}

type voteState struct {
	CandidateID    raft.ServerID
	ElectionReason string
	VotedAt        time.Time
}

type FallbackElection struct {
	config          *BootstrapConfig
	transport       BootstrapTransport
	fencing         *FencingManager
	meta            *ClusterMeta
	stateMachine    *StateMachine
	membershipStore MembershipStore
	prober          PeerProber
	secrets         *SecretsManager
	logger          *slog.Logger

	electionMu sync.Mutex
	inElection bool

	voteMu   sync.Mutex
	lastVote *voteState
}

type FallbackCandidate struct {
	ServerID         raft.ServerID
	Address          raft.ServerAddress
	StartTime        time.Time
	VotesFor         int
	VotesRequired    int
	VoterSetSnapshot []raft.ServerID
}

func NewFallbackElection(deps FallbackElectionDeps) *FallbackElection {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &FallbackElection{
		config:          deps.Config,
		transport:       deps.Transport,
		fencing:         deps.Fencing,
		meta:            deps.Meta,
		stateMachine:    deps.StateMachine,
		membershipStore: deps.MembershipStore,
		prober:          deps.Prober,
		secrets:         deps.Secrets,
		logger:          logger,
	}
}

func (e *FallbackElection) RunElection(ctx context.Context) (*FallbackCandidate, error) {
	e.electionMu.Lock()
	if e.inElection {
		e.electionMu.Unlock()
		return nil, ErrElectionInProgress
	}
	e.inElection = true
	e.electionMu.Unlock()

	defer func() {
		e.electionMu.Lock()
		e.inElection = false
		e.electionMu.Unlock()
	}()

	electionWindow := 30 * time.Second
	if e.config != nil && e.config.FallbackElectionWindow > 0 {
		electionWindow = e.config.FallbackElectionWindow
	}

	electionCtx, cancel := context.WithTimeout(ctx, electionWindow)
	defer cancel()

	voterSet, err := e.getCommittedVoterSet(electionCtx)
	if err != nil {
		return nil, &NoCommittedConfigError{
			Operation: "fallback_election",
			Cause:     err,
			Message:   "cannot run fallback election without committed membership config; ExpectedNodes fallback is unsafe after scale operations",
		}
	}

	if len(voterSet) == 0 {
		return nil, &NoCommittedConfigError{
			Operation: "fallback_election",
			Message:   "committed voter set is empty",
		}
	}

	quorum := (len(voterSet) / 2) + 1

	eligibleVoters := e.filterEligibleVoters(voterSet)

	reachable, err := e.probeReachability(electionCtx, eligibleVoters)
	if err != nil {
		return nil, err
	}

	if e.anyPeerHasCluster(ctx, reachable) {
		e.logger.Info("existing cluster found, aborting fallback election")
		return nil, ErrExistingClusterFound
	}

	reachableCount := len(reachable) + 1
	if reachableCount < quorum {
		return nil, &InsufficientPeersError{
			Reachable:     reachableCount,
			Required:      quorum,
			TotalVoterSet: len(voterSet),
			Message:       "cannot prove majority presence for fallback election",
		}
	}

	lowestServerID := e.meta.ServerID
	for _, peer := range reachable {
		if peer.ServerID < lowestServerID {
			lowestServerID = peer.ServerID
		}
	}

	if lowestServerID != e.meta.ServerID {
		e.logger.Info("not lowest ServerID, deferring to peer",
			"my_id", e.meta.ServerID,
			"lowest_id", lowestServerID,
		)
		return nil, ErrNotLowestServerID
	}

	votes, votedBy, err := e.requestVotesFromEligible(electionCtx, reachable, voterSet)
	if err != nil {
		return nil, err
	}

	if votes < quorum {
		return nil, &QuorumNotReachedError{
			Votes:         votes,
			Required:      quorum,
			TotalVoterSet: len(voterSet),
		}
	}

	candidate := &FallbackCandidate{
		ServerID:         e.meta.ServerID,
		Address:          e.meta.ServerAddress,
		StartTime:        time.Now(),
		VotesFor:         votes,
		VotesRequired:    quorum,
		VoterSetSnapshot: votedBy,
	}

	e.logger.Info("won fallback election",
		"votes", votes,
		"quorum", quorum,
		"voter_set_size", len(voterSet),
	)

	return candidate, nil
}

func (e *FallbackElection) getCommittedVoterSet(_ context.Context) ([]VoterInfo, error) {
	if e.membershipStore == nil {
		return nil, fmt.Errorf("membership store not configured")
	}

	config, err := e.membershipStore.GetLastCommittedConfiguration()
	if err != nil {
		return nil, err
	}

	if config == nil {
		return nil, fmt.Errorf("no committed configuration available")
	}

	voters := make([]VoterInfo, 0)
	for _, server := range config.Servers {
		if server.Suffrage == raft.Voter {
			voters = append(voters, VoterInfo{
				ServerID: server.ID,
				Address:  server.Address,
			})
		}
	}

	return voters, nil
}

func (e *FallbackElection) filterEligibleVoters(voterSet []VoterInfo) []VoterInfo {
	eligible := make([]VoterInfo, 0, len(voterSet))
	for _, v := range voterSet {
		if v.ServerID != e.meta.ServerID {
			eligible = append(eligible, v)
		}
	}
	return eligible
}

func (e *FallbackElection) probeReachability(ctx context.Context, voters []VoterInfo) ([]PeerInfo, error) {
	if e.prober == nil {
		return nil, ErrProberNotConfigured
	}

	results := e.prober.ProbeAll(ctx, voters)
	reachable := make([]PeerInfo, 0, len(results))

	for _, result := range results {
		if result.Reachable {
			reachable = append(reachable, PeerInfo{
				ServerID: result.ServerID,
				Address:  result.Address,
			})
		}
	}

	return reachable, nil
}

func (e *FallbackElection) requestVotesFromEligible(
	ctx context.Context,
	reachable []PeerInfo,
	voterSet []VoterInfo,
) (int, []raft.ServerID, error) {
	votes := 1
	votedBy := []raft.ServerID{e.meta.ServerID}
	var mu sync.Mutex

	voterSetMap := make(map[raft.ServerID]bool)
	for _, v := range voterSet {
		voterSetMap[v.ServerID] = true
	}

	voterSetHash := HashVoterSet(voterSet)
	quorum := (len(voterSet) / 2) + 1

	var fencingKey []byte
	if e.secrets != nil && e.secrets.HasFencingKey() {
		fencingKey = e.secrets.FencingKey()
	}

	var wg sync.WaitGroup
	for _, peer := range reachable {
		if !voterSetMap[peer.ServerID] {
			e.logger.Debug("skipping non-voter peer", "peer", peer.ServerID)
			continue
		}

		wg.Add(1)
		go func(p PeerInfo) {
			defer wg.Done()

			req := &VoteRequest{
				CandidateID:    e.meta.ServerID,
				ElectionReason: "fallback_election",
				Timestamp:      time.Now(),
				VoterSetHash:   voterSetHash,
				RequiredQuorum: quorum,
			}

			if len(fencingKey) > 0 {
				_ = SignVoteRequest(req, fencingKey)
			}

			resp, err := e.transport.RequestVote(ctx, p.Address, req)
			if err != nil {
				e.logger.Warn("vote request failed", "peer", p.ServerID, "error", err)
				return
			}

			if !resp.VoteGranted {
				e.logger.Info("vote denied", "peer", p.ServerID, "reason", resp.Reason)
				return
			}

			if len(fencingKey) > 0 {
				if !VerifyVoteResponseSignature(resp, fencingKey) {
					e.logger.Warn("vote response signature invalid, rejecting vote",
						"peer", p.ServerID,
					)
					return
				}
			}

			if !bytes.Equal(resp.VoterSetHash, voterSetHash) {
				e.logger.Warn("vote response voter set hash mismatch, rejecting vote",
					"peer", p.ServerID,
				)
				return
			}

			if !voterSetMap[resp.VoterID] {
				e.logger.Warn("vote response from non-member, rejecting vote",
					"peer", p.ServerID,
					"voter_id", resp.VoterID,
				)
				return
			}

			mu.Lock()
			votes++
			votedBy = append(votedBy, resp.VoterID)
			mu.Unlock()

			e.logger.Debug("received valid vote", "peer", p.ServerID)
		}(peer)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return votes, votedBy, ctx.Err()
	}

	return votes, votedBy, nil
}

func (e *FallbackElection) HandleVoteRequest(
	ctx context.Context,
	req *VoteRequest,
	tlsConfig *tls.Config,
	peerCert *x509.Certificate,
) (*VoteResponse, error) {
	if tlsConfig == nil || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		return &VoteResponse{VoteGranted: false, Reason: "mTLS required"}, nil
	}

	if peerCert == nil {
		return &VoteResponse{VoteGranted: false, Reason: "mTLS required: no client certificate"}, nil
	}

	if e.secrets != nil && e.secrets.HasFencingKey() {
		if !VerifyVoteRequestSignature(req, e.secrets.FencingKey()) {
			return &VoteResponse{VoteGranted: false, Reason: "invalid signature"}, nil
		}
	}

	myVoterSet, err := e.getCommittedVoterSet(ctx)
	if err == nil && len(myVoterSet) > 0 {
		myHash := HashVoterSet(myVoterSet)
		if !bytes.Equal(myHash, req.VoterSetHash) {
			return &VoteResponse{
				VoteGranted:  false,
				Reason:       "voter set mismatch",
				VoterSetHash: myHash,
			}, nil
		}

		if err := e.validatePeerCertAgainstVoterSet(req.CandidateID, peerCert, myVoterSet); err != nil {
			return &VoteResponse{
				VoteGranted: false,
				Reason:      fmt.Sprintf("certificate validation failed: %v", err),
			}, nil
		}
	}

	currentState := e.meta.State
	if e.stateMachine != nil {
		currentState = e.stateMachine.CurrentState()
	}
	if currentState == StateReady || currentState == StateRecovering {
		return &VoteResponse{VoteGranted: false, Reason: "already in cluster"}, nil
	}

	electionWindow := 30 * time.Second
	if e.config != nil && e.config.FallbackElectionWindow > 0 {
		electionWindow = e.config.FallbackElectionWindow
	}

	e.voteMu.Lock()
	defer e.voteMu.Unlock()

	if e.lastVote != nil {
		if time.Since(e.lastVote.VotedAt) < electionWindow {
			if e.lastVote.CandidateID != req.CandidateID {
				e.logger.Warn("rejecting vote: already voted for different candidate",
					"previous_candidate", e.lastVote.CandidateID,
					"requesting_candidate", req.CandidateID,
					"vote_age", time.Since(e.lastVote.VotedAt),
				)
				return &VoteResponse{
					VoteGranted: false,
					Reason:      fmt.Sprintf("already voted for %s within election window", e.lastVote.CandidateID),
				}, nil
			}
			e.logger.Debug("re-granting vote to same candidate",
				"candidate", req.CandidateID,
			)
		}
	}

	e.lastVote = &voteState{
		CandidateID:    req.CandidateID,
		ElectionReason: req.ElectionReason,
		VotedAt:        time.Now(),
	}

	resp := &VoteResponse{
		VoteGranted:  true,
		VoterID:      e.meta.ServerID,
		VoterSetHash: req.VoterSetHash,
	}

	if e.secrets != nil && e.secrets.HasFencingKey() {
		_ = SignVoteResponse(resp, e.secrets.FencingKey())
	}

	e.logger.Info("granted vote",
		"candidate", req.CandidateID,
	)

	return resp, nil
}

func (e *FallbackElection) validatePeerCertAgainstVoterSet(
	peerID raft.ServerID,
	peerCert *x509.Certificate,
	voterSet []VoterInfo,
) error {
	if peerCert == nil {
		return fmt.Errorf("no client certificate provided")
	}

	var expectedVoter *VoterInfo
	for i, v := range voterSet {
		if v.ServerID == peerID {
			expectedVoter = &voterSet[i]
			break
		}
	}

	if expectedVoter == nil {
		return fmt.Errorf("peer %s not in committed voter set", peerID)
	}

	expectedHost, _, err := net.SplitHostPort(string(expectedVoter.Address))
	if err != nil {
		expectedHost = string(expectedVoter.Address)
	}

	certValid := false

	for _, dnsName := range peerCert.DNSNames {
		if dnsName == expectedHost {
			certValid = true
			break
		}
	}

	if !certValid {
		for _, ip := range peerCert.IPAddresses {
			if ip.String() == expectedHost {
				certValid = true
				break
			}
		}
	}

	if !certValid && peerCert.Subject.CommonName == expectedHost {
		certValid = true
	}

	if !certValid {
		return &CertificateSANMismatchError{
			PeerID:          peerID,
			ExpectedAddress: expectedVoter.Address,
			CertDNSNames:    peerCert.DNSNames,
			CertIPAddresses: peerCert.IPAddresses,
			CertCN:          peerCert.Subject.CommonName,
		}
	}

	return nil
}

func (e *FallbackElection) anyPeerHasCluster(ctx context.Context, peers []PeerInfo) bool {
	if e.transport == nil {
		return false
	}

	for _, peer := range peers {
		meta, err := e.transport.GetClusterMeta(ctx, string(peer.Address))
		if err != nil {
			continue
		}
		if meta != nil && (meta.State == StateReady || meta.State == StateRecovering) {
			e.logger.Info("peer has active cluster",
				"peer", peer.ServerID,
				"peer_state", meta.State,
			)
			return true
		}
	}
	return false
}

func (e *FallbackElection) IsInElection() bool {
	e.electionMu.Lock()
	defer e.electionMu.Unlock()
	return e.inElection
}
