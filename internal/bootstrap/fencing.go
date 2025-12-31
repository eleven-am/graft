package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type FencingManager struct {
	config     *BootstrapConfig
	tokenStore TokenStore
	epochStore EpochStore
	secrets    *SecretsManager
	logger     *slog.Logger

	mu           sync.RWMutex
	currentToken *FencingToken
	localEpoch   uint64
	voters       []VoterInfo
}

func NewFencingManager(
	config *BootstrapConfig,
	tokenStore TokenStore,
	epochStore EpochStore,
	secrets *SecretsManager,
	logger *slog.Logger,
) *FencingManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &FencingManager{
		config:     config,
		tokenStore: tokenStore,
		epochStore: epochStore,
		secrets:    secrets,
		logger:     logger,
	}
}

func (m *FencingManager) Initialize(voters []VoterInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.voters = voters

	highestSeenEpoch, err := m.epochStore.LoadHighestSeenEpoch()
	if err != nil {
		return fmt.Errorf("load highest seen epoch: %w", err)
	}
	m.localEpoch = highestSeenEpoch

	exists, err := m.tokenStore.Exists()
	if err != nil {
		return fmt.Errorf("check token existence: %w", err)
	}

	if exists {
		token, err := m.tokenStore.LoadToken()
		if err != nil {
			return fmt.Errorf("load existing token: %w", err)
		}

		if m.secrets != nil && m.secrets.HasFencingKey() {
			if !VerifyTokenSignature(token, m.secrets.FencingKey()) {
				return ErrTokenSignatureInvalid
			}
		}

		if token.Epoch > m.localEpoch {
			m.localEpoch = token.Epoch
		}

		currentVoterSetHash := ComputeVoterSetHash(voters)
		expectedQuorum := (len(voters) / 2) + 1

		voterSetValid := token.VoterSetHash == currentVoterSetHash
		quorumValid := token.QuorumSize == expectedQuorum

		if voterSetValid && quorumValid {
			m.currentToken = token

			m.logger.Info("initialized fencing manager with existing token",
				"epoch", token.Epoch,
				"holder_id", token.HolderID,
				"mode", token.Mode,
				"local_epoch", m.localEpoch,
			)
		} else {
			m.logger.Warn("existing token rejected due to voter set change, re-acquisition required",
				"token_epoch", token.Epoch,
				"local_epoch", m.localEpoch,
				"voter_set_valid", voterSetValid,
				"quorum_valid", quorumValid,
				"token_quorum", token.QuorumSize,
				"expected_quorum", expectedQuorum,
			)
		}
	} else {
		m.logger.Info("initialized fencing manager without existing token",
			"highest_seen_epoch", m.localEpoch,
		)
	}

	return nil
}

func (m *FencingManager) CurrentToken() *FencingToken {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentToken
}

func (m *FencingManager) CurrentEpoch() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.localEpoch
}

func (m *FencingManager) SetVoters(voters []VoterInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.voters = voters
}

func (m *FencingManager) Voters() []VoterInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]VoterInfo, len(m.voters))
	copy(result, m.voters)
	return result
}

func (m *FencingManager) CreateProposal(mode FencingMode, serverID raft.ServerID, serverAddr raft.ServerAddress) (*FencingProposal, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.secrets == nil || !m.secrets.HasFencingKey() {
		return nil, ErrNoFencingKey
	}

	if len(m.voters) == 0 {
		return nil, fmt.Errorf("no voters configured")
	}

	expectedEpoch := m.localEpoch
	proposedEpoch := expectedEpoch + 1
	voterSetHash := ComputeVoterSetHash(m.voters)
	quorumSize := (len(m.voters) / 2) + 1

	proposal := &FencingProposal{
		ProposedEpoch:   proposedEpoch,
		ExpectedEpoch:   expectedEpoch,
		ProposerID:      serverID,
		ProposerAddress: serverAddr,
		VoterSetHash:    voterSetHash,
		QuorumSize:      quorumSize,
		Mode:            mode,
		Timestamp:       time.Now(),
	}

	if err := SignProposal(proposal, m.secrets.FencingKey()); err != nil {
		return nil, fmt.Errorf("sign proposal: %w", err)
	}

	m.logger.Debug("created fencing proposal",
		"proposed_epoch", proposedEpoch,
		"proposer_id", serverID,
		"mode", mode,
		"quorum_size", quorumSize,
		"voter_count", len(m.voters),
	)

	return proposal, nil
}

func (m *FencingManager) HandleProposal(proposal *FencingProposal, localServerID raft.ServerID) (*FencingAck, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := proposal.Validate(); err != nil {
		return m.createRejectAck(proposal.ProposedEpoch, localServerID, "invalid proposal: "+err.Error())
	}

	if m.secrets != nil && m.secrets.HasFencingKey() {
		if !VerifyProposalSignature(proposal, m.secrets.FencingKey()) {
			return m.createRejectAck(proposal.ProposedEpoch, localServerID, "invalid signature")
		}
	}

	currentVoterSetHash := ComputeVoterSetHash(m.voters)
	if proposal.VoterSetHash != currentVoterSetHash {
		return m.createRejectAck(proposal.ProposedEpoch, localServerID, "voter set mismatch")
	}

	if proposal.ExpectedEpoch != m.localEpoch {
		return m.createRejectAck(proposal.ProposedEpoch, localServerID,
			fmt.Sprintf("CAS epoch mismatch: expected %d, current %d", proposal.ExpectedEpoch, m.localEpoch))
	}

	if proposal.ProposedEpoch != m.localEpoch+1 {
		return m.createRejectAck(proposal.ProposedEpoch, localServerID,
			fmt.Sprintf("invalid proposed epoch: expected %d, got %d", m.localEpoch+1, proposal.ProposedEpoch))
	}

	if err := m.epochStore.SaveHighestSeenEpoch(proposal.ProposedEpoch); err != nil {
		return nil, fmt.Errorf("persist epoch before accept: %w", err)
	}

	m.localEpoch = proposal.ProposedEpoch

	ack := &FencingAck{
		ProposedEpoch: proposal.ProposedEpoch,
		VoterID:       localServerID,
		Accepted:      true,
		CurrentEpoch:  m.localEpoch,
		Timestamp:     time.Now(),
	}

	if m.secrets != nil && m.secrets.HasFencingKey() {
		if err := SignAck(ack, m.secrets.FencingKey()); err != nil {
			return nil, fmt.Errorf("sign ack: %w", err)
		}
	}

	m.logger.Debug("accepted fencing proposal",
		"proposed_epoch", proposal.ProposedEpoch,
		"proposer_id", proposal.ProposerID,
		"local_epoch", m.localEpoch,
	)

	return ack, nil
}

func (m *FencingManager) createRejectAck(proposedEpoch uint64, voterID raft.ServerID, reason string) (*FencingAck, error) {
	ack := &FencingAck{
		ProposedEpoch: proposedEpoch,
		VoterID:       voterID,
		Accepted:      false,
		CurrentEpoch:  m.localEpoch,
		RejectReason:  reason,
		Timestamp:     time.Now(),
	}

	if m.secrets != nil && m.secrets.HasFencingKey() {
		if err := SignAck(ack, m.secrets.FencingKey()); err != nil {
			return nil, fmt.Errorf("sign reject ack: %w", err)
		}
	}

	m.logger.Debug("rejected fencing proposal",
		"proposed_epoch", proposedEpoch,
		"reason", reason,
	)

	return ack, nil
}

func (m *FencingManager) CommitToken(proposal *FencingProposal, acks []*FencingAck) (*FencingToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	voterIDs := make(map[raft.ServerID]bool)
	for _, voter := range m.voters {
		voterIDs[voter.ServerID] = true
	}

	seenVoters := make(map[raft.ServerID]bool)
	acceptedCount := 0
	var rejectReasons []string

	for _, ack := range acks {
		if !voterIDs[ack.VoterID] {
			m.logger.Warn("ack from unknown voter rejected",
				slog.String("voter_id", string(ack.VoterID)))
			continue
		}

		if seenVoters[ack.VoterID] {
			m.logger.Warn("duplicate ack from same voter", "voter_id", ack.VoterID)
			continue
		}
		seenVoters[ack.VoterID] = true

		if ack.ProposedEpoch != proposal.ProposedEpoch {
			m.logger.Warn("ack epoch mismatch",
				"voter_id", ack.VoterID,
				"ack_epoch", ack.ProposedEpoch,
				"proposed_epoch", proposal.ProposedEpoch)
			continue
		}

		if ack.Accepted {
			if m.secrets != nil && m.secrets.HasFencingKey() {
				if !VerifyAckSignature(ack, m.secrets.FencingKey()) {
					m.logger.Warn("invalid ack signature", "voter_id", ack.VoterID)
					continue
				}
			}
			acceptedCount++
		} else {
			rejectReasons = append(rejectReasons, fmt.Sprintf("%s: %s", ack.VoterID, ack.RejectReason))
		}
	}

	if acceptedCount < proposal.QuorumSize {
		return nil, &QuorumNotReachedError{
			Votes:         acceptedCount,
			Required:      proposal.QuorumSize,
			TotalVoterSet: len(m.voters),
		}
	}

	token := &FencingToken{
		Version:      TokenVersion,
		Epoch:        proposal.ProposedEpoch,
		HolderID:     proposal.ProposerID,
		VoterSetHash: proposal.VoterSetHash,
		QuorumSize:   proposal.QuorumSize,
		Mode:         proposal.Mode,
		AcquiredAt:   time.Now(),
	}

	token.UpdateChecksum()

	if m.secrets != nil && m.secrets.HasFencingKey() {
		if err := SignToken(token, m.secrets.FencingKey()); err != nil {
			return nil, fmt.Errorf("sign token: %w", err)
		}
	}

	if err := m.tokenStore.SaveToken(token); err != nil {
		return nil, fmt.Errorf("persist token: %w", err)
	}

	m.currentToken = token
	m.localEpoch = token.Epoch

	m.logger.Info("fencing token committed",
		"epoch", token.Epoch,
		"holder_id", token.HolderID,
		"mode", token.Mode,
		"acks_received", acceptedCount,
		"quorum_required", proposal.QuorumSize,
	)

	return token, nil
}

func (m *FencingManager) ValidateToken(token *FencingToken) error {
	if token == nil {
		return fmt.Errorf("token is nil")
	}

	if err := token.Validate(); err != nil {
		return fmt.Errorf("token validation: %w", err)
	}

	if !token.VerifyChecksum() {
		return &TokenCorruptionError{
			Path:             "in-memory",
			ExpectedChecksum: token.ComputeChecksum(),
			ActualChecksum:   token.Checksum,
		}
	}

	if m.secrets != nil && m.secrets.HasFencingKey() {
		if !VerifyTokenSignature(token, m.secrets.FencingKey()) {
			return ErrTokenSignatureInvalid
		}
	}

	if token.IsExpired() {
		return ErrTokenExpired
	}

	m.mu.RLock()
	voters := m.voters
	m.mu.RUnlock()

	if err := ValidateTokenVoterSet(token, voters); err != nil {
		return err
	}

	return nil
}

func (m *FencingManager) QuorumSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.voters) == 0 {
		return 1
	}
	return (len(m.voters) / 2) + 1
}

func (m *FencingManager) HasToken() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentToken != nil
}

func (m *FencingManager) IsTokenHolder(serverID raft.ServerID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.currentToken == nil {
		return false
	}
	return m.currentToken.HolderID == serverID
}

func (m *FencingManager) ForceEpochAdvance(newEpoch uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if newEpoch <= m.localEpoch {
		return &StaleEpochError{
			ProposedEpoch: newEpoch,
			CurrentEpoch:  m.localEpoch,
		}
	}

	if m.epochStore != nil {
		if err := m.epochStore.SaveHighestSeenEpoch(newEpoch); err != nil {
			return fmt.Errorf("persist epoch advance: %w", err)
		}
	}

	m.localEpoch = newEpoch

	m.logger.Warn("forced epoch advance (force-bootstrap)",
		"new_epoch", newEpoch,
	)

	return nil
}

func (m *FencingManager) AcquireToken(
	ctx context.Context,
	mode FencingMode,
	serverID raft.ServerID,
	serverAddr raft.ServerAddress,
	transport BootstrapTransport,
) (*FencingToken, error) {
	voters := m.Voters()
	if len(voters) == 0 {
		if m.isInitialBootstrap() {
			voters = m.buildExpectedVoterSet()
			if len(voters) == 0 {
				return nil, &NoCommittedConfigError{
					Operation: "fencing_token_acquisition",
					Message:   "no voters configured and cannot build expected voter set",
				}
			}
		} else {
			return nil, &NoCommittedConfigError{
				Operation: "fencing_token_acquisition",
				Message:   "no voters configured and not initial bootstrap",
			}
		}
	}

	proposal, err := m.CreateProposal(mode, serverID, serverAddr)
	if err != nil {
		return nil, fmt.Errorf("create proposal: %w", err)
	}

	acks := m.proposeToPeers(ctx, voters, proposal, transport)

	selfAck, err := m.HandleProposal(proposal, serverID)
	if err != nil {
		return nil, fmt.Errorf("self-ack: %w", err)
	}
	acks = append(acks, selfAck)

	token, err := m.CommitToken(proposal, acks)
	if err != nil {
		return nil, err
	}

	return token, nil
}

func (m *FencingManager) isInitialBootstrap() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.currentToken != nil {
		return false
	}

	if m.localEpoch > 0 {
		return false
	}

	return true
}

func (m *FencingManager) buildExpectedVoterSet() []VoterInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.config == nil || m.config.ExpectedNodes <= 0 {
		return nil
	}

	voters := make([]VoterInfo, m.config.ExpectedNodes)
	for i := 0; i < m.config.ExpectedNodes; i++ {
		serverID := raft.ServerID(fmt.Sprintf("%s-%d", m.config.ServiceName, i))
		addr := buildAddressForOrdinal(m.config, i)
		voters[i] = VoterInfo{
			ServerID: serverID,
			Address:  addr,
			Ordinal:  i,
		}
	}
	return voters
}

func buildAddressForOrdinal(config *BootstrapConfig, ordinal int) raft.ServerAddress {
	return raft.ServerAddress(fmt.Sprintf("%s-%d.%s.%s.svc:%d",
		config.ServiceName, ordinal,
		config.ServiceName, config.Namespace,
		config.RaftPort))
}

func (m *FencingManager) proposeToPeers(
	ctx context.Context,
	voters []VoterInfo,
	proposal *FencingProposal,
	transport BootstrapTransport,
) []*FencingAck {
	var acks []*FencingAck
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, voter := range voters {
		if voter.ServerID == proposal.ProposerID {
			continue
		}

		wg.Add(1)
		go func(v VoterInfo) {
			defer wg.Done()

			ack, err := transport.ProposeToken(ctx, string(v.Address), proposal)
			if err != nil {
				m.logger.Warn("proposal to peer failed",
					"peer", v.ServerID,
					"error", err)
				return
			}

			mu.Lock()
			acks = append(acks, ack)
			mu.Unlock()
		}(voter)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		m.logger.Warn("proposal collection interrupted", "error", ctx.Err())
	}

	return acks
}
