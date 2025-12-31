package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type PreflightResult struct {
	Safe                     bool
	ReachablePeers           int
	ReachableCommittedVoters int
	CommittedVoterCount      int
	HealthyPeers             []PeerInfo
	CurrentEpoch             uint64
	Warnings                 []string
	BlockReason              string
	QuorumAbsenceProven      bool
}

type ForceBootstrapPreflightDeps struct {
	Discovery       PeerDiscovery
	Transport       BootstrapTransport
	Fencing         *FencingManager
	MembershipStore MembershipStore
	Config          *BootstrapConfig
	Logger          *slog.Logger
}

type ForceBootstrapPreflight struct {
	discovery       PeerDiscovery
	transport       BootstrapTransport
	fencing         *FencingManager
	membershipStore MembershipStore
	config          *BootstrapConfig
	logger          *slog.Logger
}

func NewForceBootstrapPreflight(deps ForceBootstrapPreflightDeps) *ForceBootstrapPreflight {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &ForceBootstrapPreflight{
		discovery:       deps.Discovery,
		transport:       deps.Transport,
		fencing:         deps.Fencing,
		membershipStore: deps.MembershipStore,
		config:          deps.Config,
		logger:          logger,
	}
}

func (p *ForceBootstrapPreflight) Check(ctx context.Context, token *ForceBootstrapToken) (*PreflightResult, error) {
	result := &PreflightResult{
		Safe: false,
	}

	if p.fencing != nil {
		result.CurrentEpoch = p.fencing.CurrentEpoch()
	}

	if token.DisasterRecoveryMode {
		return p.handleDisasterRecovery(ctx, token, result)
	}

	committedConfig, err := p.membershipStore.GetLastCommittedConfiguration()
	if err != nil {
		result.BlockReason = fmt.Sprintf("failed to load committed config: %v", err)
		return result, nil
	}

	if committedConfig == nil {
		result.BlockReason = "no committed configuration available"
		return result, nil
	}

	voters := extractVotersFromConfig(committedConfig)
	result.CommittedVoterCount = len(voters)

	currentHash := HashVoterSet(voters)
	if !bytes.Equal(currentHash, token.VoterSetHash) {
		result.BlockReason = "voter set hash mismatch - membership has changed since token issuance"
		return result, nil
	}

	reachable, healthyPeers := p.probeAllPeers(ctx, voters)
	result.ReachablePeers = reachable
	result.ReachableCommittedVoters = reachable
	result.HealthyPeers = healthyPeers

	quorum := (len(voters) / 2) + 1

	if reachable >= quorum {
		result.BlockReason = fmt.Sprintf("quorum (%d) of committed voters is reachable - force bootstrap blocked to prevent split-brain", quorum)
		return result, nil
	}

	if reachable > 0 && !token.AcknowledgeFork {
		result.BlockReason = fmt.Sprintf("%d voters reachable - requires AcknowledgeFork=true to proceed", reachable)
		return result, nil
	}

	if reachable > 0 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("WARNING: %d committed voters are reachable. Force bootstrap will create a new cluster, potentially causing split-brain if these nodes rejoin.", reachable),
		)
	}

	for _, peer := range healthyPeers {
		peerEpoch, err := p.transport.GetFencingEpoch(ctx, string(peer.Address))
		if err != nil {
			p.logger.Debug("failed to get fencing epoch from peer",
				"peer", peer.ServerID,
				"error", err,
			)
			continue
		}

		if peerEpoch > token.BoundFencingEpoch {
			result.BlockReason = fmt.Sprintf("peer %s has epoch %d > token bound epoch %d - token is stale", peer.ServerID, peerEpoch, token.BoundFencingEpoch)
			return result, nil
		}
	}

	for _, peer := range healthyPeers {
		peerMeta, err := p.transport.GetClusterMeta(ctx, string(peer.Address))
		if err != nil {
			continue
		}

		if token.PreviousClusterUUID != "" && peerMeta.ClusterUUID != token.PreviousClusterUUID {
			result.BlockReason = fmt.Sprintf("peer %s has cluster UUID %s != token previous UUID %s", peer.ServerID, peerMeta.ClusterUUID, token.PreviousClusterUUID)
			return result, nil
		}
	}

	if reachable == 0 {
		result.QuorumAbsenceProven = true
	}

	result.Safe = true
	return result, nil
}

func (p *ForceBootstrapPreflight) handleDisasterRecovery(
	ctx context.Context,
	token *ForceBootstrapToken,
	result *PreflightResult,
) (*PreflightResult, error) {
	if !token.AcknowledgeFork {
		result.BlockReason = "disaster recovery mode requires AcknowledgeFork=true"
		return result, nil
	}

	if p.config == nil {
		result.BlockReason = "config not available for disaster recovery"
		return result, nil
	}

	serverIDPattern := p.config.ForceBootstrap.GetServerIDPattern()

	expectedVoters := make([]VoterInfo, 0, p.config.ExpectedNodes)
	for ordinal := 0; ordinal < p.config.ExpectedNodes; ordinal++ {
		addr := p.discovery.AddressForOrdinal(ordinal)
		if addr == "" {
			continue
		}
		expectedVoters = append(expectedVoters, VoterInfo{
			ServerID: raft.ServerID(fmt.Sprintf(serverIDPattern, ordinal)),
			Address:  addr,
			Ordinal:  ordinal,
		})
	}

	result.CommittedVoterCount = len(expectedVoters)

	expectedHash := HashVoterSet(expectedVoters)
	if !bytes.Equal(expectedHash, token.VoterSetHash) {
		result.BlockReason = fmt.Sprintf("voter set hash mismatch for expected nodes in disaster recovery (using pattern '%s')", serverIDPattern)
		return result, nil
	}

	reachable := 0
	var healthyPeers []PeerInfo
	var peersWithExistingUUID []string

	for _, voter := range expectedVoters {
		lastIndex, err := p.transport.GetLastIndex(ctx, string(voter.Address))
		if err != nil {
			p.logger.Debug("peer unreachable in disaster recovery probe",
				"peer", voter.ServerID,
				"error", err,
			)
			continue
		}

		if lastIndex > 0 {
			result.BlockReason = fmt.Sprintf("peer %s has existing Raft log (lastIndex=%d) - not a fresh cluster", voter.ServerID, lastIndex)
			return result, nil
		}

		peerMeta, metaErr := p.transport.GetClusterMeta(ctx, string(voter.Address))
		if metaErr == nil && peerMeta != nil && peerMeta.ClusterUUID != "" {
			peersWithExistingUUID = append(peersWithExistingUUID, string(voter.ServerID))
		}

		reachable++
		healthyPeers = append(healthyPeers, PeerInfo{
			ServerID: voter.ServerID,
			Address:  voter.Address,
			Ordinal:  voter.Ordinal,
		})
	}

	result.ReachablePeers = reachable
	result.HealthyPeers = healthyPeers

	quorum := (len(expectedVoters) / 2) + 1

	if reachable >= quorum {
		if !p.config.ForceBootstrap.AllowDRQuorumOverride {
			result.BlockReason = "DR quorum override requires AllowDRQuorumOverride=true in executor config"
			return result, nil
		}

		if !token.IgnoreQuorumCheck {
			result.BlockReason = fmt.Sprintf("quorum (%d) of expected nodes reachable in disaster recovery - requires IgnoreQuorumCheck=true", quorum)
			return result, nil
		}

		if token.ConfirmIgnoreQuorumCheck != IgnoreQuorumCheckConfirmationPhrase {
			result.BlockReason = fmt.Sprintf("IgnoreQuorumCheck requires confirmation phrase '%s'", IgnoreQuorumCheckConfirmationPhrase)
			return result, nil
		}

		if len(peersWithExistingUUID) > 0 {
			result.BlockReason = fmt.Sprintf("peers %v have existing cluster UUID - they may belong to a prior cluster that could recover", peersWithExistingUUID)
			return result, nil
		}

		result.Warnings = append(result.Warnings,
			"CRITICAL: Force bootstrapping with quorum present. This is extremely dangerous and may cause split-brain.",
		)

		p.logger.Warn("CRITICAL: Force bootstrap with quorum present in disaster recovery mode",
			"reachable", reachable,
			"quorum", quorum,
		)
	}

	if reachable == 0 {
		result.QuorumAbsenceProven = true
	}

	result.Safe = true
	return result, nil
}

func (p *ForceBootstrapPreflight) probeAllPeers(ctx context.Context, voters []VoterInfo) (int, []PeerInfo) {
	if p.transport == nil {
		return 0, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	reachable := 0
	var healthyPeers []PeerInfo

	for _, voter := range voters {
		wg.Add(1)
		go func(v VoterInfo) {
			defer wg.Done()

			_, err := p.transport.GetClusterMeta(ctx, string(v.Address))
			if err != nil {
				return
			}

			mu.Lock()
			reachable++
			healthyPeers = append(healthyPeers, PeerInfo{
				ServerID: v.ServerID,
				Address:  v.Address,
				Ordinal:  v.Ordinal,
			})
			mu.Unlock()
		}(voter)
	}

	wg.Wait()
	return reachable, healthyPeers
}

func extractVotersFromConfig(config *raft.Configuration) []VoterInfo {
	voters := make([]VoterInfo, 0)
	for _, server := range config.Servers {
		if server.Suffrage == raft.Voter {
			voters = append(voters, VoterInfo{
				ServerID: server.ID,
				Address:  server.Address,
				Ordinal:  ExtractOrdinal(server.ID),
			})
		}
	}
	return voters
}

type ForceBootstrapExecutorDeps struct {
	Config          *BootstrapConfig
	Meta            *ClusterMeta
	Discovery       PeerDiscovery
	Transport       BootstrapTransport
	Fencing         *FencingManager
	MembershipStore MembershipStore
	MetaStore       MetaStore
	StateMachine    *StateMachine
	Secrets         *SecretsManager
	Logger          *slog.Logger
}

type ForceBootstrapExecutor struct {
	config          *BootstrapConfig
	meta            *ClusterMeta
	discovery       PeerDiscovery
	transport       BootstrapTransport
	fencing         *FencingManager
	membershipStore MembershipStore
	metaStore       MetaStore
	stateMachine    *StateMachine
	secrets         *SecretsManager
	logger          *slog.Logger
	usageTracker    *TokenUsageTracker
	preflight       *ForceBootstrapPreflight
}

func NewForceBootstrapExecutor(deps ForceBootstrapExecutorDeps) *ForceBootstrapExecutor {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	usageTracker := NewTokenUsageTracker(TokenUsageTrackerDeps{
		DataDir:   deps.Config.DataDir,
		Fencing:   deps.Fencing,
		Transport: deps.Transport,
		Discovery: deps.Discovery,
		Meta:      deps.Meta,
		Config:    deps.Config,
		Secrets:   deps.Secrets,
		Logger:    logger,
	})

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Discovery:       deps.Discovery,
		Transport:       deps.Transport,
		Fencing:         deps.Fencing,
		MembershipStore: deps.MembershipStore,
		Config:          deps.Config,
		Logger:          logger,
	})

	return &ForceBootstrapExecutor{
		config:          deps.Config,
		meta:            deps.Meta,
		discovery:       deps.Discovery,
		transport:       deps.Transport,
		fencing:         deps.Fencing,
		membershipStore: deps.MembershipStore,
		metaStore:       deps.MetaStore,
		stateMachine:    deps.StateMachine,
		secrets:         deps.Secrets,
		logger:          logger,
		usageTracker:    usageTracker,
		preflight:       preflight,
	}
}

func (e *ForceBootstrapExecutor) currentMeta() *ClusterMeta {
	if e.stateMachine != nil {
		if fresh := e.stateMachine.CurrentMeta(); fresh != nil {
			return fresh
		}
	}
	return e.meta
}

func (e *ForceBootstrapExecutor) CheckForToken(ctx context.Context) (bool, *ForceBootstrapToken, error) {
	if e.config == nil || e.config.ForceBootstrapFile == "" {
		return false, nil, nil
	}

	tokenFile := e.config.ForceBootstrapFile
	data, err := os.ReadFile(tokenFile)
	if os.IsNotExist(err) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("read force bootstrap file: %w", err)
	}

	var token ForceBootstrapToken
	if err := json.Unmarshal(data, &token); err != nil {
		return false, nil, fmt.Errorf("unmarshal force bootstrap token: %w", err)
	}

	if err := token.ValidateVersion(); err != nil {
		return false, nil, err
	}

	if token.IsExpired() {
		return false, nil, ErrForceBootstrapExpired
	}

	if token.TargetOrdinal >= 0 && token.TargetOrdinal != e.currentMeta().Ordinal {
		return false, nil, ErrForceBootstrapTargetMismatch
	}

	if !e.validateTokenSignature(&token) {
		return false, nil, ErrInvalidForceBootstrapSignature
	}

	return true, &token, nil
}

func (e *ForceBootstrapExecutor) Execute(ctx context.Context, token *ForceBootstrapToken) error {
	e.logger.Info("executing force bootstrap",
		"token_version", token.Version,
		"reason", token.Reason,
		"single_use", token.SingleUse,
		"disaster_recovery", token.DisasterRecoveryMode,
	)

	if token.SingleUse {
		used, usedBy, usedAt, err := e.usageTracker.CheckClusterWideUsage(ctx, token)
		if err != nil {
			e.logger.Error("failed to check cluster-wide token usage, blocking for safety",
				"error", err,
			)
			return &ForceBootstrapBlockedError{
				Reason:       fmt.Sprintf("cannot verify single-use token status: %v", err),
				CurrentEpoch: e.fencing.CurrentEpoch(),
			}
		}

		if used {
			return &ForceBootstrapBlockedError{
				Reason:       fmt.Sprintf("single-use token already used by %s at %v", usedBy, usedAt),
				CurrentEpoch: e.fencing.CurrentEpoch(),
			}
		}
	}

	result, err := e.preflight.Check(ctx, token)
	if err != nil {
		return fmt.Errorf("preflight check error: %w", err)
	}

	if !result.Safe {
		return &ForceBootstrapBlockedError{
			Reason:         result.BlockReason,
			ReachablePeers: result.ReachablePeers,
			CurrentEpoch:   result.CurrentEpoch,
			Warnings:       result.Warnings,
		}
	}

	for _, warning := range result.Warnings {
		e.logger.Warn(warning)
	}

	if token.SingleUse {
		token.UsedAt = time.Now()
		token.UsedBy = string(e.currentMeta().ServerID)

		if e.fencing != nil {
			newEpoch := token.BoundFencingEpoch + 1
			if err := e.fencing.ForceEpochAdvance(newEpoch); err != nil {
				e.logger.Error("CRITICAL: failed to advance fencing epoch during force bootstrap - aborting",
					"error", err,
					"target_epoch", newEpoch,
				)
				return fmt.Errorf("failed to advance fencing epoch: %w", err)
			}
			e.logger.Info("advanced fencing epoch", "new_epoch", newEpoch)

			if e.stateMachine != nil {
				if err := e.stateMachine.UpdateFencingInfo(newEpoch); err != nil {
					e.logger.Error("failed to persist fencing info to meta", "error", err)
				}
			}
		}

		if err := e.usageTracker.PersistUsage(token); err != nil {
			e.logger.Warn("failed to persist token usage locally",
				"error", err,
			)
		}

		if err := e.usageTracker.BroadcastUsage(ctx, token, result.HealthyPeers); err != nil {
			e.logger.Warn("failed to broadcast token usage to some peers",
				"error", err,
			)
		}
	}

	if err := e.removeForceBootstrapFile(); err != nil {
		e.logger.Warn("failed to remove force bootstrap file",
			"error", err,
		)
	}

	if e.stateMachine != nil && token.ClusterUUID != "" {
		if err := e.stateMachine.ForceSetClusterUUID(token.ClusterUUID); err != nil {
			e.logger.Error("failed to set cluster UUID from force bootstrap token",
				"error", err,
				"token_uuid", token.ClusterUUID,
			)
			return fmt.Errorf("set cluster UUID: %w", err)
		}
		e.logger.Info("set cluster UUID from force bootstrap token",
			"cluster_uuid", token.ClusterUUID,
		)
	}

	e.logger.Info("force bootstrap execution complete - ready for bootstrap",
		"cluster_uuid", token.ClusterUUID,
		"single_use_consumed", token.SingleUse,
	)

	return nil
}

func (e *ForceBootstrapExecutor) validateTokenSignature(token *ForceBootstrapToken) bool {
	var key []byte

	if e.secrets != nil {
		if e.secrets.HasForceBootstrapKey() {
			key = e.secrets.ForceBootstrapKey()
		} else if e.config != nil && e.config.ForceBootstrap.RequireDedicatedKey {
			e.logger.Error("force bootstrap key required but not available")
			return false
		} else if e.secrets.HasFencingKey() {
			key = e.secrets.FencingKey()
		}
	}

	if len(key) == 0 {
		e.logger.Warn("no key available to verify force bootstrap token signature")
		return false
	}

	return VerifyForceBootstrapTokenSignature(token, key)
}

func (e *ForceBootstrapExecutor) removeForceBootstrapFile() error {
	if e.config == nil || e.config.ForceBootstrapFile == "" {
		return nil
	}

	return os.Remove(e.config.ForceBootstrapFile)
}

func (e *ForceBootstrapExecutor) GetUsageTracker() *TokenUsageTracker {
	return e.usageTracker
}
