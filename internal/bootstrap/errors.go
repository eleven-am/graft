package bootstrap

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/hashicorp/raft"
)

var (
	ErrNoCommittedConfig = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"no committed configuration available",
		nil,
		domain.WithCode("BOOTSTRAP_NO_COMMITTED_CONFIG"),
	)

	ErrTokenCorruption = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"fencing token corruption detected",
		nil,
		domain.WithCode("BOOTSTRAP_TOKEN_CORRUPTED"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrMetaNotFound = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"cluster metadata not found",
		nil,
		domain.WithCode("BOOTSTRAP_META_NOT_FOUND"),
	)

	ErrInvalidChecksum = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"cluster metadata checksum mismatch",
		nil,
		domain.WithCode("BOOTSTRAP_INVALID_CHECKSUM"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrElectionInProgress = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"fallback election already in progress",
		nil,
		domain.WithCode("BOOTSTRAP_ELECTION_IN_PROGRESS"),
	)

	ErrOrdinalZeroReachable = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"ordinal 0 is reachable, fallback election not needed",
		nil,
		domain.WithCode("BOOTSTRAP_ORDINAL_ZERO_REACHABLE"),
	)

	ErrNotLowestOrdinal = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"not the lowest ordinal, deferring to lower ordinal node",
		nil,
		domain.WithCode("BOOTSTRAP_NOT_LOWEST_ORDINAL"),
	)

	ErrNoToken = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"no fencing token found",
		nil,
		domain.WithCode("BOOTSTRAP_NO_TOKEN"),
	)

	ErrInvalidStateTransition = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"invalid state transition",
		nil,
		domain.WithCode("BOOTSTRAP_INVALID_STATE_TRANSITION"),
	)

	ErrTokenExpired = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"fencing token has expired",
		nil,
		domain.WithCode("BOOTSTRAP_TOKEN_EXPIRED"),
	)

	ErrTokenSignatureInvalid = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"fencing token signature invalid",
		nil,
		domain.WithCode("BOOTSTRAP_TOKEN_SIGNATURE_INVALID"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrNoFencingKey = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"fencing key not configured",
		nil,
		domain.WithCode("BOOTSTRAP_NO_FENCING_KEY"),
	)

	ErrProposalSignatureInvalid = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"proposal signature invalid",
		nil,
		domain.WithCode("BOOTSTRAP_PROPOSAL_SIGNATURE_INVALID"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrAckSignatureInvalid = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"ack signature invalid",
		nil,
		domain.WithCode("BOOTSTRAP_ACK_SIGNATURE_INVALID"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrStaleNodeDetected = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"stale single-node detected",
		nil,
		domain.WithCode("BOOTSTRAP_STALE_NODE"),
		domain.WithSeverity(domain.SeverityWarning),
	)

	ErrSplitBrainDetected = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"split-brain detected via cluster UUID mismatch",
		nil,
		domain.WithCode("BOOTSTRAP_SPLIT_BRAIN"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrAutoUnfenceNotReady = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"auto-unfence conditions not met",
		nil,
		domain.WithCode("BOOTSTRAP_UNFENCE_NOT_READY"),
	)

	ErrProberNotConfigured = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"peer prober not configured",
		nil,
		domain.WithCode("BOOTSTRAP_PROBER_NOT_CONFIGURED"),
	)

	ErrDeprecatedTokenVersion = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"force-bootstrap token version 0 is deprecated and unsafe",
		nil,
		domain.WithCode("BOOTSTRAP_DEPRECATED_TOKEN_VERSION"),
	)

	ErrInvalidForceBootstrapSignature = domain.NewDomainErrorWithCategory(
		domain.CategoryPermission,
		"force-bootstrap token signature invalid",
		nil,
		domain.WithCode("BOOTSTRAP_INVALID_FORCE_TOKEN_SIGNATURE"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrForceBootstrapExpired = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"force-bootstrap token has expired",
		nil,
		domain.WithCode("BOOTSTRAP_FORCE_TOKEN_EXPIRED"),
	)

	ErrForceBootstrapTargetMismatch = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"force-bootstrap token not targeted at this ordinal",
		nil,
		domain.WithCode("BOOTSTRAP_FORCE_TOKEN_TARGET_MISMATCH"),
	)

	ErrForceBootstrapAlreadyUsed = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"force-bootstrap token has already been used",
		nil,
		domain.WithCode("BOOTSTRAP_FORCE_TOKEN_ALREADY_USED"),
	)

	ErrForceBootstrapBlocked = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"force-bootstrap blocked by safety checks",
		nil,
		domain.WithCode("BOOTSTRAP_FORCE_BLOCKED"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrMissingVoterSetHash = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"force-bootstrap token missing voter set hash",
		nil,
		domain.WithCode("BOOTSTRAP_MISSING_VOTER_SET_HASH"),
	)

	ErrIgnoreQuorumNotConfirmed = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"ignore quorum check requires confirmation phrase",
		nil,
		domain.WithCode("BOOTSTRAP_IGNORE_QUORUM_NOT_CONFIRMED"),
	)

	ErrDedicatedKeyRequired = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"dedicated force-bootstrap key required but not configured",
		nil,
		domain.WithCode("BOOTSTRAP_DEDICATED_KEY_REQUIRED"),
	)

	ErrDRQuorumOverrideNotAllowed = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"DR quorum override requires AllowDRQuorumOverride=true in executor config",
		nil,
		domain.WithCode("BOOTSTRAP_DR_QUORUM_OVERRIDE_NOT_ALLOWED"),
	)

	ErrTLSNotConfigured = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"TLS not configured but required",
		nil,
		domain.WithCode("TRANSPORT_TLS_NOT_CONFIGURED"),
	)

	ErrCertificateRevoked = domain.NewDomainErrorWithCategory(
		domain.CategoryPermission,
		"certificate has been revoked",
		nil,
		domain.WithCode("TRANSPORT_CERT_REVOKED"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrMTLSRequired = domain.NewDomainErrorWithCategory(
		domain.CategoryPermission,
		"mTLS required: no client certificate",
		nil,
		domain.WithCode("TRANSPORT_MTLS_REQUIRED"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrProtocolVersionMismatch = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"protocol version mismatch",
		nil,
		domain.WithCode("TRANSPORT_PROTOCOL_VERSION_MISMATCH"),
	)

	ErrProtocolVersionMissing = domain.NewDomainErrorWithCategory(
		domain.CategoryConfiguration,
		"protocol version header required but missing",
		nil,
		domain.WithCode("TRANSPORT_PROTOCOL_VERSION_MISSING"),
	)

	ErrPeerIdentityMismatch = domain.NewDomainErrorWithCategory(
		domain.CategoryPermission,
		"peer certificate does not match expected identity",
		nil,
		domain.WithCode("TRANSPORT_PEER_IDENTITY_MISMATCH"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrConnectionFailed = domain.NewDomainErrorWithCategory(
		domain.CategoryNetwork,
		"failed to establish connection to peer",
		nil,
		domain.WithCode("TRANSPORT_CONNECTION_FAILED"),
	)

	ErrMembershipChangeInProgress = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"membership change already in progress for this node",
		nil,
		domain.WithCode("MEMBERSHIP_CHANGE_IN_PROGRESS"),
	)

	ErrCatchupTimeout = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"learner failed to catch up within timeout",
		nil,
		domain.WithCode("MEMBERSHIP_CATCHUP_TIMEOUT"),
	)

	ErrLeadershipTransferred = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"leadership transferred, retry removal on new leader",
		nil,
		domain.WithCode("MEMBERSHIP_LEADERSHIP_TRANSFERRED"),
	)

	ErrNotLeader = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"not the leader, cannot perform membership change",
		nil,
		domain.WithCode("MEMBERSHIP_NOT_LEADER"),
	)

	ErrServerNotFound = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"server not found in configuration",
		nil,
		domain.WithCode("MEMBERSHIP_SERVER_NOT_FOUND"),
	)

	ErrDuplicateServerID = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"server ID already exists in cluster configuration",
		nil,
		domain.WithCode("MEMBERSHIP_DUPLICATE_SERVER_ID"),
	)

	ErrDuplicateServerAddress = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"server address already exists in cluster configuration",
		nil,
		domain.WithCode("MEMBERSHIP_DUPLICATE_SERVER_ADDRESS"),
	)

	ErrNoPeersForRecovery = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"no healthy peers available for recovery",
		nil,
		domain.WithCode("RECOVERY_NO_PEERS"),
	)

	ErrRecoveryInProgress = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"recovery operation already in progress",
		nil,
		domain.WithCode("RECOVERY_IN_PROGRESS"),
	)

	ErrAllPeerRecoveryFailed = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"recovery failed from all peers",
		nil,
		domain.WithCode("RECOVERY_ALL_PEERS_FAILED"),
	)

	ErrCannotFetchMissingEntries = domain.NewDomainErrorWithCategory(
		domain.CategoryRaft,
		"cannot fetch missing entries from any peer",
		nil,
		domain.WithCode("RECOVERY_CANNOT_FETCH_ENTRIES"),
	)

	ErrSnapshotChecksumMismatch = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"snapshot checksum mismatch",
		nil,
		domain.WithCode("RECOVERY_SNAPSHOT_CHECKSUM_MISMATCH"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrWALCorrupted = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"WAL corruption detected",
		nil,
		domain.WithCode("RECOVERY_WAL_CORRUPTED"),
		domain.WithSeverity(domain.SeverityCritical),
	)

	ErrSnapshotCorrupted = domain.NewDomainErrorWithCategory(
		domain.CategoryStorage,
		"snapshot corruption detected",
		nil,
		domain.WithCode("RECOVERY_SNAPSHOT_CORRUPTED"),
		domain.WithSeverity(domain.SeverityCritical),
	)
)

type NoCommittedConfigError struct {
	Operation string
	Cause     error
	Message   string
}

func (e *NoCommittedConfigError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("no committed config for %s: %s", e.Operation, e.Message)
	}
	if e.Cause != nil {
		return fmt.Sprintf("no committed config for %s: %v", e.Operation, e.Cause)
	}
	return fmt.Sprintf("no committed config for %s", e.Operation)
}

func (e *NoCommittedConfigError) Unwrap() error {
	return e.Cause
}

func (e *NoCommittedConfigError) Is(target error) bool {
	return errors.Is(target, ErrNoCommittedConfig)
}

type InsufficientPeersError struct {
	Reachable     int
	Required      int
	TotalVoterSet int
	Message       string
}

func (e *InsufficientPeersError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("insufficient peers: %s (reachable=%d, required=%d, total=%d)",
			e.Message, e.Reachable, e.Required, e.TotalVoterSet)
	}
	return fmt.Sprintf("insufficient peers: reachable=%d, required=%d, total=%d",
		e.Reachable, e.Required, e.TotalVoterSet)
}

type QuorumNotReachedError struct {
	Votes         int
	Required      int
	TotalVoterSet int
}

func (e *QuorumNotReachedError) Error() string {
	return fmt.Sprintf("quorum not reached: votes=%d, required=%d, total=%d",
		e.Votes, e.Required, e.TotalVoterSet)
}

type StaleEpochError struct {
	ProposedEpoch uint64
	CurrentEpoch  uint64
}

func (e *StaleEpochError) Error() string {
	return fmt.Sprintf("stale epoch: proposed=%d, current=%d", e.ProposedEpoch, e.CurrentEpoch)
}

type CASConflictError struct {
	ExpectedEpoch uint64
	CurrentEpoch  uint64
	ProposedEpoch uint64
}

func (e *CASConflictError) Error() string {
	return fmt.Sprintf("CAS conflict: expected epoch %d but current is %d (proposed %d)",
		e.ExpectedEpoch, e.CurrentEpoch, e.ProposedEpoch)
}

type TokenCorruptionError struct {
	Path             string
	ExpectedChecksum uint32
	ActualChecksum   uint32
}

func (e *TokenCorruptionError) Error() string {
	return fmt.Sprintf("token corruption at %s: expected checksum %08x, got %08x",
		e.Path, e.ExpectedChecksum, e.ActualChecksum)
}

func (e *TokenCorruptionError) Is(target error) bool {
	return errors.Is(target, ErrTokenCorruption)
}

type VoterSetMismatchError struct {
	TokenVoterSetHash   []byte
	CurrentVoterSetHash []byte
	TokenQuorumSize     int
	CurrentVoterCount   int
	Message             string
}

func (e *VoterSetMismatchError) Error() string {
	return fmt.Sprintf("voter set mismatch: %s (token_hash=%s, current_hash=%s, token_quorum=%d, current_voters=%d)",
		e.Message,
		hex.EncodeToString(e.TokenVoterSetHash[:min(8, len(e.TokenVoterSetHash))]),
		hex.EncodeToString(e.CurrentVoterSetHash[:min(8, len(e.CurrentVoterSetHash))]),
		e.TokenQuorumSize,
		e.CurrentVoterCount)
}

type MetaCorruptionError struct {
	Path             string
	ExpectedChecksum uint32
	ActualChecksum   uint32
	Details          string
}

func (e *MetaCorruptionError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("cluster meta corruption at %s: %s (expected checksum %08x, got %08x)",
			e.Path, e.Details, e.ExpectedChecksum, e.ActualChecksum)
	}
	return fmt.Sprintf("cluster meta corruption at %s: expected checksum %08x, got %08x",
		e.Path, e.ExpectedChecksum, e.ActualChecksum)
}

func (e *MetaCorruptionError) Is(target error) bool {
	return errors.Is(target, ErrInvalidChecksum)
}

type StateTransitionError struct {
	From    NodeState
	To      NodeState
	Message string
}

func (e *StateTransitionError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("invalid state transition from %s to %s: %s", e.From, e.To, e.Message)
	}
	return fmt.Sprintf("invalid state transition from %s to %s", e.From, e.To)
}

func (e *StateTransitionError) Is(target error) bool {
	return errors.Is(target, ErrInvalidStateTransition)
}

// CertificateSANMismatchError indicates the presented certificate does not match the expected address
// for the peer as defined in the committed voter set. This is used when validating mTLS identities
// in the fallback election handshake.
type CertificateSANMismatchError struct {
	PeerID          raft.ServerID
	ExpectedAddress raft.ServerAddress
	CertDNSNames    []string
	CertIPAddresses []net.IP
	CertCN          string
}

func (e *CertificateSANMismatchError) Error() string {
	return fmt.Sprintf("certificate SAN mismatch for peer %s: expected %s, cert DNS=%v, IPs=%v, CN=%s",
		e.PeerID, e.ExpectedAddress, e.CertDNSNames, e.CertIPAddresses, e.CertCN)
}

type StaleNodeError struct {
	Status  string
	Reason  string
	Message string
}

func (e *StaleNodeError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("stale node (%s): %s - %s", e.Status, e.Reason, e.Message)
	}
	return fmt.Sprintf("stale node (%s): %s", e.Status, e.Reason)
}

func (e *StaleNodeError) Is(target error) bool {
	if e.Status == "split_brain" {
		return errors.Is(target, ErrSplitBrainDetected)
	}
	return errors.Is(target, ErrStaleNodeDetected)
}

type ForceBootstrapBlockedError struct {
	Reason         string
	ReachablePeers int
	CurrentEpoch   uint64
	Warnings       []string
}

func (e *ForceBootstrapBlockedError) Error() string {
	return fmt.Sprintf("force-bootstrap blocked: %s (reachable_peers=%d, current_epoch=%d)",
		e.Reason, e.ReachablePeers, e.CurrentEpoch)
}

func (e *ForceBootstrapBlockedError) Is(target error) bool {
	return errors.Is(target, ErrForceBootstrapBlocked)
}

type QuorumLossError struct {
	CurrentVoters     int
	PostRemovalVoters int
	ReachableVoters   int
	RequiredQuorum    int
}

func (e *QuorumLossError) Error() string {
	return fmt.Sprintf("removal would cause quorum loss: current_voters=%d, post_removal=%d, reachable=%d, required_quorum=%d",
		e.CurrentVoters, e.PostRemovalVoters, e.ReachableVoters, e.RequiredQuorum)
}

type AdmissionControlError struct {
	Reason    string
	LogSize   uint64
	DiskUsage float64
	Threshold float64
}

func (e *AdmissionControlError) Error() string {
	switch e.Reason {
	case "log size too large":
		return fmt.Sprintf("admission control failed: %s (size=%d, threshold=%d)", e.Reason, e.LogSize, uint64(e.Threshold))
	case "disk pressure":
		return fmt.Sprintf("admission control failed: %s (usage=%.2f%%, threshold=%.2f%%)", e.Reason, e.DiskUsage*100, e.Threshold*100)
	default:
		return fmt.Sprintf("admission control failed: %s", e.Reason)
	}
}

type VersionCompatibilityError struct {
	Field       string
	LocalValue  uint32
	RemoteValue uint32
	MinRequired uint32
	MaxAllowed  uint32
	Message     string
}

func (e *VersionCompatibilityError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("version compatibility failed: %s - %s (local=%d, remote=%d)",
			e.Field, e.Message, e.LocalValue, e.RemoteValue)
	}
	return fmt.Sprintf("version compatibility failed: %s (local=%d, remote=%d, min=%d, max=%d)",
		e.Field, e.LocalValue, e.RemoteValue, e.MinRequired, e.MaxAllowed)
}

type UnrecoverableError struct {
	WALError      error
	SnapshotError error
	PeerError     error
}

func (e *UnrecoverableError) Error() string {
	return fmt.Sprintf("unrecoverable state: WAL error=%v, snapshot error=%v, peer recovery error=%v",
		e.WALError, e.SnapshotError, e.PeerError)
}

func (e *UnrecoverableError) Unwrap() error {
	if e.PeerError != nil {
		return e.PeerError
	}
	if e.WALError != nil {
		return e.WALError
	}
	return e.SnapshotError
}

type LogIndexMismatchError struct {
	Expected uint64
	Got      uint64
	Peer     raft.ServerID
}

func (e *LogIndexMismatchError) Error() string {
	return fmt.Sprintf("log index mismatch from peer %s: expected %d, got %d",
		e.Peer, e.Expected, e.Got)
}

type TermRegressionError struct {
	Index        uint64
	PreviousTerm uint64
	CurrentTerm  uint64
	Peer         raft.ServerID
}

func (e *TermRegressionError) Error() string {
	return fmt.Sprintf("term regression at index %d from peer %s: term went from %d to %d",
		e.Index, e.Peer, e.PreviousTerm, e.CurrentTerm)
}

type LogConflictError struct {
	Index          uint64
	LocalTerm      uint64
	ExpectedTerm   uint64
	Peer           raft.ServerID
	ConflictAction string
}

func (e *LogConflictError) Error() string {
	return fmt.Sprintf("log conflict at index %d: local term %d != expected term %d from peer %s (action: %s)",
		e.Index, e.LocalTerm, e.ExpectedTerm, e.Peer, e.ConflictAction)
}
