package bootstrap

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	pb "github.com/eleven-am/graft/internal/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

const CurrentProtocolVersion = 1

type RaftStateProvider interface {
	LastIndex() uint64
}

type BootstrapServerDeps struct {
	Config          *BootstrapConfig
	Fencing         *FencingManager
	MetaStore       MetaStore
	MembershipStore MembershipStore
	UsageTracker    *TokenUsageTracker
	LocalMeta       func() *ClusterMeta
	LocalRaft       RaftStateProvider
	Secrets         *SecretsManager
	TLSConfig       *tls.Config
	Logger          *slog.Logger
}

type BootstrapServer struct {
	pb.UnimplementedBootstrapServiceServer

	config          *BootstrapConfig
	fencing         *FencingManager
	metaStore       MetaStore
	membershipStore MembershipStore
	usageTracker    *TokenUsageTracker
	localMeta       func() *ClusterMeta
	localRaft       RaftStateProvider
	secrets         *SecretsManager
	tlsConfig       *tls.Config
	logger          *slog.Logger
}

func NewBootstrapServer(deps BootstrapServerDeps) *BootstrapServer {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &BootstrapServer{
		config:          deps.Config,
		fencing:         deps.Fencing,
		metaStore:       deps.MetaStore,
		membershipStore: deps.MembershipStore,
		usageTracker:    deps.UsageTracker,
		localMeta:       deps.LocalMeta,
		localRaft:       deps.LocalRaft,
		secrets:         deps.Secrets,
		tlsConfig:       deps.TLSConfig,
		logger:          deps.Logger,
	}
}

func (s *BootstrapServer) ProposeToken(ctx context.Context, req *pb.ProposeTokenRequest) (*pb.ProposeTokenResponse, error) {
	s.logger.Debug("received ProposeToken request",
		slog.String("proposer_id", req.ProposerId),
		slog.Uint64("proposed_epoch", req.ProposedEpoch))

	if err := s.checkProtocolVersion(ctx); err != nil {
		s.logger.Warn("ProposeToken rejected: protocol version check failed",
			slog.String("proposer_id", req.ProposerId),
			slog.String("error", err.Error()))
		return &pb.ProposeTokenResponse{
			Accepted:     false,
			RejectReason: fmt.Sprintf("protocol version check failed: %v", err),
		}, nil
	}

	if _, err := s.requireMTLS(ctx); err != nil {
		s.logger.Warn("ProposeToken rejected: mTLS validation failed",
			slog.String("proposer_id", req.ProposerId),
			slog.String("error", err.Error()))
		return &pb.ProposeTokenResponse{
			Accepted:     false,
			RejectReason: fmt.Sprintf("mTLS validation failed: %v", err),
		}, nil
	}

	proposal := protoToProposal(req)

	if s.secrets != nil {
		key := s.secrets.FencingKey()
		if len(key) > 0 && !VerifyProposalSignature(proposal, key) {
			s.logger.Warn("invalid proposal signature",
				slog.String("proposer_id", req.ProposerId))
			return nil, ErrProposalSignatureInvalid
		}
	}

	ack, err := s.fencing.HandleProposal(proposal, s.getLocalServerID())
	if err != nil {
		s.logger.Error("failed to handle proposal",
			slog.String("error", err.Error()))
		return nil, err
	}

	if s.secrets != nil {
		key := s.secrets.FencingKey()
		if len(key) > 0 {
			if err := SignAck(ack, key); err != nil {
				s.logger.Error("failed to sign ack",
					slog.String("error", err.Error()))
				return nil, err
			}
		}
	}

	return ackToProto(ack), nil
}

func (s *BootstrapServer) RequestVote(ctx context.Context, req *pb.VoteRequestProto) (*pb.VoteResponseProto, error) {
	s.logger.Debug("received RequestVote request",
		slog.String("candidate_id", req.CandidateId))

	if err := s.checkProtocolVersion(ctx); err != nil {
		s.logger.Warn("RequestVote rejected: protocol version check failed",
			slog.String("candidate_id", req.CandidateId),
			slog.String("error", err.Error()))
		return voteResponseToProto(&VoteResponse{
			VoteGranted: false,
			Reason:      fmt.Sprintf("protocol version check failed: %v", err),
			VoterID:     s.getLocalServerID(),
		}), nil
	}

	peerCert, err := s.requireMTLS(ctx)
	if err != nil {
		s.logger.Warn("RequestVote rejected: mTLS validation failed",
			slog.String("candidate_id", req.CandidateId),
			slog.String("error", err.Error()))
		return voteResponseToProto(&VoteResponse{
			VoteGranted: false,
			Reason:      fmt.Sprintf("mTLS validation failed: %v", err),
			VoterID:     s.getLocalServerID(),
		}), nil
	}

	voteReq := protoToVoteRequest(req)

	resp := s.handleVoteRequest(ctx, voteReq, peerCert)

	if s.secrets != nil {
		key := s.secrets.FencingKey()
		if len(key) > 0 {
			if err := SignVoteResponse(resp, key); err != nil {
				s.logger.Error("failed to sign vote response",
					slog.String("error", err.Error()))
			}
		}
	}

	return voteResponseToProto(resp), nil
}

func (s *BootstrapServer) handleVoteRequest(ctx context.Context, req *VoteRequest, peerCert *x509.Certificate) *VoteResponse {
	localMeta := s.localMeta()
	if localMeta == nil {
		return &VoteResponse{
			VoteGranted: false,
			Reason:      "local metadata not available",
			VoterID:     s.getLocalServerID(),
		}
	}

	if s.tlsConfig != nil && s.tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert {
		if peerCert == nil {
			s.logger.Warn("vote request rejected: mTLS required but no client certificate",
				slog.String("candidate_id", string(req.CandidateID)))
			return &VoteResponse{
				VoteGranted: false,
				Reason:      "mTLS required: no client certificate",
				VoterID:     localMeta.ServerID,
			}
		}
	}

	if s.secrets != nil && s.secrets.HasFencingKey() {
		if !VerifyVoteRequestSignature(req, s.secrets.FencingKey()) {
			s.logger.Warn("vote request rejected: invalid signature",
				slog.String("candidate_id", string(req.CandidateID)))
			return &VoteResponse{
				VoteGranted: false,
				Reason:      "invalid signature",
				VoterID:     localMeta.ServerID,
			}
		}
	}

	voterSet, err := s.getCommittedVoterSet()
	if err == nil && len(voterSet) > 0 {
		localHash := HashVoterSet(voterSet)
		if !bytes.Equal(localHash, req.VoterSetHash) {
			s.logger.Warn("vote request rejected: voter set hash mismatch",
				slog.String("candidate_id", string(req.CandidateID)))
			return &VoteResponse{
				VoteGranted:  false,
				Reason:       "voter set mismatch",
				VoterID:      localMeta.ServerID,
				VoterSetHash: localHash,
			}
		}

		if !s.isCandidateInVoterSet(req.CandidateID, voterSet) {
			s.logger.Warn("vote request rejected: candidate not in committed voter set",
				slog.String("candidate_id", string(req.CandidateID)))
			return &VoteResponse{
				VoteGranted: false,
				Reason:      "candidate not in committed voter set",
				VoterID:     localMeta.ServerID,
			}
		}

		if peerCert != nil {
			if err := s.validatePeerCertAgainstVoterSet(req.CandidateID, peerCert, voterSet); err != nil {
				s.logger.Warn("vote request rejected: certificate validation failed",
					slog.String("candidate_id", string(req.CandidateID)),
					slog.String("error", err.Error()))
				return &VoteResponse{
					VoteGranted: false,
					Reason:      fmt.Sprintf("certificate validation failed: %v", err),
					VoterID:     localMeta.ServerID,
				}
			}
		}
	}

	if localMeta.State == StateReady || localMeta.State == StateRecovering {
		s.logger.Info("vote request rejected: already in cluster",
			slog.String("candidate_id", string(req.CandidateID)),
			slog.String("local_state", string(localMeta.State)))
		return &VoteResponse{
			VoteGranted: false,
			Reason:      "already in cluster",
			VoterID:     localMeta.ServerID,
		}
	}

	s.logger.Info("granted vote",
		slog.String("candidate_id", string(req.CandidateID)))

	return &VoteResponse{
		VoteGranted:  true,
		Reason:       "vote granted",
		VoterID:      localMeta.ServerID,
		VoterSetHash: req.VoterSetHash,
	}
}

func (s *BootstrapServer) getCommittedVoterSet() ([]VoterInfo, error) {
	if s.membershipStore == nil {
		return nil, fmt.Errorf("membership store not configured")
	}

	config, err := s.membershipStore.GetLastCommittedConfiguration()
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

func (s *BootstrapServer) isCandidateInVoterSet(candidateID raft.ServerID, voterSet []VoterInfo) bool {
	for _, v := range voterSet {
		if v.ServerID == candidateID {
			return true
		}
	}
	return false
}

func (s *BootstrapServer) validatePeerCertAgainstVoterSet(
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

func (s *BootstrapServer) GetClusterMeta(ctx context.Context, req *pb.GetClusterMetaRequest) (*pb.ClusterMetaProto, error) {
	s.logger.Debug("received GetClusterMeta request")

	if err := s.checkProtocolVersion(ctx); err != nil {
		s.logger.Warn("GetClusterMeta rejected: protocol version check failed",
			slog.String("error", err.Error()))
		return nil, err
	}

	if _, err := s.requireMTLS(ctx); err != nil {
		s.logger.Warn("GetClusterMeta rejected: mTLS validation failed",
			slog.String("error", err.Error()))
		return nil, ErrMTLSRequired
	}

	meta := s.localMeta()
	if meta == nil {
		return nil, ErrMetaNotFound
	}

	return clusterMetaToProto(meta), nil
}

func (s *BootstrapServer) GetFencingEpoch(ctx context.Context, req *pb.GetFencingEpochRequest) (*pb.GetFencingEpochResponse, error) {
	s.logger.Debug("received GetFencingEpoch request")

	if err := s.checkProtocolVersion(ctx); err != nil {
		s.logger.Warn("GetFencingEpoch rejected: protocol version check failed",
			slog.String("error", err.Error()))
		return nil, err
	}

	if _, err := s.requireMTLS(ctx); err != nil {
		s.logger.Warn("GetFencingEpoch rejected: mTLS validation failed",
			slog.String("error", err.Error()))
		return nil, ErrMTLSRequired
	}

	epoch := s.fencing.CurrentEpoch()
	return &pb.GetFencingEpochResponse{Epoch: epoch}, nil
}

func (s *BootstrapServer) GetLastIndex(ctx context.Context, req *pb.GetLastIndexRequest) (*pb.GetLastIndexResponse, error) {
	s.logger.Debug("received GetLastIndex request")

	if err := s.checkProtocolVersion(ctx); err != nil {
		s.logger.Warn("GetLastIndex rejected: protocol version check failed",
			slog.String("error", err.Error()))
		return nil, err
	}

	if _, err := s.requireMTLS(ctx); err != nil {
		s.logger.Warn("GetLastIndex rejected: mTLS validation failed",
			slog.String("error", err.Error()))
		return nil, ErrMTLSRequired
	}

	if s.localRaft == nil {
		return &pb.GetLastIndexResponse{LastIndex: 0}, nil
	}

	return &pb.GetLastIndexResponse{LastIndex: s.localRaft.LastIndex()}, nil
}

func (s *BootstrapServer) NotifyTokenUsage(ctx context.Context, req *pb.TokenUsageNotificationProto) (*pb.NotifyTokenUsageResponse, error) {
	s.logger.Debug("received NotifyTokenUsage request",
		slog.String("token_hash", req.TokenHash),
		slog.String("used_by", req.UsedBy))

	if err := s.checkProtocolVersion(ctx); err != nil {
		s.logger.Warn("NotifyTokenUsage rejected: protocol version check failed",
			slog.String("error", err.Error()))
		return nil, err
	}

	if _, err := s.requireMTLS(ctx); err != nil {
		s.logger.Warn("NotifyTokenUsage rejected: mTLS validation failed",
			slog.String("error", err.Error()))
		return nil, ErrMTLSRequired
	}

	notification := protoToNotification(req)

	if s.usageTracker != nil {
		if err := s.usageTracker.HandleUsageNotification(notification); err != nil {
			s.logger.Warn("failed to handle usage notification",
				slog.String("error", err.Error()))
		}
	}

	return &pb.NotifyTokenUsageResponse{Acknowledged: true}, nil
}

func (s *BootstrapServer) GetTokenUsage(ctx context.Context, req *pb.GetTokenUsageRequest) (*pb.TokenUsageResponseProto, error) {
	s.logger.Debug("received GetTokenUsage request",
		slog.String("token_hash", req.TokenHash))

	if err := s.checkProtocolVersion(ctx); err != nil {
		s.logger.Warn("GetTokenUsage rejected: protocol version check failed",
			slog.String("error", err.Error()))
		return nil, err
	}

	if _, err := s.requireMTLS(ctx); err != nil {
		s.logger.Warn("GetTokenUsage rejected: mTLS validation failed",
			slog.String("error", err.Error()))
		return nil, ErrMTLSRequired
	}

	if s.usageTracker != nil {
		usage := s.usageTracker.GetUsage(req.TokenHash)
		return tokenUsageResponseToProto(usage), nil
	}

	resp := &TokenUsageResponse{
		Used:        false,
		TokenHash:   req.TokenHash,
		ResponderID: string(s.getLocalServerID()),
		Timestamp:   time.Now(),
	}

	if s.secrets != nil && s.secrets.HasFencingKey() {
		resp.Signature = SignUsageResponse(resp, s.secrets.FencingKey())
	}

	return tokenUsageResponseToProto(resp), nil
}

func (s *BootstrapServer) getLocalServerID() raft.ServerID {
	meta := s.localMeta()
	if meta == nil {
		return ""
	}
	return meta.ServerID
}

func (s *BootstrapServer) requireMTLS(ctx context.Context) (*x509.Certificate, error) {
	if s.config != nil && s.config.TLS.AllowInsecure {
		return nil, nil
	}

	if s.tlsConfig == nil {
		return nil, ErrMTLSRequired
	}

	if s.tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		return nil, ErrMTLSRequired
	}

	peerCert, err := extractPeerCert(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract peer cert: %w", err)
	}

	if peerCert == nil {
		return nil, ErrMTLSRequired
	}

	voterSet, err := s.getCommittedVoterSet()
	if err == nil && len(voterSet) > 0 {
		validated := false
		for _, voter := range voterSet {
			if err := s.validatePeerCertAgainstVoterSet(voter.ServerID, peerCert, voterSet); err == nil {
				validated = true
				break
			}
		}
		if !validated {
			return nil, fmt.Errorf("peer cert not in voter set")
		}
	}

	return peerCert, nil
}

func extractPeerCert(ctx context.Context) (*x509.Certificate, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, ErrPeerIdentityMismatch
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, ErrTLSNotConfigured
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, ErrPeerIdentityMismatch
	}

	return tlsInfo.State.PeerCertificates[0], nil
}

func (s *BootstrapServer) checkProtocolVersion(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		if s.config != nil && s.config.RequireProtocolVersion {
			s.logger.Warn("protocol version header required but no metadata present")
			return ErrProtocolVersionMissing
		}
		return nil
	}

	versions := md.Get("x-bootstrap-protocol-version")
	if len(versions) == 0 {
		if s.config != nil && s.config.RequireProtocolVersion {
			s.logger.Warn("protocol version header required but missing")
			return ErrProtocolVersionMissing
		}
		return nil
	}

	peerVersion, err := strconv.Atoi(versions[0])
	if err != nil {
		s.logger.Warn("invalid protocol version in request",
			slog.String("version", versions[0]),
			slog.String("error", err.Error()))
		return fmt.Errorf("invalid protocol version: %w", err)
	}

	if peerVersion > CurrentProtocolVersion {
		s.logger.Warn("protocol version mismatch",
			slog.Int("peer_version", peerVersion),
			slog.Int("local_version", CurrentProtocolVersion))
		return fmt.Errorf("%w: peer=%d, local=%d", ErrProtocolVersionMismatch, peerVersion, CurrentProtocolVersion)
	}

	return nil
}
