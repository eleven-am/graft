package bootstrap

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"

	pb "github.com/eleven-am/graft/internal/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/metadata"
)

func appendProtocolVersion(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-bootstrap-protocol-version", strconv.Itoa(CurrentProtocolVersion))
}

func (t *SecureTransport) ProposeToken(ctx context.Context, addr string, proposal *FencingProposal) (*FencingAck, error) {
	conn, err := t.getConnection(addr)
	if err != nil {
		return nil, err
	}

	client := pb.NewBootstrapServiceClient(conn)
	req := proposalToProto(proposal)

	resp, err := client.ProposeToken(appendProtocolVersion(ctx), req)
	if err != nil {
		t.closeConnection(addr)
		return nil, err
	}

	return protoToAck(resp), nil
}

func (t *SecureTransport) RequestVote(ctx context.Context, addr raft.ServerAddress, req *VoteRequest) (*VoteResponse, error) {
	conn, err := t.getConnection(string(addr))
	if err != nil {
		return nil, err
	}

	client := pb.NewBootstrapServiceClient(conn)
	protoReq := voteRequestToProto(req)

	resp, err := client.RequestVote(appendProtocolVersion(ctx), protoReq)
	if err != nil {
		t.closeConnection(string(addr))
		return nil, err
	}

	return protoToVoteResponse(resp), nil
}

func (t *SecureTransport) GetClusterMeta(ctx context.Context, addr string) (*ClusterMeta, error) {
	conn, err := t.getConnection(addr)
	if err != nil {
		return nil, err
	}

	client := pb.NewBootstrapServiceClient(conn)

	resp, err := client.GetClusterMeta(appendProtocolVersion(ctx), &pb.GetClusterMetaRequest{})
	if err != nil {
		t.closeConnection(addr)
		return nil, err
	}

	return protoToClusterMeta(resp), nil
}

func (t *SecureTransport) GetFencingEpoch(ctx context.Context, addr string) (uint64, error) {
	conn, err := t.getConnection(addr)
	if err != nil {
		return 0, err
	}

	client := pb.NewBootstrapServiceClient(conn)

	resp, err := client.GetFencingEpoch(appendProtocolVersion(ctx), &pb.GetFencingEpochRequest{})
	if err != nil {
		t.closeConnection(addr)
		return 0, err
	}

	return resp.Epoch, nil
}

func (t *SecureTransport) GetLastIndex(ctx context.Context, addr string) (uint64, error) {
	conn, err := t.getConnection(addr)
	if err != nil {
		return 0, err
	}

	client := pb.NewBootstrapServiceClient(conn)

	resp, err := client.GetLastIndex(appendProtocolVersion(ctx), &pb.GetLastIndexRequest{})
	if err != nil {
		t.closeConnection(addr)
		return 0, err
	}

	return resp.LastIndex, nil
}

func (t *SecureTransport) NotifyTokenUsage(ctx context.Context, addr string, notification *TokenUsageNotification) error {
	conn, err := t.getConnection(addr)
	if err != nil {
		return err
	}

	client := pb.NewBootstrapServiceClient(conn)
	req := notificationToProto(notification)

	_, err = client.NotifyTokenUsage(appendProtocolVersion(ctx), req)
	if err != nil {
		t.closeConnection(addr)
		return err
	}

	return nil
}

func (t *SecureTransport) GetTokenUsage(ctx context.Context, addr string, tokenHash string) (*TokenUsageResponse, error) {
	conn, err := t.getConnection(addr)
	if err != nil {
		return nil, err
	}

	client := pb.NewBootstrapServiceClient(conn)

	resp, err := client.GetTokenUsage(appendProtocolVersion(ctx), &pb.GetTokenUsageRequest{TokenHash: tokenHash})
	if err != nil {
		t.closeConnection(addr)
		return nil, err
	}

	return protoToTokenUsageResponse(resp), nil
}

func proposalToProto(p *FencingProposal) *pb.ProposeTokenRequest {
	return &pb.ProposeTokenRequest{
		ProposedEpoch:   p.ProposedEpoch,
		ExpectedEpoch:   p.ExpectedEpoch,
		ProposerId:      string(p.ProposerID),
		ProposerAddress: string(p.ProposerAddress),
		VoterSetHash:    hex.EncodeToString(p.VoterSetHash[:]),
		QuorumSize:      int32(p.QuorumSize),
		Mode:            fencingModeToInt32(p.Mode),
		Timestamp:       p.Timestamp.UnixNano(),
		Signature:       p.Signature,
	}
}

func protoToAck(resp *pb.ProposeTokenResponse) *FencingAck {
	return &FencingAck{
		ProposedEpoch: resp.ProposedEpoch,
		VoterID:       raft.ServerID(resp.VoterId),
		Accepted:      resp.Accepted,
		CurrentEpoch:  resp.CurrentEpoch,
		RejectReason:  resp.RejectReason,
		Timestamp:     time.Unix(0, resp.Timestamp),
		Signature:     resp.Signature,
	}
}

func voteRequestToProto(req *VoteRequest) *pb.VoteRequestProto {
	return &pb.VoteRequestProto{
		CandidateId:      string(req.CandidateID),
		CandidateOrdinal: int32(req.CandidateOrdinal),
		ElectionReason:   req.ElectionReason,
		Timestamp:        req.Timestamp.UnixNano(),
		VoterSetHash:     req.VoterSetHash,
		RequiredQuorum:   int32(req.RequiredQuorum),
		Signature:        req.Signature,
	}
}

func protoToVoteResponse(resp *pb.VoteResponseProto) *VoteResponse {
	return &VoteResponse{
		VoteGranted:  resp.VoteGranted,
		Reason:       resp.Reason,
		VoterID:      raft.ServerID(resp.VoterId),
		VoterSetHash: resp.VoterSetHash,
		Signature:    resp.Signature,
	}
}

func protoToClusterMeta(p *pb.ClusterMetaProto) *ClusterMeta {
	return &ClusterMeta{
		Version:       p.Version,
		ClusterUUID:   p.ClusterUuid,
		ServerID:      raft.ServerID(p.ServerId),
		ServerAddress: raft.ServerAddress(p.ServerAddress),
		Ordinal:       int(p.Ordinal),
		State:         int32ToNodeState(p.State),
		BootstrapTime: time.Unix(0, p.BootstrapTime),
		Checksum:      p.Checksum,
	}
}

func clusterMetaToProto(m *ClusterMeta) *pb.ClusterMetaProto {
	return &pb.ClusterMetaProto{
		Version:       m.Version,
		ClusterUuid:   m.ClusterUUID,
		ServerId:      string(m.ServerID),
		ServerAddress: string(m.ServerAddress),
		Ordinal:       int32(m.Ordinal),
		State:         nodeStateToInt32(m.State),
		BootstrapTime: m.BootstrapTime.UnixNano(),
		Checksum:      m.Checksum,
	}
}

func notificationToProto(n *TokenUsageNotification) *pb.TokenUsageNotificationProto {
	return &pb.TokenUsageNotificationProto{
		Version:   n.Version,
		TokenHash: n.TokenHash,
		UsedAt:    n.UsedAt.UnixNano(),
		UsedBy:    n.UsedBy,
		NewEpoch:  n.NewEpoch,
		Timestamp: n.Timestamp.UnixNano(),
		SenderId:  n.SenderID,
		Signature: n.Signature,
	}
}

func protoToTokenUsageResponse(p *pb.TokenUsageResponseProto) *TokenUsageResponse {
	return &TokenUsageResponse{
		Used:        p.Used,
		UsedBy:      p.UsedBy,
		UsedAt:      time.Unix(0, p.UsedAt),
		TokenHash:   p.TokenHash,
		ResponderID: p.ResponderId,
		Timestamp:   time.Unix(0, p.Timestamp),
		Signature:   p.Signature,
	}
}

func protoToNotification(p *pb.TokenUsageNotificationProto) *TokenUsageNotification {
	return &TokenUsageNotification{
		Version:   p.Version,
		TokenHash: p.TokenHash,
		UsedAt:    time.Unix(0, p.UsedAt),
		UsedBy:    p.UsedBy,
		NewEpoch:  p.NewEpoch,
		Timestamp: time.Unix(0, p.Timestamp),
		SenderID:  p.SenderId,
		Signature: p.Signature,
	}
}

func protoToProposal(req *pb.ProposeTokenRequest) *FencingProposal {
	var voterSetHash [32]byte
	hashBytes, _ := hex.DecodeString(req.VoterSetHash)
	copy(voterSetHash[:], hashBytes)

	return &FencingProposal{
		ProposedEpoch:   req.ProposedEpoch,
		ExpectedEpoch:   req.ExpectedEpoch,
		ProposerID:      raft.ServerID(req.ProposerId),
		ProposerAddress: raft.ServerAddress(req.ProposerAddress),
		VoterSetHash:    voterSetHash,
		QuorumSize:      int(req.QuorumSize),
		Mode:            int32ToFencingMode(req.Mode),
		Timestamp:       time.Unix(0, req.Timestamp),
		Signature:       req.Signature,
	}
}

func ackToProto(a *FencingAck) *pb.ProposeTokenResponse {
	return &pb.ProposeTokenResponse{
		ProposedEpoch: a.ProposedEpoch,
		VoterId:       string(a.VoterID),
		Accepted:      a.Accepted,
		CurrentEpoch:  a.CurrentEpoch,
		RejectReason:  a.RejectReason,
		Timestamp:     a.Timestamp.UnixNano(),
		Signature:     a.Signature,
	}
}

func protoToVoteRequest(req *pb.VoteRequestProto) *VoteRequest {
	return &VoteRequest{
		CandidateID:      raft.ServerID(req.CandidateId),
		CandidateOrdinal: int(req.CandidateOrdinal),
		ElectionReason:   req.ElectionReason,
		Timestamp:        time.Unix(0, req.Timestamp),
		VoterSetHash:     req.VoterSetHash,
		RequiredQuorum:   int(req.RequiredQuorum),
		Signature:        req.Signature,
	}
}

func voteResponseToProto(resp *VoteResponse) *pb.VoteResponseProto {
	return &pb.VoteResponseProto{
		VoteGranted:  resp.VoteGranted,
		Reason:       resp.Reason,
		VoterId:      string(resp.VoterID),
		VoterSetHash: resp.VoterSetHash,
		Signature:    resp.Signature,
	}
}

func tokenUsageResponseToProto(r *TokenUsageResponse) *pb.TokenUsageResponseProto {
	return &pb.TokenUsageResponseProto{
		Used:        r.Used,
		UsedBy:      r.UsedBy,
		UsedAt:      r.UsedAt.UnixNano(),
		TokenHash:   r.TokenHash,
		ResponderId: r.ResponderID,
		Timestamp:   r.Timestamp.UnixNano(),
		Signature:   r.Signature,
	}
}

func fencingModeToInt32(mode FencingMode) int32 {
	switch mode {
	case FencingModePrimary:
		return 0
	case FencingModeFallback:
		return 1
	case FencingModeRecovery:
		return 2
	default:
		return 0
	}
}

func int32ToFencingMode(val int32) FencingMode {
	switch val {
	case 0:
		return FencingModePrimary
	case 1:
		return FencingModeFallback
	case 2:
		return FencingModeRecovery
	default:
		return FencingModePrimary
	}
}

func nodeStateToInt32(state NodeState) int32 {
	switch state {
	case StateUninitialized:
		return 0
	case StateBootstrapping:
		return 1
	case StateJoining:
		return 2
	case StateReady:
		return 3
	case StateRecovering:
		return 4
	case StateDegraded:
		return 5
	case StateFenced:
		return 6
	case StateAwaitingWipe:
		return 7
	default:
		return 0
	}
}

func int32ToNodeState(val int32) NodeState {
	switch val {
	case 0:
		return StateUninitialized
	case 1:
		return StateBootstrapping
	case 2:
		return StateJoining
	case 3:
		return StateReady
	case 4:
		return StateRecovering
	case 5:
		return StateDegraded
	case 6:
		return StateFenced
	case 7:
		return StateAwaitingWipe
	default:
		return StateUninitialized
	}
}
