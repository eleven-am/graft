package bootstrap

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
)

type PeerInfo struct {
	ServerID raft.ServerID
	Address  raft.ServerAddress
}

type VoteRequest struct {
	CandidateID    raft.ServerID
	ElectionReason string
	Timestamp      time.Time
	VoterSetHash   []byte
	RequiredQuorum int
	Signature      []byte
}

type VoteResponse struct {
	VoteGranted  bool
	Reason       string
	VoterID      raft.ServerID
	VoterSetHash []byte
	Signature    []byte
}

type BootstrapTransport interface {
	ProposeToken(ctx context.Context, addr string, proposal *FencingProposal) (*FencingAck, error)
	RequestVote(ctx context.Context, addr raft.ServerAddress, req *VoteRequest) (*VoteResponse, error)
	GetClusterMeta(ctx context.Context, addr string) (*ClusterMeta, error)
	GetFencingEpoch(ctx context.Context, addr string) (uint64, error)
	GetLastIndex(ctx context.Context, addr string) (uint64, error)
	NotifyTokenUsage(ctx context.Context, addr string, notification *TokenUsageNotification) error
	GetTokenUsage(ctx context.Context, addr string, tokenHash string) (*TokenUsageResponse, error)
}

type MembershipStore interface {
	GetLastCommittedConfiguration() (*raft.Configuration, error)
}

func HashVoterSet(voters []VoterInfo) []byte {
	sorted := make([]VoterInfo, len(voters))
	copy(sorted, voters)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ServerID < sorted[j].ServerID
	})

	h := sha256.New()
	for _, v := range sorted {
		h.Write([]byte(v.ServerID))
		h.Write([]byte(v.Address))
	}
	return h.Sum(nil)
}

func (r *VoteRequest) computeSignatureData() []byte {
	h := sha256.New()
	h.Write([]byte(r.CandidateID))
	h.Write([]byte(r.ElectionReason))
	binary.Write(h, binary.BigEndian, r.Timestamp.UnixNano())
	h.Write(r.VoterSetHash)
	binary.Write(h, binary.BigEndian, int32(r.RequiredQuorum))
	return h.Sum(nil)
}

func SignVoteRequest(req *VoteRequest, key []byte) error {
	if len(key) == 0 {
		return nil
	}

	data := req.computeSignatureData()
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	req.Signature = mac.Sum(nil)
	return nil
}

func VerifyVoteRequestSignature(req *VoteRequest, key []byte) bool {
	if len(key) == 0 {
		return true
	}

	if len(req.Signature) == 0 {
		return false
	}

	data := req.computeSignatureData()
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	expected := mac.Sum(nil)

	return hmac.Equal(req.Signature, expected)
}

func (r *VoteResponse) computeSignatureData() []byte {
	h := sha256.New()
	if r.VoteGranted {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	h.Write([]byte(r.Reason))
	h.Write([]byte(r.VoterID))
	h.Write(r.VoterSetHash)
	return h.Sum(nil)
}

func SignVoteResponse(resp *VoteResponse, key []byte) error {
	if len(key) == 0 {
		return nil
	}

	data := resp.computeSignatureData()
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	resp.Signature = mac.Sum(nil)
	return nil
}

func VerifyVoteResponseSignature(resp *VoteResponse, key []byte) bool {
	if len(key) == 0 {
		return true
	}

	if len(resp.Signature) == 0 {
		return false
	}

	data := resp.computeSignatureData()
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	expected := mac.Sum(nil)

	return hmac.Equal(resp.Signature, expected)
}

type MockBootstrapTransport struct {
	voteResponses   map[raft.ServerAddress]*VoteResponse
	voteErrors      map[raft.ServerAddress]error
	clusterMetas    map[string]*ClusterMeta
	fencingEpochs   map[string]uint64
	lastIndexes     map[string]uint64
	tokenUsages     map[string]*TokenUsageResponse
	notifyUsageFn   func(addr string, notification *TokenUsageNotification) error
	proposeTokenFn  func(addr string, proposal *FencingProposal) (*FencingAck, error)
	proposeTokenAck map[string]*FencingAck
}

func NewMockBootstrapTransport() *MockBootstrapTransport {
	return &MockBootstrapTransport{
		voteResponses:   make(map[raft.ServerAddress]*VoteResponse),
		voteErrors:      make(map[raft.ServerAddress]error),
		clusterMetas:    make(map[string]*ClusterMeta),
		fencingEpochs:   make(map[string]uint64),
		lastIndexes:     make(map[string]uint64),
		tokenUsages:     make(map[string]*TokenUsageResponse),
		proposeTokenAck: make(map[string]*FencingAck),
	}
}

func (m *MockBootstrapTransport) SetVoteResponse(addr raft.ServerAddress, resp *VoteResponse) {
	m.voteResponses[addr] = resp
}

func (m *MockBootstrapTransport) SetVoteError(addr raft.ServerAddress, err error) {
	m.voteErrors[addr] = err
}

func (m *MockBootstrapTransport) SetClusterMeta(addr string, meta *ClusterMeta) {
	m.clusterMetas[addr] = meta
}

func (m *MockBootstrapTransport) SetFencingEpoch(addr string, epoch uint64) {
	m.fencingEpochs[addr] = epoch
}

func (m *MockBootstrapTransport) SetLastIndex(addr string, index uint64) {
	m.lastIndexes[addr] = index
}

func (m *MockBootstrapTransport) SetTokenUsage(addr string, resp *TokenUsageResponse) {
	m.tokenUsages[addr] = resp
}

func (m *MockBootstrapTransport) SetNotifyUsageFunc(fn func(addr string, notification *TokenUsageNotification) error) {
	m.notifyUsageFn = fn
}

func (m *MockBootstrapTransport) SetProposeTokenAck(addr string, ack *FencingAck) {
	m.proposeTokenAck[addr] = ack
}

func (m *MockBootstrapTransport) SetProposeTokenFunc(fn func(addr string, proposal *FencingProposal) (*FencingAck, error)) {
	m.proposeTokenFn = fn
}

func (m *MockBootstrapTransport) ProposeToken(_ context.Context, addr string, proposal *FencingProposal) (*FencingAck, error) {
	if m.proposeTokenFn != nil {
		return m.proposeTokenFn(addr, proposal)
	}
	if ack, ok := m.proposeTokenAck[addr]; ok {
		return ack, nil
	}
	return &FencingAck{Accepted: false, RejectReason: "no mock response configured"}, nil
}

func (m *MockBootstrapTransport) RequestVote(_ context.Context, addr raft.ServerAddress, _ *VoteRequest) (*VoteResponse, error) {
	if err, ok := m.voteErrors[addr]; ok {
		return nil, err
	}
	if resp, ok := m.voteResponses[addr]; ok {
		return resp, nil
	}
	return &VoteResponse{VoteGranted: false, Reason: "no mock response configured"}, nil
}

func (m *MockBootstrapTransport) GetClusterMeta(_ context.Context, addr string) (*ClusterMeta, error) {
	if meta, ok := m.clusterMetas[addr]; ok {
		return meta, nil
	}
	return nil, ErrMetaNotFound
}

func (m *MockBootstrapTransport) GetFencingEpoch(_ context.Context, addr string) (uint64, error) {
	if epoch, ok := m.fencingEpochs[addr]; ok {
		return epoch, nil
	}
	return 0, nil
}

func (m *MockBootstrapTransport) GetLastIndex(_ context.Context, addr string) (uint64, error) {
	if index, ok := m.lastIndexes[addr]; ok {
		return index, nil
	}
	return 0, nil
}

func (m *MockBootstrapTransport) NotifyTokenUsage(_ context.Context, addr string, notification *TokenUsageNotification) error {
	if m.notifyUsageFn != nil {
		return m.notifyUsageFn(addr, notification)
	}
	return nil
}

func (m *MockBootstrapTransport) GetTokenUsage(_ context.Context, addr string, _ string) (*TokenUsageResponse, error) {
	if resp, ok := m.tokenUsages[addr]; ok {
		return resp, nil
	}
	return &TokenUsageResponse{Used: false}, nil
}

type MockMembershipStore struct {
	config *raft.Configuration
	err    error
}

func NewMockMembershipStore() *MockMembershipStore {
	return &MockMembershipStore{}
}

func (m *MockMembershipStore) SetConfiguration(config *raft.Configuration) {
	m.config = config
}

func (m *MockMembershipStore) SetError(err error) {
	m.err = err
}

func (m *MockMembershipStore) GetLastCommittedConfiguration() (*raft.Configuration, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.config, nil
}

func NewMockSecretsManager() *SecretsManager {
	return NewSecretsManager(SecretsConfig{}, nil)
}

type MockSeeder struct {
	peers []ports.Peer
	err   error
}

func NewMockSeeder() *MockSeeder {
	return &MockSeeder{}
}

func (m *MockSeeder) SetPeers(peers []ports.Peer) {
	m.peers = peers
}

func (m *MockSeeder) SetError(err error) {
	m.err = err
}

func (m *MockSeeder) Discover(_ context.Context) ([]ports.Peer, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.peers, nil
}

func (m *MockSeeder) Name() string {
	return "mock"
}
