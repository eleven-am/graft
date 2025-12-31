package bootstrap

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

type PeerInfo struct {
	ServerID raft.ServerID
	Address  raft.ServerAddress
	Ordinal  int
}

type VoteRequest struct {
	CandidateID      raft.ServerID
	CandidateOrdinal int
	ElectionReason   string
	Timestamp        time.Time
	VoterSetHash     []byte
	RequiredQuorum   int
	Signature        []byte
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

type PeerDiscovery interface {
	AddressForOrdinal(ordinal int) raft.ServerAddress
	GetHealthyPeers(ctx context.Context) []PeerInfo
}

type LifecyclePeerDiscovery interface {
	PeerDiscovery
	Start(ctx context.Context) error
	Stop()
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

func ExtractOrdinal(serverID raft.ServerID) int {
	parts := strings.Split(string(serverID), "-")
	if len(parts) == 0 {
		return -1
	}

	lastPart := parts[len(parts)-1]
	ordinal, err := strconv.Atoi(lastPart)
	if err != nil {
		return -1
	}
	return ordinal
}

func (r *VoteRequest) computeSignatureData() []byte {
	h := sha256.New()
	h.Write([]byte(r.CandidateID))
	binary.Write(h, binary.BigEndian, int32(r.CandidateOrdinal))
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

type MockPeerDiscovery struct {
	addresses    map[int]raft.ServerAddress
	healthyPeers []PeerInfo
}

func NewMockPeerDiscovery() *MockPeerDiscovery {
	return &MockPeerDiscovery{
		addresses:    make(map[int]raft.ServerAddress),
		healthyPeers: make([]PeerInfo, 0),
	}
}

func (m *MockPeerDiscovery) SetAddress(ordinal int, addr raft.ServerAddress) {
	m.addresses[ordinal] = addr
}

func (m *MockPeerDiscovery) SetHealthyPeers(peers []PeerInfo) {
	m.healthyPeers = peers
}

func (m *MockPeerDiscovery) AddressForOrdinal(ordinal int) raft.ServerAddress {
	if addr, ok := m.addresses[ordinal]; ok {
		return addr
	}
	return ""
}

func (m *MockPeerDiscovery) GetHealthyPeers(_ context.Context) []PeerInfo {
	return m.healthyPeers
}

type MockLifecyclePeerDiscovery struct {
	MockPeerDiscovery
	started  bool
	stopped  bool
	startErr error
}

func NewMockLifecyclePeerDiscovery() *MockLifecyclePeerDiscovery {
	return &MockLifecyclePeerDiscovery{
		MockPeerDiscovery: *NewMockPeerDiscovery(),
	}
}

func (m *MockLifecyclePeerDiscovery) Start(_ context.Context) error {
	if m.startErr != nil {
		return m.startErr
	}
	m.started = true
	return nil
}

func (m *MockLifecyclePeerDiscovery) Stop() {
	m.stopped = true
}

func (m *MockLifecyclePeerDiscovery) Started() bool {
	return m.started
}

func (m *MockLifecyclePeerDiscovery) Stopped() bool {
	return m.stopped
}

func (m *MockLifecyclePeerDiscovery) SetStartError(err error) {
	m.startErr = err
}

func NewMockSecretsManager() *SecretsManager {
	return NewSecretsManager(SecretsConfig{}, nil)
}
