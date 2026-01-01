package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"google.golang.org/grpc"
)

type mockTransportForReconciler struct {
	mu              sync.Mutex
	getLeaderCalls  []string
	addVoterCalls   []addVoterCall
	leaderID        string
	leaderAddr      string
	getLeaderErr    error
	addVoterErr     error
	failUntilNthGet int
	getCalls        int
}

type addVoterCall struct {
	leaderAddr string
	nodeID     string
	nodeAddr   string
}

func (m *mockTransportForReconciler) GetLeaderInfo(ctx context.Context, peerAddr string) (string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getLeaderCalls = append(m.getLeaderCalls, peerAddr)
	m.getCalls++

	if m.failUntilNthGet > 0 && m.getCalls < m.failUntilNthGet {
		return "", "", errors.New("connection refused")
	}

	if m.getLeaderErr != nil {
		return "", "", m.getLeaderErr
	}
	return m.leaderID, m.leaderAddr, nil
}

func (m *mockTransportForReconciler) RequestAddVoter(ctx context.Context, leaderAddr, nodeID, nodeAddr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addVoterCalls = append(m.addVoterCalls, addVoterCall{leaderAddr, nodeID, nodeAddr})
	return m.addVoterErr
}

func (m *mockTransportForReconciler) RegisterEngine(engine ports.EnginePort) {}
func (m *mockTransportForReconciler) RegisterRaft(raft ports.RaftNode)       {}
func (m *mockTransportForReconciler) RegisterLoadSink(sink ports.LoadSink)   {}
func (m *mockTransportForReconciler) Start(ctx context.Context, address string, port int) error {
	return nil
}
func (m *mockTransportForReconciler) Stop() error { return nil }
func (m *mockTransportForReconciler) SendApplyCommand(ctx context.Context, nodeAddr string, cmd *domain.Command) (*domain.CommandResult, string, error) {
	return nil, "", nil
}
func (m *mockTransportForReconciler) SendJoinRequest(ctx context.Context, nodeAddr string, request *ports.JoinRequest) (*ports.JoinResponse, error) {
	return nil, nil
}
func (m *mockTransportForReconciler) SendPublishLoad(ctx context.Context, nodeAddr string, update ports.LoadUpdate) error {
	return nil
}
func (m *mockTransportForReconciler) SendTrigger(ctx context.Context, nodeAddr string, trigger domain.WorkflowTrigger) error {
	return nil
}
func (m *mockTransportForReconciler) RegisterService(registrar func(*grpc.Server)) {}

func TestReconciler_State_InitialPending(t *testing.T) {
	t.Parallel()

	r := NewReconciler(ReconcilerConfig{
		Transport: &mockTransportForReconciler{},
		Peers:     []domain.RaftPeerSpec{{ID: "peer-1", Address: "peer1:9000"}},
		NodeID:    "node-1",
		NodeAddr:  "node1:9000",
	})

	if r.State() != ReconcilerStatePending {
		t.Fatalf("expected initial state Pending, got %s", r.State())
	}
}

func TestReconciler_Reconcile_NoTransport(t *testing.T) {
	t.Parallel()

	r := NewReconciler(ReconcilerConfig{
		Transport: nil,
		Peers:     []domain.RaftPeerSpec{{ID: "peer-1", Address: "peer1:9000"}},
		NodeID:    "node-1",
		NodeAddr:  "node1:9000",
	})

	err := r.Reconcile(context.Background())

	if !errors.Is(err, ErrNoTransport) {
		t.Fatalf("expected ErrNoTransport, got %v", err)
	}
	if r.State() != ReconcilerStateFailed {
		t.Fatalf("expected state Failed, got %s", r.State())
	}
}

func TestReconciler_Reconcile_NoPeers(t *testing.T) {
	t.Parallel()

	r := NewReconciler(ReconcilerConfig{
		Transport: &mockTransportForReconciler{},
		Peers:     nil,
		NodeID:    "node-1",
		NodeAddr:  "node1:9000",
	})

	err := r.Reconcile(context.Background())

	if !errors.Is(err, ErrNoPeersAvailable) {
		t.Fatalf("expected ErrNoPeersAvailable, got %v", err)
	}
	if r.State() != ReconcilerStateFailed {
		t.Fatalf("expected state Failed, got %s", r.State())
	}
}

func TestReconciler_Reconcile_LeaderNotFound(t *testing.T) {
	t.Parallel()

	transport := &mockTransportForReconciler{
		getLeaderErr: errors.New("connection refused"),
	}

	r := NewReconciler(ReconcilerConfig{
		Transport: transport,
		Peers: []domain.RaftPeerSpec{
			{ID: "peer-1", Address: "peer1:9000"},
			{ID: "peer-2", Address: "peer2:9000"},
		},
		NodeID:   "node-1",
		NodeAddr: "node1:9000",
	})

	err := r.Reconcile(context.Background())

	if !errors.Is(err, ErrLeaderNotFound) {
		t.Fatalf("expected ErrLeaderNotFound, got %v", err)
	}
	if r.State() != ReconcilerStateFailed {
		t.Fatalf("expected state Failed, got %s", r.State())
	}

	transport.mu.Lock()
	if len(transport.getLeaderCalls) != 2 {
		t.Fatalf("expected 2 getLeader calls, got %d", len(transport.getLeaderCalls))
	}
	transport.mu.Unlock()
}

func TestReconciler_Reconcile_Success(t *testing.T) {
	t.Parallel()

	transport := &mockTransportForReconciler{
		leaderID:   "leader-node",
		leaderAddr: "leader:9000",
	}

	r := NewReconciler(ReconcilerConfig{
		Transport: transport,
		Peers:     []domain.RaftPeerSpec{{ID: "peer-1", Address: "peer1:9000"}},
		NodeID:    "node-1",
		NodeAddr:  "node1:9000",
	})

	err := r.Reconcile(context.Background())

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if r.State() != ReconcilerStateSucceeded {
		t.Fatalf("expected state Succeeded, got %s", r.State())
	}

	transport.mu.Lock()
	defer transport.mu.Unlock()

	if len(transport.getLeaderCalls) != 1 {
		t.Fatalf("expected 1 getLeader call, got %d", len(transport.getLeaderCalls))
	}
	if transport.getLeaderCalls[0] != "peer1:9000" {
		t.Fatalf("expected getLeader call to peer1:9000, got %s", transport.getLeaderCalls[0])
	}

	if len(transport.addVoterCalls) != 1 {
		t.Fatalf("expected 1 addVoter call, got %d", len(transport.addVoterCalls))
	}
	call := transport.addVoterCalls[0]
	if call.leaderAddr != "leader:9000" {
		t.Fatalf("expected addVoter to leader:9000, got %s", call.leaderAddr)
	}
	if call.nodeID != "node-1" {
		t.Fatalf("expected nodeID node-1, got %s", call.nodeID)
	}
	if call.nodeAddr != "node1:9000" {
		t.Fatalf("expected nodeAddr node1:9000, got %s", call.nodeAddr)
	}
}

func TestReconciler_Reconcile_AddVoterFails(t *testing.T) {
	t.Parallel()

	transport := &mockTransportForReconciler{
		leaderID:    "leader-node",
		leaderAddr:  "leader:9000",
		addVoterErr: errors.New("not the leader"),
	}

	r := NewReconciler(ReconcilerConfig{
		Transport: transport,
		Peers:     []domain.RaftPeerSpec{{ID: "peer-1", Address: "peer1:9000"}},
		NodeID:    "node-1",
		NodeAddr:  "node1:9000",
	})

	err := r.Reconcile(context.Background())

	if !errors.Is(err, ErrAddVoterRejected) {
		t.Fatalf("expected ErrAddVoterRejected, got %v", err)
	}
	if r.State() != ReconcilerStateFailed {
		t.Fatalf("expected state Failed, got %s", r.State())
	}
}

func TestReconciler_Reconcile_AlreadySucceeded(t *testing.T) {
	t.Parallel()

	transport := &mockTransportForReconciler{
		leaderID:   "leader-node",
		leaderAddr: "leader:9000",
	}

	r := NewReconciler(ReconcilerConfig{
		Transport: transport,
		Peers:     []domain.RaftPeerSpec{{ID: "peer-1", Address: "peer1:9000"}},
		NodeID:    "node-1",
		NodeAddr:  "node1:9000",
	})

	_ = r.Reconcile(context.Background())

	transport.mu.Lock()
	callsBefore := len(transport.addVoterCalls)
	transport.mu.Unlock()

	err := r.Reconcile(context.Background())

	if err != nil {
		t.Fatalf("expected no error for already succeeded, got %v", err)
	}

	transport.mu.Lock()
	callsAfter := len(transport.addVoterCalls)
	transport.mu.Unlock()

	if callsAfter != callsBefore {
		t.Fatalf("expected no additional calls when already succeeded")
	}
}

func TestReconciler_FindLeader_UsesPeerAddressAsFallback(t *testing.T) {
	t.Parallel()

	transport := &mockTransportForReconciler{
		leaderID:   "",
		leaderAddr: "",
	}

	r := NewReconciler(ReconcilerConfig{
		Transport: transport,
		Peers:     []domain.RaftPeerSpec{{ID: "peer-1", Address: "peer1:9000"}},
		NodeID:    "node-1",
		NodeAddr:  "node1:9000",
	})

	err := r.Reconcile(context.Background())

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	transport.mu.Lock()
	defer transport.mu.Unlock()

	if len(transport.addVoterCalls) != 1 {
		t.Fatalf("expected 1 addVoter call, got %d", len(transport.addVoterCalls))
	}
	if transport.addVoterCalls[0].leaderAddr != "peer1:9000" {
		t.Fatalf("expected fallback to peer address peer1:9000, got %s", transport.addVoterCalls[0].leaderAddr)
	}
}

func TestReconciler_FindLeader_TriesMultiplePeers(t *testing.T) {
	t.Parallel()

	transport := &mockTransportForReconciler{
		leaderID:        "leader-node",
		leaderAddr:      "leader:9000",
		failUntilNthGet: 2,
	}

	r := NewReconciler(ReconcilerConfig{
		Transport: transport,
		Peers: []domain.RaftPeerSpec{
			{ID: "peer-1", Address: "peer1:9000"},
			{ID: "peer-2", Address: "peer2:9000"},
		},
		NodeID:   "node-1",
		NodeAddr: "node1:9000",
	})

	err := r.Reconcile(context.Background())

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	transport.mu.Lock()
	defer transport.mu.Unlock()

	if len(transport.getLeaderCalls) != 2 {
		t.Fatalf("expected 2 getLeader calls (first fails, second succeeds), got %d", len(transport.getLeaderCalls))
	}
}

func TestReconciler_ReconcileWithRetry_SucceedsOnRetry(t *testing.T) {
	t.Parallel()

	transport := &mockTransportForReconciler{
		leaderID:        "leader-node",
		leaderAddr:      "leader:9000",
		failUntilNthGet: 3,
	}

	r := NewReconciler(ReconcilerConfig{
		Transport: transport,
		Peers: []domain.RaftPeerSpec{
			{ID: "peer-1", Address: "peer1:9000"},
		},
		NodeID:   "node-1",
		NodeAddr: "node1:9000",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := r.ReconcileWithRetry(ctx, 5, 10*time.Millisecond)

	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if r.State() != ReconcilerStateSucceeded {
		t.Fatalf("expected state Succeeded, got %s", r.State())
	}
}

func TestReconciler_ReconcileWithRetry_ExhaustsRetries(t *testing.T) {
	t.Parallel()

	transport := &mockTransportForReconciler{
		getLeaderErr: errors.New("always fails"),
	}

	r := NewReconciler(ReconcilerConfig{
		Transport: transport,
		Peers: []domain.RaftPeerSpec{
			{ID: "peer-1", Address: "peer1:9000"},
		},
		NodeID:   "node-1",
		NodeAddr: "node1:9000",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := r.ReconcileWithRetry(ctx, 3, 10*time.Millisecond)

	if err == nil {
		t.Fatalf("expected error after exhausting retries")
	}
	if r.State() != ReconcilerStateFailed {
		t.Fatalf("expected state Failed, got %s", r.State())
	}
}

func TestReconciler_ReconcileWithRetry_RespectsContextCancellation(t *testing.T) {
	t.Parallel()

	transport := &mockTransportForReconciler{
		getLeaderErr: errors.New("always fails"),
	}

	r := NewReconciler(ReconcilerConfig{
		Transport: transport,
		Peers: []domain.RaftPeerSpec{
			{ID: "peer-1", Address: "peer1:9000"},
		},
		NodeID:   "node-1",
		NodeAddr: "node1:9000",
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := r.ReconcileWithRetry(ctx, 100, 100*time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if elapsed > 500*time.Millisecond {
		t.Fatalf("expected quick exit on context cancellation, took %v", elapsed)
	}
}
