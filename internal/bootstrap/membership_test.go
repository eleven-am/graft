package bootstrap

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

type mockRaftMembership struct {
	mu            sync.Mutex
	servers       []raft.Server
	state         raft.RaftState
	appliedIndex  uint64
	stats         map[string]string
	addVoterErr   error
	addNonvoterErr error
	removeErr     error
	getConfigErr  error
	transferErr   error
}

func newMockRaftMembership() *mockRaftMembership {
	return &mockRaftMembership{
		servers: []raft.Server{
			{ID: "node-0", Address: "localhost:8000", Suffrage: raft.Voter},
			{ID: "node-1", Address: "localhost:8001", Suffrage: raft.Voter},
			{ID: "node-2", Address: "localhost:8002", Suffrage: raft.Voter},
		},
		state:        raft.Leader,
		appliedIndex: 100,
		stats:        make(map[string]string),
	}
}

type mockIndexFuture struct {
	err error
}

func (f *mockIndexFuture) Error() error { return f.err }
func (f *mockIndexFuture) Index() uint64 { return 0 }

type mockFuture struct {
	err error
}

func (f *mockFuture) Error() error { return f.err }

type mockConfigFuture struct {
	config raft.Configuration
	err    error
}

func (f *mockConfigFuture) Error() error                   { return f.err }
func (f *mockConfigFuture) Index() uint64                  { return 0 }
func (f *mockConfigFuture) Configuration() raft.Configuration { return f.config }

func (m *mockRaftMembership) AddVoter(id raft.ServerID, addr raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.addVoterErr != nil {
		return &mockIndexFuture{err: m.addVoterErr}
	}
	for i, s := range m.servers {
		if s.ID == id {
			m.servers[i].Suffrage = raft.Voter
			return &mockIndexFuture{}
		}
	}
	m.servers = append(m.servers, raft.Server{ID: id, Address: addr, Suffrage: raft.Voter})
	return &mockIndexFuture{}
}

func (m *mockRaftMembership) AddNonvoter(id raft.ServerID, addr raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.addNonvoterErr != nil {
		return &mockIndexFuture{err: m.addNonvoterErr}
	}
	for i, s := range m.servers {
		if s.ID == id {
			m.servers[i].Suffrage = raft.Nonvoter
			return &mockIndexFuture{}
		}
	}
	m.servers = append(m.servers, raft.Server{ID: id, Address: addr, Suffrage: raft.Nonvoter})
	return &mockIndexFuture{}
}

func (m *mockRaftMembership) RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removeErr != nil {
		return &mockIndexFuture{err: m.removeErr}
	}
	for i, s := range m.servers {
		if s.ID == id {
			m.servers = append(m.servers[:i], m.servers[i+1:]...)
			return &mockIndexFuture{}
		}
	}
	return &mockIndexFuture{}
}

func (m *mockRaftMembership) GetConfiguration() raft.ConfigurationFuture {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getConfigErr != nil {
		return &mockConfigFuture{err: m.getConfigErr}
	}
	return &mockConfigFuture{
		config: raft.Configuration{Servers: m.servers},
	}
}

func (m *mockRaftMembership) AppliedIndex() uint64 {
	return m.appliedIndex
}

func (m *mockRaftMembership) State() raft.RaftState {
	return m.state
}

func (m *mockRaftMembership) LeadershipTransfer() raft.Future {
	if m.transferErr != nil {
		return &mockFuture{err: m.transferErr}
	}
	return &mockFuture{}
}

func (m *mockRaftMembership) Stats() map[string]string {
	return m.stats
}

type mockProberForMembership struct {
	reachable map[raft.ServerAddress]bool
}

func newMockProberForMembership() *mockProberForMembership {
	return &mockProberForMembership{
		reachable: make(map[raft.ServerAddress]bool),
	}
}

func (p *mockProberForMembership) ProbePeer(_ context.Context, addr raft.ServerAddress) (*PeerProbeResult, error) {
	reachable, ok := p.reachable[addr]
	if !ok {
		reachable = true
	}
	return &PeerProbeResult{Address: addr, Reachable: reachable}, nil
}

func (p *mockProberForMembership) ProbeAll(ctx context.Context, peers []VoterInfo) []PeerProbeResult {
	results := make([]PeerProbeResult, 0, len(peers))
	for _, peer := range peers {
		result, _ := p.ProbePeer(ctx, peer.Address)
		result.ServerID = peer.ServerID
		results = append(results, *result)
	}
	return results
}

func TestNewMembershipManager(t *testing.T) {
	mockRaft := newMockRaftMembership()

	deps := MembershipManagerDeps{
		Config: &MembershipConfig{
			LearnerCatchupThreshold: 50,
			JoinRateLimit:           10,
			JoinBurst:               5,
			MaxConcurrentJoins:      3,
		},
		Raft: mockRaft,
	}

	manager := NewMembershipManager(deps)

	if manager == nil {
		t.Fatal("NewMembershipManager returned nil")
	}

	if manager.config.LearnerCatchupThreshold != 50 {
		t.Errorf("LearnerCatchupThreshold = %d, want 50", manager.config.LearnerCatchupThreshold)
	}
}

func TestMembershipManager_currentQuorum(t *testing.T) {
	mockRaft := newMockRaftMembership()
	manager := NewMembershipManager(MembershipManagerDeps{Raft: mockRaft})

	quorum := manager.currentQuorum()
	if quorum != 2 {
		t.Errorf("currentQuorum() = %d, want 2 (for 3 voters)", quorum)
	}
}

func TestMembershipManager_postRemovalQuorum(t *testing.T) {
	mockRaft := newMockRaftMembership()
	manager := NewMembershipManager(MembershipManagerDeps{Raft: mockRaft})

	quorum := manager.postRemovalQuorum("node-0")
	if quorum != 2 {
		t.Errorf("postRemovalQuorum() = %d, want 2 (for 2 remaining voters)", quorum)
	}
}

func TestAddLearner_Success(t *testing.T) {
	mockRaft := newMockRaftMembership()
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.AddLearner(ctx, "node-3", "localhost:8003", nil)
	if err != nil {
		t.Fatalf("AddLearner() error = %v", err)
	}

	mockRaft.mu.Lock()
	found := false
	for _, s := range mockRaft.servers {
		if s.ID == "node-3" && s.Suffrage == raft.Nonvoter {
			found = true
			break
		}
	}
	mockRaft.mu.Unlock()

	if !found {
		t.Error("node-3 not found as nonvoter in configuration")
	}
}

func TestAddLearner_AlreadyInProgress(t *testing.T) {
	mockRaft := newMockRaftMembership()
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	manager.mu.Lock()
	manager.pendingChanges["node-3"] = &MembershipChange{
		ServerID: "node-3",
		Stage:    StageLearner,
	}
	manager.mu.Unlock()

	ctx := context.Background()
	err := manager.AddLearner(ctx, "node-3", "localhost:8003", nil)
	if !errors.Is(err, ErrMembershipChangeInProgress) {
		t.Errorf("AddLearner() error = %v, want ErrMembershipChangeInProgress", err)
	}
}

func TestAddLearner_NotLeader(t *testing.T) {
	mockRaft := newMockRaftMembership()
	mockRaft.state = raft.Follower

	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.AddLearner(ctx, "node-3", "localhost:8003", nil)
	if !errors.Is(err, ErrNotLeader) {
		t.Errorf("AddLearner() error = %v, want ErrNotLeader", err)
	}
}

func TestAddLearner_AdmissionControl_LogSize(t *testing.T) {
	mockRaft := newMockRaftMembership()
	mockRaft.stats["last_log_index"] = "1000"
	mockRaft.stats["first_log_index"] = "1"

	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
			MaxLogSizeForJoin:  100,
		},
	})

	ctx := context.Background()
	joinReq := &JoinRequest{
		ServerID: "node-3",
		Address:  "localhost:8003",
	}

	err := manager.AddLearner(ctx, "node-3", "localhost:8003", joinReq)
	if err == nil {
		t.Fatal("AddLearner() should fail with log size too large")
	}

	var admErr *AdmissionControlError
	if !errors.As(err, &admErr) {
		t.Errorf("error should be AdmissionControlError, got %T", err)
	}
}

func TestAddLearner_VersionCompatibility(t *testing.T) {
	mockRaft := newMockRaftMembership()
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
			VersionCompatibility: VersionCompatibilityConfig{
				MinRaftProtocolVersion: 3,
			},
		},
	})

	ctx := context.Background()
	joinReq := &JoinRequest{
		ServerID:    "node-3",
		Address:     "localhost:8003",
		RaftVersion: 2,
	}

	err := manager.AddLearner(ctx, "node-3", "localhost:8003", joinReq)
	if err == nil {
		t.Fatal("AddLearner() should fail with version incompatibility")
	}

	var verErr *VersionCompatibilityError
	if !errors.As(err, &verErr) {
		t.Errorf("error should be VersionCompatibilityError, got %T", err)
	}
}

func TestAddLearner_DuplicateServerID(t *testing.T) {
	mockRaft := newMockRaftMembership()
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.AddLearner(ctx, "node-0", "localhost:9000", nil)
	if !errors.Is(err, ErrDuplicateServerID) {
		t.Errorf("AddLearner() error = %v, want ErrDuplicateServerID", err)
	}
}

func TestAddLearner_DuplicateServerAddress(t *testing.T) {
	mockRaft := newMockRaftMembership()
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.AddLearner(ctx, "node-99", "localhost:8000", nil)
	if !errors.Is(err, ErrDuplicateServerAddress) {
		t.Errorf("AddLearner() error = %v, want ErrDuplicateServerAddress", err)
	}
}

func TestSafeRemoveVoter_Success(t *testing.T) {
	mockRaft := newMockRaftMembership()
	prober := newMockProberForMembership()
	prober.reachable[raft.ServerAddress("localhost:8001")] = true
	prober.reachable[raft.ServerAddress("localhost:8002")] = true

	localMeta := &ClusterMeta{ServerID: "node-0"}
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft:      mockRaft,
		Prober:    prober,
		LocalMeta: func() *ClusterMeta { return localMeta },
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.SafeRemoveVoter(ctx, "node-2")
	if err != nil {
		t.Fatalf("SafeRemoveVoter() error = %v", err)
	}

	mockRaft.mu.Lock()
	for _, s := range mockRaft.servers {
		if s.ID == "node-2" {
			t.Error("node-2 should have been removed")
		}
	}
	mockRaft.mu.Unlock()
}

func TestSafeRemoveVoter_QuorumProtection(t *testing.T) {
	mockRaft := newMockRaftMembership()
	prober := newMockProberForMembership()
	prober.reachable[raft.ServerAddress("localhost:8001")] = false
	prober.reachable[raft.ServerAddress("localhost:8002")] = false

	localMeta := &ClusterMeta{ServerID: "node-0"}
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft:      mockRaft,
		Prober:    prober,
		LocalMeta: func() *ClusterMeta { return localMeta },
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.SafeRemoveVoter(ctx, "node-2")
	if err == nil {
		t.Fatal("SafeRemoveVoter() should fail due to quorum loss")
	}

	var quorumErr *QuorumLossError
	if !errors.As(err, &quorumErr) {
		t.Errorf("error should be QuorumLossError, got %T: %v", err, err)
	}
}

func TestSafeRemoveVoter_LeadershipTransfer(t *testing.T) {
	mockRaft := newMockRaftMembership()
	prober := newMockProberForMembership()
	prober.reachable[raft.ServerAddress("localhost:8001")] = true
	prober.reachable[raft.ServerAddress("localhost:8002")] = true

	localMeta := &ClusterMeta{ServerID: "node-0"}
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft:      mockRaft,
		Prober:    prober,
		LocalMeta: func() *ClusterMeta { return localMeta },
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.SafeRemoveVoter(ctx, "node-0")
	if !errors.Is(err, ErrLeadershipTransferred) {
		t.Errorf("SafeRemoveVoter() error = %v, want ErrLeadershipTransferred", err)
	}
}

func TestSafeRemoveVoter_NotLeader(t *testing.T) {
	mockRaft := newMockRaftMembership()
	mockRaft.state = raft.Follower

	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.SafeRemoveVoter(ctx, "node-2")
	if !errors.Is(err, ErrNotLeader) {
		t.Errorf("SafeRemoveVoter() error = %v, want ErrNotLeader", err)
	}
}

func TestCanSafelyRemove_Safe(t *testing.T) {
	mockRaft := newMockRaftMembership()
	prober := newMockProberForMembership()
	prober.reachable[raft.ServerAddress("localhost:8001")] = true
	prober.reachable[raft.ServerAddress("localhost:8002")] = true

	localMeta := &ClusterMeta{ServerID: "node-0"}
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft:      mockRaft,
		Prober:    prober,
		LocalMeta: func() *ClusterMeta { return localMeta },
	})

	ctx := context.Background()
	err := manager.CanSafelyRemove(ctx, "node-2")
	if err != nil {
		t.Errorf("CanSafelyRemove() error = %v, want nil", err)
	}
}

func TestCanSafelyRemove_Unsafe(t *testing.T) {
	mockRaft := newMockRaftMembership()
	prober := newMockProberForMembership()
	prober.reachable[raft.ServerAddress("localhost:8001")] = false
	prober.reachable[raft.ServerAddress("localhost:8002")] = false

	localMeta := &ClusterMeta{ServerID: "node-0"}
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft:      mockRaft,
		Prober:    prober,
		LocalMeta: func() *ClusterMeta { return localMeta },
	})

	ctx := context.Background()
	err := manager.CanSafelyRemove(ctx, "node-2")

	var quorumErr *QuorumLossError
	if !errors.As(err, &quorumErr) {
		t.Errorf("CanSafelyRemove() error should be QuorumLossError, got %T", err)
	}
}

func TestRemoveLearner_Success(t *testing.T) {
	mockRaft := newMockRaftMembership()
	mockRaft.servers = append(mockRaft.servers, raft.Server{
		ID:       "node-3",
		Address:  "localhost:8003",
		Suffrage: raft.Nonvoter,
	})

	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.RemoveLearner(ctx, "node-3")
	if err != nil {
		t.Fatalf("RemoveLearner() error = %v", err)
	}

	mockRaft.mu.Lock()
	for _, s := range mockRaft.servers {
		if s.ID == "node-3" {
			t.Error("node-3 should have been removed")
		}
	}
	mockRaft.mu.Unlock()
}

func TestGetClusterStatus(t *testing.T) {
	mockRaft := newMockRaftMembership()
	mockRaft.servers = append(mockRaft.servers, raft.Server{
		ID:       "node-3",
		Address:  "localhost:8003",
		Suffrage: raft.Nonvoter,
	})

	prober := newMockProberForMembership()
	prober.reachable[raft.ServerAddress("localhost:8001")] = true
	prober.reachable[raft.ServerAddress("localhost:8002")] = true
	prober.reachable[raft.ServerAddress("localhost:8003")] = true

	localMeta := &ClusterMeta{ServerID: "node-0"}
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft:      mockRaft,
		Prober:    prober,
		LocalMeta: func() *ClusterMeta { return localMeta },
	})

	ctx := context.Background()
	status, err := manager.GetClusterStatus(ctx)
	if err != nil {
		t.Fatalf("GetClusterStatus() error = %v", err)
	}

	if status.TotalVoters != 3 {
		t.Errorf("TotalVoters = %d, want 3", status.TotalVoters)
	}

	if len(status.Learners) != 1 {
		t.Errorf("len(Learners) = %d, want 1", len(status.Learners))
	}

	if !status.HasQuorum {
		t.Error("HasQuorum should be true")
	}
}

func TestMembershipStage_String(t *testing.T) {
	tests := []struct {
		stage MembershipStage
		want  string
	}{
		{StageRequested, "requested"},
		{StageLearner, "learner"},
		{StageCatchingUp, "catching_up"},
		{StagePromoting, "promoting"},
		{StageVoter, "voter"},
		{StageFailed, "failed"},
		{MembershipStage(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.stage.String(); got != tt.want {
			t.Errorf("MembershipStage(%d).String() = %s, want %s", tt.stage, got, tt.want)
		}
	}
}

func TestDemoteVoter(t *testing.T) {
	mockRaft := newMockRaftMembership()
	manager := NewMembershipManager(MembershipManagerDeps{
		Raft: mockRaft,
		Config: &MembershipConfig{
			JoinRateLimit:      100,
			JoinBurst:          10,
			MaxConcurrentJoins: 5,
		},
	})

	ctx := context.Background()
	err := manager.DemoteVoter(ctx, "node-2")
	if err != nil {
		t.Fatalf("DemoteVoter() error = %v", err)
	}

	mockRaft.mu.Lock()
	for _, s := range mockRaft.servers {
		if s.ID == "node-2" {
			if s.Suffrage != raft.Nonvoter {
				t.Error("node-2 should be demoted to Nonvoter")
			}
			break
		}
	}
	mockRaft.mu.Unlock()
}

func TestQuorumLossError(t *testing.T) {
	err := &QuorumLossError{
		CurrentVoters:     3,
		PostRemovalVoters: 2,
		ReachableVoters:   1,
		RequiredQuorum:    2,
	}

	expected := "removal would cause quorum loss: current_voters=3, post_removal=2, reachable=1, required_quorum=2"
	if err.Error() != expected {
		t.Errorf("QuorumLossError.Error() = %q, want %q", err.Error(), expected)
	}
}

func TestAdmissionControlError(t *testing.T) {
	tests := []struct {
		err  *AdmissionControlError
		want string
	}{
		{
			&AdmissionControlError{Reason: "log size too large", LogSize: 1000, Threshold: 100},
			"admission control failed: log size too large (size=1000, threshold=100)",
		},
		{
			&AdmissionControlError{Reason: "disk pressure", DiskUsage: 0.95, Threshold: 0.90},
			"admission control failed: disk pressure (usage=95.00%, threshold=90.00%)",
		},
		{
			&AdmissionControlError{Reason: "other reason"},
			"admission control failed: other reason",
		},
	}

	for _, tt := range tests {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("AdmissionControlError.Error() = %q, want %q", got, tt.want)
		}
	}
}

func TestVersionCompatibilityError(t *testing.T) {
	err := &VersionCompatibilityError{
		Field:       "raft_protocol_version",
		LocalValue:  3,
		RemoteValue: 1,
		MinRequired: 2,
		MaxAllowed:  4,
	}

	expected := "version compatibility failed: raft_protocol_version (local=3, remote=1, min=2, max=4)"
	if err.Error() != expected {
		t.Errorf("VersionCompatibilityError.Error() = %q, want %q", err.Error(), expected)
	}
}
