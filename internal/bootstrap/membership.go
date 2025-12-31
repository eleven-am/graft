package bootstrap

import (
	"log/slog"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"golang.org/x/time/rate"
)

type MembershipStage int

const (
	StageRequested MembershipStage = iota
	StageLearner
	StageCatchingUp
	StagePromoting
	StageVoter
	StageFailed
)

func (s MembershipStage) String() string {
	switch s {
	case StageRequested:
		return "requested"
	case StageLearner:
		return "learner"
	case StageCatchingUp:
		return "catching_up"
	case StagePromoting:
		return "promoting"
	case StageVoter:
		return "voter"
	case StageFailed:
		return "failed"
	default:
		return "unknown"
	}
}

type MembershipChange struct {
	ServerID  raft.ServerID
	Address   raft.ServerAddress
	Stage     MembershipStage
	StartTime time.Time
	LastIndex uint64
	Error     error
}

type JoinRequest struct {
	ServerID        raft.ServerID      `json:"server_id"`
	Address         raft.ServerAddress `json:"address"`
	RaftVersion     uint32             `json:"raft_version"`
	StateMachineVer uint32             `json:"state_machine_version"`
	FeatureFlags    []string           `json:"feature_flags,omitempty"`
	Metadata        map[string]string  `json:"metadata,omitempty"`
}

type JoinResponse struct {
	Accepted      bool               `json:"accepted"`
	RejectReason  string             `json:"reject_reason,omitempty"`
	AssignedID    raft.ServerID      `json:"assigned_id,omitempty"`
	ClusterUUID   string             `json:"cluster_uuid,omitempty"`
	LeaderID      raft.ServerID      `json:"leader_id,omitempty"`
	LeaderAddress raft.ServerAddress `json:"leader_address,omitempty"`
	VoterSet      []VoterInfo        `json:"voter_set,omitempty"`
}

type FollowerState struct {
	MatchIndex         uint64
	LastSnapshotIndex  uint64
	SnapshotInProgress bool
}

type RaftMembership interface {
	AddVoter(id raft.ServerID, addr raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	AddNonvoter(id raft.ServerID, addr raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	GetConfiguration() raft.ConfigurationFuture
	AppliedIndex() uint64
	State() raft.RaftState
	LeadershipTransfer() raft.Future
	Stats() map[string]string
}

type MembershipManagerDeps struct {
	Config    *MembershipConfig
	Raft      RaftMembership
	Prober    PeerProber
	MetaStore MetaStore
	LocalMeta func() *ClusterMeta
	Logger    *slog.Logger
}

type MembershipManager struct {
	config    *MembershipConfig
	raft      RaftMembership
	prober    PeerProber
	metaStore MetaStore
	localMeta func() *ClusterMeta
	logger    *slog.Logger

	mu             sync.Mutex
	pendingChanges map[raft.ServerID]*MembershipChange
	rateLimiter    *rate.Limiter
	concurrencySem chan struct{}
}

func NewMembershipManager(deps MembershipManagerDeps) *MembershipManager {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	config := deps.Config
	if config == nil {
		config = &MembershipConfig{
			LearnerCatchupThreshold: 100,
			SnapshotCatchupTimeout:  5 * time.Minute,
			JoinRateLimit:           1,
			JoinBurst:               3,
			MaxConcurrentJoins:      2,
		}
	}

	limiter := rate.NewLimiter(config.JoinRateLimit, config.JoinBurst)

	maxConcurrent := config.MaxConcurrentJoins
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	return &MembershipManager{
		config:         config,
		raft:           deps.Raft,
		prober:         deps.Prober,
		metaStore:      deps.MetaStore,
		localMeta:      deps.LocalMeta,
		logger:         logger,
		pendingChanges: make(map[raft.ServerID]*MembershipChange),
		rateLimiter:    limiter,
		concurrencySem: make(chan struct{}, maxConcurrent),
	}
}

func (m *MembershipManager) currentQuorum() int {
	if m.raft == nil {
		return 0
	}

	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return 0
	}

	voters := m.countVoters(configFuture.Configuration())
	return (voters / 2) + 1
}

func (m *MembershipManager) postRemovalQuorum(excludeID raft.ServerID) int {
	if m.raft == nil {
		return 0
	}

	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return 0
	}

	voters := 0
	for _, server := range configFuture.Configuration().Servers {
		if server.Suffrage == raft.Voter && server.ID != excludeID {
			voters++
		}
	}

	return (voters / 2) + 1
}

func (m *MembershipManager) isLeader() bool {
	if m.raft == nil {
		return false
	}
	return m.raft.State() == raft.Leader
}

func (m *MembershipManager) localServerID() raft.ServerID {
	if m.localMeta == nil {
		return ""
	}
	meta := m.localMeta()
	if meta == nil {
		return ""
	}
	return meta.ServerID
}

func (m *MembershipManager) countVoters(config raft.Configuration) int {
	count := 0
	for _, server := range config.Servers {
		if server.Suffrage == raft.Voter {
			count++
		}
	}
	return count
}

func (m *MembershipManager) GetPendingChanges() map[raft.ServerID]*MembershipChange {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[raft.ServerID]*MembershipChange, len(m.pendingChanges))
	for id, change := range m.pendingChanges {
		changeCopy := *change
		result[id] = &changeCopy
	}
	return result
}

func (m *MembershipManager) GetConfiguration() (raft.Configuration, error) {
	if m.raft == nil {
		return raft.Configuration{}, ErrNotLeader
	}

	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return raft.Configuration{}, err
	}

	return configFuture.Configuration(), nil
}
