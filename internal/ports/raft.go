package ports

import (
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
)

type NodeState int

const (
	NodeFollower NodeState = iota
	NodeCandidate
	NodeLeader
)

type Peer struct {
	ID      string
	Address string
	Port    int
}

type RaftNodeInfo struct {
	ID      string
	Address string
	State   NodeState
}

type ClusterInfo struct {
	Leader  *RaftNodeInfo
	Members []RaftNodeInfo
	NodeID  string
	Term    uint64
	Index   uint64
}

type HealthStatus struct {
	Healthy                bool                   `json:"healthy"`
	Error                  string                 `json:"error,omitempty"`
	Details                map[string]interface{} `json:"details,omitempty"`
	StaleConfigDetected    bool                   `json:"stale_config_detected,omitempty"`
	ReconciliationState    string                 `json:"reconciliation_state,omitempty"`
	PersistedMemberCount   int                    `json:"persisted_member_count,omitempty"`
	ExpectedMemberCount    int                    `json:"expected_member_count,omitempty"`
	StaleAddressRecovered  bool                   `json:"stale_address_recovered,omitempty"`
	ConfigMismatchDetected bool                   `json:"config_mismatch_detected,omitempty"`
}

type HealthCheckProvider interface {
	GetHealth() HealthStatus
}

type SystemMetrics struct {
	Engine         interface{} `json:"engine,omitempty"`
	Cluster        interface{} `json:"cluster,omitempty"`
	Raft           interface{} `json:"raft,omitempty"`
	Queue          interface{} `json:"queue,omitempty"`
	Storage        interface{} `json:"storage,omitempty"`
	CircuitBreaker interface{} `json:"circuit_breaker,omitempty"`
	RateLimiter    interface{} `json:"rate_limiter,omitempty"`
	Tracing        interface{} `json:"tracing,omitempty"`
}

type MetricsProvider interface {
	GetMetrics() SystemMetrics
}

type RaftMetrics struct {
	NodeID        string `json:"node_id"`
	State         string `json:"state"`
	Term          uint64 `json:"term"`
	LastLogIndex  uint64 `json:"last_log_index"`
	LastLogTerm   uint64 `json:"last_log_term"`
	CommitIndex   uint64 `json:"commit_index"`
	AppliedIndex  uint64 `json:"applied_index"`
	LeaderID      string `json:"leader_id,omitempty"`
	LeaderAddress string `json:"leader_address,omitempty"`
	IsLeader      bool   `json:"is_leader"`
	ClusterSize   int    `json:"cluster_size"`
}

type RaftRawLeader struct {
	ID   string `json:"leader_id"`
	Addr string `json:"leader_addr"`
}

type RaftStatsInfo struct {
	LastLogIndex uint64 `json:"last_log_index"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
}

type RaftStatus struct {
	Leadership            RaftLeadershipInfo `json:"leadership"`
	RawState              string             `json:"state"`
	RawLeader             RaftRawLeader      `json:"raw_leader"`
	Stats                 RaftStatsInfo      `json:"stats"`
	Config                []RaftNodeInfo     `json:"config,omitempty"`
	StaleAddressRecovered bool               `json:"stale_address_recovered,omitempty"`
}

type RaftNode interface {
	Apply(cmd domain.Command, timeout time.Duration) (*domain.CommandResult, error)
	IsLeader() bool
	LeaderAddr() string
	StateDB() *badger.DB
	GetClusterInfo() ClusterInfo
	GetHealth() HealthStatus
	GetMetrics() RaftMetrics
	GetRaftStatus() RaftStatus
	GetLeadershipInfo() RaftLeadershipInfo
	TransferLeadership() error
	TransferLeadershipTo(serverID string) error
	Stop() error
}

type RaftLeadershipState string

const (
	RaftLeadershipUnknown   RaftLeadershipState = "unknown"
	RaftLeadershipLeader    RaftLeadershipState = "leader"
	RaftLeadershipFollower  RaftLeadershipState = "follower"
	RaftLeadershipCandidate RaftLeadershipState = "candidate"
)

type RaftLeadershipInfo struct {
	State         RaftLeadershipState
	LeaderID      string
	LeaderAddress string
	Term          uint64
}

type RaftPeer struct {
	ID      string
	Address string
}
