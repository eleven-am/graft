package ports

import (
	"context"
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
	Healthy bool                   `json:"healthy"`
	Error   string                 `json:"error,omitempty"`
	Details map[string]interface{} `json:"details,omitempty"`
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

type RaftNode interface {
	Start(ctx context.Context, existingPeers []Peer) error
	Apply(cmd domain.Command, timeout time.Duration) (*domain.CommandResult, error)
	IsLeader() bool
	IsProvisional() bool
	GetBootMetadata() (bootID string, launchTimestamp int64)
	SetReadinessCallback(callback func(bool))
	DemoteAndJoin(ctx context.Context, peer Peer) error
	LeaderAddr() string
	AddVoter(nodeID string, address string) error
	RemoveServer(nodeID string) error
	AddNode(nodeID, address string) error
	Shutdown() error
	StateDB() *badger.DB
	GetClusterInfo() ClusterInfo
	GetHealth() HealthStatus
	GetMetrics() RaftMetrics
	ReadStale(key string) ([]byte, error)
	TransferLeadership() error
	TransferLeadershipTo(serverID string) error
	Stop() error
	GetLocalAddress() string
	WaitForLeader(ctx context.Context) error
}

// -----------------------------------------------------------------------------
// Raft v2 experimental interfaces (subject to refinement)
// -----------------------------------------------------------------------------

// RaftController represents the lifecycle and command surface of a Raft node.
type RaftController interface {
	Start(ctx context.Context, opts domain.RaftControllerOptions) error
	Stop() error

	WaitForLeadership(ctx context.Context) error
	LeadershipInfo() RaftLeadershipInfo

	Apply(cmd domain.Command, timeout time.Duration) (*domain.CommandResult, error)
	Demote(ctx context.Context, peer RaftPeer) error

	Metadata() map[string]string
}

// RaftLeadershipState enumerates high-level leadership states emitted by the controller.
type RaftLeadershipState string

const (
	RaftLeadershipUnknown     RaftLeadershipState = "unknown"
	RaftLeadershipProvisional RaftLeadershipState = "provisional"
	RaftLeadershipLeader      RaftLeadershipState = "leader"
	RaftLeadershipFollower    RaftLeadershipState = "follower"
	RaftLeadershipJoining     RaftLeadershipState = "joining"
	RaftLeadershipDemoted     RaftLeadershipState = "demoted"
)

// RaftLeadershipInfo describes the current leadership view of the controller.
type RaftLeadershipInfo struct {
	State         RaftLeadershipState
	LeaderID      string
	LeaderAddress string
	Term          uint64
}

// RaftPeer identifies a peer in the cluster.
type RaftPeer struct {
	ID       string
	Address  string
	Metadata map[string]string
}

// BootstrapCoordinator drives bootstrap/join flows based on discovery events.
type BootstrapCoordinator interface {
	Start(ctx context.Context) error
	Stop() error
	Events() <-chan BootstrapEvent
}

// BootstrapEventType enumerates state transitions emitted during bootstrap.
type BootstrapEventType string

const (
	BootstrapEventProvisional BootstrapEventType = "provisional"
	BootstrapEventLeader      BootstrapEventType = "leader"
	BootstrapEventFollower    BootstrapEventType = "follower"
	BootstrapEventJoinStart   BootstrapEventType = "join_start"
	BootstrapEventJoinSuccess BootstrapEventType = "join_success"
	BootstrapEventJoinFailed  BootstrapEventType = "join_failed"
	BootstrapEventDemotion    BootstrapEventType = "demotion"
)

// BootstrapEvent represents a high-level bootstrap transition notification.
type BootstrapEvent struct {
	Type      BootstrapEventType
	Timestamp time.Time
	NodeID    string
	Details   map[string]string
}

// ReadinessState enumerates readiness states published to consumers.
type ReadinessState string

const (
	ReadinessStateUnknown ReadinessState = "unknown"
	ReadinessStatePending ReadinessState = "pending"
	ReadinessStateReady   ReadinessState = "ready"
	ReadinessStatePaused  ReadinessState = "paused"
)

// ReadinessGate coordinates readiness checks for workflow intake and health.
type ReadinessGate interface {
	SetState(state ReadinessState)
	State() ReadinessState
	IsReady() bool
	WaitUntilReady(ctx context.Context) error
}

// RaftTransportFactory creates transport implementations suitable for the controller.
type RaftTransportFactory interface {
	Create(ctx context.Context, bindAddr string, handler RaftRPCHandler) (RaftTransport, string, error)
}

// RaftTransport represents the transport layer used by the controller.
type RaftTransport interface {
	Close() error
}

// RaftRPCHandler abstracts handling of Raft RPC payloads.
type RaftRPCHandler interface {
	HandleRPC(ctx context.Context, data []byte) ([]byte, error)
}

// RaftStorageFactory creates storage implementations for Raft metadata/logs.
type RaftStorageFactory interface {
	Open(ctx context.Context, nodeID string) (RaftStorage, error)
}

// RaftStorage represents the log/state storage backing a controller.
type RaftStorage interface {
	Close() error
}

// Tracing interfaces moved to ports/tracing.go
