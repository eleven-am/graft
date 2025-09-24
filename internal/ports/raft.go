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

type Span interface {
	SetTag(key string, value interface{})
	SetError(err error)
	AddEvent(name string, attributes map[string]interface{})
	Finish()
	Context() SpanContext
}

type SpanContext interface {
	TraceID() string
	SpanID() string
	TraceFlags() byte
	TraceState() string
}

type Tracer interface {
	StartSpan(operationName string) Span
	StartSpanWithParent(operationName string, parent SpanContext) Span
	InjectContext(ctx context.Context, span SpanContext) context.Context
	ExtractContext(ctx context.Context) (SpanContext, bool)
}

type TracingProvider interface {
	GetTracer(name string) Tracer
	Shutdown() error
	ForceFlush() error
}
