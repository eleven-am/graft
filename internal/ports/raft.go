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