package ports

import (
	"context"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
)

type RaftPort interface {
	Start(ctx context.Context, existingPeers []Peer) error
	Stop() error
	Apply(ctx context.Context, command *domain.Command) (*domain.CommandResult, error)
	IsLeader() bool
	GetLeader() (nodeID string, address string)
	Join(peers []Peer) error
	Leave() error
	GetClusterInfo() ClusterInfo
	WaitForLeader(ctx context.Context) error
	GetStateDB() *badger.DB
}

type ClusterInfo struct {
	Leader  *RaftNodeInfo
	Members []RaftNodeInfo
	NodeID  string
	Term    uint64
	Index   uint64
}

type RaftNodeInfo struct {
	ID      string
	Address string
	State   NodeState
}

type NodeState int

const (
	NodeFollower NodeState = iota
	NodeCandidate
	NodeLeader
)
