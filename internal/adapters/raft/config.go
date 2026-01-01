package raft

import (
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

// Config mirrors the legacy raft adapter configuration so the manager can
// continue to construct nodes without large refactors while the underlying
// implementation is replaced by raft components.
type Config struct {
	NodeID              string
	ClusterID           string
	BindAddr            string
	AdvertiseAddr       string
	DataDir             string
	ClusterPolicy       domain.ClusterPolicy
	SnapshotInterval    time.Duration
	SnapshotThreshold   uint64
	MaxSnapshots        int
	MaxJoinAttempts     int
	HeartbeatTimeout    time.Duration
	ElectionTimeout     time.Duration
	CommitTimeout       time.Duration
	MaxAppendEntries    int
	ShutdownOnRemove    bool
	TrailingLogs        uint64
	LeaderLeaseTimeout  time.Duration
	IgnoreExistingState bool
	InMemoryStorage     bool
}

// DefaultRaftConfig reproduces the old helper so existing callers continue to
// compile. Values feed into raft runtime configuration internally.
func DefaultRaftConfig(nodeID, clusterID, bindAddr, advertiseAddr, dataDir string, clusterPolicy domain.ClusterPolicy) *Config {
	return &Config{
		NodeID:             nodeID,
		ClusterID:          clusterID,
		BindAddr:           bindAddr,
		AdvertiseAddr:      advertiseAddr,
		DataDir:            dataDir,
		ClusterPolicy:      clusterPolicy,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  1024,
		MaxSnapshots:       5,
		MaxJoinAttempts:    5,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      500 * time.Millisecond,
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       10240,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}
}
