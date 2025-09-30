package domain

import "time"

// RaftPeerSpec describes a peer that may participate in the cluster.
type RaftPeerSpec struct {
	ID       string
	Address  string
	Metadata map[string]string
}

// RaftRuntimeConfig captures timing and replication parameters for the raft2
// controller runtime. These values are intentionally a subset of the legacy
// raft adapter configuration to keep the new subsystem focused on essential
// behaviour first.
type RaftRuntimeConfig struct {
	ClusterID          string
	ClusterPolicy      ClusterPolicy
	SnapshotInterval   time.Duration
	SnapshotThreshold  uint64
	MaxSnapshots       int
	MaxJoinAttempts    int
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	CommitTimeout      time.Duration
	MaxAppendEntries   int
	ShutdownOnRemove   bool
	TrailingLogs       uint64
	LeaderLeaseTimeout time.Duration
}

// RaftControllerOptions captures the configuration required to start a raft
// controller instance within the new raft2 subsystem.
type RaftControllerOptions struct {
	NodeID            string
	BindAddress       string
	DataDir           string
	BootstrapMetadata map[string]string
	Peers             []RaftPeerSpec
	RuntimeConfig     RaftRuntimeConfig
}
