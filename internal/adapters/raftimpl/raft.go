package raftimpl

import (
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/hashicorp/raft"
)

type RaftConfig struct {
	NodeID             string
	BindAddr           string
	DataDir            string
	SnapshotInterval   time.Duration
	SnapshotThreshold  uint64
	MaxSnapshots       int
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	CommitTimeout      time.Duration
	MaxAppendEntries   int
	ShutdownOnRemove   bool
	TrailingLogs       uint64
	SnapshotRetention  time.Duration
	LeaderLeaseTimeout time.Duration
	LocalID            raft.ServerID
}

type RaftInterface interface {
	State() raft.RaftState
	Apply([]byte, time.Duration) raft.ApplyFuture
	LeaderWithID() (raft.ServerAddress, raft.ServerID)
	GetConfiguration() raft.ConfigurationFuture
	RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	AddVoter(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	Stats() map[string]string
	Shutdown() raft.Future
	Snapshot() raft.SnapshotFuture
	BootstrapCluster(configuration raft.Configuration) raft.Future
}

var _ RaftInterface = (*raft.Raft)(nil)

type RaftNode struct {
	raft   RaftInterface
	config *RaftConfig
	store  *Store
	fsm    *GraftFSM
	trans  raft.Transport
	logger *slog.Logger
}

func NewRaftNode(config *RaftConfig, store *Store, fsm *GraftFSM, logger *slog.Logger) (*RaftNode, error) {
	logger.Debug("entered NewRaftNode", "node_id", config.NodeID)

	logger.Debug("setting up raft configuration")
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	raftConfig.HeartbeatTimeout = config.HeartbeatTimeout
	raftConfig.ElectionTimeout = config.ElectionTimeout
	raftConfig.CommitTimeout = config.CommitTimeout
	raftConfig.MaxAppendEntries = config.MaxAppendEntries
	raftConfig.ShutdownOnRemove = config.ShutdownOnRemove
	raftConfig.TrailingLogs = config.TrailingLogs
	raftConfig.SnapshotInterval = config.SnapshotInterval
	raftConfig.SnapshotThreshold = config.SnapshotThreshold
	raftConfig.LeaderLeaseTimeout = config.LeaderLeaseTimeout
	logger.Debug("raft config setup complete")

	logger.Debug("resolving TCP address", "bind_addr", config.BindAddr)
	addr, err := net.ResolveTCPAddr("tcp", config.BindAddr)
	if err != nil {
		logger.Error("failed to resolve TCP address", "bind_addr", config.BindAddr, "error", err.Error())
		return nil, domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "invalid raft bind address",
			Details: map[string]interface{}{
				"bind_addr": config.BindAddr,
				"error":     err.Error(),
			},
		}
	}
	logger.Debug("TCP address resolved successfully", "resolved_addr", addr.String())

	logger.Debug("creating TCP transport", "bind_addr", config.BindAddr)
	transport, err := raft.NewTCPTransport(config.BindAddr, addr, 3, 10*time.Second, io.Discard)
	if err != nil {
		logger.Error("failed to create TCP transport", "bind_addr", config.BindAddr, "error", err.Error())
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create raft TCP transport",
			Details: map[string]interface{}{
				"bind_addr": config.BindAddr,
				"error":     err.Error(),
			},
		}
	}
	logger.Debug("TCP transport created successfully")

	logger.Debug("creating raft configuration for bootstrap", "node_id", config.NodeID)
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(config.NodeID),
				Address: raft.ServerAddress(config.BindAddr),
			},
		},
	}

	logger.Debug("calling raft.BootstrapCluster", "node_id", config.NodeID)
	bootstrapErr := raft.BootstrapCluster(raftConfig, store.LogStore(), store.StableStore(), store.SnapshotStore(), transport, configuration)
	if bootstrapErr != nil {
		if bootstrapErr == raft.ErrCantBootstrap {
			logger.Debug("cluster already bootstrapped, continuing normally", "node_id", config.NodeID)
		} else {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to bootstrap raft cluster",
				Details: map[string]interface{}{
					"node_id": config.NodeID,
					"error":   bootstrapErr.Error(),
				},
			}
		}
	} else {
		logger.Info("successfully bootstrapped new raft cluster", "node_id", config.NodeID)
	}

	logger.Debug("creating new raft instance", "node_id", config.NodeID)
	r, err := raft.NewRaft(
		raftConfig,
		fsm,
		store.LogStore(),
		store.StableStore(),
		store.SnapshotStore(),
		transport,
	)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create raft instance",
			Details: map[string]interface{}{
				"node_id": config.NodeID,
				"error":   err.Error(),
			},
		}
	}

	return &RaftNode{
		raft:   r,
		config: config,
		store:  store,
		fsm:    fsm,
		trans:  transport,
		logger: logger,
	}, nil
}

func (n *RaftNode) Raft() RaftInterface {
	return n.raft
}

func (n *RaftNode) IsLeader() bool {
	if n.raft == nil {
		return false
	}
	return n.raft.State() == raft.Leader
}

func (n *RaftNode) LeaderAddr() string {
	if n.raft == nil {
		return ""
	}
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

func (n *RaftNode) AddVoter(id, addr string) error {
	n.logger.Info("adding voter", "id", id, "addr", addr)

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get raft configuration",
			Details: map[string]interface{}{
				"node_id": id,
				"error":   err.Error(),
			},
		}
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(id) || srv.Address == raft.ServerAddress(addr) {
			if srv.ID == raft.ServerID(id) && srv.Address == raft.ServerAddress(addr) {
				n.logger.Info("node already member of cluster", "id", id)
				return nil
			}

			future := n.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to remove existing raft server",
					Details: map[string]interface{}{
						"server_id": string(srv.ID),
						"error":     err.Error(),
					},
				}
			}
		}
	}

	f := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	if err := f.Error(); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to add raft voter",
			Details: map[string]interface{}{
				"voter_id": id,
				"addr":     addr,
				"error":    err.Error(),
			},
		}
	}

	n.logger.Info("voter added successfully", "id", id, "addr", addr)
	return nil
}

func (n *RaftNode) RemoveServer(id string) error {
	n.logger.Info("removing server", "id", id)

	f := n.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := f.Error(); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to remove raft server",
			Details: map[string]interface{}{
				"server_id": id,
				"error":     err.Error(),
			},
		}
	}

	n.logger.Info("server removed successfully", "id", id)
	return nil
}

func (n *RaftNode) Apply(cmd []byte, timeout time.Duration) error {
	if !n.IsLeader() {
		var stateStr string
		if n.raft == nil {
			stateStr = "nil"
		} else {
			stateStr = n.raft.State().String()
		}
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "raft node is not the leader",
			Details: map[string]interface{}{
				"node_id": n.config.NodeID,
				"state":   stateStr,
			},
		}
	}

	f := n.raft.Apply(cmd, timeout)
	if err := f.Error(); err != nil {
		return err
	}

	resp := f.Response()
	if result, ok := resp.(*CommandResult); ok && !result.Success {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "raft command execution failed",
			Details: map[string]interface{}{
				"error": result.Error,
			},
		}
	}

	return nil
}

func (n *RaftNode) Stats() map[string]string {
	return n.raft.Stats()
}

func (n *RaftNode) Shutdown() error {
	n.logger.Info("shutting down raft node")

	future := n.raft.Shutdown()
	if err := future.Error(); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to shutdown raft node",
			Details: map[string]interface{}{
				"node_id": n.config.NodeID,
				"error":   err.Error(),
			},
		}
	}

	if n.store != nil {
		if err := n.store.Close(); err != nil {
			n.logger.Error("failed to close store", "error", err)
		}
	}

	return nil
}

func (n *RaftNode) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			addr, _ := n.raft.LeaderWithID()
			if addr != "" {
				n.logger.Info("leader elected", "leader_addr", string(addr))
				return nil
			}
		case <-timer.C:
			return domain.Error{
				Type:    domain.ErrorTypeTimeout,
				Message: "timeout waiting for raft leader election",
				Details: map[string]interface{}{
					"node_id": n.config.NodeID,
					"timeout": timeout.String(),
				},
			}
		}
	}
}

func (n *RaftNode) TakeSnapshot() error {
	n.logger.Info("taking manual snapshot")

	future := n.raft.Snapshot()
	if err := future.Error(); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to take raft snapshot",
			Details: map[string]interface{}{
				"node_id": n.config.NodeID,
				"error":   err.Error(),
			},
		}
	}

	n.logger.Info("snapshot taken successfully")
	return nil
}

type raftLogger struct {
	logger *slog.Logger
}

func (l *raftLogger) Write(p []byte) (n int, err error) {
	l.logger.Info(string(p))
	return len(p), nil
}

func DefaultRaftConfig(nodeID, bindAddr, dataDir string) *RaftConfig {
	return &RaftConfig{
		NodeID:             nodeID,
		BindAddr:           bindAddr,
		DataDir:            filepath.Join(dataDir, nodeID),
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		MaxSnapshots:       3,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       10240,
		SnapshotRetention:  120 * time.Second,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		LocalID:            raft.ServerID(nodeID),
	}
}
