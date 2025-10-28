package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"log/slog"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/helpers/metadata"
	"github.com/eleven-am/graft/internal/ports"
)

type Node struct {
	config   *Config
	storage  *Storage
	events   ports.EventManager
	logger   *slog.Logger
	metadata map[string]string

	runtime     *Runtime
	controller  *Controller
	coordinator *Coordinator
	readiness   *Readiness

	bootID          string
	launchTimestamp int64
	readinessCb     func(bool)
	cbMu            sync.RWMutex
	stopOnce        sync.Once
	provisionalMu   sync.RWMutex
	isProvisional   bool
}

func NewNode(cfg *Config, storage *Storage, eventManager ports.EventManager, appTransport ports.TransportPort, logger *slog.Logger) (ports.RaftNode, error) {
	_ = appTransport
	if cfg == nil {
		return nil, errors.New("raft: config is required")
	}
	if storage == nil {
		return nil, errors.New("raft: storage is required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	bootMeta := metadata.GetGlobalBootstrapMetadata()
	bootID := metadata.GetBootID(bootMeta)
	launch := metadata.ExtractLaunchTimestamp(bootMeta)

	readiness := NewReadiness()

	runtime := NewRuntime(RuntimeDeps{
		StorageProvider:   &staticStorageProvider{resources: storage.resources()},
		TransportProvider: &TCPTransportProvider{},
		FSMFactory:        &FSMFactory{EventManager: eventManager, Logger: logger},
		Logger:            logger,
	})

	strategy := NewPollingStrategy(runtime, 250*time.Millisecond, logger)
	coordinator := NewCoordinator(strategy, CoordinatorConfig{NodeID: cfg.NodeID, Logger: logger})

	controller, err := NewController(ControllerDeps{
		Coordinator: coordinator,
		Readiness:   readiness,
		Runtime:     runtime,
		Logger:      logger,
	})
	if err != nil {
		return nil, err
	}

	n := &Node{
		config:          cfg,
		storage:         storage,
		events:          eventManager,
		logger:          logger.With("component", "raft.node", "node_id", cfg.NodeID),
		metadata:        bootMeta,
		runtime:         runtime,
		controller:      controller,
		coordinator:     coordinator,
		readiness:       readiness,
		bootID:          bootID,
		launchTimestamp: launch,
		isProvisional:   false,
	}

	return n, nil
}

func (n *Node) Start(ctx context.Context, existingPeers []ports.Peer) error {
	n.setProvisional(len(existingPeers) == 0)
	options := domain.RaftControllerOptions{
		NodeID:            n.config.NodeID,
		BindAddress:       n.config.BindAddr,
		DataDir:           n.config.DataDir,
		BootstrapMetadata: metadata.ExtendMetadata(nil, n.metadata),
		Peers:             convertPeers(existingPeers),
		RuntimeConfig: domain.RaftRuntimeConfig{
			ClusterID:          n.config.ClusterID,
			ClusterPolicy:      n.config.ClusterPolicy,
			SnapshotInterval:   n.config.SnapshotInterval,
			SnapshotThreshold:  n.config.SnapshotThreshold,
			MaxSnapshots:       n.config.MaxSnapshots,
			MaxJoinAttempts:    n.config.MaxJoinAttempts,
			HeartbeatTimeout:   n.config.HeartbeatTimeout,
			ElectionTimeout:    n.config.ElectionTimeout,
			CommitTimeout:      n.config.CommitTimeout,
			MaxAppendEntries:   n.config.MaxAppendEntries,
			ShutdownOnRemove:   n.config.ShutdownOnRemove,
			TrailingLogs:       n.config.TrailingLogs,
			LeaderLeaseTimeout: n.config.LeaderLeaseTimeout,
		},
	}

	if err := n.controller.Start(ctx, options); err != nil {
		return err
	}

	return nil
}

func convertPeers(peers []ports.Peer) []domain.RaftPeerSpec {
	if len(peers) == 0 {
		return nil
	}
	result := make([]domain.RaftPeerSpec, 0, len(peers))
	for _, peer := range peers {
		result = append(result, domain.RaftPeerSpec{
			ID:       peer.ID,
			Address:  net.JoinHostPort(peer.Address, strconv.Itoa(peer.Port)),
			Metadata: peer.Metadata,
		})
	}
	return result
}

func (n *Node) Stop() error {
	var stopErr error
	n.stopOnce.Do(func() {
		stopErr = errors.Join(stopErr, n.controller.Stop())
	})
	return stopErr
}

func (n *Node) Shutdown() error { return n.Stop() }

func (n *Node) Apply(cmd domain.Command, timeout time.Duration) (*domain.CommandResult, error) {
	return n.controller.Apply(cmd, timeout)
}

func (n *Node) WaitForLeader(ctx context.Context) error {
	return n.controller.WaitForLeadership(ctx)
}

func (n *Node) SetConnectorLeaseCleaner(cleaner ports.ConnectorLeaseCleaner) {
	if n == nil || n.runtime == nil {
		return
	}
	n.runtime.SetConnectorLeaseCleaner(cleaner)
}

func (n *Node) IsLeader() bool {
	info := n.controller.LeadershipInfo()
	return info.State == ports.RaftLeadershipLeader && info.LeaderID == n.config.NodeID
}

func (n *Node) IsProvisional() bool {
	n.provisionalMu.RLock()
	provisional := n.isProvisional
	n.provisionalMu.RUnlock()

	if !provisional {
		return false
	}

	info := n.controller.LeadershipInfo()
	if info.State == ports.RaftLeadershipFollower {
		n.setProvisional(false)
		return false
	}

	cluster := n.runtime.ClusterInfo()
	if len(cluster.Members) > 1 {
		n.setProvisional(false)
		return false
	}

	return true
}

func (n *Node) GetBootMetadata() (string, int64) {
	return n.bootID, n.launchTimestamp
}

func (n *Node) SetReadinessCallback(cb func(bool)) {
	n.cbMu.Lock()
	n.readinessCb = cb
	n.cbMu.Unlock()

	if cb != nil {
		n.readiness.SetListener(func(state ports.ReadinessState) {
			n.cbMu.RLock()
			callback := n.readinessCb
			n.cbMu.RUnlock()
			if callback != nil {
				callback(state == ports.ReadinessStateReady)
			}
		})
		cb(n.readiness.State() == ports.ReadinessStateReady)
	} else {
		n.readiness.SetListener(nil)
	}
}

func (n *Node) DemoteAndJoin(ctx context.Context, peer ports.Peer) error {
	return n.controller.Demote(ctx, ports.RaftPeer{ID: peer.ID, Address: net.JoinHostPort(peer.Address, fmt.Sprintf("%d", peer.Port)), Metadata: peer.Metadata})
}

func (n *Node) LeaderAddr() string {
	info := n.controller.LeadershipInfo()
	return info.LeaderAddress
}

func (n *Node) AddVoter(nodeID string, address string) error {
	if err := n.runtime.AddVoter(nodeID, address); err != nil {
		return err
	}
	n.setProvisional(false)
	return nil
}

func (n *Node) RemoveServer(nodeID string) error {
	return n.runtime.RemoveServer(nodeID)
}

func (n *Node) AddNode(nodeID, address string) error {
	if err := n.runtime.AddVoter(nodeID, address); err != nil {
		return err
	}
	n.setProvisional(false)
	return nil
}

func (n *Node) TransferLeadership() error {
	return n.runtime.TransferLeadership()
}

func (n *Node) TransferLeadershipTo(serverID string) error {
	return n.runtime.TransferLeadershipTo(serverID)
}

func (n *Node) StateDB() *badger.DB {
	return n.runtime.StateDB()
}

func (n *Node) ReadStale(key string) ([]byte, error) {
	return n.runtime.ReadStale(key)
}

func (n *Node) GetClusterInfo() ports.ClusterInfo {
	return n.runtime.ClusterInfo()
}

func (n *Node) GetMetrics() ports.RaftMetrics {
	return n.runtime.Metrics()
}

func (n *Node) GetHealth() ports.HealthStatus {
	return n.runtime.Health()
}

func (n *Node) GetLocalAddress() string {
	return n.runtime.LocalAddress()
}

func (n *Node) WaitForConfiguration(ctx context.Context, minMembers int) error {
	return n.runtime.WaitForConfiguration(ctx, minMembers)
}

func (n *Node) SetEventManager(manager ports.EventManager) {
	n.events = manager
	n.runtime.SetEventManager(manager)
}

func (n *Node) setProvisional(value bool) {
	n.provisionalMu.Lock()
	if n.isProvisional != value {
		n.logger.Debug("updating provisional state", "from", n.isProvisional, "to", value)
	}
	n.isProvisional = value
	n.provisionalMu.Unlock()
}

type staticStorageProvider struct {
	resources *StorageResources
}

func (s *staticStorageProvider) Create(context.Context, domain.RaftControllerOptions) (*StorageResources, error) {
	return s.resources, nil
}
