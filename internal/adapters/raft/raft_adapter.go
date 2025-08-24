package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	errordomain "github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
)

type Config struct {
	NodeID             string
	BindAddr           string
	DataDir            string
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

func DefaultRaftConfig(nodeID, bindAddr, dataDir string) *Config {
	return &Config{
		NodeID:             nodeID,
		BindAddr:           bindAddr,
		DataDir:            dataDir,
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

type Adapter struct {
	raft    *raft.Raft
	config  *Config
	store   *Store
	fsm     *GraftFSM
	trans   raft.Transport
	logger  *slog.Logger
	started bool
	stopped bool
	mu      sync.Mutex
}

func NewAdapter(config *Config, logger *slog.Logger) (*Adapter, error) {
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("component", "raft", "node_id", config.NodeID)

	storeConfig := &StoreConfig{
		DataDir:            config.DataDir,
		RetainSnapshots:    config.MaxSnapshots,
		SnapshotThreshold:  config.SnapshotThreshold,
		ValueLogFileSize:   64 << 20,
		NumMemtables:       3,
		NumLevelZeroTables: 2,
		NumCompactors:      2,
	}

	store, err := NewStore(storeConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	fsm := NewGraftFSM(store.StateDB(), logger)

	return &Adapter{
		config: config,
		store:  store,
		fsm:    fsm,
		logger: logger,
	}, nil
}

func (r *Adapter) Start(ctx context.Context, existingPeers []ports.Peer) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if r.started {
		return fmt.Errorf("raft adapter already started")
	}

	r.logger.Info("starting raft adapter", "bind_addr", r.config.BindAddr)

	// Recreate store if it was closed
	if r.store == nil {
		storeConfig := &StoreConfig{
			DataDir:            r.config.DataDir,
			RetainSnapshots:    r.config.MaxSnapshots,
			SnapshotThreshold:  r.config.SnapshotThreshold,
			ValueLogFileSize:   64 << 20,
			NumMemtables:       3,
			NumLevelZeroTables: 2,
			NumCompactors:      2,
		}

		store, err := NewStore(storeConfig, r.logger)
		if err != nil {
			return fmt.Errorf("failed to create store: %w", err)
		}
		r.store = store

		r.fsm = NewGraftFSM(store.StateDB(), r.logger)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(r.config.NodeID)
	raftConfig.HeartbeatTimeout = r.config.HeartbeatTimeout
	raftConfig.ElectionTimeout = r.config.ElectionTimeout
	raftConfig.CommitTimeout = r.config.CommitTimeout
	raftConfig.MaxAppendEntries = r.config.MaxAppendEntries
	raftConfig.ShutdownOnRemove = r.config.ShutdownOnRemove
	raftConfig.TrailingLogs = r.config.TrailingLogs
	raftConfig.SnapshotInterval = r.config.SnapshotInterval
	raftConfig.SnapshotThreshold = r.config.SnapshotThreshold
	raftConfig.LeaderLeaseTimeout = r.config.LeaderLeaseTimeout

	addr, err := net.ResolveTCPAddr("tcp", r.config.BindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address %s: %w", r.config.BindAddr, err)
	}

	transport, err := raft.NewTCPTransport(r.config.BindAddr, addr, 3, 10*time.Second, io.Discard)
	if err != nil {
		return fmt.Errorf("failed to create TCP transport: %w", err)
	}
	r.trans = transport

	actualAddr := string(transport.LocalAddr())
	if actualAddr != r.config.BindAddr {
		r.logger.Debug("updated bind address from dynamic allocation", "original", r.config.BindAddr, "actual", actualAddr)
		r.config.BindAddr = actualAddr
	}

	if len(existingPeers) == 0 {
		r.logger.Debug("no existing peers found, bootstrapping single-node cluster", "node_id", r.config.NodeID)
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(r.config.NodeID),
					Address: raft.ServerAddress(r.config.BindAddr),
				},
			},
		}

		bootstrapErr := raft.BootstrapCluster(raftConfig, r.store.LogStore(), r.store.StableStore(), r.store.SnapshotStore(), transport, configuration)
		if bootstrapErr != nil {
			if errors.Is(bootstrapErr, raft.ErrCantBootstrap) {
				r.logger.Debug("cluster already bootstrapped, continuing with existing state")
			} else {
				return fmt.Errorf("failed to bootstrap cluster: %w", bootstrapErr)
			}
		} else {
			r.logger.Info("successfully bootstrapped new single-node cluster")
		}
	} else {
		r.logger.Info("found existing peers, will join cluster after startup", "peer_count", len(existingPeers))
	}

	raftNode, err := raft.NewRaft(raftConfig, r.fsm, r.store.LogStore(), r.store.StableStore(), r.store.SnapshotStore(), transport)
	if err != nil {
		return fmt.Errorf("failed to create raft node: %w", err)
	}
	r.raft = raftNode

	if len(existingPeers) > 0 {
		var joinErr error
		for attempt := 1; attempt <= r.config.MaxJoinAttempts; attempt++ {
			r.logger.Info("attempting to join existing cluster", "attempt", attempt, "max_attempts", r.config.MaxJoinAttempts)
			joinErr = r.Join(existingPeers)
			if joinErr == nil {
				r.logger.Info("successfully joined existing cluster")
				break
			}
			r.logger.Error("failed to join existing cluster", "error", joinErr)
			time.Sleep(2 * time.Second)
		}
		if joinErr != nil {
			r.logger.Error("exceeded maximum join attempts, continuing as standalone node", "error", joinErr)
		}
	}

	r.started = true
	r.logger.Info("raft adapter started successfully")

	return nil
}

func (r *Adapter) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return fmt.Errorf("raft adapter not started")
	}

	r.logger.Info("stopping raft adapter")

	if r.raft != nil {
		future := r.raft.Shutdown()
		if err := future.Error(); err != nil {
			r.logger.Error("failed to shutdown raft", "error", err)
		}
		r.raft = nil
	}

	if r.trans != nil {
		// Type assert to *raft.NetworkTransport which has Close() method
		if tcpTransport, ok := r.trans.(*raft.NetworkTransport); ok {
			if err := tcpTransport.Close(); err != nil {
				r.logger.Error("failed to close transport", "error", err)
			}
		}
		r.trans = nil
	}

	if r.store != nil {
		if err := r.store.Close(); err != nil {
			r.logger.Error("failed to close store", "error", err)
		}
		r.store = nil
	}

	r.started = false
	r.logger.Info("raft adapter stopped")
	return nil
}

func (r *Adapter) Restart(ctx context.Context, existingPeers []ports.Peer) error {
	if err := r.Stop(); err != nil {
		r.logger.Error("failed to stop adapter during restart", "error", err)
	}
	return r.Start(ctx, existingPeers)
}

func (r *Adapter) Apply(ctx context.Context, command *domain.Command) (*domain.CommandResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if !r.started {
		return nil, fmt.Errorf("raft adapter not started")
	}

	if command == nil {
		return nil, fmt.Errorf("command cannot be nil")
	}

	if err := r.validateCommand(command); err != nil {
		return nil, err
	}

	if len(command.Key) > 512 {
		return nil, errordomain.NewConfigError("command key size", errors.New("key size exceeds 512B limit"))
	}

	if len(command.Value) > 1024*1024 {
		return nil, errordomain.NewConfigError("command value size", errors.New("value size exceeds 1MB limit"))
	}

	data, err := command.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal command: %w", err)
	}

	future := r.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raft apply failed: %w", err)
	}

	if result, ok := future.Response().(*domain.CommandResult); ok {
		return result, nil
	}

	return nil, fmt.Errorf("unexpected response type from raft apply")
}

func (r *Adapter) IsLeader() bool {
	if r.raft == nil {
		return false
	}
	return r.raft.State() == raft.Leader
}

func (r *Adapter) validateCommand(command *domain.Command) error {
	if err := r.validateCommandStructure(command); err != nil {
		return fmt.Errorf("command structure validation failed: %w", err)
	}

	if err := r.validateCommandType(command); err != nil {
		return fmt.Errorf("command type validation failed: %w", err)
	}

	if err := r.validateCommandData(command); err != nil {
		return fmt.Errorf("command data validation failed: %w", err)
	}

	return nil
}

func (r *Adapter) validateCommandStructure(command *domain.Command) error {
	if command.Type != domain.CommandBatch && command.Type != domain.CommandAtomicBatch {
		if len(command.Key) == 0 {
			return fmt.Errorf("command key cannot be empty")
		}
	}

	if len(command.Key) > 256 {
		return fmt.Errorf("command key too large (max 256 characters)")
	}

	if command.IdempotencyKey != nil && len(*command.IdempotencyKey) == 0 {
		return fmt.Errorf("idempotency key cannot be empty if specified")
	}

	if command.IdempotencyKey != nil && len(*command.IdempotencyKey) > 128 {
		return fmt.Errorf("idempotency key too large (max 128 characters)")
	}

	return nil
}

func (r *Adapter) validateCommandType(command *domain.Command) error {
	switch command.Type {
	case domain.CommandPut, domain.CommandDelete, domain.CommandBatch, domain.CommandAtomicBatch,
		domain.CommandStateUpdate, domain.CommandQueueOperation,
		domain.CommandCleanupWorkflow, domain.CommandCompleteWorkflowPurge,
		domain.CommandResourceLock, domain.CommandResourceUnlock:
		return nil
	default:
		return fmt.Errorf("invalid command type: %d", uint8(command.Type))
	}
}

func (r *Adapter) validateCommandData(command *domain.Command) error {
	switch command.Type {
	case domain.CommandPut, domain.CommandStateUpdate, domain.CommandQueueOperation, domain.CommandResourceLock:
		if len(command.Value) == 0 {
			return fmt.Errorf("command value cannot be empty for %s operations", command.Type.String())
		}

	case domain.CommandBatch, domain.CommandAtomicBatch:
		if len(command.Batch) == 0 {
			return fmt.Errorf("batch operations cannot be empty")
		}

		if len(command.Batch) > 100 {
			return fmt.Errorf("batch too large (max 100 operations)")
		}

		for i, op := range command.Batch {
			if len(op.Key) == 0 {
				return fmt.Errorf("batch operation %d: key cannot be empty", i)
			}

			if len(op.Key) > 256 {
				return fmt.Errorf("batch operation %d: key too large (max 256 characters)", i)
			}

			if op.Type == domain.CommandPut && len(op.Value) == 0 {
				return fmt.Errorf("batch operation %d: PUT operation requires non-empty value", i)
			}

			if op.Type != domain.CommandPut && op.Type != domain.CommandDelete {
				return fmt.Errorf("batch operation %d: unsupported operation type %s", i, op.Type.String())
			}
		}

	case domain.CommandDelete, domain.CommandCleanupWorkflow, domain.CommandCompleteWorkflowPurge, domain.CommandResourceUnlock:

	default:
		return fmt.Errorf("validation not implemented for command type: %s", command.Type.String())
	}

	return nil
}

func (r *Adapter) GetLeader() (nodeID string, address string) {
	if r.raft == nil {
		return "", ""
	}

	addr, id := r.raft.LeaderWithID()
	return string(id), string(addr)
}

func (r *Adapter) GetLocalAddress() string {
	if r.trans == nil {
		return ""
	}
	return string(r.trans.LocalAddr())
}

func (r *Adapter) Join(peers []ports.Peer) error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	for _, peer := range peers {
		if peer.ID == r.config.NodeID {
			continue
		}

		r.logger.Info("joining existing cluster", "node_id", peer.ID, "address", fmt.Sprintf("%s:%d", peer.Address, peer.Port))
		if err := r.join(peer.ID, fmt.Sprintf("%s:%d", peer.Address, peer.Port)); err != nil {
			r.logger.Error("failed to join cluster via peer", "peer_id", peer.ID, "address", fmt.Sprintf("%s:%d", peer.Address, peer.Port), "error", err)
			return fmt.Errorf("failed to join cluster via peer %s: %w", peer.ID, err)
		} else {
			r.logger.Info("successfully joined cluster via peer", "peer_id", peer.ID, "address", fmt.Sprintf("%s:%d", peer.Address, peer.Port))
			return nil
		}
	}

	return fmt.Errorf("failed to join cluster: no reachable peers")
}

func (r *Adapter) join(nodeID, address string) error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	if nodeID == "" {
		return fmt.Errorf("nodeID cannot be empty")
	}

	if len(nodeID) > 1024 {
		return fmt.Errorf("nodeID too large (max 1024 characters)")
	}

	if address == "" {
		return fmt.Errorf("address cannot be empty")
	}

	if _, err := net.ResolveTCPAddr("tcp", address); err != nil {
		return fmt.Errorf("invalid address format: %w", err)
	}

	future := r.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 10*time.Second)
	return future.Error()
}

func (r *Adapter) Leave() error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := r.raft.RemoveServer(raft.ServerID(r.config.NodeID), 0, 10*time.Second)
	return future.Error()
}

func (r *Adapter) GetClusterInfo() ports.ClusterInfo {
	if r.raft == nil {
		return ports.ClusterInfo{
			NodeID: r.config.NodeID,
		}
	}

	leaderAddr, leaderID := r.raft.LeaderWithID()
	stats := r.raft.Stats()

	info := ports.ClusterInfo{
		NodeID: r.config.NodeID,
	}

	if leaderID != "" {
		info.Leader = &ports.RaftNodeInfo{
			ID:      string(leaderID),
			Address: string(leaderAddr),
			State:   ports.NodeLeader,
		}
	}

	configFuture := r.raft.GetConfiguration()
	if err := configFuture.Error(); err == nil {
		config := configFuture.Configuration()
		for _, server := range config.Servers {
			nodeState := ports.NodeFollower
			if server.ID == leaderID {
				nodeState = ports.NodeLeader
			} else if server.ID == raft.ServerID(r.config.NodeID) && r.raft.State() == raft.Candidate {
				nodeState = ports.NodeCandidate
			}

			info.Members = append(info.Members, ports.RaftNodeInfo{
				ID:      string(server.ID),
				Address: string(server.Address),
				State:   nodeState,
			})
		}
	}

	if term, ok := stats["term"]; ok {
		fmt.Sscanf(term, "%d", &info.Term)
	}
	if index, ok := stats["last_log_index"]; ok {
		fmt.Sscanf(index, "%d", &info.Index)
	}

	return info
}

func (r *Adapter) WaitForLeader(ctx context.Context) error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, leaderID := r.raft.LeaderWithID()
			if leaderID != "" {
				r.logger.Info("leader established", "leader_id", leaderID)
				return nil
			}
		}
	}
}

func (r *Adapter) GetStateDB() *badger.DB {
	if r.store == nil {
		return nil
	}
	return r.store.StateDB()
}

func (r *Adapter) AddNode(nodeID, address string) error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	if !r.IsLeader() {
		return fmt.Errorf("only leader can add nodes")
	}

	if nodeID == "" {
		return fmt.Errorf("nodeID cannot be empty")
	}

	if address == "" {
		return fmt.Errorf("address cannot be empty")
	}

	if _, err := net.ResolveTCPAddr("tcp", address); err != nil {
		return fmt.Errorf("invalid address format: %w", err)
	}

	future := r.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 10*time.Second)
	return future.Error()
}
