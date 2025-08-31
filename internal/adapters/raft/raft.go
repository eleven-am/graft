package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

type Config struct {
	NodeID             string
	ClusterID          string
	BindAddr           string
	DataDir            string
	ClusterPolicy      domain.ClusterPolicy
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

func DefaultRaftConfig(nodeID, clusterID, bindAddr, dataDir string, clusterPolicy domain.ClusterPolicy) *Config {
	return &Config{
		NodeID:             nodeID,
		ClusterID:          clusterID,
		BindAddr:           bindAddr,
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

type Node struct {
	raft         *raft.Raft
	config       *Config
	storage      *Storage
	fsm          *FSM
	transport    raft.Transport
	logger       *slog.Logger
	started      bool
	stopped      bool
	mu           sync.Mutex
	eventManager ports.EventManager
	observer     *raft.Observer
	observerChan chan raft.Observation
}

func NewNode(config *Config, storage *Storage, eventManager ports.EventManager, logger *slog.Logger) (*Node, error) {
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("component", "raft", "node_id", config.NodeID)
	fsm := NewFSM(storage.StateDB(), eventManager, config.NodeID, config.ClusterID, config.ClusterPolicy, logger)

	observerChan := make(chan raft.Observation, 100)
	observer := raft.NewObserver(observerChan, false, nil)

	return &Node{
		config:       config,
		storage:      storage,
		fsm:          fsm,
		logger:       logger,
		eventManager: eventManager,
		observer:     observer,
		observerChan: observerChan,
	}, nil
}

func (r *Node) Start(ctx context.Context, existingPeers []ports.Peer) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if r.started {
		return fmt.Errorf("raft node already started")
	}

	r.logger.Info("starting raft node", "bind_addr", r.config.BindAddr)

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
	raftConfig.Logger = slogToHcLogger(r.logger)

	addr, err := net.ResolveTCPAddr("tcp", r.config.BindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address %s: %w", r.config.BindAddr, err)
	}

	transport, err := raft.NewTCPTransport(r.config.BindAddr, addr, 3, 10*time.Second, io.Discard)
	if err != nil {
		return fmt.Errorf("failed to create TCP transport: %w", err)
	}
	r.transport = transport

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

		bootstrapErr := raft.BootstrapCluster(raftConfig, r.storage.LogStore(), r.storage.StableStore(), r.storage.SnapshotStore(), transport, configuration)
		if bootstrapErr != nil {
			if errors.Is(bootstrapErr, raft.ErrCantBootstrap) {
				r.logger.Debug("cluster already bootstrapped, continuing with existing state")
			} else {
				transport.Close()
				return fmt.Errorf("failed to bootstrap cluster: %w", bootstrapErr)
			}
		} else {
			r.logger.Info("successfully bootstrapped new single-node cluster")
		}
	} else {
		r.logger.Info("found existing peers, will join cluster after startup", "peer_count", len(existingPeers))
	}

	raftNode, err := raft.NewRaft(raftConfig, r.fsm, r.storage.LogStore(), r.storage.StableStore(), r.storage.SnapshotStore(), transport)
	if err != nil {
		transport.Close()
		return fmt.Errorf("failed to create raft node: %w", err)
	}
	r.raft = raftNode

	r.raft.RegisterObserver(r.observer)
	go r.processObservations(ctx)

	if len(existingPeers) > 0 {
		peerStrings := make([]string, len(existingPeers))
		for i, peer := range existingPeers {
			peerStrings[i] = peer.Address
		}
		go r.joinClusterWithBackoff(peerStrings)
	}

	r.started = true
	return nil
}

func (r *Node) Apply(cmd domain.Command, timeout time.Duration) (*domain.CommandResult, error) {
	if r.raft == nil {
		return nil, domain.ErrNotFound
	}
	if r.raft.State() != raft.Leader {
		return nil, domain.ErrNotFound
	}

	data, err := cmd.Marshal()
	if err != nil {
		return nil, domain.ErrInvalidInput
	}

	future := r.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, domain.ErrTimeout
	}

	result, ok := future.Response().(*domain.CommandResult)
	if !ok {
		return nil, domain.ErrInvalidInput
	}

	return result, nil
}

func (r *Node) IsLeader() bool {
	return r.raft.State() == raft.Leader
}

func (r *Node) LeaderAddr() string {
	addr, _ := r.raft.LeaderWithID()
	return string(addr)
}

func (r *Node) AddVoter(nodeID string, address string) error {
	if r.raft.State() != raft.Leader {
		return domain.ErrNotFound
	}

	future := r.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 10*time.Second)
	return future.Error()
}

func (r *Node) RemoveServer(nodeID string) error {
	if r.raft.State() != raft.Leader {
		return domain.ErrNotFound
	}

	future := r.raft.RemoveServer(raft.ServerID(nodeID), 0, 10*time.Second)
	return future.Error()
}

func (r *Node) Join(peers []ports.Peer) error {
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
			continue
		} else {
			r.logger.Info("successfully joined cluster via peer", "peer_id", peer.ID, "address", fmt.Sprintf("%s:%d", peer.Address, peer.Port))
			return nil
		}
	}

	return fmt.Errorf("failed to join cluster: no reachable peers")
}

func (r *Node) join(nodeID, address string) error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
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

func (r *Node) GetClusterInfo() ports.ClusterInfo {
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

func (r *Node) GetHealth() ports.HealthStatus {
	if r.raft == nil {
		return ports.HealthStatus{
			Healthy: false,
			Error:   "raft not initialized",
		}
	}

	stats := r.raft.Stats()

	health := ports.HealthStatus{
		Healthy: true,
		Details: make(map[string]interface{}),
	}

	health.Details["state"] = r.raft.State().String()
	health.Details["term"] = stats["term"]
	health.Details["last_log_index"] = stats["last_log_index"]
	health.Details["last_log_term"] = stats["last_log_term"]
	health.Details["commit_index"] = stats["commit_index"]
	health.Details["applied_index"] = stats["applied_index"]

	leaderAddr, leaderID := r.raft.LeaderWithID()
	if leaderID != "" {
		health.Details["leader_id"] = string(leaderID)
		health.Details["leader_address"] = string(leaderAddr)
	}

	configFuture := r.raft.GetConfiguration()
	if err := configFuture.Error(); err == nil {
		config := configFuture.Configuration()
		health.Details["cluster_size"] = len(config.Servers)

		if r.raft.State() == raft.Leader {
			health.Details["is_leader"] = true
		} else if leaderID == "" {
			health.Healthy = false
			health.Error = "no leader elected"
		}
	} else {
		health.Healthy = false
		health.Error = fmt.Sprintf("failed to get cluster configuration: %v", err)
	}

	return health
}

func (r *Node) GetMetrics() ports.RaftMetrics {
	if r.raft == nil {
		return ports.RaftMetrics{}
	}

	stats := r.raft.Stats()
	metrics := ports.RaftMetrics{
		NodeID: r.config.NodeID,
		State:  r.raft.State().String(),
	}

	if term, ok := stats["term"]; ok {
		fmt.Sscanf(term, "%d", &metrics.Term)
	}
	if lastLogIndex, ok := stats["last_log_index"]; ok {
		fmt.Sscanf(lastLogIndex, "%d", &metrics.LastLogIndex)
	}
	if lastLogTerm, ok := stats["last_log_term"]; ok {
		fmt.Sscanf(lastLogTerm, "%d", &metrics.LastLogTerm)
	}
	if commitIndex, ok := stats["commit_index"]; ok {
		fmt.Sscanf(commitIndex, "%d", &metrics.CommitIndex)
	}
	if appliedIndex, ok := stats["applied_index"]; ok {
		fmt.Sscanf(appliedIndex, "%d", &metrics.AppliedIndex)
	}

	leaderAddr, leaderID := r.raft.LeaderWithID()
	if leaderID != "" {
		metrics.LeaderID = string(leaderID)
		metrics.LeaderAddress = string(leaderAddr)
		metrics.IsLeader = r.raft.State() == raft.Leader
	}

	configFuture := r.raft.GetConfiguration()
	if err := configFuture.Error(); err == nil {
		config := configFuture.Configuration()
		metrics.ClusterSize = len(config.Servers)
	}

	return metrics
}

func (r *Node) joinClusterWithBackoff(peers []string) {
	backoffDuration := 2 * time.Second
	maxBackoff := 30 * time.Second

	for attempt := 1; attempt <= r.config.MaxJoinAttempts; attempt++ {
		r.logger.Info("attempting to join existing cluster", "attempt", attempt, "max_attempts", r.config.MaxJoinAttempts)

		selectedPeer := r.selectBestPeer(peers)
		if selectedPeer == "" {
			r.logger.Error("no suitable peer found for joining")
			continue
		}

		err := r.JoinPeer(selectedPeer)
		if err == nil {
			r.logger.Info("successfully joined existing cluster", "peer", selectedPeer)
			return
		}

		r.logger.Error("failed to join cluster via peer", "error", err, "peer", selectedPeer, "attempt", attempt)

		if attempt < r.config.MaxJoinAttempts {
			r.logger.Info("backing off before retry", "backoff_duration", backoffDuration)
			time.Sleep(backoffDuration)
			backoffDuration = backoffDuration * 2
			if backoffDuration > maxBackoff {
				backoffDuration = maxBackoff
			}
		}
	}

	r.logger.Error("exceeded maximum join attempts, continuing as standalone node")
}

func (r *Node) selectBestPeer(peers []string) string {
	if len(peers) == 0 {
		return ""
	}

	sort.Strings(peers)
	return peers[0]
}

func (r *Node) JoinPeer(peer string) error {
	peers := []ports.Peer{
		{
			ID:      "",
			Address: peer,
		},
	}
	return r.Join(peers)
}

func (r *Node) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return fmt.Errorf("raft node not started")
	}

	r.logger.Info("stopping raft node")

	if r.raft != nil {
		future := r.raft.Shutdown()
		if err := future.Error(); err != nil {
			r.logger.Error("failed to shutdown raft", "error", err)
		}
		r.raft = nil
	}

	if r.transport != nil {
		if closer, ok := r.transport.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				r.logger.Error("failed to close transport", "error", err)
			}
		}
		r.transport = nil
	}

	if r.storage != nil {
		if err := r.storage.Close(); err != nil {
			r.logger.Error("failed to close storage", "error", err)
		}
		r.storage = nil
	}

	r.started = false
	r.stopped = true
	r.logger.Info("raft node stopped")
	return nil
}

func (r *Node) GetLocalAddress() string {
	if r.transport == nil {
		return ""
	}
	return string(r.transport.LocalAddr())
}

func (r *Node) WaitForLeader(ctx context.Context) error {
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

func (r *Node) AddNode(nodeID, address string) error {
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

func (r *Node) Shutdown() error {
	if err := r.raft.Shutdown().Error(); err != nil {
		r.logger.Error("failed to shutdown raft", "error", err)
	}

	if closer, ok := r.transport.(interface{ Close() error }); ok {
		closer.Close()
	}

	return nil
}

func (r *Node) StateDB() *badger.DB {
	return r.storage.StateDB()
}

func (r *Node) ReadStale(key string) ([]byte, error) {
	if r.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	db := r.storage.StateDB()
	if db == nil {
		return nil, fmt.Errorf("state database not available")
	}

	var value []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value = append([]byte(nil), val...)
			return nil
		})
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, domain.ErrNotFound
	}

	return value, err
}

func (r *Node) TransferLeadership() error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
	}
	return r.raft.LeadershipTransfer().Error()
}

func (r *Node) TransferLeadershipTo(serverID string) error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
	}
	return r.raft.LeadershipTransferToServer(raft.ServerID(serverID), raft.ServerAddress("")).Error()
}

func slogToHcLogger(logger *slog.Logger) hclog.Logger {
	return &slogAdapter{logger: logger}
}

type slogAdapter struct {
	logger  *slog.Logger
	implied []interface{}
}

func (s *slogAdapter) isEnabled(level slog.Level) bool {
	return s.logger.Enabled(context.Background(), level)
}

func (s *slogAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Trace:
		if s.IsTrace() {
			s.logger.Debug(msg, args...)
		}
	case hclog.Debug:
		if s.IsDebug() {
			s.logger.Debug(msg, args...)
		}
	case hclog.Info:
		if s.IsInfo() {
			s.logger.Info(msg, args...)
		}
	case hclog.Warn:
		if s.IsWarn() {
			s.logger.Warn(msg, args...)
		}
	case hclog.Error:
		if s.IsError() {
			s.logger.Error(msg, args...)
		}
	default:
		if s.IsInfo() {
			s.logger.Info(msg, args...)
		}
	}
}

func (s *slogAdapter) Trace(msg string, args ...interface{}) {
	if s.IsTrace() {
		s.logger.Debug(msg, args...)
	}
}

func (s *slogAdapter) Debug(msg string, args ...interface{}) {
	if s.IsDebug() {
		s.logger.Debug(msg, args...)
	}
}

func (s *slogAdapter) Info(msg string, args ...interface{}) {
	if s.IsInfo() {
		s.logger.Info(msg, args...)
	}
}

func (s *slogAdapter) Warn(msg string, args ...interface{}) {
	if s.IsWarn() {
		s.logger.Warn(msg, args...)
	}
}

func (s *slogAdapter) Error(msg string, args ...interface{}) {
	if s.IsError() {
		s.logger.Error(msg, args...)
	}
}

func (s *slogAdapter) IsTrace() bool { return s.isEnabled(slog.LevelDebug - 4) }
func (s *slogAdapter) IsDebug() bool { return s.isEnabled(slog.LevelDebug) }
func (s *slogAdapter) IsInfo() bool  { return s.isEnabled(slog.LevelInfo) }
func (s *slogAdapter) IsWarn() bool  { return s.isEnabled(slog.LevelWarn) }
func (s *slogAdapter) IsError() bool { return s.isEnabled(slog.LevelError) }

func (s *slogAdapter) ImpliedArgs() []interface{} {
	return s.implied
}

func (s *slogAdapter) With(args ...interface{}) hclog.Logger {
	return &slogAdapter{
		logger:  s.logger.With(args...),
		implied: append(s.implied, args...),
	}
}

func (s *slogAdapter) Name() string {
	return "slog"
}

func (s *slogAdapter) Named(name string) hclog.Logger {
	return &slogAdapter{
		logger:  s.logger.With("component", name),
		implied: s.implied,
	}
}

func (s *slogAdapter) ResetNamed(name string) hclog.Logger {
	return &slogAdapter{
		logger:  s.logger.With("component", name),
		implied: nil,
	}
}

func (s *slogAdapter) SetLevel(level hclog.Level) {}

func (s *slogAdapter) GetLevel() hclog.Level {
	if s.isEnabled(slog.LevelDebug - 4) {
		return hclog.Trace
	}
	if s.isEnabled(slog.LevelDebug) {
		return hclog.Debug
	}
	if s.isEnabled(slog.LevelInfo) {
		return hclog.Info
	}
	if s.isEnabled(slog.LevelWarn) {
		return hclog.Warn
	}
	if s.isEnabled(slog.LevelError) {
		return hclog.Error
	}
	return hclog.Off
}

func (s *slogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return slog.NewLogLogger(s.logger.Handler(), slog.LevelInfo)
}

func (s *slogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return os.Stderr
}

func (r *Node) processObservations(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case obs := <-r.observerChan:
			r.handleObservation(obs)
		}
	}
}

func (r *Node) handleObservation(obs raft.Observation) {
	if r.eventManager == nil {
		return
	}

	switch data := obs.Data.(type) {
	case raft.PeerObservation:
		if data.Removed {
			event := &domain.NodeLeftEvent{
				NodeID:   string(data.Peer.ID),
				Address:  string(data.Peer.Address),
				LeftAt:   time.Now(),
				Metadata: make(map[string]interface{}),
			}
			if notifier, ok := r.eventManager.(interface{ NotifyNodeLeft(*domain.NodeLeftEvent) }); ok {
				notifier.NotifyNodeLeft(event)
			}
			r.logger.Info("node left cluster", "node_id", event.NodeID, "address", event.Address)
		} else {
			event := &domain.NodeJoinedEvent{
				NodeID:   string(data.Peer.ID),
				Address:  string(data.Peer.Address),
				JoinedAt: time.Now(),
				Metadata: make(map[string]interface{}),
			}
			if notifier, ok := r.eventManager.(interface{ NotifyNodeJoined(*domain.NodeJoinedEvent) }); ok {
				notifier.NotifyNodeJoined(event)
			}
			r.logger.Info("node joined cluster", "node_id", event.NodeID, "address", event.Address)
		}

	case raft.LeaderObservation:
		event := &domain.LeaderChangedEvent{
			NewLeaderID:   string(data.LeaderID),
			NewLeaderAddr: string(data.LeaderAddr),
			ChangedAt:     time.Now(),
			Metadata:      make(map[string]interface{}),
		}
		if notifier, ok := r.eventManager.(interface {
			NotifyLeaderChanged(*domain.LeaderChangedEvent)
		}); ok {
			notifier.NotifyLeaderChanged(event)
		}
		r.logger.Info("leader changed", "new_leader", event.NewLeaderID, "address", event.NewLeaderAddr)
	}
}

func makeAdvertiseAddr(bindAddr string) (string, error) {
	host, port, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return "", fmt.Errorf("invalid bind address %q: %w", bindAddr, err)
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return bindAddr, nil
	}

	if ip.IsUnspecified() {
		if ip.To4() != nil {
			return net.JoinHostPort("127.0.0.1", port), nil
		}
		if ip.To16() != nil {
			return net.JoinHostPort("::1", port), nil
		}
	}

	return bindAddr, nil
}
