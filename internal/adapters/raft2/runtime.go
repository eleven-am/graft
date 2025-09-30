package raft2

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"log/slog"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// StorageResources captures the raft storage handles required by the runtime.
type StorageResources struct {
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	StateDB       *badger.DB
	Cleanup       func() error
}

// StorageProvider creates the storage resources used by the raft runtime.
type StorageProvider interface {
	Create(ctx context.Context, opts domain.RaftControllerOptions) (*StorageResources, error)
}

// fsmFactory produces the finite state machine instance used by the raft runtime.
type fsmFactory interface {
	Create(ctx context.Context, opts domain.RaftControllerOptions, storage *StorageResources) (raft.FSM, error)
}

// TransportProvider creates the raft transport implementation and resolves the
// advertised address for the node.
type TransportProvider interface {
	Create(ctx context.Context, opts domain.RaftControllerOptions) (raft.Transport, raft.ServerAddress, error)
}

// RuntimeDeps captures collaborators required by the runtime.
type RuntimeDeps struct {
	StorageProvider   StorageProvider
	TransportProvider TransportProvider
	FSMFactory        fsmFactory
	Logger            *slog.Logger
	Clock             func() time.Time
}

// Runtime manages a single hashicorp/raft node instance and presents it through
// the NodeRuntime interface consumed by the controller.
type Runtime struct {
	mu sync.RWMutex

	deps RuntimeDeps

	ctx    context.Context
	cancel context.CancelFunc

	started bool
	stopped bool

	leadership ports.RaftLeadershipInfo

	options domain.RaftControllerOptions

	storage   *StorageResources
	fsm       raft.FSM
	transport raft.Transport

	observerCh chan raft.Observation
	observer   *raft.Observer

	raft      *raft.Raft
	logger    *slog.Logger
	clock     func() time.Time
	runDone   chan struct{}
	localAddr string
}

// NewRuntime constructs a runtime with the supplied dependencies.
func NewRuntime(deps RuntimeDeps) *Runtime {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}

	return &Runtime{
		deps:       deps,
		logger:     logger.With("component", "raft2.runtime"),
		clock:      clock,
		leadership: ports.RaftLeadershipInfo{State: ports.RaftLeadershipUnknown},
	}
}

// Start boots the raft runtime using the provided controller options.
func (r *Runtime) Start(ctx context.Context, opts domain.RaftControllerOptions) error {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return errControllerAlreadyStarted
	}
	r.started = true
	r.mu.Unlock()

	if r.deps.StorageProvider == nil {
		return errors.New("raft2: storage provider is required")
	}
	if r.deps.TransportProvider == nil {
		return errors.New("raft2: transport provider is required")
	}
	if r.deps.FSMFactory == nil {
		return errors.New("raft2: FSM factory is required")
	}

	runtimeCtx, cancel := context.WithCancel(ctx)
	r.ctx = runtimeCtx
	r.cancel = cancel
	r.runDone = make(chan struct{})
	r.options = opts

	storage, err := r.deps.StorageProvider.Create(runtimeCtx, opts)
	if err != nil {
		cancel()
		return fmt.Errorf("raft2: storage init failed: %w", err)
	}

	r.mu.Lock()
	r.storage = storage
	r.mu.Unlock()

	fsm, err := r.deps.FSMFactory.Create(runtimeCtx, opts, storage)
	if err != nil {
		cancel()
		r.closeStores()
		return fmt.Errorf("raft2: fsm init failed: %w", err)
	}

	transport, advertise, err := r.deps.TransportProvider.Create(runtimeCtx, opts)
	if err != nil {
		cancel()
		r.closeStores()
		return fmt.Errorf("raft2: transport init failed: %w", err)
	}

	config := r.buildRaftConfig(opts)

	if len(opts.Peers) == 0 {
		bootstrapConfig := raft.Configuration{
			Servers: []raft.Server{{
				ID:      raft.ServerID(opts.NodeID),
				Address: advertise,
			}},
		}
		if err := raft.BootstrapCluster(config, storage.LogStore, storage.StableStore, storage.SnapshotStore, transport, bootstrapConfig); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			cancel()
			r.closeTransport(transport)
			r.closeStores()
			return fmt.Errorf("raft2: bootstrap failed: %w", err)
		}
	}

	r.deps.Logger.Debug("creating raft instance", "node_id", opts.NodeID)
	instance, err := raft.NewRaft(config, fsm, storage.LogStore, storage.StableStore, storage.SnapshotStore, transport)
	if err != nil {
		cancel()
		r.closeTransport(transport)
		r.closeStores()
		return fmt.Errorf("raft2: new raft failed: %w", err)
	}
	r.deps.Logger.Debug("raft instance created successfully", "node_id", opts.NodeID)

	observerCh := make(chan raft.Observation, 128)
	observer := raft.NewObserver(observerCh, false, nil)
	instance.RegisterObserver(observer)

	r.mu.Lock()
	r.fsm = fsm
	r.transport = transport
	r.observerCh = observerCh
	r.observer = observer
	r.raft = instance
	r.leadership = ports.RaftLeadershipInfo{State: ports.RaftLeadershipProvisional}
	r.localAddr = string(advertise)
	r.mu.Unlock()

	r.deps.Logger.Debug("raft runtime started", "node_id", opts.NodeID, "peers", len(opts.Peers))

	go r.observe(runtimeCtx)

	// Update leadership info to provisional leader until observations come in.
	stats := instance.Stats()
	term := uint64(0)
	if termStr, ok := stats["current_term"]; ok {
		term = parseUint(termStr)
	}
	r.updateLeadership(ports.RaftLeadershipInfo{
		State:         ports.RaftLeadershipProvisional,
		LeaderID:      opts.NodeID,
		LeaderAddress: string(advertise),
		Term:          term,
	})

	return nil
}

// Stop terminates the raft runtime and releases resources.
func (r *Runtime) Stop() error {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return errControllerNotStarted
	}
	if r.stopped {
		r.mu.Unlock()
		return nil
	}
	r.stopped = true
	cancel := r.cancel
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var shutdownErr error
	r.mu.RLock()
	instance := r.raft
	observer := r.observer
	transport := r.transport
	r.mu.RUnlock()

	if instance != nil {
		future := instance.Shutdown()
		if err := future.Error(); err != nil && !errors.Is(err, raft.ErrRaftShutdown) {
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}

	if observer != nil && instance != nil {
		instance.DeregisterObserver(observer)
	}

	r.closeTransport(transport)
	r.closeStores()

	r.updateLeadership(ports.RaftLeadershipInfo{State: ports.RaftLeadershipUnknown})

	return shutdownErr
}

// Apply forwards a command to the underlying raft instance.
func (r *Runtime) Apply(cmd domain.Command, timeout time.Duration) (*domain.CommandResult, error) {
	r.mu.RLock()
	instance := r.raft
	r.mu.RUnlock()

	if instance == nil {
		return nil, errControllerNotStarted
	}

	data, err := cmd.Marshal()
	if err != nil {
		return nil, fmt.Errorf("raft2: marshal command failed: %w", err)
	}

	future := instance.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, err
	}

	res, ok := future.Response().(*domain.CommandResult)
	if !ok {
		return nil, fmt.Errorf("raft2: command response type mismatch")
	}

	return res, nil
}

// Demote requests leadership transfer to the supplied peer.
func (r *Runtime) Demote(ctx context.Context, peer ports.RaftPeer) error {
	r.mu.RLock()
	instance := r.raft
	r.mu.RUnlock()

	if instance == nil {
		return errControllerNotStarted
	}

	serverID := raft.ServerID(peer.ID)
	serverAddr := raft.ServerAddress(peer.Address)

	future := instance.LeadershipTransferToServer(serverID, serverAddr)

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- future.Error()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-doneCh:
		return err
	}
}

// LeadershipInfo returns the last observed leadership snapshot.
func (r *Runtime) LeadershipInfo() ports.RaftLeadershipInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leadership
}

func (r *Runtime) observe(ctx context.Context) {
	observerCh := r.observerCh
	instance := r.raft
	runDone := r.runDone

	defer func() {
		if runDone != nil {
			close(runDone)
		}
		r.mu.Lock()
		r.observerCh = nil
		if instance != nil {
			observer := r.observer
			if observer != nil {
				instance.DeregisterObserver(observer)
			}
		}
		r.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case obs, ok := <-observerCh:
			if !ok {
				return
			}
			r.handleObservation(obs)
		}
	}
}

func (r *Runtime) handleObservation(obs raft.Observation) {
	switch data := obs.Data.(type) {
	case raft.LeaderObservation:
		r.updateLeadership(r.mapLeaderObservation(data))
	}
}

func (r *Runtime) mapLeaderObservation(obs raft.LeaderObservation) ports.RaftLeadershipInfo {
	info := r.currentLeadership()
	if obs.LeaderAddr != "" {
		info.LeaderAddress = string(obs.LeaderAddr)
	}
	info.LeaderID = string(obs.LeaderID)
	if obs.LeaderID == raft.ServerID(r.options.NodeID) {
		info.State = ports.RaftLeadershipLeader
	} else if obs.LeaderID == "" {
		cluster := r.ClusterInfo()
		if len(cluster.Members) > 1 {
			info.State = ports.RaftLeadershipFollower
		} else {
			info.State = ports.RaftLeadershipProvisional
		}
	} else {
		info.State = ports.RaftLeadershipFollower
	}
	return info
}

func (r *Runtime) currentLeadership() ports.RaftLeadershipInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leadership
}

func (r *Runtime) updateLeadership(info ports.RaftLeadershipInfo) {
	r.mu.Lock()
	r.leadership = info
	r.mu.Unlock()
}

func (r *Runtime) buildRaftConfig(opts domain.RaftControllerOptions) *raft.Config {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(opts.NodeID)
	config.Logger = hclog.NewNullLogger()

	cfg := opts.RuntimeConfig
	if cfg.HeartbeatTimeout > 0 {
		config.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	if cfg.ElectionTimeout > 0 {
		config.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.CommitTimeout > 0 {
		config.CommitTimeout = cfg.CommitTimeout
	}
	if cfg.LeaderLeaseTimeout > 0 {
		config.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	}
	if cfg.MaxAppendEntries > 0 {
		config.MaxAppendEntries = cfg.MaxAppendEntries
	}
	if cfg.TrailingLogs > 0 {
		config.TrailingLogs = cfg.TrailingLogs
	}
	if cfg.SnapshotThreshold > 0 {
		config.SnapshotThreshold = cfg.SnapshotThreshold
	}
	if cfg.SnapshotInterval > 0 {
		config.SnapshotInterval = cfg.SnapshotInterval
	}

	return config
}

func (r *Runtime) closeStores() {
	r.mu.Lock()
	storage := r.storage
	r.storage = nil
	r.mu.Unlock()

	if storage == nil {
		return
	}

	if storage.Cleanup != nil {
		_ = storage.Cleanup()
	}
}

func (r *Runtime) closeTransport(transport raft.Transport) {
	if transport == nil {
		return
	}
	if closer, ok := transport.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

func (r *Runtime) AddVoter(nodeID, address string) error {
	r.mu.RLock()
	instance := r.raft
	r.mu.RUnlock()
	if instance == nil {
		return errControllerNotStarted
	}
	return instance.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 0).Error()
}

func (r *Runtime) AddNonVoter(nodeID, address string) error {
	r.mu.RLock()
	instance := r.raft
	r.mu.RUnlock()
	if instance == nil {
		return errControllerNotStarted
	}
	return instance.AddNonvoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 0).Error()
}

func (r *Runtime) RemoveServer(nodeID string) error {
	r.mu.RLock()
	instance := r.raft
	r.mu.RUnlock()
	if instance == nil {
		return errControllerNotStarted
	}
	return instance.RemoveServer(raft.ServerID(nodeID), 0, 0).Error()
}

func (r *Runtime) TransferLeadership() error {
	r.mu.RLock()
	instance := r.raft
	r.mu.RUnlock()
	if instance == nil {
		return errControllerNotStarted
	}
	return instance.LeadershipTransfer().Error()
}

func (r *Runtime) TransferLeadershipTo(serverID string) error {
	r.mu.RLock()
	instance := r.raft
	r.mu.RUnlock()
	if instance == nil {
		return errControllerNotStarted
	}
	return instance.LeadershipTransferToServer(raft.ServerID(serverID), "").Error()
}

func (r *Runtime) StateDB() *badger.DB {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.storage == nil {
		return nil
	}
	return r.storage.StateDB
}

func (r *Runtime) ReadStale(key string) ([]byte, error) {
	db := r.StateDB()
	if db == nil {
		return nil, errors.New("raft2: state db unavailable")
	}
	var value []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

func (r *Runtime) ClusterInfo() ports.ClusterInfo {
	r.mu.RLock()
	instance := r.raft
	opts := r.options
	r.mu.RUnlock()
	info := ports.ClusterInfo{}
	if instance == nil {
		return info
	}

	info.NodeID = opts.NodeID
	stats := instance.Stats()
	if termStr, ok := stats["current_term"]; ok {
		info.Term = parseUint(termStr)
	}
	info.Index = instance.LastIndex()

	if future := instance.GetConfiguration(); future.Error() == nil {
		servers := future.Configuration().Servers
		members := make([]ports.RaftNodeInfo, 0, len(servers))
		leaderAddr := instance.Leader()
		currentState := instance.State()
		for _, srv := range servers {
			state := ports.NodeFollower
			if string(srv.ID) == info.NodeID {
				switch currentState {
				case raft.Leader:
					state = ports.NodeLeader
				case raft.Candidate:
					state = ports.NodeCandidate
				case raft.Follower:
					state = ports.NodeFollower
				}
			} else if leaderAddr != "" && string(srv.Address) == string(leaderAddr) {
				state = ports.NodeLeader
			}
			members = append(members, ports.RaftNodeInfo{ID: string(srv.ID), Address: string(srv.Address), State: state})
		}
		info.Members = members
	}

	leadership := r.LeadershipInfo()
	if leadership.LeaderID != "" {
		info.Leader = &ports.RaftNodeInfo{ID: leadership.LeaderID, Address: leadership.LeaderAddress, State: ports.NodeLeader}
	}

	return info
}

func (r *Runtime) Metrics() ports.RaftMetrics {
	r.mu.RLock()
	instance := r.raft
	leadership := r.leadership
	opts := r.options
	r.mu.RUnlock()

	metrics := ports.RaftMetrics{Term: leadership.Term, IsLeader: leadership.State == ports.RaftLeadershipLeader, LeaderID: leadership.LeaderID, LeaderAddress: leadership.LeaderAddress}
	if instance == nil {
		return metrics
	}
	metrics.NodeID = opts.NodeID
	stats := instance.Stats()
	metrics.State = stats["state"]
	metrics.ClusterSize = len(stats)
	if idx, ok := stats["last_log_index"]; ok {
		metrics.LastLogIndex = parseUint(idx)
	}
	if term, ok := stats["last_log_term"]; ok {
		metrics.LastLogTerm = parseUint(term)
	}
	if commit, ok := stats["commit_index"]; ok {
		metrics.CommitIndex = parseUint(commit)
	}
	if applied, ok := stats["applied_index"]; ok {
		metrics.AppliedIndex = parseUint(applied)
	}
	return metrics
}

func (r *Runtime) Health() ports.HealthStatus {
	r.mu.RLock()
	instance := r.raft
	r.mu.RUnlock()
	if instance == nil {
		return ports.HealthStatus{Healthy: false, Error: "raft not started"}
	}
	return ports.HealthStatus{Healthy: true}
}

func (r *Runtime) LocalAddress() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.localAddr
}

func (r *Runtime) SetEventManager(manager ports.EventManager) {
	r.mu.Lock()
	if fsm, ok := r.fsm.(*FSM); ok {
		fsm.SetEventManager(manager)
	}
	r.mu.Unlock()
}

func mapRaftState(state raft.RaftState) ports.RaftLeadershipState {
	switch state {
	case raft.Candidate:
		return ports.RaftLeadershipProvisional
	case raft.Leader:
		return ports.RaftLeadershipLeader
	case raft.Follower:
		return ports.RaftLeadershipFollower
	case raft.Shutdown:
		return ports.RaftLeadershipDemoted
	default:
		return ports.RaftLeadershipUnknown
	}
}

func parseUint(val string) uint64 {
	parsed, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func (r *Runtime) WaitForConfiguration(ctx context.Context, minMembers int) error {
	r.deps.Logger.Info("waiting for raft configuration", "min_members", minMembers)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	attemptCount := 0
	for {
		select {
		case <-ctx.Done():
			r.deps.Logger.Warn("configuration wait canceled", "attempts", attemptCount, "error", ctx.Err())
			return ctx.Err()
		case <-ticker.C:
			attemptCount++
			r.mu.RLock()
			instance := r.raft
			r.mu.RUnlock()

			if instance == nil {
				if attemptCount%10 == 0 {
					r.deps.Logger.Debug("raft instance not yet initialized", "attempt", attemptCount)
				}
				continue
			}

			future := instance.GetConfiguration()
			if err := future.Error(); err != nil {
				r.deps.Logger.Debug("configuration error", "attempt", attemptCount, "error", err)
				continue
			}

			servers := future.Configuration().Servers
			if attemptCount%10 == 0 || len(servers) >= minMembers {
				r.deps.Logger.Debug("checking configuration", "attempt", attemptCount, "servers", len(servers), "min_members", minMembers)
			}
			if len(servers) >= minMembers {
				r.deps.Logger.Info("configuration ready", "servers", len(servers), "attempts", attemptCount)
				return nil
			}
		}
	}
}
