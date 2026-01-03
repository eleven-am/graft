package raft

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v3"
	autoconsensus "github.com/eleven-am/auto-consensus"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	json "github.com/eleven-am/graft/internal/xjson"
	"github.com/hashicorp/raft"
)

type Adapter struct {
	node           *autoconsensus.Node
	storageFactory *StorageFactory
	lazyFSM        *lazyFSM
	logger         *slog.Logger
	nodeID         string
}

type lazyFSM struct {
	eventManager  ports.EventManager
	nodeID        string
	clusterID     string
	clusterPolicy domain.ClusterPolicy
	logger        *slog.Logger
	inner         *FSM
}

func (l *lazyFSM) init(db *badger.DB) {
	l.inner = NewFSM(db, l.eventManager, l.nodeID, l.clusterID, l.clusterPolicy, l.logger)
}

func (l *lazyFSM) Apply(log *raft.Log) interface{} {
	if l.inner == nil {
		return &domain.CommandResult{Success: false, Error: "FSM not initialized"}
	}
	return l.inner.Apply(log)
}

func (l *lazyFSM) Snapshot() (raft.FSMSnapshot, error) {
	if l.inner == nil {
		return nil, errors.New("FSM not initialized")
	}
	return l.inner.Snapshot()
}

func (l *lazyFSM) Restore(rc io.ReadCloser) error {
	if l.inner == nil {
		return errors.New("FSM not initialized")
	}
	return l.inner.Restore(rc)
}

func (l *lazyFSM) SetEventManager(manager ports.EventManager) {
	if l.inner != nil {
		l.inner.SetEventManager(manager)
	}
	l.eventManager = manager
}

type AdapterConfig struct {
	NodeID         string
	GossipPort     int
	RaftPort       int
	SecretKey      []byte
	AdvertiseAddr  string
	Discoverer     autoconsensus.Discoverer
	DataDir        string
	InMemory       bool
	ClusterID      string
	ClusterPolicy  domain.ClusterPolicy
	Logger         *slog.Logger
}

func NewAdapter(cfg AdapterConfig, eventManager ports.EventManager) (*Adapter, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	storageFactory := NewStorageFactory(cfg.DataDir, cfg.InMemory, cfg.Logger)

	lazyFSM := &lazyFSM{
		eventManager:  eventManager,
		nodeID:        cfg.NodeID,
		clusterID:     cfg.ClusterID,
		clusterPolicy: cfg.ClusterPolicy,
		logger:        cfg.Logger,
	}

	wrappedFactory := &fsmInitStorageFactory{
		storage: storageFactory,
		lazyFSM: lazyFSM,
	}

	consensusCfg := autoconsensus.Config{
		NodeID:         cfg.NodeID,
		GossipPort:     cfg.GossipPort,
		RaftPort:       cfg.RaftPort,
		SecretKey:      cfg.SecretKey,
		StorageFactory: wrappedFactory,
		FSM:            lazyFSM,
		Discoverer:     cfg.Discoverer,
		AdvertiseAddr:  cfg.AdvertiseAddr,
		Logger:         cfg.Logger,
	}

	node, err := autoconsensus.New(consensusCfg)
	if err != nil {
		return nil, err
	}

	return &Adapter{
		node:           node,
		storageFactory: storageFactory,
		lazyFSM:        lazyFSM,
		logger:         cfg.Logger.With("component", "raft.adapter"),
		nodeID:         cfg.NodeID,
	}, nil
}

type fsmInitStorageFactory struct {
	storage *StorageFactory
	lazyFSM *lazyFSM
}

func (f *fsmInitStorageFactory) Create() (autoconsensus.Storages, error) {
	storages, err := f.storage.Create()
	if err != nil {
		return storages, err
	}

	f.lazyFSM.init(f.storage.StateDB())
	return storages, nil
}

func (f *fsmInitStorageFactory) Reset() error {
	return f.storage.Reset()
}

func (a *Adapter) Start(ctx context.Context) error {
	if err := a.node.Start(ctx); err != nil {
		return err
	}
	return a.node.WaitForReady(ctx)
}

func (a *Adapter) SetEventManager(manager ports.EventManager) {
	a.lazyFSM.SetEventManager(manager)
}

func (a *Adapter) Apply(cmd domain.Command, timeout time.Duration) (*domain.CommandResult, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	future, err := a.node.Apply(data, timeout)
	if err != nil {
		return nil, err
	}

	if err := future.Error(); err != nil {
		return nil, err
	}

	resp := future.Response()
	if result, ok := resp.(*domain.CommandResult); ok {
		return result, nil
	}

	return &domain.CommandResult{Success: false, Error: "unexpected response type"}, nil
}

func (a *Adapter) IsLeader() bool {
	return a.node.IsLeader()
}

func (a *Adapter) LeaderAddr() string {
	_, addr := a.node.Leader()
	return addr
}

func (a *Adapter) StateDB() *badger.DB {
	return a.storageFactory.StateDB()
}

func (a *Adapter) GetClusterInfo() ports.ClusterInfo {
	r := a.node.Raft()
	if r == nil {
		return ports.ClusterInfo{NodeID: a.nodeID}
	}

	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		a.logger.Warn("failed to get raft configuration", "error", err)
		return ports.ClusterInfo{NodeID: a.nodeID}
	}

	cfg := future.Configuration()
	members := make([]ports.RaftNodeInfo, 0, len(cfg.Servers))
	var leader *ports.RaftNodeInfo

	leaderID, leaderAddr := a.node.Leader()

	for _, srv := range cfg.Servers {
		state := ports.NodeFollower
		if string(srv.ID) == leaderID {
			state = ports.NodeLeader
		}

		info := ports.RaftNodeInfo{
			ID:      string(srv.ID),
			Address: string(srv.Address),
			State:   state,
		}
		members = append(members, info)

		if string(srv.ID) == leaderID {
			leader = &ports.RaftNodeInfo{
				ID:      leaderID,
				Address: leaderAddr,
				State:   ports.NodeLeader,
			}
		}
	}

	stats := r.Stats()
	var term, index uint64
	if termStr, ok := stats["term"]; ok {
		term, _ = strconv.ParseUint(termStr, 10, 64)
	}
	if indexStr, ok := stats["last_log_index"]; ok {
		index, _ = strconv.ParseUint(indexStr, 10, 64)
	}

	return ports.ClusterInfo{
		Leader:  leader,
		Members: members,
		NodeID:  a.nodeID,
		Term:    term,
		Index:   index,
	}
}

func (a *Adapter) GetHealth() ports.HealthStatus {
	r := a.node.Raft()
	if r == nil {
		return ports.HealthStatus{
			Healthy: false,
			Error:   "raft not running",
		}
	}

	state := a.node.State()
	if state == autoconsensus.StateFailed {
		return ports.HealthStatus{
			Healthy: false,
			Error:   "consensus failed",
		}
	}

	raftState := r.State()
	healthy := raftState == raft.Leader || raftState == raft.Follower

	return ports.HealthStatus{
		Healthy: healthy,
		Details: map[string]interface{}{
			"state":           raftState.String(),
			"consensus_state": state.String(),
		},
	}
}

func (a *Adapter) GetMetrics() ports.RaftMetrics {
	r := a.node.Raft()
	if r == nil {
		return ports.RaftMetrics{NodeID: a.nodeID}
	}

	stats := r.Stats()
	leaderID, leaderAddr := a.node.Leader()

	var term, lastLogIndex, lastLogTerm, commitIndex, appliedIndex uint64
	term, _ = strconv.ParseUint(stats["term"], 10, 64)
	lastLogIndex, _ = strconv.ParseUint(stats["last_log_index"], 10, 64)
	lastLogTerm, _ = strconv.ParseUint(stats["last_log_term"], 10, 64)
	commitIndex, _ = strconv.ParseUint(stats["commit_index"], 10, 64)
	appliedIndex, _ = strconv.ParseUint(stats["applied_index"], 10, 64)

	numPeers, _ := strconv.Atoi(stats["num_peers"])

	return ports.RaftMetrics{
		NodeID:        a.nodeID,
		State:         stats["state"],
		Term:          term,
		LastLogIndex:  lastLogIndex,
		LastLogTerm:   lastLogTerm,
		CommitIndex:   commitIndex,
		AppliedIndex:  appliedIndex,
		LeaderID:      leaderID,
		LeaderAddress: leaderAddr,
		IsLeader:      a.node.IsLeader(),
		ClusterSize:   numPeers + 1,
	}
}

func (a *Adapter) GetRaftStatus() ports.RaftStatus {
	leadership := a.GetLeadershipInfo()
	leaderID, leaderAddr := a.node.Leader()

	r := a.node.Raft()
	var rawState string
	var stats ports.RaftStatsInfo
	var config []ports.RaftNodeInfo

	if r != nil {
		rawState = r.State().String()
		statsMap := r.Stats()

		stats.LastLogIndex, _ = strconv.ParseUint(statsMap["last_log_index"], 10, 64)
		stats.CommitIndex, _ = strconv.ParseUint(statsMap["commit_index"], 10, 64)
		stats.AppliedIndex, _ = strconv.ParseUint(statsMap["applied_index"], 10, 64)

		future := r.GetConfiguration()
		if err := future.Error(); err == nil {
			cfg := future.Configuration()
			for _, srv := range cfg.Servers {
				state := ports.NodeFollower
				if string(srv.ID) == leaderID {
					state = ports.NodeLeader
				}
				config = append(config, ports.RaftNodeInfo{
					ID:      string(srv.ID),
					Address: string(srv.Address),
					State:   state,
				})
			}
		}
	}

	return ports.RaftStatus{
		Leadership: leadership,
		RawState:   rawState,
		RawLeader: ports.RaftRawLeader{
			ID:   leaderID,
			Addr: leaderAddr,
		},
		Stats:  stats,
		Config: config,
	}
}

func (a *Adapter) GetLeadershipInfo() ports.RaftLeadershipInfo {
	r := a.node.Raft()
	if r == nil {
		return ports.RaftLeadershipInfo{State: ports.RaftLeadershipUnknown}
	}

	leaderID, leaderAddr := a.node.Leader()
	stats := r.Stats()
	term, _ := strconv.ParseUint(stats["term"], 10, 64)

	var state ports.RaftLeadershipState
	switch r.State() {
	case raft.Leader:
		state = ports.RaftLeadershipLeader
	case raft.Follower:
		state = ports.RaftLeadershipFollower
	case raft.Candidate:
		state = ports.RaftLeadershipCandidate
	default:
		state = ports.RaftLeadershipUnknown
	}

	return ports.RaftLeadershipInfo{
		State:         state,
		LeaderID:      leaderID,
		LeaderAddress: leaderAddr,
		Term:          term,
	}
}

func (a *Adapter) TransferLeadership() error {
	r := a.node.Raft()
	if r == nil {
		return ErrNotRunning
	}
	return r.LeadershipTransfer().Error()
}

func (a *Adapter) TransferLeadershipTo(serverID string) error {
	r := a.node.Raft()
	if r == nil {
		return ErrNotRunning
	}

	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	for _, srv := range future.Configuration().Servers {
		if string(srv.ID) == serverID {
			return r.LeadershipTransferToServer(srv.ID, srv.Address).Error()
		}
	}

	return ErrNodeNotFound
}

func (a *Adapter) Stop() error {
	if err := a.node.Stop(); err != nil {
		a.logger.Warn("error stopping consensus node", "error", err)
	}
	return a.storageFactory.Close()
}

func (a *Adapter) Node() *autoconsensus.Node {
	return a.node
}

func (a *Adapter) Subscribe() <-chan autoconsensus.StateChange {
	return a.node.Subscribe()
}

var _ ports.RaftNode = (*Adapter)(nil)
