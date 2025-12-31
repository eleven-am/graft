package bootstrap

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/hashicorp/raft"
)

type RaftLogStore interface {
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(index uint64, log *raft.Log) error
	StoreLog(log *raft.Log) error
	StoreLogs(logs []*raft.Log) error
	DeleteRange(min, max uint64) error
	GetUint64(key []byte) (uint64, error)
}

type RaftSnapshotStore interface {
	List() ([]*raft.SnapshotMeta, error)
	Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error)
}

type RecoverySnapshotSink interface {
	Write(p []byte) (n int, err error)
	Close() error
	Cancel() error
	ID() string
}

type RecoverySnapshotStore interface {
	RaftSnapshotStore
	CreateRecoverySink(meta *SnapshotMeta) (RecoverySnapshotSink, error)
}

type SnapshotStream interface {
	ReceiveMeta() (*SnapshotMeta, error)
	Read(p []byte) (n int, err error)
	Close() error
}

type SnapshotMeta struct {
	Version            raft.SnapshotVersion
	Index              uint64
	Term               uint64
	Configuration      raft.Configuration
	ConfigurationIndex uint64
	Size               int64
	Checksum           []byte
}

type RecoveryTransport interface {
	FetchLogEntries(ctx context.Context, addr raft.ServerAddress, fromIdx, toIdx uint64) ([]raft.Log, uint64, uint64, error)
	GetLogTerm(ctx context.Context, addr raft.ServerAddress, index uint64) (uint64, error)
	RequestSnapshot(ctx context.Context, addr raft.ServerAddress) (SnapshotStream, error)
	GetClusterMeta(ctx context.Context, addr string) (*ClusterMeta, error)
}

type RecoveryManagerDeps struct {
	LogStore              RaftLogStore
	SnapshotStore         RaftSnapshotStore
	RecoverySnapshotStore RecoverySnapshotStore
	MetaStore             MetaStore
	Discovery             PeerDiscovery
	Transport             RecoveryTransport
	Config                *BootstrapConfig
	Logger                *slog.Logger
}

type RecoveryManager struct {
	logStore              RaftLogStore
	snapshotStore         RaftSnapshotStore
	recoverySnapshotStore RecoverySnapshotStore
	metaStore             MetaStore
	discovery             PeerDiscovery
	transport             RecoveryTransport
	config                *BootstrapConfig
	logger                *slog.Logger
	lastRecoveredSnapshot *SnapshotMeta

	mu         sync.Mutex
	recovering bool
}

func NewRecoveryManager(deps RecoveryManagerDeps) *RecoveryManager {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	config := deps.Config
	if config == nil {
		config = &BootstrapConfig{}
	}

	return &RecoveryManager{
		logStore:              deps.LogStore,
		snapshotStore:         deps.SnapshotStore,
		recoverySnapshotStore: deps.RecoverySnapshotStore,
		metaStore:             deps.MetaStore,
		discovery:             deps.Discovery,
		transport:             deps.Transport,
		config:                config,
		logger:                logger,
	}
}

func (r *RecoveryManager) RecoverFromRestart(ctx context.Context) error {
	r.mu.Lock()
	if r.recovering {
		r.mu.Unlock()
		return ErrRecoveryInProgress
	}
	r.recovering = true
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.recovering = false
		r.mu.Unlock()
	}()

	walOK, walErr := r.validateWAL()
	snapOK, snapErr := r.validateSnapshot()

	if walErr != nil && snapErr != nil {
		r.logger.Error("both WAL and snapshot corrupted, attempting peer recovery",
			slog.Any("wal_error", walErr),
			slog.Any("snapshot_error", snapErr),
		)

		if err := r.recoverFromPeers(ctx); err != nil {
			return &UnrecoverableError{
				WALError:      walErr,
				SnapshotError: snapErr,
				PeerError:     err,
			}
		}
		return nil
	}

	if !walOK && snapOK {
		r.logger.Warn("WAL corrupted, recovering from snapshot")
		if err := r.recoverFromSnapshot(ctx); err != nil {
			if peerErr := r.recoverFromPeers(ctx); peerErr != nil {
				return fmt.Errorf("snapshot recovery failed and peer recovery failed: %w", peerErr)
			}
		}
	}

	if walOK && !snapOK {
		r.logger.Warn("Snapshot corrupted, attempting repair from peers")
		if err := r.fetchSnapshotFromPeers(ctx); err != nil {
			r.logger.Warn("snapshot repair failed, continuing with WAL only",
				slog.Any("error", err),
			)
		}
	}

	meta, err := r.metaStore.LoadMeta()
	if err != nil {
		if err == ErrMetaNotFound {
			r.logger.Info("no cluster meta found, fresh start")
			return nil
		}
		return fmt.Errorf("load meta: %w", err)
	}

	if err := r.handleTermIndexMismatch(ctx, meta); err != nil {
		return err
	}

	return nil
}

func (r *RecoveryManager) validateWAL() (bool, error) {
	if r.logStore == nil {
		return true, nil
	}

	firstIdx, err := r.logStore.FirstIndex()
	if err != nil {
		return false, fmt.Errorf("get first index: %w", err)
	}

	lastIdx, err := r.logStore.LastIndex()
	if err != nil {
		return false, fmt.Errorf("get last index: %w", err)
	}

	if lastIdx == 0 {
		return true, nil
	}

	var log raft.Log
	if err := r.logStore.GetLog(lastIdx, &log); err != nil {
		return false, fmt.Errorf("get last log entry: %w", err)
	}

	if firstIdx > 0 {
		if err := r.logStore.GetLog(firstIdx, &log); err != nil {
			return false, fmt.Errorf("get first log entry: %w", err)
		}
	}

	r.logger.Debug("WAL validation passed",
		slog.Uint64("first_index", firstIdx),
		slog.Uint64("last_index", lastIdx),
	)

	return true, nil
}

func (r *RecoveryManager) validateSnapshot() (bool, error) {
	if r.snapshotStore == nil {
		return true, nil
	}

	snapshots, err := r.snapshotStore.List()
	if err != nil {
		return false, fmt.Errorf("list snapshots: %w", err)
	}

	if len(snapshots) == 0 {
		return true, nil
	}

	latestSnapshot := snapshots[0]
	meta, reader, err := r.snapshotStore.Open(latestSnapshot.ID)
	if err != nil {
		return false, fmt.Errorf("open snapshot %s: %w", latestSnapshot.ID, err)
	}
	defer reader.Close()

	r.logger.Debug("snapshot validation passed",
		slog.String("id", meta.ID),
		slog.Uint64("index", meta.Index),
		slog.Uint64("term", meta.Term),
	)

	return true, nil
}

func (r *RecoveryManager) recoverFromPeers(ctx context.Context) error {
	if r.discovery == nil {
		return ErrNoPeersForRecovery
	}

	peers := r.discovery.GetHealthyPeers(ctx)
	if len(peers) == 0 {
		return ErrNoPeersForRecovery
	}

	for _, peer := range peers {
		r.logger.Info("attempting recovery from peer",
			slog.String("peer_id", string(peer.ServerID)),
			slog.String("peer_address", string(peer.Address)),
		)

		if err := r.fetchSnapshotFromPeer(ctx, peer); err != nil {
			r.logger.Warn("snapshot fetch failed",
				slog.String("peer_id", string(peer.ServerID)),
				slog.Any("error", err),
			)
			continue
		}

		if err := r.fetchMetaFromPeer(ctx, peer); err != nil {
			r.logger.Warn("meta fetch failed",
				slog.String("peer_id", string(peer.ServerID)),
				slog.Any("error", err),
			)
			continue
		}

		if err := r.syncWALWithSnapshot(); err != nil {
			r.logger.Warn("WAL sync with snapshot failed",
				slog.String("peer_id", string(peer.ServerID)),
				slog.Any("error", err),
			)
			continue
		}

		r.logger.Info("recovery from peer successful",
			slog.String("peer_id", string(peer.ServerID)),
		)
		return nil
	}

	return ErrAllPeerRecoveryFailed
}

func (r *RecoveryManager) syncWALWithSnapshot() error {
	if r.logStore == nil {
		return nil
	}

	lastIdx, err := r.logStore.LastIndex()
	if err != nil {
		return fmt.Errorf("get last index: %w", err)
	}

	if lastIdx == 0 {
		return nil
	}

	var snapshotIdx uint64
	if r.lastRecoveredSnapshot != nil {
		snapshotIdx = r.lastRecoveredSnapshot.Index
	}

	if snapshotIdx > 0 && lastIdx <= snapshotIdx {
		if err := r.logStore.DeleteRange(1, lastIdx); err != nil {
			return fmt.Errorf("clear WAL covered by snapshot: %w", err)
		}
		r.logger.Info("cleared WAL entries covered by recovered snapshot",
			slog.Uint64("cleared_up_to", lastIdx),
			slog.Uint64("snapshot_index", snapshotIdx),
		)
	} else if snapshotIdx > 0 {
		if err := r.logStore.DeleteRange(1, snapshotIdx); err != nil {
			return fmt.Errorf("clear WAL up to snapshot: %w", err)
		}
		r.logger.Info("cleared WAL entries up to snapshot index",
			slog.Uint64("cleared_up_to", snapshotIdx),
			slog.Uint64("wal_last_index", lastIdx),
		)
	} else {
		if err := r.logStore.DeleteRange(1, lastIdx); err != nil {
			return fmt.Errorf("clear entire WAL: %w", err)
		}
		r.logger.Info("cleared entire WAL (no snapshot recovered)",
			slog.Uint64("cleared_up_to", lastIdx),
		)
	}

	return nil
}

func (r *RecoveryManager) LastRecoveredSnapshot() *SnapshotMeta {
	return r.lastRecoveredSnapshot
}

func (r *RecoveryManager) recoverFromSnapshot(_ context.Context) error {
	if r.snapshotStore == nil {
		return fmt.Errorf("no snapshot store configured")
	}

	snapshots, err := r.snapshotStore.List()
	if err != nil {
		return fmt.Errorf("list snapshots: %w", err)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("no snapshots available for recovery")
	}

	latestSnapshot := snapshots[0]
	r.logger.Info("recovering from local snapshot",
		slog.String("id", latestSnapshot.ID),
		slog.Uint64("index", latestSnapshot.Index),
		slog.Uint64("term", latestSnapshot.Term),
	)

	meta, reader, err := r.snapshotStore.Open(latestSnapshot.ID)
	if err != nil {
		return fmt.Errorf("open snapshot for verification: %w", err)
	}

	testBuf := make([]byte, 4096)
	if _, err := reader.Read(testBuf); err != nil && err != io.EOF {
		reader.Close()
		return fmt.Errorf("snapshot read verification failed: %w", err)
	}
	reader.Close()

	r.logger.Debug("snapshot verified readable before WAL cleanup",
		slog.String("id", meta.ID),
		slog.Uint64("index", meta.Index),
	)

	if r.logStore != nil {
		lastIdx, _ := r.logStore.LastIndex()
		if lastIdx > 0 && lastIdx < latestSnapshot.Index {
			if err := r.logStore.DeleteRange(1, lastIdx); err != nil {
				return fmt.Errorf("clear WAL: %w", err)
			}
		}
	}

	return nil
}

func (r *RecoveryManager) GetRecoveryStatus() *RecoveryStatus {
	status := &RecoveryStatus{}

	if r.logStore != nil {
		firstIdx, _ := r.logStore.FirstIndex()
		lastIdx, _ := r.logStore.LastIndex()
		status.WALFirstIndex = firstIdx
		status.WALLastIndex = lastIdx
	}

	if r.snapshotStore != nil {
		snapshots, _ := r.snapshotStore.List()
		if len(snapshots) > 0 {
			status.LatestSnapshotIndex = snapshots[0].Index
			status.LatestSnapshotTerm = snapshots[0].Term
		}
	}

	return status
}

type RecoveryStatus struct {
	WALFirstIndex       uint64
	WALLastIndex        uint64
	LatestSnapshotIndex uint64
	LatestSnapshotTerm  uint64
}
