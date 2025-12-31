package bootstrap

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hashicorp/raft"
)

func (r *RecoveryManager) handleTermIndexMismatch(ctx context.Context, meta *ClusterMeta) error {
	if r.logStore == nil {
		return nil
	}

	currentTerm, err := r.logStore.GetUint64([]byte("CurrentTerm"))
	if err != nil {
		r.logger.Debug("could not get current term from log store",
			slog.Any("error", err),
		)
		currentTerm = 0
	}

	lastIdx, err := r.logStore.LastIndex()
	if err != nil {
		return fmt.Errorf("get last index: %w", err)
	}

	if meta.LastRaftTerm > 0 && currentTerm < meta.LastRaftTerm {
		r.logger.Error("term regression detected, fetching from peers",
			slog.Uint64("persisted_term", currentTerm),
			slog.Uint64("meta_term", meta.LastRaftTerm),
		)
		return r.recoverFromPeers(ctx)
	}

	if meta.LastRaftIndex > 0 && lastIdx < meta.LastRaftIndex {
		r.logger.Warn("index regression detected",
			slog.Uint64("persisted_index", lastIdx),
			slog.Uint64("meta_index", meta.LastRaftIndex),
		)

		if r.snapshotStore != nil {
			snapshots, _ := r.snapshotStore.List()
			if len(snapshots) > 0 && snapshots[0].Index >= meta.LastRaftIndex {
				r.logger.Info("snapshot covers missing entries, proceeding")
				return nil
			}
		}

		r.logger.Warn("attempting to fetch missing entries from peers")
		return r.fetchMissingEntries(ctx, lastIdx+1, meta.LastRaftIndex)
	}

	return nil
}

func (r *RecoveryManager) fetchMissingEntries(ctx context.Context, fromIdx, toIdx uint64) error {
	if r.logStore == nil {
		return fmt.Errorf("no log store configured")
	}

	if r.discovery == nil || r.transport == nil {
		return ErrNoPeersForRecovery
	}

	localLastIdx, err := r.logStore.LastIndex()
	if err != nil {
		return fmt.Errorf("get local last index: %w", err)
	}

	var localLastTerm uint64
	if localLastIdx > 0 {
		var localLastLog raft.Log
		if err := r.logStore.GetLog(localLastIdx, &localLastLog); err != nil {
			return fmt.Errorf("get local last log: %w", err)
		}
		localLastTerm = localLastLog.Term
	}

	peers := r.discovery.GetHealthyPeers(ctx)
	if len(peers) == 0 {
		return ErrNoPeersForRecovery
	}

	for _, peer := range peers {
		entries, prevLogIndex, prevLogTerm, err := r.transport.FetchLogEntries(
			ctx, peer.Address, fromIdx, toIdx,
		)
		if err != nil {
			r.logger.Warn("fetch entries failed",
				slog.String("peer_id", string(peer.ServerID)),
				slog.Any("error", err),
			)
			continue
		}

		if prevLogIndex != localLastIdx {
			r.logger.Warn("prev log index mismatch",
				slog.String("peer_id", string(peer.ServerID)),
				slog.Uint64("expected", localLastIdx),
				slog.Uint64("got", prevLogIndex),
			)
			continue
		}

		if prevLogTerm != localLastTerm {
			r.logger.Warn("prev log term mismatch - need to truncate",
				slog.String("peer_id", string(peer.ServerID)),
				slog.Uint64("local_term", localLastTerm),
				slog.Uint64("peer_term", prevLogTerm),
			)

			commonIdx, err := r.findCommonAncestor(ctx, peer, localLastIdx)
			if err != nil {
				r.logger.Warn("failed to find common ancestor",
					slog.String("peer_id", string(peer.ServerID)),
					slog.Any("error", err),
				)
				continue
			}

			if err := r.logStore.DeleteRange(commonIdx+1, localLastIdx); err != nil {
				return fmt.Errorf("truncate divergent logs: %w", err)
			}

			r.logger.Info("truncated divergent log entries",
				slog.Uint64("from", commonIdx+1),
				slog.Uint64("to", localLastIdx),
			)

			var refetchPrevIdx, refetchPrevTerm uint64
			entries, refetchPrevIdx, refetchPrevTerm, err = r.transport.FetchLogEntries(
				ctx, peer.Address, commonIdx+1, toIdx,
			)
			if err != nil {
				r.logger.Warn("fetch after truncate failed",
					slog.String("peer_id", string(peer.ServerID)),
					slog.Any("error", err),
				)
				continue
			}

			localLastIdx = commonIdx
			if localLastIdx > 0 {
				var log raft.Log
				if err := r.logStore.GetLog(localLastIdx, &log); err != nil {
					r.logger.Warn("failed to get log at common ancestor",
						slog.String("peer_id", string(peer.ServerID)),
						slog.Uint64("index", localLastIdx),
						slog.Any("error", err),
					)
					continue
				}
				localLastTerm = log.Term
			} else {
				localLastTerm = 0
			}

			if refetchPrevIdx != localLastIdx {
				r.logger.Warn("re-fetched prev log index mismatch after truncation",
					slog.String("peer_id", string(peer.ServerID)),
					slog.Uint64("expected", localLastIdx),
					slog.Uint64("got", refetchPrevIdx),
				)
				continue
			}

			if refetchPrevTerm != localLastTerm {
				r.logger.Warn("re-fetched prev log term mismatch after truncation",
					slog.String("peer_id", string(peer.ServerID)),
					slog.Uint64("expected", localLastTerm),
					slog.Uint64("got", refetchPrevTerm),
				)
				continue
			}

			r.logger.Debug("re-fetch alignment verified after truncation",
				slog.String("peer_id", string(peer.ServerID)),
				slog.Uint64("prev_idx", refetchPrevIdx),
				slog.Uint64("prev_term", refetchPrevTerm),
			)
		}

		if len(entries) == 0 {
			r.logger.Info("no entries to fetch",
				slog.String("peer_id", string(peer.ServerID)),
			)
			return nil
		}

		if err := r.storeEntriesWithValidation(entries, peer, localLastIdx, localLastTerm); err != nil {
			r.logger.Warn("store entries failed",
				slog.String("peer_id", string(peer.ServerID)),
				slog.Any("error", err),
			)
			continue
		}

		r.logger.Info("successfully fetched and verified missing entries",
			slog.String("peer_id", string(peer.ServerID)),
			slog.Int("count", len(entries)),
			slog.Uint64("from", fromIdx),
			slog.Uint64("to", toIdx),
		)

		return nil
	}

	return ErrCannotFetchMissingEntries
}

func (r *RecoveryManager) storeEntriesWithValidation(entries []raft.Log, peer PeerInfo, prevIdx, prevTerm uint64) error {
	for i, entry := range entries {
		expectedIdx := prevIdx + 1
		if entry.Index != expectedIdx {
			return &LogIndexMismatchError{
				Expected: expectedIdx,
				Got:      entry.Index,
				Peer:     peer.ServerID,
			}
		}

		if entry.Term < prevTerm {
			return &TermRegressionError{
				Index:        entry.Index,
				PreviousTerm: prevTerm,
				CurrentTerm:  entry.Term,
				Peer:         peer.ServerID,
			}
		}

		if err := r.logStore.StoreLog(&entries[i]); err != nil {
			return fmt.Errorf("store log entry %d: %w", entry.Index, err)
		}

		r.logger.Debug("stored log entry",
			slog.Uint64("index", entry.Index),
			slog.Uint64("term", entry.Term),
			slog.String("peer_id", string(peer.ServerID)),
			slog.Int("entry_num", i+1),
			slog.Int("total", len(entries)),
		)

		prevTerm = entry.Term
		prevIdx = entry.Index
	}

	return nil
}

func (r *RecoveryManager) findCommonAncestor(ctx context.Context, peer PeerInfo, startIdx uint64) (uint64, error) {
	if r.transport == nil {
		return 0, fmt.Errorf("no transport configured")
	}

	for idx := startIdx; idx > 0; idx-- {
		var localLog raft.Log
		if err := r.logStore.GetLog(idx, &localLog); err != nil {
			continue
		}

		peerTerm, err := r.transport.GetLogTerm(ctx, peer.Address, idx)
		if err != nil {
			return 0, fmt.Errorf("get peer log term at index %d: %w", idx, err)
		}

		if peerTerm == localLog.Term {
			r.logger.Debug("found common ancestor",
				slog.Uint64("index", idx),
				slog.Uint64("term", localLog.Term),
				slog.String("peer_id", string(peer.ServerID)),
			)
			return idx, nil
		}
	}

	return 0, nil
}

func (r *RecoveryManager) detectAndHandleConflict(ctx context.Context, idx uint64, expectedTerm, localTerm uint64, peer PeerInfo) (bool, error) {
	if localTerm == expectedTerm {
		return false, nil
	}

	r.logger.Warn("log conflict detected",
		slog.Uint64("index", idx),
		slog.Uint64("local_term", localTerm),
		slog.Uint64("expected_term", expectedTerm),
		slog.String("peer_id", string(peer.ServerID)),
	)

	commonIdx, err := r.findCommonAncestor(ctx, peer, idx-1)
	if err != nil {
		return true, fmt.Errorf("find common ancestor: %w", err)
	}

	localLastIdx, err := r.logStore.LastIndex()
	if err != nil {
		return true, fmt.Errorf("get local last index: %w", err)
	}

	if err := r.logStore.DeleteRange(commonIdx+1, localLastIdx); err != nil {
		return true, fmt.Errorf("truncate conflicting logs: %w", err)
	}

	r.logger.Info("truncated conflicting log entries for retry",
		slog.Uint64("from", commonIdx+1),
		slog.Uint64("to", localLastIdx),
		slog.Uint64("common_ancestor", commonIdx),
	)

	return true, nil
}
