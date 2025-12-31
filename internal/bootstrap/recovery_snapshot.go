package bootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
)

func (r *RecoveryManager) fetchSnapshotFromPeers(ctx context.Context) error {
	peers, err := r.getCommittedPeers()
	if err != nil || len(peers) == 0 {
		return ErrNoPeersForRecovery
	}

	for _, peer := range peers {
		r.logger.Info("attempting snapshot fetch from peer",
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

		r.logger.Info("snapshot fetch from peer successful",
			slog.String("peer_id", string(peer.ServerID)),
		)
		return nil
	}

	return ErrAllPeerRecoveryFailed
}

func (r *RecoveryManager) fetchSnapshotFromPeer(ctx context.Context, peer VoterInfo) error {
	if r.transport == nil {
		return fmt.Errorf("no transport configured")
	}

	stream, err := r.transport.RequestSnapshot(ctx, peer.Address)
	if err != nil {
		return fmt.Errorf("request snapshot: %w", err)
	}
	defer stream.Close()

	snapshotMeta, err := stream.ReceiveMeta()
	if err != nil {
		return fmt.Errorf("receive snapshot meta: %w", err)
	}

	r.logger.Info("receiving snapshot",
		slog.String("peer_id", string(peer.ServerID)),
		slog.Uint64("index", snapshotMeta.Index),
		slog.Uint64("term", snapshotMeta.Term),
		slog.Int64("size", snapshotMeta.Size),
	)

	bytesWritten, err := r.streamSnapshotToStore(stream, snapshotMeta, peer)
	if err != nil {
		return err
	}

	r.lastRecoveredSnapshot = snapshotMeta

	r.logger.Info("snapshot received and persisted successfully",
		slog.String("peer_id", string(peer.ServerID)),
		slog.Uint64("index", snapshotMeta.Index),
		slog.Uint64("term", snapshotMeta.Term),
		slog.Int64("bytes", bytesWritten),
	)

	return nil
}

func (r *RecoveryManager) streamSnapshotToStore(stream io.Reader, meta *SnapshotMeta, peer VoterInfo) (int64, error) {
	checksumEnabled := len(meta.Checksum) > 0
	var hasher = sha256.New()

	if r.recoverySnapshotStore == nil {
		r.logger.Warn("no recovery snapshot store configured, snapshot not persisted",
			slog.String("peer_id", string(peer.ServerID)),
			slog.Uint64("index", meta.Index),
		)

		var reader io.Reader = stream
		if checksumEnabled {
			reader = io.TeeReader(stream, hasher)
		}

		n, err := io.Copy(io.Discard, reader)
		if err != nil {
			return 0, fmt.Errorf("discard snapshot data: %w", err)
		}

		if checksumEnabled {
			computed := hasher.Sum(nil)
			if !bytes.Equal(computed, meta.Checksum) {
				return 0, ErrSnapshotChecksumMismatch
			}
		}

		return n, nil
	}

	sink, err := r.recoverySnapshotStore.CreateRecoverySink(meta)
	if err != nil {
		return 0, fmt.Errorf("create recovery sink: %w", err)
	}

	var reader io.Reader = stream
	if checksumEnabled {
		hasher.Reset()
		reader = io.TeeReader(stream, hasher)
	}

	n, err := io.Copy(sink, reader)
	if err != nil {
		sink.Cancel()
		return 0, fmt.Errorf("stream snapshot data: %w", err)
	}

	if meta.Size > 0 && n != meta.Size {
		sink.Cancel()
		return 0, fmt.Errorf("snapshot size mismatch: wrote %d bytes, expected %d", n, meta.Size)
	}

	if checksumEnabled {
		computed := hasher.Sum(nil)
		if !bytes.Equal(computed, meta.Checksum) {
			sink.Cancel()
			return 0, ErrSnapshotChecksumMismatch
		}
		r.logger.Debug("snapshot checksum verified",
			slog.String("peer_id", string(peer.ServerID)),
		)
	}

	if err := sink.Close(); err != nil {
		return 0, fmt.Errorf("close snapshot sink: %w", err)
	}

	r.logger.Info("snapshot persisted to store",
		slog.String("snapshot_id", sink.ID()),
		slog.String("peer_id", string(peer.ServerID)),
		slog.Uint64("index", meta.Index),
		slog.Uint64("term", meta.Term),
	)

	return n, nil
}

func (r *RecoveryManager) fetchMetaFromPeer(ctx context.Context, peer VoterInfo) error {
	if r.transport == nil {
		return fmt.Errorf("no transport configured")
	}

	if r.metaStore == nil {
		return fmt.Errorf("no meta store configured")
	}

	meta, err := r.transport.GetClusterMeta(ctx, string(peer.Address))
	if err != nil {
		return fmt.Errorf("get cluster meta: %w", err)
	}

	if meta == nil {
		return fmt.Errorf("peer returned nil meta")
	}

	if err := r.metaStore.SaveMeta(meta); err != nil {
		return fmt.Errorf("save meta: %w", err)
	}

	r.logger.Info("fetched and saved cluster meta from peer",
		slog.String("peer_id", string(peer.ServerID)),
		slog.String("cluster_uuid", meta.ClusterUUID),
		slog.String("state", meta.State.String()),
	)

	return nil
}

func (r *RecoveryManager) verifySnapshotChecksum(data []byte, expected []byte) bool {
	hash := sha256.Sum256(data)
	return bytes.Equal(hash[:], expected)
}

func (r *RecoveryManager) findBestRecoveryPeer(ctx context.Context) (*VoterInfo, error) {
	peers, err := r.getCommittedPeers()
	if err != nil || len(peers) == 0 {
		return nil, ErrNoPeersForRecovery
	}

	var bestPeer *VoterInfo
	var bestIndex uint64
	var bestTerm uint64

	for i := range peers {
		peer := &peers[i]

		meta, err := r.transport.GetClusterMeta(ctx, string(peer.Address))
		if err != nil {
			r.logger.Debug("failed to get meta from peer",
				slog.String("peer_id", string(peer.ServerID)),
				slog.Any("error", err),
			)
			continue
		}

		if meta.LastRaftTerm > bestTerm || (meta.LastRaftTerm == bestTerm && meta.LastRaftIndex > bestIndex) {
			bestPeer = peer
			bestTerm = meta.LastRaftTerm
			bestIndex = meta.LastRaftIndex
		}
	}

	if bestPeer == nil {
		return nil, ErrNoPeersForRecovery
	}

	r.logger.Info("selected best recovery peer",
		slog.String("peer_id", string(bestPeer.ServerID)),
		slog.Uint64("term", bestTerm),
		slog.Uint64("index", bestIndex),
	)

	return bestPeer, nil
}
