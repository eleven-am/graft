package readiness

import (
	"log/slog"
	"strconv"

	"github.com/eleven-am/graft/internal/helpers/metadata"
	"github.com/eleven-am/graft/internal/ports"
)

func FindSeniorPeer(currentNodeID string, peers []ports.Peer, logger *slog.Logger) *ports.Peer {
	var seniorPeer *ports.Peer
	var earliestTimestamp int64 = -1

	currentBootID := ""
	currentTimestamp := int64(0)

	for _, peer := range peers {
		if peer.ID == currentNodeID {
			currentBootID = metadata.GetBootID(peer.Metadata)
			currentTimestamp = metadata.ExtractLaunchTimestamp(peer.Metadata)
			continue
		}

		if !metadata.HasBootstrapMetadata(peer.Metadata) {
			logger.Debug("peer missing bootstrap metadata", "peer_id", peer.ID)
			continue
		}

		peerTimestamp := metadata.ExtractLaunchTimestamp(peer.Metadata)
		peerBootID := metadata.GetBootID(peer.Metadata)

		logger.Debug("evaluating peer for seniority",
			"peer_id", peer.ID,
			"peer_boot_id", peerBootID,
			"peer_timestamp", peerTimestamp,
			"current_boot_id", currentBootID,
			"current_timestamp", currentTimestamp)

		if peerTimestamp > 0 && (earliestTimestamp == -1 || peerTimestamp < earliestTimestamp) {
			earliestTimestamp = peerTimestamp
			seniorPeer = &peer
		}
	}

	if seniorPeer != nil && currentTimestamp > 0 && earliestTimestamp < currentTimestamp {
		logger.Info("senior peer detected",
			"senior_peer_id", seniorPeer.ID,
			"senior_timestamp", earliestTimestamp,
			"current_timestamp", currentTimestamp,
			"timestamp_diff_ms", (currentTimestamp-earliestTimestamp)/1000000)
		return seniorPeer
	}

	return nil
}

func LogPeerMetadata(peers []ports.Peer, logger *slog.Logger) {
	for _, peer := range peers {
		bootID := metadata.GetBootID(peer.Metadata)
		timestamp := metadata.ExtractLaunchTimestamp(peer.Metadata)

		logger.Debug("peer metadata",
			"peer_id", peer.ID,
			"address", peer.Address+":"+strconv.Itoa(peer.Port),
			"boot_id", bootID,
			"launch_timestamp", timestamp,
			"has_metadata", metadata.HasBootstrapMetadata(peer.Metadata))
	}
}
