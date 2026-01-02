package consensus

import (
	"context"
	"log/slog"

	"github.com/eleven-am/auto-consensus/consensus"
	"github.com/eleven-am/graft/internal/domain"
)

type RaftStarter interface {
	Start(ctx context.Context, existingPeers []domain.RaftPeerSpec, bootstrapMultiNode bool) error
	AddVoter(nodeID string, address string) error
	RemoveServer(nodeID string) error
}

type RaftCallbacksAdapter struct {
	raft   RaftStarter
	ctx    context.Context
	logger *slog.Logger
}

func NewRaftCallbacksAdapter(ctx context.Context, raft RaftStarter, logger *slog.Logger) *RaftCallbacksAdapter {
	return &RaftCallbacksAdapter{
		raft:   raft,
		ctx:    ctx,
		logger: logger,
	}
}

func (a *RaftCallbacksAdapter) OnBootstrap(self consensus.NodeInfo) error {
	a.logger.Info("bootstrapping raft cluster", "node_id", self.ID, "raft_addr", self.RaftAddr)
	return a.raft.Start(a.ctx, nil, true)
}

func (a *RaftCallbacksAdapter) OnJoin(self consensus.NodeInfo, peers []consensus.NodeInfo) error {
	a.logger.Info("joining raft cluster", "node_id", self.ID, "peer_count", len(peers))

	peerSpecs := make([]domain.RaftPeerSpec, len(peers))
	for i, p := range peers {
		peerSpecs[i] = domain.RaftPeerSpec{
			ID:      p.ID,
			Address: p.RaftAddr,
		}
	}

	return a.raft.Start(a.ctx, peerSpecs, false)
}

func (a *RaftCallbacksAdapter) OnPeerJoin(peer consensus.NodeInfo) {
	a.logger.Info("peer joining cluster", "peer_id", peer.ID, "peer_addr", peer.RaftAddr)
	if err := a.raft.AddVoter(peer.ID, peer.RaftAddr); err != nil {
		a.logger.Error("failed to add voter", "peer_id", peer.ID, "error", err)
	}
}

func (a *RaftCallbacksAdapter) OnPeerLeave(peer consensus.NodeInfo) {
	a.logger.Info("peer leaving cluster", "peer_id", peer.ID)
	if err := a.raft.RemoveServer(peer.ID); err != nil {
		a.logger.Error("failed to remove server", "peer_id", peer.ID, "error", err)
	}
}

var _ consensus.RaftCallbacks = (*RaftCallbacksAdapter)(nil)
