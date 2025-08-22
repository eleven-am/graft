package static

import (
	"context"

	"github.com/eleven-am/graft/internal/ports"
)

type HealthCheckerInterface interface {
	Start(ctx context.Context, peers []ports.Peer)
	UpdatePeers(peers []ports.Peer)
	GetHealthyPeers() []ports.Peer
	GetPeerHealth(peerID string) (*PeerHealth, bool)
}