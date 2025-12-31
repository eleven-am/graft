package discovery

import (
	"context"

	"github.com/eleven-am/graft/internal/ports"
)

type StaticSeeder struct {
	peers []ports.Peer
}

func NewStaticSeeder(peers []ports.Peer) *StaticSeeder {
	return &StaticSeeder{peers: peers}
}

func (s *StaticSeeder) Discover(_ context.Context) ([]ports.Peer, error) {
	return s.peers, nil
}

func (s *StaticSeeder) Name() string {
	return "static"
}
