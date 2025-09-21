package discovery

import (
	"context"

	"github.com/eleven-am/graft/internal/ports"
)

type StaticProvider struct {
	peers []ports.Peer
}

func NewStaticProvider(peers []ports.Peer) *StaticProvider {
	return &StaticProvider{
		peers: peers,
	}
}

func (s *StaticProvider) Start(ctx context.Context, announce ports.NodeInfo) error {
	return nil
}

func (s *StaticProvider) Stop() error {
	return nil
}

func (s *StaticProvider) Snapshot() []ports.Peer {
	return s.peers
}

func (s *StaticProvider) Events() <-chan ports.Event {
	return nil
}

func (s *StaticProvider) Name() string {
	return "static"
}
