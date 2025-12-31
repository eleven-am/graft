package discovery

import (
	"context"
	"sort"

	"github.com/eleven-am/graft/internal/ports"
)

type StaticProvider struct {
	peers    []ports.Peer
	selfInfo ports.NodeInfo
}

func NewStaticProvider(peers []ports.Peer) *StaticProvider {
	return &StaticProvider{
		peers: peers,
	}
}

func (s *StaticProvider) Start(ctx context.Context, announce ports.NodeInfo) error {
	s.selfInfo = announce
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

func (s *StaticProvider) GetSelfOrdinal() int {
	if s.selfInfo.ID == "" {
		return -1
	}

	allNodes := make([]string, 0, len(s.peers)+1)
	allNodes = append(allNodes, s.selfInfo.ID)
	for _, p := range s.peers {
		allNodes = append(allNodes, p.ID)
	}

	sort.Strings(allNodes)

	for i, id := range allNodes {
		if id == s.selfInfo.ID {
			return i
		}
	}

	return -1
}
