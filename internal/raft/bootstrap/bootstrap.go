package bootstrap

import (
	"net"
	"strconv"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

// Spec captures normalized peers and bootstrap parameters for starting Raft.
type Spec struct {
	Peers              []domain.RaftPeerSpec
	BootstrapMultiNode bool
	ExpectedConfig     []domain.RaftPeerSpec
}

// BuildSpec normalizes peers to the raft transport port and decides whether to bootstrap multi-node.
// - discoveredPeers: peers from discovery/static
// - selfID: node ID
// - bindAddr: raft bind address (host:port) used to derive the raft transport port
// - hasPersistedState: whether raft state already exists (influences bootstrapMultiNode decision)
func BuildSpec(discoveredPeers []ports.Peer, selfID, bindAddr string, hasPersistedState bool) Spec {
	raftPort := parsePort(bindAddr)

	normalizedPeers := make([]domain.RaftPeerSpec, 0, len(discoveredPeers))
	for _, p := range discoveredPeers {
		addr := normalizeAddress(p.Address, raftPort)
		normalizedPeers = append(normalizedPeers, domain.RaftPeerSpec{
			ID:       p.ID,
			Address:  addr,
			Metadata: p.Metadata,
		})
	}

	bootstrapMulti := len(normalizedPeers) > 0 || hasPersistedState

	expected := make([]domain.RaftPeerSpec, 0, len(normalizedPeers)+1)
	expected = append(expected, domain.RaftPeerSpec{
		ID:      selfID,
		Address: normalizeAddress(extractHost(bindAddr), raftPort),
	})
	expected = append(expected, normalizedPeers...)

	return Spec{
		Peers:              normalizedPeers,
		BootstrapMultiNode: bootstrapMulti,
		ExpectedConfig:     expected,
	}
}

func normalizeAddress(host string, port int) string {
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		h = host
	}
	if port <= 0 {
		return host
	}
	return net.JoinHostPort(h, strconv.Itoa(port))
}

func parsePort(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		return 0
	}
	return port
}

func extractHost(addr string) string {
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return h
}
