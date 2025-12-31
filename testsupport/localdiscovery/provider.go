package localdiscovery

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/eleven-am/graft/internal/ports"
)

type Hub struct {
	logger *slog.Logger
	mu     sync.RWMutex
	peers  map[string]ports.Peer
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{
		logger: logger.With("component", "inmemory-discovery"),
		peers:  make(map[string]ports.Peer),
	}
}

func (h *Hub) NewSeeder() ports.Seeder {
	return &seeder{hub: h}
}

func (h *Hub) Register(nodeID, address string, port int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	peer := ports.Peer{
		ID:      nodeID,
		Address: sanitizeAddress(address),
		Port:    port,
	}

	h.peers[nodeID] = peer
	h.logger.Debug("registered peer in hub", "node_id", nodeID, "address", peer.Address, "port", peer.Port)
}

func (h *Hub) Unregister(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.peers, nodeID)
	h.logger.Debug("unregistered peer", "node_id", nodeID)
}

func (h *Hub) snapshot(except string) []ports.Peer {
	h.mu.RLock()
	defer h.mu.RUnlock()

	peers := make([]ports.Peer, 0, len(h.peers))
	for id, peer := range h.peers {
		if id == except {
			continue
		}
		peers = append(peers, peer)
	}

	return peers
}

type seeder struct {
	hub    *Hub
	nodeID string
}

func (s *seeder) SetNodeID(nodeID string) {
	s.nodeID = nodeID
}

func (s *seeder) Discover(_ context.Context) ([]ports.Peer, error) {
	return s.hub.snapshot(s.nodeID), nil
}

func (s *seeder) Name() string {
	return "inmemory"
}

func sanitizeAddress(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
