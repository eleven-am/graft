package localdiscovery

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/eleven-am/graft/internal/ports"
)

type Hub struct {
	logger    *slog.Logger
	mu        sync.RWMutex
	peers     map[string]ports.Peer
	providers map[string]*provider
}

type provider struct {
	hub    *Hub
	nodeID string
	events chan ports.Event
	mu     sync.Mutex
	closed bool
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{
		logger:    logger.With("component", "inmemory-discovery"),
		peers:     make(map[string]ports.Peer),
		providers: make(map[string]*provider),
	}
}

func (h *Hub) NewProvider() ports.Provider {
	return &provider{
		hub:    h,
		events: make(chan ports.Event, 32),
	}
}

func (h *Hub) register(node ports.NodeInfo, p *provider) {
	h.mu.Lock()
	defer h.mu.Unlock()

	p.nodeID = node.ID
	h.providers[node.ID] = p

	peer := ports.Peer{
		ID:       node.ID,
		Address:  sanitizeAddress(node.Address),
		Port:     node.Port,
		Metadata: cloneMetadata(node.Metadata),
	}

	h.logger.Info("registering new peer in hub", "node_id", node.ID, "address", peer.Address, "port", peer.Port, "existing_peers", len(h.peers), "existing_providers", len(h.providers))

	notifiedCount := 0
	for id, existing := range h.providers {
		if id == node.ID {
			continue
		}
		h.logger.Debug("notifying existing provider about new peer", "existing_provider", id, "new_peer", node.ID)
		existing.emit(ports.Event{Type: ports.PeerAdded, Peer: peer})
		notifiedCount++
	}

	receivedCount := 0
	for id, existingPeer := range h.peers {
		if id == node.ID {
			continue
		}
		h.logger.Debug("sending existing peer to new provider", "existing_peer", id, "new_provider", node.ID)
		p.emit(ports.Event{Type: ports.PeerAdded, Peer: existingPeer})
		receivedCount++
	}

	h.peers[node.ID] = peer
	h.logger.Info("peer registration complete", "node_id", node.ID, "notified_providers", notifiedCount, "sent_existing_peers", receivedCount, "total_peers", len(h.peers))
}

func (h *Hub) unregister(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.providers, nodeID)
	if _, ok := h.peers[nodeID]; !ok {
		return
	}
	delete(h.peers, nodeID)

	for id, existing := range h.providers {
		if id == nodeID {
			continue
		}
		existing.emit(ports.Event{Type: ports.PeerRemoved, Peer: ports.Peer{ID: nodeID}})
	}

	h.logger.Debug("unregistered peer", "node_id", nodeID, "remaining_peers", len(h.peers))
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

func (p *provider) emit(event ports.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	select {
	case p.events <- event:
	default:
	}
}

func (p *provider) Start(ctx context.Context, announce ports.NodeInfo) error {
	p.hub.register(announce, p)

	go func() {
		<-ctx.Done()
		p.Stop()
	}()

	return nil
}

func (p *provider) Stop() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.events)
	p.mu.Unlock()

	if p.nodeID != "" {
		p.hub.unregister(p.nodeID)
	}

	return nil
}

func (p *provider) Snapshot() []ports.Peer {
	return p.hub.snapshot(p.nodeID)
}

func (p *provider) Events() <-chan ports.Event {
	return p.events
}

func (p *provider) Name() string {
	return "inmemory"
}

func (p *provider) GetSelfOrdinal() int {
	return -1
}

func cloneMetadata(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	copy := make(map[string]string, len(input))
	for k, v := range input {
		copy[k] = v
	}
	return copy
}

func sanitizeAddress(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
