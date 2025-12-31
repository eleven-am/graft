package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/eleven-am/graft/internal/bootstrap"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
)

type DiscoveryAdapterConfig struct {
	Discovery   ports.Discovery
	ServiceName string
	LocalID     string
	Logger      *slog.Logger
}

type DiscoveryAdapter struct {
	manager     ports.Discovery
	serviceName string
	localID     string
	logger      *slog.Logger

	mu         sync.RWMutex
	peersByOrd map[int]ports.Peer
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewDiscoveryAdapter(config DiscoveryAdapterConfig) *DiscoveryAdapter {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &DiscoveryAdapter{
		manager:     config.Discovery,
		serviceName: config.ServiceName,
		localID:     config.LocalID,
		logger:      logger.With("component", "discovery", "adapter", "bootstrap"),
		peersByOrd:  make(map[int]ports.Peer),
	}
}

func (a *DiscoveryAdapter) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cancel != nil {
		return fmt.Errorf("adapter already started")
	}

	adapterCtx, cancel := context.WithCancel(ctx)
	a.cancel = cancel

	a.refreshPeersLocked()

	events, unsubscribe := a.manager.Subscribe()
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer unsubscribe()
		a.watchEvents(adapterCtx, events)
	}()

	a.logger.Debug("discovery adapter started",
		"service_name", a.serviceName,
		"initial_peers", len(a.peersByOrd))

	return nil
}

func (a *DiscoveryAdapter) Stop() {
	a.mu.Lock()
	cancel := a.cancel
	a.cancel = nil
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	a.wg.Wait()

	a.mu.Lock()
	a.peersByOrd = make(map[int]ports.Peer)
	a.mu.Unlock()

	a.logger.Debug("discovery adapter stopped")
}

func (a *DiscoveryAdapter) AddressForOrdinal(ordinal int) raft.ServerAddress {
	a.mu.RLock()
	defer a.mu.RUnlock()

	peer, exists := a.peersByOrd[ordinal]
	if !exists {
		return ""
	}

	return a.peerToAddress(peer)
}

func (a *DiscoveryAdapter) GetHealthyPeers(ctx context.Context) []bootstrap.PeerInfo {
	a.mu.Lock()
	a.refreshPeersLocked()
	a.mu.Unlock()

	a.mu.RLock()
	defer a.mu.RUnlock()

	peers := make([]bootstrap.PeerInfo, 0, len(a.peersByOrd))
	for ordinal, peer := range a.peersByOrd {
		if a.localID != "" && peer.ID == a.localID {
			continue
		}

		serverID := a.peerToServerID(peer, ordinal)
		address := a.peerToAddress(peer)

		peers = append(peers, bootstrap.PeerInfo{
			ServerID: serverID,
			Address:  address,
			Ordinal:  ordinal,
		})
	}

	return peers
}

func (a *DiscoveryAdapter) watchEvents(ctx context.Context, events <-chan ports.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			a.handleEvent(event)
		}
	}
}

func (a *DiscoveryAdapter) handleEvent(event ports.Event) {
	a.mu.Lock()
	defer a.mu.Unlock()

	ordinal := a.extractOrdinalFromPeer(event.Peer)
	if ordinal < 0 {
		a.logger.Debug("ignoring peer with invalid ordinal",
			"peer_id", event.Peer.ID,
			"event_type", event.Type)
		return
	}

	switch event.Type {
	case ports.PeerAdded, ports.PeerUpdated:
		a.peersByOrd[ordinal] = event.Peer
		a.logger.Debug("peer updated in adapter",
			"ordinal", ordinal,
			"peer_id", event.Peer.ID,
			"event_type", event.Type)
	case ports.PeerRemoved:
		delete(a.peersByOrd, ordinal)
		a.logger.Debug("peer removed from adapter",
			"ordinal", ordinal,
			"peer_id", event.Peer.ID)
	}
}

func (a *DiscoveryAdapter) refreshPeersLocked() {
	peers := a.manager.GetPeers()
	a.peersByOrd = make(map[int]ports.Peer, len(peers))

	for _, peer := range peers {
		ordinal := a.extractOrdinalFromPeer(peer)
		if ordinal >= 0 {
			a.peersByOrd[ordinal] = peer
		}
	}
}

func (a *DiscoveryAdapter) extractOrdinalFromPeer(peer ports.Peer) int {
	if ordStr, ok := peer.Metadata["ordinal"]; ok {
		if ord, err := strconv.Atoi(ordStr); err == nil {
			return ord
		}
	}

	return extractOrdinalFromID(peer.ID)
}

func extractOrdinalFromID(id string) int {
	parts := strings.Split(id, "-")
	if len(parts) == 0 {
		return -1
	}

	lastPart := parts[len(parts)-1]
	ordinal, err := strconv.Atoi(lastPart)
	if err != nil {
		return -1
	}
	return ordinal
}

func (a *DiscoveryAdapter) peerToServerID(peer ports.Peer, ordinal int) raft.ServerID {
	if a.serviceName != "" {
		return raft.ServerID(fmt.Sprintf("%s-%d", a.serviceName, ordinal))
	}
	return raft.ServerID(peer.ID)
}

func (a *DiscoveryAdapter) peerToAddress(peer ports.Peer) raft.ServerAddress {
	if grpcAddr, ok := peer.Metadata["grpc_addr"]; ok && grpcAddr != "" {
		return raft.ServerAddress(grpcAddr)
	}

	if grpcPort, ok := peer.Metadata["grpc_port"]; ok {
		port, err := strconv.Atoi(grpcPort)
		if err == nil && port > 0 {
			return raft.ServerAddress(fmt.Sprintf("%s:%d", peer.Address, port))
		}
	}

	return raft.ServerAddress(fmt.Sprintf("%s:%d", peer.Address, peer.Port))
}
