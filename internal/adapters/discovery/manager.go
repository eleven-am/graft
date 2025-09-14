package discovery

import (
    "context"
    "sync"
    "time"

    "github.com/eleven-am/graft/internal/ports"
    "log/slog"
)

type Manager struct {
	NodeID    string
	logger    *slog.Logger
	providers []ports.Provider
	peers     map[string]ports.Peer
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	events    chan ports.Event
}

func NewManager(nodeID string, logger *slog.Logger) *Manager {
	return &Manager{
		NodeID:    nodeID,
		logger:    logger.With("component", "discovery", "subcomponent", "manager"),
		providers: make([]ports.Provider, 0),
		peers:     make(map[string]ports.Peer),
		events:    make(chan ports.Event, 100),
	}
}

func (m *Manager) Add(provider ports.Provider) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.providers = append(m.providers, provider)
	m.logger.Debug("added discovery provider", "name", provider.Name())
	return nil
}

func (m *Manager) MDNS(service, domain string) error {
	provider := NewMDNSProvider(service, domain, m.NodeID, m.logger)
	return m.Add(provider)
}

func (m *Manager) Static(peers []ports.Peer) error {
	provider := NewStaticProvider(peers)
	return m.Add(provider)
}

func (m *Manager) GetPeers() []ports.Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]ports.Peer, 0, len(m.peers))
	for _, peer := range m.peers {
		result = append(result, peer)
	}

	return result
}

func (m *Manager) Start(ctx context.Context, address string, port int) error {
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.mu.Unlock()

	node := ports.NodeInfo{
		ID:      m.NodeID,
		Address: address,
		Port:    port,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	for _, provider := range m.providers {
		if err := provider.Start(ctx, node); err != nil {
			return err
		}

		go m.watchProvider(provider)
	}

	go m.aggregateEvents()

	// Periodic snapshot refresh to pick up providers without Events()
	go m.snapshotLoop()

	m.updateSnapshots()

	return nil
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cancel != nil {
		m.cancel()
	}

	for _, provider := range m.providers {
		if err := provider.Stop(); err != nil {
			m.logger.Error("failed to stop provider", "name", provider.Name(), "error", err)
		}
	}

	close(m.events)
	m.providers = nil
	m.peers = make(map[string]ports.Peer)

	return nil
}

func (m *Manager) watchProvider(provider ports.Provider) {
	events := provider.Events()
	if events == nil {
		return
	}

	for {
		select {
		case <-m.ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			m.events <- event
		}
	}
}

func (m *Manager) aggregateEvents() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case event, ok := <-m.events:
			if !ok {
				return
			}
			m.handleEvent(event)
		}
	}
}

func (m *Manager) snapshotLoop() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        m.mu.RLock()
        ctx := m.ctx
        m.mu.RUnlock()
        if ctx == nil {
            return
        }
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            m.updateSnapshots()
        }
    }
}

func (m *Manager) handleEvent(event ports.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch event.Type {
	case ports.PeerAdded, ports.PeerUpdated:
		m.peers[event.Peer.ID] = event.Peer
		m.logger.Debug("peer updated", "id", event.Peer.ID, "address", event.Peer.Address)
	case ports.PeerRemoved:
		delete(m.peers, event.Peer.ID)
		m.logger.Debug("peer removed", "id", event.Peer.ID)
	}
}

func (m *Manager) updateSnapshots() {
	m.mu.Lock()
	defer m.mu.Unlock()

	allPeers := make(map[string]ports.Peer)

	for _, provider := range m.providers {
		snapshot := provider.Snapshot()
		for _, peer := range snapshot {
			allPeers[peer.ID] = peer
		}
	}

	m.peers = allPeers
}
