package discovery

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/helpers/metadata"
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
	wg        sync.WaitGroup

	subscribers    map[int]chan ports.Event
	subscribersMu  sync.RWMutex
	nextSubscriber int
}

func NewManager(nodeID string, logger *slog.Logger) *Manager {
	return &Manager{
		NodeID:      nodeID,
		logger:      logger.With("component", "discovery", "subcomponent", "manager"),
		providers:   make([]ports.Provider, 0),
		peers:       make(map[string]ports.Peer),
		events:      make(chan ports.Event, 100),
		subscribers: make(map[int]chan ports.Event),
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
	hostname := m.NodeID
	if hostname != "" && !strings.HasSuffix(hostname, ".") {
		hostname += "."
	}
	provider := NewMDNSProvider(service, domain, hostname, m.logger)
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

func (m *Manager) Start(ctx context.Context, address string, port int, grpcPort int) error {
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.events = make(chan ports.Event, 100)
	m.mu.Unlock()

	bootMetadata := metadata.GetGlobalBootstrapMetadata()
	nodeMetadata := metadata.ExtendMetadata(map[string]string{
		"version":   "1.0.0",
		"grpc_port": strconv.Itoa(grpcPort),
	}, bootMetadata)

	node := ports.NodeInfo{
		ID:       m.NodeID,
		Address:  address,
		Port:     port,
		Metadata: nodeMetadata,
	}

	for _, provider := range m.providers {
		if err := provider.Start(ctx, node); err != nil {
			return err
		}

		m.wg.Add(1)
		go func(p ports.Provider) {
			defer m.wg.Done()
			m.watchProvider(p)
		}(provider)
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.aggregateEvents()
	}()

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

	m.wg.Wait()

	m.providers = nil
	m.peers = make(map[string]ports.Peer)
	m.ctx = nil
	m.cancel = nil

	m.subscribersMu.Lock()
	m.subscribers = make(map[int]chan ports.Event)
	m.nextSubscriber = 0
	m.subscribersMu.Unlock()

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
			select {
			case <-m.ctx.Done():
				return
			case m.events <- event:
			}
		}
	}
}

func (m *Manager) aggregateEvents() {
	defer m.closeSubscribers()

	for {
		select {
		case <-m.ctx.Done():
			return
		case event, ok := <-m.events:
			if !ok {
				return
			}
			m.handleEvent(event)
			m.broadcastEvent(event)
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
	case ports.PeerRemoved:
		delete(m.peers, event.Peer.ID)
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

func (m *Manager) Subscribe() (<-chan ports.Event, func()) {
	m.subscribersMu.Lock()
	defer m.subscribersMu.Unlock()

	id := m.nextSubscriber
	m.nextSubscriber++

	ch := make(chan ports.Event, 10)
	m.subscribers[id] = ch

	unsubscribe := func() {
		m.removeSubscriber(id)
	}

	return ch, unsubscribe
}

func (m *Manager) broadcastEvent(event ports.Event) {
	m.subscribersMu.RLock()
	subscribers := make([]chan ports.Event, 0, len(m.subscribers))
	for _, ch := range m.subscribers {
		subscribers = append(subscribers, ch)
	}
	m.subscribersMu.RUnlock()

	for _, ch := range subscribers {
		if !m.sendToSubscriber(ch, event) {
			return
		}
	}
}

func (m *Manager) sendToSubscriber(ch chan ports.Event, event ports.Event) bool {
	defer func() {
		if r := recover(); r != nil {
			// subscriber channel closed concurrently; ignore
		}
	}()

	select {
	case ch <- event:
		return true
	case <-m.ctx.Done():
		return false
	}
}

func (m *Manager) removeSubscriber(id int) {
	m.subscribersMu.Lock()
	ch, ok := m.subscribers[id]
	if ok {
		delete(m.subscribers, id)
	}
	m.subscribersMu.Unlock()

	if ok {
		close(ch)
	}
}

func (m *Manager) closeSubscribers() {
	m.subscribersMu.Lock()
	defer m.subscribersMu.Unlock()

	for id, ch := range m.subscribers {
		close(ch)
		delete(m.subscribers, id)
	}
}
