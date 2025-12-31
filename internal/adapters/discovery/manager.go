package discovery

import (
	"context"
	"errors"
	"fmt"
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
	localNode ports.NodeInfo
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

func (m *Manager) MDNS(service, domain string, disableIPv6 bool) error {
	hostname := m.NodeID
	if hostname != "" && !strings.HasSuffix(hostname, ".") {
		hostname += "."
	}
	provider := NewMDNSProvider(service, domain, hostname, disableIPv6, m.logger)
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

func (m *Manager) GetSelfOrdinal() int {
	m.mu.RLock()
	providers := m.providers
	m.mu.RUnlock()

	for _, p := range providers {
		if ordinal := p.GetSelfOrdinal(); ordinal >= 0 {
			return ordinal
		}
	}

	return -1
}

func (m *Manager) Start(ctx context.Context, address string, port int, grpcPort int, clusterID string) error {
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.events = make(chan ports.Event, 100)
	managerCtx := m.ctx
	m.mu.Unlock()

	bootMetadata := metadata.GetGlobalBootstrapMetadata()
	nodeMetadata := metadata.ExtendMetadata(map[string]string{
		"version":    "1.0.0",
		"grpc_port":  strconv.Itoa(grpcPort),
		"cluster_id": clusterID,
	}, bootMetadata)

	node := ports.NodeInfo{
		ID:       m.NodeID,
		Address:  address,
		Port:     port,
		Metadata: nodeMetadata,
	}

	m.mu.Lock()
	m.localNode = node
	m.mu.Unlock()

	for _, provider := range m.providers {
		if err := provider.Start(managerCtx, node); err != nil {
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

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.snapshotLoop()
	}()

	m.updateSnapshots()

	return nil
}

func (m *Manager) UpdateMetadata(updateFn func(map[string]string) map[string]string) error {
	m.mu.Lock()
	if m.cancel == nil {
		m.mu.Unlock()
		return fmt.Errorf("discovery manager not started")
	}

	current := cloneManagerMetadata(m.localNode.Metadata)
	if updateFn != nil {
		modified := updateFn(cloneManagerMetadata(current))
		if modified != nil {
			current = cloneManagerMetadata(modified)
		}
	}

	m.localNode.Metadata = current
	nodeCopy := m.localNode
	providers := append([]ports.Provider(nil), m.providers...)
	m.mu.Unlock()

	var updateErr error
	for _, provider := range providers {
		if updater, ok := provider.(interface{ UpdateMetadata(ports.NodeInfo) error }); ok {
			if err := updater.UpdateMetadata(nodeCopy); err != nil {
				updateErr = errors.Join(updateErr, err)
			}
		}
	}

	return updateErr
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	cancel := m.cancel
	providers := append([]ports.Provider(nil), m.providers...)
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	for _, provider := range providers {
		if err := provider.Stop(); err != nil {
			m.logger.Error("failed to stop provider", "name", provider.Name(), "error", err)
		}
	}

	m.wg.Wait()

	m.mu.Lock()
	m.providers = nil
	m.peers = make(map[string]ports.Peer)
	m.ctx = nil
	m.cancel = nil
	m.mu.Unlock()

	m.subscribersMu.Lock()
	m.subscribers = make(map[int]chan ports.Event)
	m.nextSubscriber = 0
	m.subscribersMu.Unlock()

	return nil
}

func cloneManagerMetadata(src map[string]string) map[string]string {
	if src == nil {
		return make(map[string]string)
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
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
		m.removeSubscriber(id, true)
	}

	return ch, unsubscribe
}

func (m *Manager) broadcastEvent(event ports.Event) {
	type subscriber struct {
		id int
		ch chan ports.Event
	}

	m.subscribersMu.RLock()
	subscribers := make([]subscriber, 0, len(m.subscribers))
	for id, ch := range m.subscribers {
		subscribers = append(subscribers, subscriber{id: id, ch: ch})
	}
	m.subscribersMu.RUnlock()

	var prune []int
	for _, sub := range subscribers {
		cont, remove := m.sendToSubscriber(sub.id, sub.ch, event)
		if remove {
			prune = append(prune, sub.id)
		}
		if !cont {
			break
		}
	}

	if len(prune) > 0 {
		m.pruneSubscribers(prune)
	}
}

func (m *Manager) sendToSubscriber(id int, ch chan ports.Event, event ports.Event) (continueSending bool, shouldPrune bool) {
	continueSending = true
	shouldPrune = false
	defer func() {
		if r := recover(); r != nil {
			shouldPrune = true
			continueSending = true
			m.logger.Debug("recovered from subscriber send", "subscriber_id", id, "error", r)
		}
	}()

	select {
	case ch <- event:
	case <-m.ctx.Done():
		continueSending = false
	default:
		shouldPrune = true
		m.logger.Warn("dropping discovery event for slow subscriber", "subscriber_id", id)
	}

	return continueSending, shouldPrune
}

func (m *Manager) removeSubscriber(id int, closeChan bool) {
	m.subscribersMu.Lock()
	ch, ok := m.subscribers[id]
	if ok {
		delete(m.subscribers, id)
	}
	m.subscribersMu.Unlock()

	if ok && closeChan {
		closeSubscriberChannel(ch)
	}
}

func (m *Manager) closeSubscribers() {
	m.subscribersMu.Lock()
	defer m.subscribersMu.Unlock()

	for id, ch := range m.subscribers {
		closeSubscriberChannel(ch)
		delete(m.subscribers, id)
	}
}

func (m *Manager) pruneSubscribers(ids []int) {
	m.subscribersMu.Lock()
	defer m.subscribersMu.Unlock()

	for _, id := range ids {
		if ch, ok := m.subscribers[id]; ok {
			closeSubscriberChannel(ch)
			delete(m.subscribers, id)
		}
	}
}

func closeSubscriberChannel(ch chan ports.Event) {
	defer func() {
		if r := recover(); r != nil {

		}
	}()

	close(ch)
}
