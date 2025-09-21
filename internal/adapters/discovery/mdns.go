package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/mdns"
)

type MDNSProvider struct {
	mu       sync.RWMutex
	logger   *slog.Logger
	server   *mdns.Server
	peers    map[string]ports.Peer
	ctx      context.Context
	cancel   context.CancelFunc
	nodeInfo ports.NodeInfo
	service  string
	domain   string
	host     string
	events   chan ports.Event
}

func NewMDNSProvider(service, domain, host string, logger *slog.Logger) *MDNSProvider {
	if logger == nil {
		logger = slog.Default()
	}

	if service == "" {
		service = "_graft._tcp"
	}
	if domain == "" {
		domain = "local."
	}

	return &MDNSProvider{
		logger:  logger.With("component", "discovery", "provider", "mdns"),
		peers:   make(map[string]ports.Peer),
		service: service,
		domain:  domain,
		host:    host,
		events:  make(chan ports.Event, 100),
	}
}

func (m *MDNSProvider) Start(ctx context.Context, announce ports.NodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx != nil {
		return domain.NewDiscoveryError("mdns", "start", domain.ErrAlreadyStarted)
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.nodeInfo = announce
	m.logger.Debug("starting mDNS discovery provider")

	if err := m.announce(); err != nil {
		return err
	}

	go m.discoveryLoop()

	m.logger.Debug("mDNS discovery provider started")
	return nil
}

func (m *MDNSProvider) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cancel == nil {
		return domain.NewDiscoveryError("mdns", "stop", domain.ErrNotStarted)
	}

	m.logger.Debug("stopping mDNS discovery provider")

	if m.server != nil {
		m.server.Shutdown()
		m.server = nil
	}

	m.cancel()
	m.ctx = nil
	m.cancel = nil
	m.peers = make(map[string]ports.Peer)

	m.logger.Debug("mDNS discovery provider stopped")
	close(m.events)
	return nil
}

func (m *MDNSProvider) Snapshot() []ports.Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]ports.Peer, 0, len(m.peers))
	for _, peer := range m.peers {
		peers = append(peers, peer)
	}

	return peers
}

func (m *MDNSProvider) Events() <-chan ports.Event {
	return m.events
}

func (m *MDNSProvider) Name() string {
	return "mdns"
}

func (m *MDNSProvider) announce() error {
	m.logger.Debug("announcing service via mDNS",
		"id", m.nodeInfo.ID,
		"address", m.nodeInfo.Address,
		"port", m.nodeInfo.Port)

	if m.server != nil {
		m.server.Shutdown()
	}

	service, err := mdns.NewMDNSService(
		m.nodeInfo.ID,
		m.service,
		m.domain,
		m.host,
		m.nodeInfo.Port,
		[]net.IP{net.ParseIP(m.nodeInfo.Address)},
		[]string{fmt.Sprintf("id=%s", m.nodeInfo.ID)},
	)
	if err != nil {
		return domain.NewDiscoveryError("mdns", "service_create", err)
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return domain.NewDiscoveryError("mdns", "server_create", err)
	}

	m.server = server
	m.logger.Debug("mDNS service announced successfully", "id", m.nodeInfo.ID)

	return nil
}

func (m *MDNSProvider) discoveryLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	m.performDiscovery()

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
			m.performDiscovery()
		}
	}
}

func (m *MDNSProvider) performDiscovery() {
	entries := make(chan *mdns.ServiceEntry, 100)
	defer close(entries)

	seen := make(map[string]struct{})

	go func() {
		for entry := range entries {
			if id, peer, changed := m.processMDNSEntry(entry); id != "" {
				seen[id] = struct{}{}
				if changed && peer != nil {
					select {
					case m.events <- ports.Event{Type: ports.PeerUpdated, Peer: *peer}:
					default:

					}
				}
			}
		}
	}()

	params := &mdns.QueryParam{
		Service: m.service,
		Domain:  m.domain,
		Timeout: 5 * time.Second,
		Entries: entries,
	}

	if err := mdns.Query(params); err != nil {
		m.logger.Error("mDNS query failed", "error", err)
		return
	}

	time.Sleep(100 * time.Millisecond)

	m.mu.Lock()
	for id, p := range m.peers {
		if id == m.nodeInfo.ID {
			continue
		}
		if _, ok := seen[id]; !ok {
			delete(m.peers, id)
			select {
			case m.events <- ports.Event{Type: ports.PeerRemoved, Peer: p}:
			default:
			}
		}
	}
	m.mu.Unlock()
}

func (m *MDNSProvider) processMDNSEntry(entry *mdns.ServiceEntry) (string, *ports.Peer, bool) {
	if len(entry.AddrV4) == 0 {
		return "", nil, false
	}

	peerID := m.extractPeerID(entry)
	if peerID == "" {
		peerID = fmt.Sprintf("%s:%d", entry.AddrV4.String(), entry.Port)
	}

	if peerID == m.nodeInfo.ID {
		return "", nil, false
	}

	peer := ports.Peer{
		ID:      peerID,
		Address: entry.AddrV4.String(),
		Port:    entry.Port,
		Metadata: map[string]string{
			"host": entry.Host,
			"name": entry.Name,
		},
	}

	m.mu.Lock()
	prev, existed := m.peers[peerID]
	m.peers[peerID] = peer
	m.mu.Unlock()

	if !existed || prev.Address != peer.Address || prev.Port != peer.Port {
		return peerID, &peer, true
	}
	return peerID, &peer, false
}

func (m *MDNSProvider) extractPeerID(entry *mdns.ServiceEntry) string {
	for _, txt := range entry.InfoFields {
		if len(txt) > 3 && txt[:3] == "id=" {
			return txt[3:]
		}
	}
	return ""
}
