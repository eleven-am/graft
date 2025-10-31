package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/mdns"
)

var mdnsQueryContext = mdns.QueryContext

type MDNSProvider struct {
	mu          sync.RWMutex
	logger      *slog.Logger
	server      *mdns.Server
	peers       map[string]ports.Peer
	ctx         context.Context
	cancel      context.CancelFunc
	nodeInfo    ports.NodeInfo
	service     string
	domain      string
	host        string
	disableIPv6 bool
	events      chan ports.Event
	wg          sync.WaitGroup
}

func NewMDNSProvider(service, domain, host string, disableIPv6 bool, logger *slog.Logger) *MDNSProvider {
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
		logger:      logger.With("component", "discovery", "provider", "mdns"),
		peers:       make(map[string]ports.Peer),
		service:     service,
		domain:      domain,
		host:        host,
		disableIPv6: disableIPv6,
		events:      make(chan ports.Event, 100),
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
	if m.events == nil {
		m.events = make(chan ports.Event, 100)
	}
	m.logger.Debug("starting mDNS discovery provider")

	if err := m.announce(); err != nil {
		return err
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.discoveryLoop()
	}()

	m.logger.Debug("mDNS discovery provider started")
	return nil
}

func (m *MDNSProvider) Stop() error {
	m.mu.Lock()
	if m.cancel == nil {
		m.mu.Unlock()
		return domain.NewDiscoveryError("mdns", "stop", domain.ErrNotStarted)
	}

	cancel := m.cancel
	server := m.server
	events := m.events

	m.logger.Debug("stopping mDNS discovery provider")

	m.cancel = nil
	m.ctx = nil
	m.server = nil
	m.peers = make(map[string]ports.Peer)
	m.events = nil

	m.mu.Unlock()

	if server != nil {
		server.Shutdown()
	}

	cancel()
	m.wg.Wait()

	m.logger.Debug("mDNS discovery provider stopped")
	if events != nil {
		close(events)
	}
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

	txtRecords := []string{fmt.Sprintf("id=%s", m.nodeInfo.ID)}

	for key, value := range m.nodeInfo.Metadata {
		txtRecords = append(txtRecords, fmt.Sprintf("%s=%s", key, value))
	}

	service, err := mdns.NewMDNSService(
		m.nodeInfo.ID,
		m.service,
		m.domain,
		m.host,
		m.nodeInfo.Port,
		[]net.IP{net.ParseIP(m.nodeInfo.Address)},
		txtRecords,
	)
	if err != nil {
		m.logger.Error("failed to create mDNS service",
			"id", m.nodeInfo.ID,
			"service", m.service,
			"domain", m.domain,
			"host", m.host,
			"port", m.nodeInfo.Port,
			"address", m.nodeInfo.Address,
			"error", err)
		return domain.NewDiscoveryError("mdns", "service_create", err)
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		m.logger.Error("failed to create mDNS server", "error", err)
		return domain.NewDiscoveryError("mdns", "server_create", err)
	}

	m.server = server
	m.logger.Debug("mDNS service announced successfully", "id", m.nodeInfo.ID)

	return nil
}

func (m *MDNSProvider) discoveryLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		m.mu.RLock()
		ctx := m.ctx
		m.mu.RUnlock()

		if ctx == nil {
			return
		}

		m.performDiscovery(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *MDNSProvider) performDiscovery(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	entries := make(chan *mdns.ServiceEntry, 100)
	var wg sync.WaitGroup
	seen := make(map[string]struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()

		for entry := range entries {
			if ctx.Err() != nil {
				return
			}

			if id, peer, changed := m.processMDNSEntry(entry); id != "" {
				seen[id] = struct{}{}
				if changed && peer != nil {
					select {
					case m.events <- ports.Event{Type: ports.PeerUpdated, Peer: *peer}:
					case <-ctx.Done():
						return
					default:
					}
				}
			}
		}
	}()

	params := &mdns.QueryParam{
		Service:     m.service,
		Domain:      m.domain,
		Timeout:     5 * time.Second,
		Entries:     entries,
		DisableIPv6: m.disableIPv6,
	}

	if err := mdnsQueryContext(ctx, params); err != nil && !errors.Is(err, context.Canceled) {
		m.logger.Error("mDNS query failed", "error", err)
	}

	close(entries)
	wg.Wait()

	if ctx.Err() != nil {
		return
	}

	m.mu.Lock()
	for id, p := range m.peers {
		if id == m.nodeInfo.ID {
			continue
		}
		if _, ok := seen[id]; !ok {
			delete(m.peers, id)
			select {
			case m.events <- ports.Event{Type: ports.PeerRemoved, Peer: p}:
			case <-ctx.Done():
				m.mu.Unlock()
				return
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

	peerMetadata := m.extractAllMetadata(entry)
	peerMetadata["host"] = entry.Host
	peerMetadata["name"] = entry.Name

	peer := ports.Peer{
		ID:       peerID,
		Address:  entry.AddrV4.String(),
		Port:     entry.Port,
		Metadata: peerMetadata,
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

func (m *MDNSProvider) extractAllMetadata(entry *mdns.ServiceEntry) map[string]string {
	metadata := make(map[string]string)

	for _, txt := range entry.InfoFields {
		if parts := strings.SplitN(txt, "=", 2); len(parts) == 2 {
			key, value := parts[0], parts[1]
			if key != "id" {
				metadata[key] = value
			}
		}
	}

	return metadata
}
