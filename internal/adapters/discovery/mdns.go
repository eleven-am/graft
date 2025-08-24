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

type MDNSAdapter struct {
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
}

func NewMDNSAdapter(service, domain, host string, logger *slog.Logger) *MDNSAdapter {
	if logger == nil {
		logger = slog.Default()
	}

	return &MDNSAdapter{
		logger:  logger.With("component", "discovery", "adapter", "mdns"),
		peers:   make(map[string]ports.Peer),
		service: service,
		domain:  domain,
		host:    host,
	}
}

func (m *MDNSAdapter) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx != nil {
		return domain.NewDiscoveryError("mdns", "start", domain.ErrAlreadyStarted)
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.logger.Debug("starting mDNS discovery adapter")

	go m.discoveryLoop()

	m.logger.Debug("mDNS discovery adapter started")
	return nil
}

func (m *MDNSAdapter) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cancel == nil {
		return domain.NewDiscoveryError("mdns", "stop", domain.ErrNotStarted)
	}

	m.logger.Debug("stopping mDNS discovery adapter")

	if m.server != nil {
		m.server.Shutdown()
		m.server = nil
	}

	m.cancel()
	m.ctx = nil
	m.cancel = nil
	m.peers = make(map[string]ports.Peer)

	m.logger.Debug("mDNS discovery adapter stopped")
	return nil
}

func (m *MDNSAdapter) GetPeers() []ports.Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]ports.Peer, 0, len(m.peers))
	for _, peer := range m.peers {
		peers = append(peers, peer)
	}

	return peers
}

func (m *MDNSAdapter) Announce(nodeInfo ports.NodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodeInfo = nodeInfo
	m.logger.Debug("announcing service via mDNS",
		"id", nodeInfo.ID,
		"address", nodeInfo.Address,
		"port", nodeInfo.Port)

	if m.ctx == nil {
		return domain.NewDiscoveryError("mdns", "announce", domain.ErrNotStarted)
	}

	if m.server != nil {
		m.server.Shutdown()
	}

	service, err := mdns.NewMDNSService(
		nodeInfo.ID,
		fmt.Sprintf("_%s._tcp", m.service),
		m.domain,
		m.host,
		nodeInfo.Port,
		[]net.IP{net.ParseIP(nodeInfo.Address)},
		[]string{fmt.Sprintf("id=%s", nodeInfo.ID)},
	)
	if err != nil {
		return domain.NewDiscoveryError("mdns", "service_create", err)
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return domain.NewDiscoveryError("mdns", "server_create", err)
	}

	m.server = server
	m.logger.Debug("mDNS service announced successfully", "id", nodeInfo.ID)

	return nil
}

func (m *MDNSAdapter) discoveryLoop() {
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

func (m *MDNSAdapter) performDiscovery() {
	entries := make(chan *mdns.ServiceEntry, 100)
	defer close(entries)

	go func() {
		for entry := range entries {
			m.processMDNSEntry(entry)
		}
	}()

	params := &mdns.QueryParam{
		Service: "_graft._tcp",
		Domain:  "local",
		Timeout: 5 * time.Second,
		Entries: entries,
	}

	if err := mdns.Query(params); err != nil {
		m.logger.Error("mDNS query failed", "error", err)
		return
	}

	time.Sleep(100 * time.Millisecond)
}

func (m *MDNSAdapter) processMDNSEntry(entry *mdns.ServiceEntry) {
	if len(entry.AddrV4) == 0 {
		return
	}

	peerID := m.extractPeerID(entry)
	if peerID == "" {
		peerID = fmt.Sprintf("%s:%d", entry.AddrV4.String(), entry.Port)
	}

	if peerID == m.nodeInfo.ID {
		return
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
	m.peers[peerID] = peer
	m.mu.Unlock()
}

func (m *MDNSAdapter) extractPeerID(entry *mdns.ServiceEntry) string {
	for _, txt := range entry.InfoFields {
		if len(txt) > 3 && txt[:3] == "id=" {
			return txt[3:]
		}
	}
	return ""
}
