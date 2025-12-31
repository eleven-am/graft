package discovery

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/mdns"

	"github.com/eleven-am/graft/internal/ports"
)

type MDNSSeeder struct {
	service string
	domain  string
	timeout time.Duration

	mu       sync.Mutex
	server   *mdns.Server
	nodeID   string
	nodeAddr string
	nodePort int
}

func NewMDNSSeeder(service, domain string) *MDNSSeeder {
	if domain == "" {
		domain = "local"
	}
	return &MDNSSeeder{
		service: service,
		domain:  domain,
		timeout: 5 * time.Second,
	}
}

func (m *MDNSSeeder) StartAdvertising(nodeID, addr string, port int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.server != nil {
		return nil
	}

	m.nodeID = nodeID
	m.nodeAddr = addr
	m.nodePort = port

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}

	ips := []net.IP{}
	if ip := net.ParseIP(host); ip != nil {
		ips = append(ips, ip)
	} else {
		if addrs, err := net.LookupIP(host); err == nil {
			ips = addrs
		}
	}

	info := []string{fmt.Sprintf("id=%s", nodeID)}

	service, err := mdns.NewMDNSService(
		nodeID,
		m.service,
		m.domain,
		"",
		port,
		ips,
		info,
	)
	if err != nil {
		return fmt.Errorf("failed to create mdns service: %w", err)
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return fmt.Errorf("failed to create mdns server: %w", err)
	}

	m.server = server
	return nil
}

func (m *MDNSSeeder) StopAdvertising() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.server != nil {
		m.server.Shutdown()
		m.server = nil
	}
}

func (m *MDNSSeeder) Discover(ctx context.Context) ([]ports.Peer, error) {
	entriesCh := make(chan *mdns.ServiceEntry, 10)
	peers := make([]ports.Peer, 0)

	m.mu.Lock()
	selfID := m.nodeID
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	go func() {
		for entry := range entriesCh {
			peer := m.entryToPeer(entry)
			if peer.ID != "" && peer.ID != selfID {
				peers = append(peers, peer)
			}
		}
	}()

	params := &mdns.QueryParam{
		Service:             m.service,
		Domain:              m.domain,
		Timeout:             m.timeout,
		Entries:             entriesCh,
		WantUnicastResponse: true,
	}

	if err := mdns.Query(params); err != nil {
		return nil, fmt.Errorf("mdns query failed: %w", err)
	}

	close(entriesCh)

	return peers, nil
}

func (m *MDNSSeeder) entryToPeer(entry *mdns.ServiceEntry) ports.Peer {
	var id string
	for _, field := range entry.InfoFields {
		if strings.HasPrefix(field, "id=") {
			id = strings.TrimPrefix(field, "id=")
			break
		}
	}

	addr := ""
	if entry.AddrV4 != nil {
		addr = entry.AddrV4.String()
	} else if entry.AddrV6 != nil {
		addr = entry.AddrV6.String()
	}

	return ports.Peer{
		ID:      id,
		Address: addr,
		Port:    entry.Port,
	}
}

func (m *MDNSSeeder) Name() string {
	return "mdns"
}
