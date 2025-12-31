package discovery

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type DNSSeeder struct {
	hostname string
	port     int
	timeout  time.Duration
}

func NewDNSSeeder(hostname string, port int) *DNSSeeder {
	return &DNSSeeder{
		hostname: hostname,
		port:     port,
		timeout:  5 * time.Second,
	}
}

func (d *DNSSeeder) Discover(ctx context.Context) ([]ports.Peer, error) {
	resolver := &net.Resolver{}

	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	addrs, err := resolver.LookupHost(ctx, d.hostname)
	if err != nil {
		return nil, fmt.Errorf("dns lookup failed: %w", err)
	}

	peers := make([]ports.Peer, 0, len(addrs))
	for _, addr := range addrs {
		peers = append(peers, ports.Peer{
			ID:      addr,
			Address: addr,
			Port:    d.port,
		})
	}

	return peers, nil
}

func (d *DNSSeeder) Name() string {
	return "dns"
}
