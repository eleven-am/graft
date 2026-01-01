package discovery

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type DNSSeeder struct {
	hostname string
	service  string
	timeout  time.Duration
}

func NewDNSSeeder(hostname, service string) *DNSSeeder {
	return &DNSSeeder{
		hostname: hostname,
		service:  service,
		timeout:  5 * time.Second,
	}
}

func (d *DNSSeeder) Discover(ctx context.Context) ([]ports.Peer, error) {
	resolver := &net.Resolver{}

	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	_, targets, err := resolver.LookupSRV(ctx, d.service, "tcp", d.hostname)
	if err != nil {
		return nil, fmt.Errorf("srv lookup failed: %w", err)
	}

	peers := make([]ports.Peer, 0, len(targets))
	for _, target := range targets {
		hostname := strings.TrimSuffix(target.Target, ".")
		podName := extractPodName(hostname)

		peers = append(peers, ports.Peer{
			ID:      podName,
			Address: hostname,
			Port:    int(target.Port),
		})
	}

	return peers, nil
}

func extractPodName(fqdn string) string {
	parts := strings.Split(fqdn, ".")
	if len(parts) > 0 {
		return parts[0]
	}
	return fqdn
}

func (d *DNSSeeder) Name() string {
	return "dns"
}
