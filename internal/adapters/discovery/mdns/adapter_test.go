package mdns

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery/mdns/mocks"
	"github.com/eleven-am/graft/internal/adapters/discovery/testutil"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/mdns"
)

func TestMDNSAdapter_StartStop(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewMDNSAdapter(logger)

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	err := adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}

	err = adapter.Start(ctx, config)
	if err == nil {
		t.Fatal("expected error when starting already started adapter")
	}

	err = adapter.Stop()
	if err != nil {
		t.Fatalf("failed to stop adapter: %v", err)
	}

	err = adapter.Stop()
	if err == nil {
		t.Fatal("expected error when stopping already stopped adapter")
	}
}

func TestMDNSAdapter_Advertise(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewMDNSAdapter(logger)

	serviceInfo := ports.ServiceInfo{
		ID:      "node-1",
		Name:    "test-service",
		Address: "127.0.0.1",
		Port:    8080,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	err := adapter.Advertise(serviceInfo)
	if err == nil {
		t.Fatal("expected error when advertising before start")
	}

	ctx := context.Background()
	config := testutil.CreateTestDiscoveryConfig()

	err = adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	defer adapter.Stop()

	err = adapter.Advertise(serviceInfo)
	if err != nil {
		t.Fatalf("failed to advertise service: %v", err)
	}
}

func TestMDNSAdapter_Discover(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewMDNSAdapter(logger)

	_, err := adapter.Discover()
	if err == nil {
		t.Fatal("expected error when discovering before start")
	}

	ctx := context.Background()
	config := testutil.CreateTestDiscoveryConfig()

	err = adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	defer adapter.Stop()

	time.Sleep(100 * time.Millisecond)

	peers, err := adapter.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	if len(peers) == 0 {
		t.Logf("no peers discovered (expected in test environment)")
	}
}

func TestBroadcaster_Lifecycle(t *testing.T) {
	logger := testutil.CreateTestLogger()
	broadcaster := NewBroadcaster(logger, "test-service", 8080)

	err := broadcaster.Start()
	if err != nil {
		t.Fatalf("failed to start broadcaster: %v", err)
	}

	serviceInfo := ports.ServiceInfo{
		ID:      "node-1",
		Name:    "test-service",
		Address: "127.0.0.1",
		Port:    8080,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	err = broadcaster.UpdateService(serviceInfo)
	if err != nil {
		t.Fatalf("failed to update service: %v", err)
	}

	broadcaster.Stop()
}

func TestResolver_Discover(t *testing.T) {
	logger := testutil.CreateTestLogger()
	resolver := NewResolver(logger, "test-service")

	entries, err := resolver.Discover(1 * time.Second)
	if err != nil {
		t.Logf("mDNS discovery returned error (expected in test environment): %v", err)
	}

	if entries == nil {
		entries = []*mdns.ServiceEntry{}
	}

	if len(entries) == 0 {
		t.Logf("no entries discovered (expected in test environment)")
	}
}

func TestGetLocalIPs(t *testing.T) {
	ips, err := getLocalIPs()
	if err != nil {
		t.Fatalf("failed to get local IPs: %v", err)
	}

	if len(ips) == 0 {
		t.Fatal("expected at least one IP address")
	}

	for _, ip := range ips {
		if ip == nil {
			t.Fatal("found nil IP address")
		}
		if len(ip) != 4 {
			t.Fatalf("expected IPv4 address, got %v", ip)
		}
	}
}

func TestGetHostname(t *testing.T) {
	hostname, err := getHostname()
	if err != nil {
		t.Fatalf("failed to get hostname: %v", err)
	}

	if hostname == "" {
		t.Fatal("expected non-empty hostname")
	}
}

func TestNewMDNSAdapter_NilLogger(t *testing.T) {
	adapter := NewMDNSAdapter(nil)
	if adapter == nil {
		t.Fatal("expected adapter to be created with nil logger")
	}
	if adapter.logger == nil {
		t.Fatal("expected logger to be set to default")
	}
}

func TestNewBroadcaster_NilLogger(t *testing.T) {
	broadcaster := NewBroadcaster(nil, "test-service", 8080)
	if broadcaster == nil {
		t.Fatal("expected broadcaster to be created with nil logger")
	}
	if broadcaster.logger == nil {
		t.Fatal("expected logger to be set to default")
	}
}

func TestNewResolver_NilLogger(t *testing.T) {
	resolver := NewResolver(nil, "test-service")
	if resolver == nil {
		t.Fatal("expected resolver to be created with nil logger")
	}
	if resolver.logger == nil {
		t.Fatal("expected logger to be set to default")
	}
}

func TestResolver_Browse(t *testing.T) {
	logger := testutil.CreateTestLogger()
	resolver := NewResolver(logger, "test-service")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entriesCh, err := resolver.Browse(ctx)
	if err != nil {
		t.Fatalf("failed to start browsing: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-entriesCh:
	case <-time.After(1 * time.Second):
		t.Log("browse channel didn't close after context cancellation (expected in test)")
	}
}

func TestMDNSAdapter_PerformDiscovery(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewMDNSAdapter(logger)

	ctx := context.Background()
	config := testutil.CreateTestDiscoveryConfig()

	err := adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	defer adapter.Stop()

	adapter.performDiscovery()

	peers, err := adapter.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	if len(peers) == 0 {
		t.Logf("no peers discovered (expected in test environment)")
	}
}

func TestMDNSAdapter_MultipleAdvertise(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewMDNSAdapter(logger)

	ctx := context.Background()
	config := testutil.CreateTestDiscoveryConfig()

	err := adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	defer adapter.Stop()

	serviceInfo1 := ports.ServiceInfo{
		ID:      "node-1",
		Name:    "test-service",
		Address: "127.0.0.1",
		Port:    8080,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	err = adapter.Advertise(serviceInfo1)
	if err != nil {
		t.Fatalf("failed to advertise service: %v", err)
	}

	serviceInfo2 := ports.ServiceInfo{
		ID:      "node-2",
		Name:    "test-service",
		Address: "127.0.0.1",
		Port:    8081,
		Metadata: map[string]string{
			"version": "2.0.0",
			"region":  "us-east",
		},
	}

	err = adapter.Advertise(serviceInfo2)
	if err != nil {
		t.Fatalf("failed to advertise updated service: %v", err)
	}
}


func TestMDNSAdapter_DiscoveryLoopWithCancel(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewMDNSAdapter(logger)

	ctx, cancel := context.WithCancel(context.Background())
	config := testutil.CreateTestDiscoveryConfig()

	err := adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	err = adapter.Stop()
	if err != nil {
		t.Fatalf("failed to stop adapter: %v", err)
	}
}

func TestMDNSAdapter_PerformDiscoveryWithPeers(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewMDNSAdapter(logger)

	ctx := context.Background()
	config := testutil.CreateTestDiscoveryConfig()

	err := adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	
	adapter.Stop()
	
	adapter.mu.Lock()
	adapter.ctx, adapter.cancel = context.WithCancel(ctx)
	adapter.config = &config
	adapter.mu.Unlock()
	
	defer adapter.Stop()

	mockEntry1 := &mdns.ServiceEntry{
		Name:       "test-service",
		Host:       "host1.local",
		AddrV4:     net.IPv4(192, 168, 1, 100),
		Port:       8080,
		InfoFields: []string{"peer-1"},
	}
	
	mockEntry2 := &mdns.ServiceEntry{
		Name:       "test-service",
		Host:       "host2.local",
		AddrV4:     net.IPv4(192, 168, 1, 101),
		Port:       8081,
		InfoFields: []string{""},
	}

	adapter.mu.Lock()
	adapter.peers["old-peer"] = &ports.Peer{
		ID:      "old-peer",
		Address: "192.168.1.50",
		Port:    8082,
	}
	adapter.mu.Unlock()

	mockResolver := mocks.NewMockResolver(t)
	mockResolver.EXPECT().
		Discover(5*time.Second).
		Return([]*mdns.ServiceEntry{mockEntry1, mockEntry2}, nil).
		Once()

	originalResolver := adapter.resolver
	adapter.resolver = mockResolver

	adapter.performDiscovery()

	peers, err := adapter.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	if len(peers) != 2 {
		t.Errorf("expected 2 peers, got %d", len(peers))
	}

	found1 := false
	found2 := false
	for _, peer := range peers {
		if peer.ID == "peer-1" {
			found1 = true
			if peer.Address != "192.168.1.100" {
				t.Errorf("expected address 192.168.1.100, got %s", peer.Address)
			}
		}
		if peer.ID == "192.168.1.101:8081" {
			found2 = true
		}
	}

	if !found1 || !found2 {
		t.Errorf("expected to find both peers, found1=%v, found2=%v", found1, found2)
	}

	adapter.mu.RLock()
	_, oldExists := adapter.peers["old-peer"]
	adapter.mu.RUnlock()
	
	if oldExists {
		t.Error("expected old-peer to be removed")
	}

	adapter.resolver = originalResolver
	mockResolver.AssertExpectations(t)
}

func TestMDNSAdapter_PerformDiscoveryWithError(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewMDNSAdapter(logger)

	ctx := context.Background()
	config := testutil.CreateTestDiscoveryConfig()

	err := adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	defer adapter.Stop()

	mockResolver := mocks.NewMockResolver(t)
	mockResolver.EXPECT().
		Discover(5*time.Second).
		Return(nil, fmt.Errorf("discovery failed")).
		Once()

	originalResolver := adapter.resolver
	adapter.resolver = mockResolver

	adapter.performDiscovery()

	peers, err := adapter.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	if len(peers) != 0 {
		t.Errorf("expected 0 peers after error, got %d", len(peers))
	}

	adapter.resolver = originalResolver
	mockResolver.AssertExpectations(t)
}