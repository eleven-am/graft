package discovery

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type mockDNSResolver struct {
	mu            sync.Mutex
	srvRecords    map[string][]*net.SRV
	srvConfigured map[string]bool
	hostRecords   map[string][]string
	srvErr        error
	hostErr       error
	callCount     int
}

func newMockDNSResolver() *mockDNSResolver {
	return &mockDNSResolver{
		srvRecords:    make(map[string][]*net.SRV),
		srvConfigured: make(map[string]bool),
		hostRecords:   make(map[string][]string),
	}
}

func (m *mockDNSResolver) LookupSRV(_ context.Context, service, proto, name string) (string, []*net.SRV, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++

	if m.srvErr != nil {
		return "", nil, m.srvErr
	}

	key := fmt.Sprintf("_%s._%s.%s", service, proto, name)
	if m.srvConfigured[key] {
		return "", m.srvRecords[key], nil
	}
	records, ok := m.srvRecords[key]
	if !ok || len(records) == 0 {
		return "", nil, fmt.Errorf("no SRV records for %s", key)
	}

	return "", records, nil
}

func (m *mockDNSResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.hostErr != nil {
		return nil, m.hostErr
	}

	addrs, ok := m.hostRecords[host]
	if !ok || len(addrs) == 0 {
		return nil, fmt.Errorf("no host records for %s", host)
	}

	return addrs, nil
}

func (m *mockDNSResolver) SetSRVRecords(key string, records []*net.SRV) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.srvRecords[key] = records
	m.srvConfigured[key] = true
}

func (m *mockDNSResolver) SetHostRecords(host string, addrs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hostRecords[host] = addrs
}

func (m *mockDNSResolver) SetSRVError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.srvErr = err
}

func (m *mockDNSResolver) SetHostError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hostErr = err
}

func (m *mockDNSResolver) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

func TestKubernetesProvider_Start_Stop(t *testing.T) {
	config := KubernetesConfig{
		ServiceName: "test-service",
		Namespace:   "default",
	}

	provider := NewKubernetesProvider(config, nil)
	resolver := newMockDNSResolver()
	provider.WithResolver(resolver)

	ctx := context.Background()
	announce := ports.NodeInfo{
		ID:      "test-node-0",
		Address: "10.0.0.1",
		Port:    8080,
		Metadata: map[string]string{
			"cluster_id": "test-cluster",
		},
	}

	if err := provider.Start(ctx, announce); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if provider.Name() != "kubernetes" {
		t.Errorf("Name() = %v, want kubernetes", provider.Name())
	}

	if err := provider.Start(ctx, announce); err == nil {
		t.Error("Start() should fail when already started")
	}

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if err := provider.Stop(); err == nil {
		t.Error("Stop() should fail when not started")
	}
}

func TestKubernetesProvider_SRVLookup_Success(t *testing.T) {
	config := KubernetesConfig{
		ServiceName:   "test-service",
		Namespace:     "default",
		RefreshPeriod: 100 * time.Millisecond,
	}

	provider := NewKubernetesProvider(config, nil)
	resolver := newMockDNSResolver()
	provider.WithResolver(resolver)

	resolver.SetSRVRecords("_grpc._tcp.test-service.default.svc.cluster.local", []*net.SRV{
		{Target: "test-service-0.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
		{Target: "test-service-1.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
		{Target: "test-service-2.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
	})
	resolver.SetHostRecords("test-service-0.test-service.default.svc.cluster.local", []string{"10.0.0.1"})
	resolver.SetHostRecords("test-service-1.test-service.default.svc.cluster.local", []string{"10.0.0.2"})
	resolver.SetHostRecords("test-service-2.test-service.default.svc.cluster.local", []string{"10.0.0.3"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	announce := ports.NodeInfo{
		ID:      "test-service-0",
		Address: "10.0.0.1",
		Port:    9000,
	}

	if err := provider.Start(ctx, announce); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	peers := provider.Snapshot()
	if len(peers) != 2 {
		t.Errorf("Snapshot() returned %d peers, want 2 (excluding self)", len(peers))
	}

	for _, peer := range peers {
		if peer.ID == "test-service-0" {
			t.Error("Snapshot() should exclude self node")
		}
		if peer.Port != 9000 {
			t.Errorf("Peer port = %d, want 9000", peer.Port)
		}
		if peer.Metadata["grpc_addr"] == "" {
			t.Error("Peer should have grpc_addr metadata")
		}
	}

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestKubernetesProvider_Fallback_ToARecords(t *testing.T) {
	config := KubernetesConfig{
		ServiceName:   "test-service",
		Namespace:     "default",
		DefaultPort:   9000,
		RefreshPeriod: 100 * time.Millisecond,
	}

	provider := NewKubernetesProvider(config, nil)
	resolver := newMockDNSResolver()
	provider.WithResolver(resolver)

	resolver.SetSRVError(fmt.Errorf("no SRV records"))
	resolver.SetHostRecords("test-service.default.svc.cluster.local", []string{"10.0.0.1", "10.0.0.2"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	announce := ports.NodeInfo{
		ID:      "other-node",
		Address: "10.0.0.99",
		Port:    9000,
	}

	if err := provider.Start(ctx, announce); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	peers := provider.Snapshot()
	if len(peers) != 2 {
		t.Errorf("Snapshot() returned %d peers, want 2", len(peers))
	}

	for _, peer := range peers {
		if peer.Port != 9000 {
			t.Errorf("Peer port = %d, want 9000", peer.Port)
		}
		if peer.Metadata["fallback"] != "true" {
			t.Error("Fallback peers should have fallback=true metadata")
		}
	}

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestKubernetesProvider_Cache_StaleButValid(t *testing.T) {
	config := KubernetesConfig{
		ServiceName:   "test-service",
		Namespace:     "default",
		RefreshPeriod: 50 * time.Millisecond,
	}

	provider := NewKubernetesProvider(config, nil)
	resolver := newMockDNSResolver()
	provider.WithResolver(resolver)

	resolver.SetSRVRecords("_grpc._tcp.test-service.default.svc.cluster.local", []*net.SRV{
		{Target: "test-service-0.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
	})
	resolver.SetHostRecords("test-service-0.test-service.default.svc.cluster.local", []string{"10.0.0.1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	announce := ports.NodeInfo{
		ID:      "other-node",
		Address: "10.0.0.99",
		Port:    9000,
	}

	if err := provider.Start(ctx, announce); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	peers := provider.Snapshot()
	if len(peers) != 1 {
		t.Fatalf("Initial snapshot returned %d peers, want 1", len(peers))
	}

	resolver.SetSRVError(fmt.Errorf("DNS temporarily unavailable"))
	resolver.SetHostError(fmt.Errorf("DNS temporarily unavailable"))

	time.Sleep(100 * time.Millisecond)

	peersAfterFailure := provider.Snapshot()
	if len(peersAfterFailure) != 1 {
		t.Errorf("Snapshot after DNS failure returned %d peers, want 1 (stale cache)", len(peersAfterFailure))
	}

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestKubernetesProvider_Events_PeerAdded(t *testing.T) {
	config := KubernetesConfig{
		ServiceName:   "test-service",
		Namespace:     "default",
		RefreshPeriod: 50 * time.Millisecond,
	}

	provider := NewKubernetesProvider(config, nil)
	resolver := newMockDNSResolver()
	provider.WithResolver(resolver)

	resolver.SetSRVRecords("_grpc._tcp.test-service.default.svc.cluster.local", []*net.SRV{
		{Target: "test-service-1.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
	})
	resolver.SetHostRecords("test-service-1.test-service.default.svc.cluster.local", []string{"10.0.0.2"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	announce := ports.NodeInfo{
		ID:      "test-service-0",
		Address: "10.0.0.1",
		Port:    9000,
	}

	if err := provider.Start(ctx, announce); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	events := provider.Events()
	var receivedEvents []ports.Event

	timeout := time.After(500 * time.Millisecond)
	done := false
	for !done {
		select {
		case event := <-events:
			receivedEvents = append(receivedEvents, event)
		case <-timeout:
			done = true
		}
	}

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	foundPeerAdded := false
	for _, event := range receivedEvents {
		if event.Type == ports.PeerAdded {
			foundPeerAdded = true
			break
		}
	}

	if !foundPeerAdded {
		t.Error("Expected PeerAdded event but none received")
	}
}

func TestKubernetesProvider_Events_PeerRemoved(t *testing.T) {
	config := KubernetesConfig{
		ServiceName:   "test-service",
		Namespace:     "default",
		RefreshPeriod: 50 * time.Millisecond,
	}

	provider := NewKubernetesProvider(config, nil)
	resolver := newMockDNSResolver()
	provider.WithResolver(resolver)

	resolver.SetSRVRecords("_grpc._tcp.test-service.default.svc.cluster.local", []*net.SRV{
		{Target: "test-service-1.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
	})
	resolver.SetHostRecords("test-service-1.test-service.default.svc.cluster.local", []string{"10.0.0.2"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	announce := ports.NodeInfo{
		ID:      "test-service-0",
		Address: "10.0.0.1",
		Port:    9000,
	}

	if err := provider.Start(ctx, announce); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	resolver.SetSRVRecords("_grpc._tcp.test-service.default.svc.cluster.local", []*net.SRV{})

	events := provider.Events()
	var receivedEvents []ports.Event

	timeout := time.After(500 * time.Millisecond)
	done := false
	for !done {
		select {
		case event := <-events:
			receivedEvents = append(receivedEvents, event)
		case <-timeout:
			done = true
		}
	}

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	foundPeerRemoved := false
	for _, event := range receivedEvents {
		if event.Type == ports.PeerRemoved {
			foundPeerRemoved = true
			break
		}
	}

	if !foundPeerRemoved {
		t.Error("Expected PeerRemoved event but none received")
	}
}

func TestKubernetesProvider_OrdinalExtraction(t *testing.T) {
	provider := NewKubernetesProvider(KubernetesConfig{}, nil)

	tests := []struct {
		hostname string
		want     int
	}{
		{"test-service-0", 0},
		{"test-service-1", 1},
		{"test-service-99", 99},
		{"my-app-prod-5", 5},
		{"test-service-0.test-service.default.svc.cluster.local", 0},
		{"no-ordinal", -1},
		{"", -1},
	}

	for _, tt := range tests {
		t.Run(tt.hostname, func(t *testing.T) {
			got := provider.extractOrdinal(tt.hostname)
			if got != tt.want {
				t.Errorf("extractOrdinal(%q) = %d, want %d", tt.hostname, got, tt.want)
			}
		})
	}
}

func TestKubernetesProvider_Snapshot(t *testing.T) {
	config := KubernetesConfig{
		ServiceName:   "test-service",
		Namespace:     "default",
		RefreshPeriod: 50 * time.Millisecond,
	}

	provider := NewKubernetesProvider(config, nil)
	resolver := newMockDNSResolver()
	provider.WithResolver(resolver)

	resolver.SetSRVRecords("_grpc._tcp.test-service.default.svc.cluster.local", []*net.SRV{
		{Target: "test-service-1.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
		{Target: "test-service-2.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
	})
	resolver.SetHostRecords("test-service-1.test-service.default.svc.cluster.local", []string{"10.0.0.2"})
	resolver.SetHostRecords("test-service-2.test-service.default.svc.cluster.local", []string{"10.0.0.3"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	announce := ports.NodeInfo{
		ID:      "test-service-0",
		Address: "10.0.0.1",
		Port:    9000,
	}

	if err := provider.Start(ctx, announce); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	snapshot := provider.Snapshot()

	if len(snapshot) != 2 {
		t.Errorf("Snapshot() returned %d peers, want 2", len(snapshot))
	}

	snapshot2 := provider.Snapshot()
	if len(snapshot2) != 2 {
		t.Errorf("Second Snapshot() returned %d peers, want 2", len(snapshot2))
	}

	snapshot[0].ID = "modified"
	snapshot3 := provider.Snapshot()
	for _, p := range snapshot3 {
		if p.ID == "modified" {
			t.Error("Snapshot() should return a copy, not a reference")
		}
	}

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestKubernetesProvider_AddressNormalization(t *testing.T) {
	config := KubernetesConfig{
		ServiceName:   "test-service",
		Namespace:     "default",
		RefreshPeriod: 50 * time.Millisecond,
	}

	provider := NewKubernetesProvider(config, nil)
	resolver := newMockDNSResolver()
	provider.WithResolver(resolver)

	resolver.SetSRVRecords("_grpc._tcp.test-service.default.svc.cluster.local", []*net.SRV{
		{Target: "test-service-1.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
	})
	resolver.SetHostRecords("test-service-1.test-service.default.svc.cluster.local", []string{"10.0.0.2"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	announce := ports.NodeInfo{
		ID:      "test-service-0",
		Address: "10.0.0.1",
		Port:    9000,
	}

	if err := provider.Start(ctx, announce); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	peers := provider.Snapshot()
	if len(peers) != 1 {
		t.Fatalf("Snapshot() returned %d peers, want 1", len(peers))
	}

	peer := peers[0]
	grpcAddr := peer.Metadata["grpc_addr"]
	expectedAddr := "10.0.0.2:9000"
	if grpcAddr != expectedAddr {
		t.Errorf("grpc_addr = %q, want %q", grpcAddr, expectedAddr)
	}

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestKubernetesProvider_ConcurrentAccess(t *testing.T) {
	config := KubernetesConfig{
		ServiceName:   "test-service",
		Namespace:     "default",
		RefreshPeriod: 20 * time.Millisecond,
	}

	provider := NewKubernetesProvider(config, nil)
	resolver := newMockDNSResolver()
	provider.WithResolver(resolver)

	resolver.SetSRVRecords("_grpc._tcp.test-service.default.svc.cluster.local", []*net.SRV{
		{Target: "test-service-1.test-service.default.svc.cluster.local.", Port: 9000, Priority: 0, Weight: 1},
	})
	resolver.SetHostRecords("test-service-1.test-service.default.svc.cluster.local", []string{"10.0.0.2"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	announce := ports.NodeInfo{
		ID:      "test-service-0",
		Address: "10.0.0.1",
		Port:    9000,
	}

	if err := provider.Start(ctx, announce); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = provider.Snapshot()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestKubernetesConfig_Defaults(t *testing.T) {
	config := KubernetesConfig{
		ServiceName: "test",
		Namespace:   "default",
	}
	config.applyDefaults()

	if config.DNSSuffix != DefaultKubernetesDNSSuffix {
		t.Errorf("DNSSuffix = %q, want %q", config.DNSSuffix, DefaultKubernetesDNSSuffix)
	}
	if config.GRPCPortName != DefaultKubernetesGRPCPortName {
		t.Errorf("GRPCPortName = %q, want %q", config.GRPCPortName, DefaultKubernetesGRPCPortName)
	}
	if config.RefreshPeriod != DefaultKubernetesRefreshPeriod {
		t.Errorf("RefreshPeriod = %v, want %v", config.RefreshPeriod, DefaultKubernetesRefreshPeriod)
	}
	if config.QueryTimeout != DefaultKubernetesQueryTimeout {
		t.Errorf("QueryTimeout = %v, want %v", config.QueryTimeout, DefaultKubernetesQueryTimeout)
	}
}

func TestKubernetesConfig_FQDN(t *testing.T) {
	tests := []struct {
		config   KubernetesConfig
		wantFQDN string
	}{
		{
			config:   KubernetesConfig{ServiceName: "my-svc", Namespace: "prod", DNSSuffix: "svc.cluster.local"},
			wantFQDN: "my-svc.prod.svc.cluster.local",
		},
		{
			config:   KubernetesConfig{ServiceName: "graft", Namespace: "default", DNSSuffix: "svc.cluster.local"},
			wantFQDN: "graft.default.svc.cluster.local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.wantFQDN, func(t *testing.T) {
			got := tt.config.fqdn()
			if got != tt.wantFQDN {
				t.Errorf("fqdn() = %q, want %q", got, tt.wantFQDN)
			}
		})
	}
}
