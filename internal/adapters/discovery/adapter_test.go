package discovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
)

type mockDiscovery struct {
	mu          sync.RWMutex
	peers       []ports.Peer
	subscribers []chan ports.Event
}

func newMockDiscovery() *mockDiscovery {
	return &mockDiscovery{
		peers:       make([]ports.Peer, 0),
		subscribers: make([]chan ports.Event, 0),
	}
}

func (m *mockDiscovery) Start(_ context.Context, _ string, _, _ int, _ string) error {
	return nil
}

func (m *mockDiscovery) GetPeers() []ports.Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]ports.Peer, len(m.peers))
	copy(result, m.peers)
	return result
}

func (m *mockDiscovery) Subscribe() (<-chan ports.Event, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan ports.Event, 10)
	m.subscribers = append(m.subscribers, ch)

	unsubscribe := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for i, sub := range m.subscribers {
			if sub == ch {
				m.subscribers = append(m.subscribers[:i], m.subscribers[i+1:]...)
				close(ch)
				break
			}
		}
	}

	return ch, unsubscribe
}

func (m *mockDiscovery) SetPeers(peers []ports.Peer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers = peers
}

func (m *mockDiscovery) EmitEvent(event ports.Event) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, ch := range m.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func TestDiscoveryAdapter_AddressForOrdinal_Found(t *testing.T) {
	discovery := newMockDiscovery()
	discovery.SetPeers([]ports.Peer{
		{
			ID:      "test-service-0",
			Address: "10.0.0.1",
			Port:    9000,
			Metadata: map[string]string{
				"ordinal":   "0",
				"grpc_addr": "10.0.0.1:9000",
			},
		},
		{
			ID:      "test-service-1",
			Address: "10.0.0.2",
			Port:    9000,
			Metadata: map[string]string{
				"ordinal":   "1",
				"grpc_addr": "10.0.0.2:9000",
			},
		},
	})

	adapter := NewDiscoveryAdapter(DiscoveryAdapterConfig{
		Discovery:   discovery,
		ServiceName: "test-service",
	})

	ctx := context.Background()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer adapter.Stop()

	addr := adapter.AddressForOrdinal(0)
	if addr != "10.0.0.1:9000" {
		t.Errorf("AddressForOrdinal(0) = %q, want %q", addr, "10.0.0.1:9000")
	}

	addr = adapter.AddressForOrdinal(1)
	if addr != "10.0.0.2:9000" {
		t.Errorf("AddressForOrdinal(1) = %q, want %q", addr, "10.0.0.2:9000")
	}
}

func TestDiscoveryAdapter_AddressForOrdinal_NotFound(t *testing.T) {
	discovery := newMockDiscovery()
	discovery.SetPeers([]ports.Peer{
		{
			ID:      "test-service-0",
			Address: "10.0.0.1",
			Port:    9000,
			Metadata: map[string]string{
				"ordinal":   "0",
				"grpc_addr": "10.0.0.1:9000",
			},
		},
	})

	adapter := NewDiscoveryAdapter(DiscoveryAdapterConfig{
		Discovery:   discovery,
		ServiceName: "test-service",
	})

	ctx := context.Background()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer adapter.Stop()

	addr := adapter.AddressForOrdinal(99)
	if addr != "" {
		t.Errorf("AddressForOrdinal(99) = %q, want empty string", addr)
	}
}

func TestDiscoveryAdapter_GetHealthyPeers(t *testing.T) {
	discovery := newMockDiscovery()
	discovery.SetPeers([]ports.Peer{
		{
			ID:      "test-service-0",
			Address: "10.0.0.1",
			Port:    9000,
			Metadata: map[string]string{
				"ordinal":   "0",
				"grpc_addr": "10.0.0.1:9000",
			},
		},
		{
			ID:      "test-service-1",
			Address: "10.0.0.2",
			Port:    9000,
			Metadata: map[string]string{
				"ordinal":   "1",
				"grpc_addr": "10.0.0.2:9000",
			},
		},
		{
			ID:      "test-service-2",
			Address: "10.0.0.3",
			Port:    9000,
			Metadata: map[string]string{
				"ordinal":   "2",
				"grpc_addr": "10.0.0.3:9000",
			},
		},
	})

	adapter := NewDiscoveryAdapter(DiscoveryAdapterConfig{
		Discovery:   discovery,
		ServiceName: "test-service",
		LocalID:     "test-service-0",
	})

	ctx := context.Background()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer adapter.Stop()

	peers := adapter.GetHealthyPeers(ctx)

	if len(peers) != 2 {
		t.Fatalf("GetHealthyPeers() returned %d peers, want 2 (excluding self)", len(peers))
	}

	for _, peer := range peers {
		if peer.ServerID == raft.ServerID("test-service-0") {
			t.Error("GetHealthyPeers() should exclude local node")
		}
		if peer.Address == "" {
			t.Error("Peer address should not be empty")
		}
		if peer.Ordinal < 0 {
			t.Error("Peer ordinal should be non-negative")
		}
	}
}

func TestDiscoveryAdapter_GetHealthyPeers_Empty(t *testing.T) {
	discovery := newMockDiscovery()

	adapter := NewDiscoveryAdapter(DiscoveryAdapterConfig{
		Discovery:   discovery,
		ServiceName: "test-service",
	})

	ctx := context.Background()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer adapter.Stop()

	peers := adapter.GetHealthyPeers(ctx)

	if len(peers) != 0 {
		t.Errorf("GetHealthyPeers() returned %d peers, want 0", len(peers))
	}
}

func TestDiscoveryAdapter_EventSubscription(t *testing.T) {
	discovery := newMockDiscovery()

	adapter := NewDiscoveryAdapter(DiscoveryAdapterConfig{
		Discovery:   discovery,
		ServiceName: "test-service",
	})

	ctx := context.Background()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer adapter.Stop()

	addr := adapter.AddressForOrdinal(0)
	if addr != "" {
		t.Errorf("AddressForOrdinal(0) should be empty initially, got %q", addr)
	}

	discovery.EmitEvent(ports.Event{
		Type: ports.PeerAdded,
		Peer: ports.Peer{
			ID:      "test-service-0",
			Address: "10.0.0.1",
			Port:    9000,
			Metadata: map[string]string{
				"ordinal":   "0",
				"grpc_addr": "10.0.0.1:9000",
			},
		},
	})

	time.Sleep(50 * time.Millisecond)

	addr = adapter.AddressForOrdinal(0)
	if addr != "10.0.0.1:9000" {
		t.Errorf("AddressForOrdinal(0) = %q, want %q after PeerAdded event", addr, "10.0.0.1:9000")
	}

	discovery.EmitEvent(ports.Event{
		Type: ports.PeerRemoved,
		Peer: ports.Peer{
			ID: "test-service-0",
			Metadata: map[string]string{
				"ordinal": "0",
			},
		},
	})

	time.Sleep(50 * time.Millisecond)

	addr = adapter.AddressForOrdinal(0)
	if addr != "" {
		t.Errorf("AddressForOrdinal(0) = %q, want empty string after PeerRemoved event", addr)
	}
}

func TestDiscoveryAdapter_OrdinalMapping(t *testing.T) {
	discovery := newMockDiscovery()
	discovery.SetPeers([]ports.Peer{
		{
			ID:      "app-prod-5",
			Address: "10.0.0.5",
			Port:    9000,
			Metadata: map[string]string{
				"grpc_addr": "10.0.0.5:9000",
			},
		},
	})

	adapter := NewDiscoveryAdapter(DiscoveryAdapterConfig{
		Discovery:   discovery,
		ServiceName: "test-service",
	})

	ctx := context.Background()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer adapter.Stop()

	addr := adapter.AddressForOrdinal(5)
	if addr != "10.0.0.5:9000" {
		t.Errorf("AddressForOrdinal(5) = %q, want %q", addr, "10.0.0.5:9000")
	}
}

func TestDiscoveryAdapter_ServerIDGeneration(t *testing.T) {
	discovery := newMockDiscovery()
	discovery.SetPeers([]ports.Peer{
		{
			ID:      "some-random-id",
			Address: "10.0.0.1",
			Port:    9000,
			Metadata: map[string]string{
				"ordinal":   "0",
				"grpc_addr": "10.0.0.1:9000",
			},
		},
	})

	adapter := NewDiscoveryAdapter(DiscoveryAdapterConfig{
		Discovery:   discovery,
		ServiceName: "my-service",
	})

	ctx := context.Background()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer adapter.Stop()

	peers := adapter.GetHealthyPeers(ctx)
	if len(peers) != 1 {
		t.Fatalf("GetHealthyPeers() returned %d peers, want 1", len(peers))
	}

	expectedID := raft.ServerID("my-service-0")
	if peers[0].ServerID != expectedID {
		t.Errorf("ServerID = %q, want %q", peers[0].ServerID, expectedID)
	}
}

func TestDiscoveryAdapter_ConcurrentAccess(t *testing.T) {
	discovery := newMockDiscovery()
	discovery.SetPeers([]ports.Peer{
		{
			ID:      "test-service-0",
			Address: "10.0.0.1",
			Port:    9000,
			Metadata: map[string]string{
				"ordinal":   "0",
				"grpc_addr": "10.0.0.1:9000",
			},
		},
	})

	adapter := NewDiscoveryAdapter(DiscoveryAdapterConfig{
		Discovery:   discovery,
		ServiceName: "test-service",
	})

	ctx := context.Background()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer adapter.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = adapter.AddressForOrdinal(0)
				_ = adapter.GetHealthyPeers(ctx)
			}
		}()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(ordinal int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				discovery.EmitEvent(ports.Event{
					Type: ports.PeerUpdated,
					Peer: ports.Peer{
						ID:      "test-service-0",
						Address: "10.0.0.1",
						Port:    9000 + j,
						Metadata: map[string]string{
							"ordinal": "0",
						},
					},
				})
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
}

func TestDiscoveryAdapter_AddressFallbacks(t *testing.T) {
	tests := []struct {
		name     string
		peer     ports.Peer
		wantAddr raft.ServerAddress
	}{
		{
			name: "use grpc_addr from metadata",
			peer: ports.Peer{
				ID:      "test-0",
				Address: "10.0.0.1",
				Port:    8080,
				Metadata: map[string]string{
					"ordinal":   "0",
					"grpc_addr": "10.0.0.1:9000",
				},
			},
			wantAddr: "10.0.0.1:9000",
		},
		{
			name: "use grpc_port from metadata",
			peer: ports.Peer{
				ID:      "test-0",
				Address: "10.0.0.1",
				Port:    8080,
				Metadata: map[string]string{
					"ordinal":   "0",
					"grpc_port": "9000",
				},
			},
			wantAddr: "10.0.0.1:9000",
		},
		{
			name: "fallback to peer port",
			peer: ports.Peer{
				ID:      "test-0",
				Address: "10.0.0.1",
				Port:    8080,
				Metadata: map[string]string{
					"ordinal": "0",
				},
			},
			wantAddr: "10.0.0.1:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discovery := newMockDiscovery()
			discovery.SetPeers([]ports.Peer{tt.peer})

			adapter := NewDiscoveryAdapter(DiscoveryAdapterConfig{
				Discovery:   discovery,
				ServiceName: "test",
			})

			ctx := context.Background()
			if err := adapter.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			defer adapter.Stop()

			got := adapter.AddressForOrdinal(0)
			if got != tt.wantAddr {
				t.Errorf("AddressForOrdinal(0) = %q, want %q", got, tt.wantAddr)
			}
		})
	}
}

func TestExtractOrdinalFromID(t *testing.T) {
	tests := []struct {
		id   string
		want int
	}{
		{"test-service-0", 0},
		{"test-service-1", 1},
		{"test-service-99", 99},
		{"my-app-prod-5", 5},
		{"no-ordinal", -1},
		{"", -1},
		{"single", -1},
		{"with-suffix-abc", -1},
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			got := extractOrdinalFromID(tt.id)
			if got != tt.want {
				t.Errorf("extractOrdinalFromID(%q) = %d, want %d", tt.id, got, tt.want)
			}
		})
	}
}
