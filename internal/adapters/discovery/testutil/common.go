package testutil

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

func CreateTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	}))
}

func CreateTestDiscoveryConfig() ports.DiscoveryConfig {
	return ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
		Namespace:   "",
		Metadata:    make(map[string]string),
	}
}

func CreateTestServiceInfo(id, name, address string, port int) ports.ServiceInfo {
	return ports.ServiceInfo{
		ID:       id,
		Name:     name,
		Address:  address,
		Port:     port,
		Metadata: make(map[string]string),
	}
}

func AssertPeerCount(t *testing.T, peers []ports.ServiceInfo, expected int) {
	t.Helper()
	if len(peers) != expected {
		t.Errorf("expected %d peers, got %d", expected, len(peers))
	}
}

func AssertPeerDiscovery(t *testing.T, peers []ports.ServiceInfo, expectedPeers []ports.ServiceInfo) {
	t.Helper()
	
	if len(peers) != len(expectedPeers) {
		t.Errorf("expected %d peers, got %d", len(expectedPeers), len(peers))
		return
	}

	peerMap := make(map[string]ports.ServiceInfo)
	for _, peer := range peers {
		peerMap[peer.ID] = peer
	}

	for _, expected := range expectedPeers {
		peer, found := peerMap[expected.ID]
		if !found {
			t.Errorf("expected peer %s not found", expected.ID)
			continue
		}

		if peer.Name != expected.Name {
			t.Errorf("peer %s: expected name %s, got %s", expected.ID, expected.Name, peer.Name)
		}
		if peer.Address != expected.Address {
			t.Errorf("peer %s: expected address %s, got %s", expected.ID, expected.Address, peer.Address)
		}
		if peer.Port != expected.Port {
			t.Errorf("peer %s: expected port %d, got %d", expected.ID, expected.Port, peer.Port)
		}
	}
}

func WaitForDiscovery(t *testing.T, discovery ports.DiscoveryPort, expectedCount int, timeout time.Duration) []ports.Peer {
	t.Helper()
	
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for discovery of %d peers", expectedCount)
		case <-ticker.C:
			peers, err := discovery.Discover()
			if err != nil {
				continue
			}
			if len(peers) >= expectedCount {
				return peers
			}
		}
	}
}

func CreateTestContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func AssertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}