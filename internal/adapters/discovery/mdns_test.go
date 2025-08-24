package discovery

import (
	"context"
	"testing"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func TestMDNSAdapter_BasicLifecycle(t *testing.T) {
	adapter := NewMDNSAdapter("graft", "local.", "test-host.", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := adapter.Start(ctx)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	peers := adapter.GetPeers()
	if len(peers) != 0 {
		t.Errorf("Expected 0 peers initially, got %d", len(peers))
	}

	nodeInfo := ports.NodeInfo{
		ID:      "test-node",
		Address: "127.0.0.1",
		Port:    8080,
	}

	err = adapter.Announce(nodeInfo)
	if err != nil {
		t.Errorf("Announce() failed: %v", err)
	}

	err = adapter.Stop()
	if err != nil {
		t.Errorf("Stop() failed: %v", err)
	}
}

func TestMDNSAdapter_CannotStartTwice(t *testing.T) {
	adapter := NewMDNSAdapter("graft", "local.", "test-host.", nil)
	ctx := context.Background()

	err := adapter.Start(ctx)
	if err != nil {
		t.Fatalf("First Start() failed: %v", err)
	}
	defer adapter.Stop()

	err = adapter.Start(ctx)
	if err == nil {
		t.Error("Expected Start() to fail when called twice")
	} else {
		if !domain.IsDiscoveryError(err) {
			t.Errorf("Expected discovery error, got: %v", err)
		}
		if !domain.IsAlreadyStarted(err) {
			t.Errorf("Expected already started error, got: %v", err)
		}
	}
}

func TestMDNSAdapter_CannotAnnounceWhenNotStarted(t *testing.T) {
	adapter := NewMDNSAdapter("graft", "local.", "test-host.", nil)

	nodeInfo := ports.NodeInfo{
		ID:      "test-node",
		Address: "127.0.0.1",
		Port:    8080,
	}

	err := adapter.Announce(nodeInfo)
	if err == nil {
		t.Error("Expected Announce() to fail when adapter not started")
	} else {
		if !domain.IsDiscoveryError(err) {
			t.Errorf("Expected discovery error, got: %v", err)
		}
		if !domain.IsNotStarted(err) {
			t.Errorf("Expected not started error, got: %v", err)
		}
	}
}

func TestMDNSAdapter_Subscribe(t *testing.T) {
	adapter := NewMDNSAdapter("graft", "local.", "test-host.", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := adapter.Start(ctx)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	defer adapter.Stop()

}
