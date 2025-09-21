package discovery

import (
	"context"
	"testing"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func TestMDNSProvider_BasicLifecycle(t *testing.T) {
	provider := NewMDNSProvider("_graft._tcp", "local.", "test-host.", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeInfo := ports.NodeInfo{
		ID:      "test-node",
		Address: "127.0.0.1",
		Port:    8080,
	}

	err := provider.Start(ctx, nodeInfo)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	peers := provider.Snapshot()
	if len(peers) != 0 {
		t.Errorf("Expected 0 peers initially, got %d", len(peers))
	}

	if provider.Name() != "mdns" {
		t.Errorf("Expected name 'mdns', got '%s'", provider.Name())
	}

	if provider.Events() != nil {
		t.Error("Expected Events() to return nil")
	}

	err = provider.Stop()
	if err != nil {
		t.Errorf("Stop() failed: %v", err)
	}
}

func TestMDNSProvider_CannotStartTwice(t *testing.T) {
	provider := NewMDNSProvider("_graft._tcp", "local.", "test-host.", nil)
	ctx := context.Background()

	nodeInfo := ports.NodeInfo{
		ID:      "test-node",
		Address: "127.0.0.1",
		Port:    8080,
	}

	err := provider.Start(ctx, nodeInfo)
	if err != nil {
		t.Fatalf("First Start() failed: %v", err)
	}
	defer provider.Stop()

	err = provider.Start(ctx, nodeInfo)
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

func TestMDNSProvider_CannotStopWhenNotStarted(t *testing.T) {
	provider := NewMDNSProvider("_graft._tcp", "local.", "test-host.", nil)

	err := provider.Stop()
	if err == nil {
		t.Error("Expected Stop() to fail when provider not started")
	} else {
		if !domain.IsDiscoveryError(err) {
			t.Errorf("Expected discovery error, got: %v", err)
		}
		if !domain.IsNotStarted(err) {
			t.Errorf("Expected not started error, got: %v", err)
		}
	}
}

func TestMDNSProvider_DefaultService(t *testing.T) {
	provider := NewMDNSProvider("", "", "test-host.", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeInfo := ports.NodeInfo{
		ID:      "test-node",
		Address: "127.0.0.1",
		Port:    8080,
	}

	err := provider.Start(ctx, nodeInfo)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	defer provider.Stop()

	if provider.service != "_graft._tcp" {
		t.Errorf("Expected default service '_graft._tcp', got '%s'", provider.service)
	}

	if provider.domain != "local." {
		t.Errorf("Expected default domain 'local', got '%s'", provider.domain)
	}
}
