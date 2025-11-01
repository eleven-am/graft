package discovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/mdns"
)

func stubMDNSQuery(t *testing.T, fn func(context.Context, *mdns.QueryParam) error) {
	t.Helper()

	original := mdnsQueryContext
	mdnsQueryContext = fn
	t.Cleanup(func() {
		mdnsQueryContext = original
	})
}

func TestMDNSProvider_BasicLifecycle(t *testing.T) {
	provider := NewMDNSProvider("_graft._tcp", "local.", "test-host.", true, nil)

	stubMDNSQuery(t, func(ctx context.Context, params *mdns.QueryParam) error {
		return nil
	})

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

	if provider.Events() == nil {
		t.Error("Expected Events() to return a channel, got nil")
	}

	err = provider.Stop()
	if err != nil {
		t.Errorf("Stop() failed: %v", err)
	}
}

func TestMDNSProvider_CannotStartTwice(t *testing.T) {
	provider := NewMDNSProvider("_graft._tcp", "local.", "test-host.", true, nil)
	ctx := context.Background()

	stubMDNSQuery(t, func(ctx context.Context, params *mdns.QueryParam) error {
		return nil
	})

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
	provider := NewMDNSProvider("_graft._tcp", "local.", "test-host.", true, nil)

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
	provider := NewMDNSProvider("", "", "test-host.", true, nil)

	stubMDNSQuery(t, func(ctx context.Context, params *mdns.QueryParam) error {
		return nil
	})

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

func TestMDNSProvider_StopWaitsForSenders(t *testing.T) {
	provider := NewMDNSProvider("_graft._tcp", "local.", "test-host.", true, nil)

	provider.mu.Lock()
	provider.ctx, provider.cancel = context.WithCancel(context.Background())
	provider.events = make(chan ports.Event, 1)
	provider.wg.Add(1)
	provider.mu.Unlock()

	events := provider.events

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer provider.wg.Done()
		time.Sleep(10 * time.Millisecond)
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("send panicked: %v", r)
			}
		}()
		events <- ports.Event{Type: ports.PeerUpdated}
	}()

	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	<-done
}

func TestMDNSProvider_StopCancelsInFlightDiscovery(t *testing.T) {
	provider := NewMDNSProvider("_graft._tcp", "local.", "test-host.", true, nil)

	queryStarted := make(chan struct{})
	var once sync.Once

	stubMDNSQuery(t, func(ctx context.Context, params *mdns.QueryParam) error {
		once.Do(func() { close(queryStarted) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return nil
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeInfo := ports.NodeInfo{
		ID:      "test-node",
		Address: "127.0.0.1",
		Port:    8080,
	}

	if err := provider.Start(ctx, nodeInfo); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	select {
	case <-queryStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("mdnsQueryContext was not invoked in time")
	}

	stopStart := time.Now()
	if err := provider.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	if elapsed := time.Since(stopStart); elapsed > 100*time.Millisecond {
		t.Fatalf("Stop() took too long: %v", elapsed)
	}
}
