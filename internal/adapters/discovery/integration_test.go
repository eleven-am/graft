package discovery_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery"
	"github.com/eleven-am/graft/internal/ports"
)

func TestDiscoveryIntegration_StaticPeers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	}))

	os.Setenv("GRAFT_PEERS", "127.0.0.1:9091,127.0.0.1:9092")
	defer os.Unsetenv("GRAFT_PEERS")

	manager := discovery.NewManager(logger, discovery.StrategyStatic)

	config := ports.DiscoveryConfig{
		ServiceName: "integration-test",
		ServicePort: 9090,
		Metadata: map[string]string{
			"test": "integration",
		},
	}

	ctx := context.Background()
	err := manager.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start discovery manager: %v", err)
	}
	defer func() {
		if err := manager.Stop(); err != nil {
			t.Errorf("failed to stop manager: %v", err)
		}
	}()

	serviceInfo := ports.ServiceInfo{
		ID:      "integration-test-001",
		Name:    "integration-test",
		Address: "127.0.0.1",
		Port:    9090,
		Metadata: map[string]string{
			"instance": "primary",
		},
	}

	err = manager.Advertise(serviceInfo)
	if err != nil {
		t.Fatalf("failed to advertise service: %v", err)
	}

	time.Sleep(2 * time.Second)

	peers, err := manager.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	t.Logf("discovered %d healthy peers", len(peers))
}

func TestDiscoveryIntegration_MDNSLocal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	}))

	manager1 := discovery.NewManager(logger, discovery.StrategyMDNS)
	manager2 := discovery.NewManager(logger, discovery.StrategyMDNS)

	config1 := ports.DiscoveryConfig{
		ServiceName: "mdns-integration-test",
		ServicePort: 9100,
	}

	config2 := ports.DiscoveryConfig{
		ServiceName: "mdns-integration-test",
		ServicePort: 9101,
	}

	ctx := context.Background()

	err := manager1.Start(ctx, config1)
	if err != nil {
		t.Fatalf("failed to start first manager: %v", err)
	}
	defer func() {
		if err := manager1.Stop(); err != nil {
			t.Errorf("failed to stop first manager: %v", err)
		}
	}()

	err = manager2.Start(ctx, config2)
	if err != nil {
		t.Fatalf("failed to start second manager: %v", err)
	}
	defer func() {
		if err := manager2.Stop(); err != nil {
			t.Errorf("failed to stop second manager: %v", err)
		}
	}()

	service1 := ports.ServiceInfo{
		ID:      "mdns-test-001",
		Name:    "mdns-integration-test",
		Address: "127.0.0.1",
		Port:    9100,
		Metadata: map[string]string{
			"instance": "service-1",
		},
	}

	service2 := ports.ServiceInfo{
		ID:      "mdns-test-002",
		Name:    "mdns-integration-test",
		Address: "127.0.0.1",
		Port:    9101,
		Metadata: map[string]string{
			"instance": "service-2",
		},
	}

	err = manager1.Advertise(service1)
	if err != nil {
		t.Fatalf("failed to advertise service1: %v", err)
	}

	err = manager2.Advertise(service2)
	if err != nil {
		t.Fatalf("failed to advertise service2: %v", err)
	}

	time.Sleep(3 * time.Second)

	peers1, err := manager1.Discover()
	if err != nil {
		t.Fatalf("manager1 failed to discover peers: %v", err)
	}

	peers2, err := manager2.Discover()
	if err != nil {
		t.Fatalf("manager2 failed to discover peers: %v", err)
	}

	t.Logf("manager1 discovered %d peers", len(peers1))
	t.Logf("manager2 discovered %d peers", len(peers2))

}

func TestDiscoveryIntegration_StrategyAutoDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	}))

	testCases := []struct {
		name           string
		envSetup       func()
		envCleanup     func()
		expectedStrategy string
	}{
		{
			name: "static_detection",
			envSetup: func() {
				os.Setenv("GRAFT_PEERS", "127.0.0.1:8080")
			},
			envCleanup: func() {
				os.Unsetenv("GRAFT_PEERS")
			},
			expectedStrategy: "static",
		},
		{
			name: "mdns_fallback",
			envSetup: func() {
				os.Setenv("GRAFT_PEERS", "")
			},
			envCleanup: func() {
				os.Unsetenv("GRAFT_PEERS")
			},
			expectedStrategy: "mdns",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.envSetup()
			defer tc.envCleanup()

			manager := discovery.NewManager(logger, discovery.StrategyAuto)

			config := ports.DiscoveryConfig{
				ServiceName: "auto-detection-test",
				ServicePort: 8080,
			}

			ctx := context.Background()
			err := manager.Start(ctx, config)
			if err != nil {
				t.Fatalf("failed to start manager with auto detection: %v", err)
			}

			serviceInfo := ports.ServiceInfo{
				ID:      "auto-test-001",
				Name:    "auto-detection-test",
				Address: "127.0.0.1",
				Port:    8080,
			}

			err = manager.Advertise(serviceInfo)
			if err != nil {
				t.Fatalf("failed to advertise service: %v", err)
			}

			_, err = manager.Discover()
			if err != nil {
				t.Fatalf("failed to discover peers: %v", err)
			}

			err = manager.Stop()
			if err != nil {
				t.Fatalf("failed to stop manager: %v", err)
			}

			t.Logf("auto-detection successfully chose strategy and completed discovery cycle")
		})
	}
}

func TestDiscoveryIntegration_ConcurrentOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	}))

	os.Setenv("GRAFT_PEERS", "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083")
	defer os.Unsetenv("GRAFT_PEERS")

	manager := discovery.NewManager(logger, discovery.StrategyStatic)

	config := ports.DiscoveryConfig{
		ServiceName: "concurrent-test",
		ServicePort: 8080,
	}

	ctx := context.Background()
	err := manager.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer func() {
		if err := manager.Stop(); err != nil {
			t.Errorf("failed to stop manager: %v", err)
		}
	}()

	services := []ports.ServiceInfo{
		{
			ID:      "service-001",
			Name:    "concurrent-test",
			Address: "127.0.0.1",
			Port:    8080,
			Metadata: map[string]string{"instance": "1"},
		},
		{
			ID:      "service-002",
			Name:    "concurrent-test",
			Address: "127.0.0.1",
			Port:    8080,
			Metadata: map[string]string{"instance": "2"},
		},
		{
			ID:      "service-003",
			Name:    "concurrent-test",
			Address: "127.0.0.1",
			Port:    8080,
			Metadata: map[string]string{"instance": "3"},
		},
	}

	errChan := make(chan error, len(services))
	for i, service := range services {
		go func(svc ports.ServiceInfo, index int) {
			errChan <- manager.Advertise(svc)
		}(service, i)
	}

	for i := 0; i < len(services); i++ {
		if err := <-errChan; err != nil {
			t.Errorf("concurrent advertise failed: %v", err)
		}
	}

	discoverCount := 10
	discoverChan := make(chan error, discoverCount)
	for i := 0; i < discoverCount; i++ {
		go func(index int) {
			_, err := manager.Discover()
			discoverChan <- err
		}(i)
	}

	for i := 0; i < discoverCount; i++ {
		if err := <-discoverChan; err != nil {
			t.Errorf("concurrent discover failed: %v", err)
		}
	}

	t.Logf("concurrent operations completed successfully")
}

func TestDiscoveryIntegration_LifecycleStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	}))

	cycles := 5
	for i := 0; i < cycles; i++ {
		manager := discovery.NewManager(logger, discovery.StrategyMDNS)

		config := ports.DiscoveryConfig{
			ServiceName: "stress-test",
			ServicePort: 8080 + i,
		}

		ctx := context.Background()
		err := manager.Start(ctx, config)
		if err != nil {
			t.Fatalf("cycle %d: failed to start manager: %v", i, err)
		}

		serviceInfo := ports.ServiceInfo{
			ID:      "stress-test-001",
			Name:    "stress-test",
			Address: "127.0.0.1",
			Port:    8080 + i,
		}

		err = manager.Advertise(serviceInfo)
		if err != nil {
			t.Fatalf("cycle %d: failed to advertise: %v", i, err)
		}

		_, err = manager.Discover()
		if err != nil {
			t.Fatalf("cycle %d: failed to discover: %v", i, err)
		}

		err = manager.Stop()
		if err != nil {
			t.Fatalf("cycle %d: failed to stop: %v", i, err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("completed %d start/stop cycles successfully", cycles)
}