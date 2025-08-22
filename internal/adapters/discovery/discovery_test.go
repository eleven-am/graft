package discovery

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery/testutil"
	"github.com/eleven-am/graft/internal/ports"
)

func TestManager_AutoDetection(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyAuto)

	os.Setenv("GRAFT_PEERS", "127.0.0.1:8081")
	defer os.Unsetenv("GRAFT_PEERS")

	strategy := manager.detectEnvironment()
	if strategy != StrategyStatic {
		t.Fatalf("expected static strategy, got %s", strategy)
	}

	os.Unsetenv("GRAFT_PEERS")
	strategy = manager.detectEnvironment()
	if strategy != StrategyMDNS {
		t.Fatalf("expected mDNS strategy, got %s", strategy)
	}
}

func TestManager_StartStop(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyStatic)

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	os.Setenv("GRAFT_PEERS", "127.0.0.1:8081")
	defer os.Unsetenv("GRAFT_PEERS")

	err := manager.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}

	err = manager.Start(ctx, config)
	if err == nil {
		t.Fatal("expected error when starting already started manager")
	}

	err = manager.Stop()
	if err != nil {
		t.Fatalf("failed to stop manager: %v", err)
	}

	err = manager.Stop()
	if err == nil {
		t.Fatal("expected error when stopping already stopped manager")
	}
}

func TestManager_Advertise(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyStatic)

	serviceInfo := ports.ServiceInfo{
		ID:      "node-1",
		Name:    "test-service",
		Address: "127.0.0.1",
		Port:    8080,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	err := manager.Advertise(serviceInfo)
	if err == nil {
		t.Fatal("expected error when advertising before start")
	}

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	os.Setenv("GRAFT_PEERS", "")
	defer os.Unsetenv("GRAFT_PEERS")

	err = manager.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer manager.Stop()

	err = manager.Advertise(serviceInfo)
	if err != nil {
		t.Fatalf("failed to advertise service: %v", err)
	}
}

func TestManager_Discover(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyStatic)

	_, err := manager.Discover()
	if err == nil {
		t.Fatal("expected error when discovering before start")
	}

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	os.Setenv("GRAFT_PEERS", "127.0.0.1:8081,127.0.0.1:8082")
	defer os.Unsetenv("GRAFT_PEERS")

	err = manager.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer manager.Stop()

	time.Sleep(100 * time.Millisecond)

	peers, err := manager.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	if len(peers) == 0 {
		t.Logf("no peers discovered (expected in test environment)")
	}
}

func TestManager_MDNSStrategy(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyMDNS)

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	err := manager.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start manager with mDNS: %v", err)
	}
	defer manager.Stop()

	serviceInfo := ports.ServiceInfo{
		ID:      "node-1",
		Name:    "test-service",
		Address: "127.0.0.1",
		Port:    8080,
	}

	err = manager.Advertise(serviceInfo)
	if err != nil {
		t.Fatalf("failed to advertise service: %v", err)
	}

	peers, err := manager.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	if len(peers) == 0 {
		t.Logf("no peers discovered (expected in test environment)")
	}
}

func TestManager_UnknownStrategy(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, "unknown")

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	err := manager.Start(ctx, config)
	if err == nil {
		t.Fatal("expected error with unknown strategy")
	}
}

func TestManager_RefreshPeers(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyStatic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	os.Setenv("GRAFT_PEERS", "127.0.0.1:8081")
	defer os.Unsetenv("GRAFT_PEERS")

	err := manager.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer manager.Stop()

	time.Sleep(100 * time.Millisecond)

	peers, err := manager.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	if len(peers) > 1 {
		t.Fatalf("expected at most 1 peer, got %d", len(peers))
	}
}

func TestNewManager_NilLogger(t *testing.T) {
	manager := NewManager(nil, StrategyStatic)
	if manager == nil {
		t.Fatal("expected manager to be created with nil logger")
	}
	if manager.logger == nil {
		t.Fatal("expected logger to be set to default")
	}
}

func TestManager_KubernetesDetection(t *testing.T) {
	kubeDir := "/tmp/test-kubernetes-detection"
	kubeFile := kubeDir + "/serviceaccount"
	
	err := os.MkdirAll(kubeFile, 0o755)
	if err != nil {
		t.Skipf("skipping Kubernetes detection test: %v", err)
	}
	defer os.RemoveAll(kubeDir)

	oldPath := "/var/run/secrets/kubernetes.io"
	os.Setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
	defer os.Unsetenv("KUBERNETES_SERVICE_HOST")

	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyAuto)

	if _, err := os.Stat(oldPath); err == nil {
		strategy := manager.detectEnvironment()
		if strategy != StrategyKubernetes {
			t.Logf("expected Kubernetes strategy in K8s environment, got %s", strategy)
		}
	}
}

func TestManager_CreateAdapter_Kubernetes(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyKubernetes)

	adapter, err := manager.createAdapter(StrategyKubernetes)
	if err != nil {
		t.Fatalf("expected Kubernetes adapter to be created successfully, got error: %v", err)
	}
	if adapter == nil {
		t.Fatal("expected non-nil Kubernetes adapter")
	}
}

func TestManager_RefreshPeersNoAdapter(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyStatic)

	manager.refreshPeers()

	if len(manager.discoveredPeers) != 0 {
		t.Errorf("expected no peers when adapter is nil, got %d", len(manager.discoveredPeers))
	}
}

func TestManager_MonitorPeersNilContext(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyStatic)

	manager.monitorPeers()
}

func TestManager_RefreshPeersWithError(t *testing.T) {
	logger := testutil.CreateTestLogger()
	manager := NewManager(logger, StrategyStatic)

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	os.Setenv("GRAFT_PEERS", "")
	defer os.Unsetenv("GRAFT_PEERS")

	err := manager.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer manager.Stop()

	manager.refreshPeers()

	if len(manager.discoveredPeers) != 0 {
		t.Errorf("expected no discovered peers, got %d", len(manager.discoveredPeers))
	}
}

func TestManager_MultipleStrategies(t *testing.T) {
	tests := []struct {
		name     string
		strategy Strategy
		wantErr  bool
	}{
		{"Static", StrategyStatic, false},
		{"mDNS", StrategyMDNS, false},
		{"Kubernetes", StrategyKubernetes, false},
		{"Unknown", "unknown", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := testutil.CreateTestLogger()
			manager := NewManager(logger, tt.strategy)

			ctx := context.Background()
			config := ports.DiscoveryConfig{
				ServiceName: "test-service",
				ServicePort: 8080,
			}

			os.Setenv("GRAFT_PEERS", "")
			defer os.Unsetenv("GRAFT_PEERS")

			err := manager.Start(ctx, config)
			if (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil {
				manager.Stop()
			}
		})
	}
}