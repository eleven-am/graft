package kubernetes

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewKubernetesAdapter(t *testing.T) {
	logger := slog.Default()
	adapter := NewKubernetesAdapter(logger)

	if adapter == nil {
		t.Fatal("expected adapter to be created")
	}

	if adapter.peers == nil {
		t.Fatal("expected peers map to be initialized")
	}

	if adapter.logger == nil {
		t.Fatal("expected logger to be set")
	}
}

func TestNewKubernetesAdapter_NilLogger(t *testing.T) {
	adapter := NewKubernetesAdapter(nil)

	if adapter == nil {
		t.Fatal("expected adapter to be created with nil logger")
	}

	if adapter.logger == nil {
		t.Fatal("expected default logger to be set")
	}
}

func TestKubernetesAdapter_StartStop(t *testing.T) {
	logger := slog.Default()
	adapter := NewKubernetesAdapter(logger)

	clientset := fake.NewSimpleClientset()
	adapter.clientset = clientset

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
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

func TestKubernetesAdapter_Advertise(t *testing.T) {
	logger := slog.Default()
	adapter := NewKubernetesAdapter(logger)

	serviceInfo := ports.ServiceInfo{
		ID:      "node-1",
		Name:    "test-service",
		Address: "127.0.0.1",
		Port:    8080,
	}

	err := adapter.Advertise(serviceInfo)
	if err == nil {
		t.Fatal("expected error when advertising before start")
	}

	clientset := fake.NewSimpleClientset()
	adapter.clientset = clientset

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

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

func TestKubernetesAdapter_Discover(t *testing.T) {
	logger := slog.Default()
	adapter := NewKubernetesAdapter(logger)

	_, err := adapter.Discover()
	if err == nil {
		t.Fatal("expected error when discovering before start")
	}

	clientset := fake.NewSimpleClientset()
	adapter.clientset = clientset

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	err = adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	defer adapter.Stop()

	peers, err := adapter.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	if len(peers) == 0 {
		t.Logf("no peers discovered (expected in test environment)")
	}
}

func TestKubernetesAdapter_IsInCluster(t *testing.T) {
	logger := slog.Default()
	adapter := NewKubernetesAdapter(logger)

	inCluster := adapter.IsInCluster()
	if inCluster {
		t.Log("running in cluster environment")
	} else {
		t.Log("not running in cluster environment (expected in test)")
	}
}

func TestKubernetesAdapter_GetNamespace(t *testing.T) {
	logger := slog.Default()
	adapter := NewKubernetesAdapter(logger)

	namespace := adapter.GetNamespace()
	if namespace == "" {
		t.Fatal("expected non-empty namespace")
	}

	if namespace != "default" {
		t.Logf("using namespace: %s", namespace)
	}
}

func TestKubernetesAdapter_PerformDiscovery(t *testing.T) {
	logger := slog.Default()
	adapter := NewKubernetesAdapter(logger)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
			Labels: map[string]string{
				"app": "test-service",
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port: 8080,
				},
			},
		},
	}

	endpoints := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
		},
		Subsets: []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{
					{
						IP: "192.168.1.100",
					},
					{
						IP: "192.168.1.101",
					},
				},
				Ports: []corev1.EndpointPort{
					{
						Port: 8080,
					},
				},
			},
		},
	}

	objects := []runtime.Object{service, endpoints}
	clientset := fake.NewSimpleClientset(objects...)
	adapter.clientset = clientset

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	err := adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	defer adapter.Stop()

	time.Sleep(100 * time.Millisecond)

	if adapter.watcher != nil {
		adapter.watcher.processService(service, endpoints)
	}

	adapter.performDiscovery()

	peers, err := adapter.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	expectedPeers := 2
	if len(peers) != expectedPeers {
		t.Errorf("expected %d peers, got %d", expectedPeers, len(peers))
	}

	found1 := false
	found2 := false
	for _, peer := range peers {
		if peer.ID == "192.168.1.100:8080" {
			found1 = true
			if peer.Address != "192.168.1.100" {
				t.Errorf("expected address 192.168.1.100, got %s", peer.Address)
			}
			if peer.Port != 8080 {
				t.Errorf("expected port 8080, got %d", peer.Port)
			}
		}
		if peer.ID == "192.168.1.101:8080" {
			found2 = true
			if peer.Address != "192.168.1.101" {
				t.Errorf("expected address 192.168.1.101, got %s", peer.Address)
			}
		}
	}

	if !found1 || !found2 {
		t.Errorf("expected to find both peers, found1=%v, found2=%v", found1, found2)
	}
}

func TestKubernetesAdapter_DiscoveryLoop(t *testing.T) {
	logger := slog.Default()
	adapter := NewKubernetesAdapter(logger)

	clientset := fake.NewSimpleClientset()
	adapter.clientset = clientset

	ctx, cancel := context.WithCancel(context.Background())
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

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

func TestKubernetesAdapter_CreateKubernetesClient(t *testing.T) {
	logger := slog.Default()
	adapter := NewKubernetesAdapter(logger)

	_, err := adapter.createKubernetesClient()
	if err != nil {
		t.Logf("expected error when creating kubernetes client outside cluster: %v", err)
	}
}