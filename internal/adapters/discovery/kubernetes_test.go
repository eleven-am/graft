package discovery

import (
	"context"
	"os"
	"testing"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	v1 "k8s.io/api/core/v1"
)

func TestKubernetesAdapter_BasicLifecycle(t *testing.T) {
	config := &ports.KubernetesConfig{
		AuthMethod: ports.AuthInCluster,
		Discovery: ports.DiscoveryStrategy{
			Method:        ports.DiscoveryLabelSelector,
			LabelSelector: map[string]string{"app": "test"},
		},
		PeerID: ports.PeerIDStrategy{
			Source: ports.PeerIDPodName,
		},
		Port: ports.PortStrategy{
			Source:      ports.PortNamedPort,
			PortName:    "http",
			DefaultPort: 8080,
		},
	}
	adapter := NewKubernetesAdapter(config, nil)

	peers := adapter.GetPeers()
	if len(peers) != 0 {
		t.Errorf("Expected 0 peers initially, got %d", len(peers))
	}

	nodeInfo := ports.NodeInfo{
		ID:      "test-pod",
		Address: "10.0.0.1",
		Port:    8080,
	}

	err := adapter.Announce(nodeInfo)
	if err != nil {
		t.Errorf("Announce() failed: %v", err)
	}

	err = adapter.Stop()
	if err == nil {
		t.Error("Expected Stop() to fail when adapter not started")
	} else {
		if !domain.IsDiscoveryError(err) {
			t.Errorf("Expected discovery error, got: %v", err)
		}
		if !domain.IsNotStarted(err) {
			t.Errorf("Expected not started error, got: %v", err)
		}
	}
}

func TestKubernetesAdapter_StartFailsOutsideCluster(t *testing.T) {
	config := &ports.KubernetesConfig{
		AuthMethod: ports.AuthInCluster,
		Discovery: ports.DiscoveryStrategy{
			Method:        ports.DiscoveryLabelSelector,
			LabelSelector: map[string]string{"app": "test"},
		},
	}
	adapter := NewKubernetesAdapter(config, nil)
	ctx := context.Background()

	err := adapter.Start(ctx)
	if err == nil {
		adapter.Stop()
		t.Skip("Skipping test - running inside K8s cluster")
		return
	}

	if err.Error() == "" {
		t.Error("Expected non-empty error message when starting outside cluster")
	}
}

func TestKubernetesAdapter_CannotStartTwice(t *testing.T) {
	if !isInKubernetes() {
		t.Skip("Skipping K8s test - not running in cluster")
	}

	config := &ports.KubernetesConfig{
		AuthMethod: ports.AuthInCluster,
		Discovery: ports.DiscoveryStrategy{
			Method:        ports.DiscoveryLabelSelector,
			LabelSelector: map[string]string{"app": "test"},
		},
	}
	adapter := NewKubernetesAdapter(config, nil)
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

func TestKubernetesAdapter_NamespaceDetection(t *testing.T) {
	config := &ports.KubernetesConfig{}
	adapter := NewKubernetesAdapter(config, nil)

	namespace := adapter.detectNamespace()

	if namespace == "" {
		t.Error("Expected non-empty namespace")
	}

	if namespace != "default" {
		t.Logf("Detected namespace: %s", namespace)
	}
}

func TestKubernetesAdapter_PortExtraction(t *testing.T) {
	config := &ports.KubernetesConfig{
		Port: ports.PortStrategy{
			Source:      ports.PortNamedPort,
			PortName:    "http",
			DefaultPort: 9000,
		},
	}
	adapter := NewKubernetesAdapter(config, nil)

	emptyPod := &v1.Pod{}
	port := adapter.extractPort(emptyPod)
	if port != 9000 {
		t.Errorf("Expected configured default port 9000, got %d", port)
	}
}

func TestKubernetesAdapter_PortExtractionFallback(t *testing.T) {
	config := &ports.KubernetesConfig{
		Port: ports.PortStrategy{
			Source:   ports.PortNamedPort,
			PortName: "nonexistent",
		},
	}
	adapter := NewKubernetesAdapter(config, nil)

	emptyPod := &v1.Pod{}
	port := adapter.extractPort(emptyPod)
	if port != 8080 {
		t.Errorf("Expected final fallback port 8080, got %d", port)
	}
}

func TestKubernetesAdapter_LabelSelector(t *testing.T) {
	config := &ports.KubernetesConfig{
		Discovery: ports.DiscoveryStrategy{
			Method: ports.DiscoveryLabelSelector,
			LabelSelector: map[string]string{
				"app":  "graft",
				"tier": "backend",
			},
		},
	}
	adapter := NewKubernetesAdapter(config, nil)

	selector := adapter.buildLabelSelector()
	expected := "app=graft,tier=backend"
	expected2 := "tier=backend,app=graft"

	if selector != expected && selector != expected2 {
		t.Errorf("Expected selector %s or %s, got %s", expected, expected2, selector)
	}
}

func TestKubernetesAdapter_PeerIDExtraction(t *testing.T) {
	config1 := &ports.KubernetesConfig{
		PeerID: ports.PeerIDStrategy{
			Source: ports.PeerIDPodName,
		},
	}
	adapter1 := NewKubernetesAdapter(config1, nil)

	pod := &v1.Pod{}
	pod.Name = "test-pod-123"

	peerID := adapter1.extractPeerID(pod)
	if peerID != "test-pod-123" {
		t.Errorf("Expected peer ID 'test-pod-123', got '%s'", peerID)
	}

	config2 := &ports.KubernetesConfig{
		PeerID: ports.PeerIDStrategy{
			Source: ports.PeerIDAnnotation,
			Key:    "graft.io/peer-id",
		},
	}
	adapter2 := NewKubernetesAdapter(config2, nil)

	pod.Annotations = map[string]string{
		"graft.io/peer-id": "custom-peer-id",
	}

	peerID2 := adapter2.extractPeerID(pod)
	if peerID2 != "custom-peer-id" {
		t.Errorf("Expected peer ID 'custom-peer-id', got '%s'", peerID2)
	}
}

func TestKubernetesAdapter_ConfigurationValidation(t *testing.T) {
	adapter := NewKubernetesAdapter(nil, nil)
	if adapter == nil {
		t.Error("Expected adapter to be created even with nil config")
	}

	emptyConfig := &ports.KubernetesConfig{}
	adapter2 := NewKubernetesAdapter(emptyConfig, nil)
	if adapter2 == nil {
		t.Error("Expected adapter to be created with empty config")
	}
}

func isInKubernetes() bool {
	_, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token")
	return err == nil
}
