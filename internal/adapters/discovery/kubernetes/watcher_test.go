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

func TestNewServiceWatcher(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	if watcher == nil {
		t.Fatal("expected watcher to be created")
	}

	if watcher.services == nil {
		t.Fatal("expected services map to be initialized")
	}

	if watcher.logger == nil {
		t.Fatal("expected logger to be set")
	}
}

func TestNewServiceWatcher_NilLogger(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	watcher := NewServiceWatcher(clientset, nil)

	if watcher == nil {
		t.Fatal("expected watcher to be created with nil logger")
	}

	if watcher.logger == nil {
		t.Fatal("expected default logger to be set")
	}
}

func TestServiceWatcher_StartStop(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	err := watcher.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	err = watcher.Start(ctx, config)
	if err == nil {
		t.Fatal("expected error when starting already started watcher")
	}

	watcher.Stop()
	watcher.Stop()
}

func TestServiceWatcher_GetServices(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	services := watcher.GetServices()
	if len(services) != 0 {
		t.Errorf("expected 0 services initially, got %d", len(services))
	}
}

func TestServiceWatcher_InitialDiscovery(t *testing.T) {
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
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
	}

	err := watcher.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}
	defer watcher.Stop()

	time.Sleep(50 * time.Millisecond)

	services := watcher.GetServices()
	if len(services) == 0 {
		t.Error("expected at least one service after initial discovery")
	}

	for _, svc := range services {
		if svc.Name == "test-service" {
			if len(svc.Endpoints) != 1 {
				t.Errorf("expected 1 endpoint, got %d", len(svc.Endpoints))
			}

			if svc.Endpoints[0].IP != "192.168.1.100" {
				t.Errorf("expected IP 192.168.1.100, got %s", svc.Endpoints[0].IP)
			}

			if svc.Endpoints[0].Port != 8080 {
				t.Errorf("expected port 8080, got %d", svc.Endpoints[0].Port)
			}
		}
	}
}

func TestServiceWatcher_ProcessService(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
			Labels: map[string]string{
				"app": "test-service",
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

	watcher.processService(service, endpoints)

	services := watcher.GetServices()
	if len(services) != 1 {
		t.Errorf("expected 1 service, got %d", len(services))
	}

	svc := services[0]
	if svc.Name != "test-service" {
		t.Errorf("expected service name 'test-service', got %s", svc.Name)
	}

	if len(svc.Endpoints) != 2 {
		t.Errorf("expected 2 endpoints, got %d", len(svc.Endpoints))
	}
}

func TestServiceWatcher_HandleServiceEvent(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
			Labels: map[string]string{
				"app":     "test-service",
				"version": "1.0.0",
			},
		},
	}

	watcher.handleServiceEvent(service)

	services := watcher.GetServices()
	if len(services) != 1 {
		t.Errorf("expected 1 service, got %d", len(services))
	}

	svc := services[0]
	if svc.Name != "test-service" {
		t.Errorf("expected service name 'test-service', got %s", svc.Name)
	}

	if svc.Labels["version"] != "1.0.0" {
		t.Errorf("expected version label '1.0.0', got %s", svc.Labels["version"])
	}
}

func TestServiceWatcher_HandleServiceDeletion(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
		},
	}

	watcher.handleServiceEvent(service)

	services := watcher.GetServices()
	if len(services) != 1 {
		t.Errorf("expected 1 service after creation, got %d", len(services))
	}

	watcher.handleServiceDeletion(service)

	services = watcher.GetServices()
	if len(services) != 0 {
		t.Errorf("expected 0 services after deletion, got %d", len(services))
	}
}

func TestServiceWatcher_HandleEndpointsEvent(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

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
				},
				Ports: []corev1.EndpointPort{
					{
						Port: 8080,
					},
				},
			},
		},
	}

	watcher.handleEndpointsEvent(endpoints)

	services := watcher.GetServices()
	if len(services) != 1 {
		t.Errorf("expected 1 service, got %d", len(services))
	}

	svc := services[0]
	if len(svc.Endpoints) != 1 {
		t.Errorf("expected 1 endpoint, got %d", len(svc.Endpoints))
	}

	if svc.Endpoints[0].IP != "192.168.1.100" {
		t.Errorf("expected IP 192.168.1.100, got %s", svc.Endpoints[0].IP)
	}
}

func TestServiceWatcher_HandleEndpointsDeletion(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
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
				},
				Ports: []corev1.EndpointPort{
					{
						Port: 8080,
					},
				},
			},
		},
	}

	watcher.handleServiceEvent(service)
	watcher.handleEndpointsEvent(endpoints)

	services := watcher.GetServices()
	svc := services[0]
	if len(svc.Endpoints) != 1 {
		t.Errorf("expected 1 endpoint before deletion, got %d", len(svc.Endpoints))
	}

	watcher.handleEndpointsDeletion(endpoints)

	services = watcher.GetServices()
	svc = services[0]
	if len(svc.Endpoints) != 0 {
		t.Errorf("expected 0 endpoints after deletion, got %d", len(svc.Endpoints))
	}
}

func TestServiceWatcher_GetNamespace(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	namespace := watcher.getNamespace("")
	if namespace == "" {
		t.Fatal("expected non-empty namespace")
	}

	if namespace != "default" {
		t.Logf("using namespace: %s", namespace)
	}
}

func TestServiceWatcher_GetCurrentNamespace(t *testing.T) {
	defaultNamespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
		},
	}

	objects := []runtime.Object{defaultNamespace}
	clientset := fake.NewSimpleClientset(objects...)
	logger := slog.Default()
	watcher := NewServiceWatcher(clientset, logger)

	namespace, err := watcher.getCurrentNamespace()
	if err != nil {
		t.Fatalf("failed to get current namespace: %v", err)
	}

	if namespace != "default" {
		t.Errorf("expected namespace 'default', got %s", namespace)
	}
}