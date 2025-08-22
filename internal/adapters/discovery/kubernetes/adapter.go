package kubernetes

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type KubernetesAdapter struct {
	clientset   kubernetes.Interface
	watcher     *ServiceWatcher
	logger      *slog.Logger
	config      ports.DiscoveryConfig
	peers       map[string]*ports.Peer
	mu          sync.RWMutex
	started     bool
	cancel      context.CancelFunc
}

func NewKubernetesAdapter(logger *slog.Logger) *KubernetesAdapter {
	if logger == nil {
		logger = slog.Default()
	}

	return &KubernetesAdapter{
		logger: logger.With("component", "discovery", "type", "kubernetes"),
		peers:  make(map[string]*ports.Peer),
	}
}

func (k *KubernetesAdapter) Start(ctx context.Context, config ports.DiscoveryConfig) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.started {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "kubernetes adapter already started",
			Details: map[string]interface{}{
				"service_name": config.ServiceName,
			},
		}
	}

	k.config = config
	k.logger.Info("starting kubernetes discovery adapter", "serviceName", config.ServiceName)

	clientset, err := k.createKubernetesClient()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create kubernetes client",
			Details: map[string]interface{}{
				"service_name": config.ServiceName,
				"error":        err.Error(),
			},
		}
	}
	k.clientset = clientset

	watchCtx, cancel := context.WithCancel(ctx)
	k.cancel = cancel

	k.watcher = NewServiceWatcher(k.clientset, k.logger)
	if err := k.watcher.Start(watchCtx, config); err != nil {
		cancel()
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to start service watcher",
			Details: map[string]interface{}{
				"service_name": config.ServiceName,
				"error":        err.Error(),
			},
		}
	}

	k.started = true
	k.logger.Info("kubernetes discovery adapter started successfully")

	go k.discoveryLoop(watchCtx)

	return nil
}

func (k *KubernetesAdapter) Stop() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if !k.started {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "kubernetes adapter not started",
			Details: map[string]interface{}{},
		}
	}

	k.logger.Info("stopping kubernetes discovery adapter")

	if k.cancel != nil {
		k.cancel()
	}

	if k.watcher != nil {
		k.watcher.Stop()
	}

	k.started = false
	k.peers = make(map[string]*ports.Peer)

	k.logger.Info("kubernetes discovery adapter stopped")
	return nil
}

func (k *KubernetesAdapter) Advertise(serviceInfo ports.ServiceInfo) error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if !k.started {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "kubernetes adapter not started",
			Details: map[string]interface{}{
				"service_id": serviceInfo.ID,
			},
		}
	}

	k.logger.Info("advertising service in kubernetes", 
		"serviceId", serviceInfo.ID,
		"serviceName", serviceInfo.Name,
		"address", serviceInfo.Address,
		"port", serviceInfo.Port)

	return nil
}

func (k *KubernetesAdapter) Discover() ([]ports.Peer, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if !k.started {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "kubernetes adapter not started",
			Details: map[string]interface{}{},
		}
	}

	peers := make([]ports.Peer, 0, len(k.peers))
	for _, peer := range k.peers {
		peers = append(peers, *peer)
	}

	k.logger.Debug("discovered peers", "count", len(peers))
	return peers, nil
}

func (k *KubernetesAdapter) createKubernetesClient() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		k.logger.Debug("not running in cluster, trying kubeconfig")
		
		config, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to build kubeconfig",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create kubernetes clientset",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return clientset, nil
}

func (k *KubernetesAdapter) discoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	backoff := time.Second
	maxBackoff := 60 * time.Second
	failureCount := 0

	for {
		select {
		case <-ctx.Done():
			k.logger.Debug("discovery loop stopped")
			return
		case <-ticker.C:
			err := k.performDiscoveryWithBackoff()
			if err != nil {
				failureCount++
				backoff = time.Duration(float64(backoff) * 1.5)
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				k.logger.Warn("kubernetes discovery failed, backing off",
					"error", err,
					"backoff", backoff,
					"failures", failureCount)
				time.Sleep(backoff)
			} else {
				if failureCount > 0 {
					k.logger.Info("kubernetes discovery recovered", "previousFailures", failureCount)
				}
				failureCount = 0
				backoff = time.Second
			}
		}
	}
}

func (k *KubernetesAdapter) performDiscovery() {
	if k.watcher == nil {
		return
	}

	services := k.watcher.GetServices()
	
	k.mu.Lock()
	defer k.mu.Unlock()

	newPeers := make(map[string]*ports.Peer)

	for _, service := range services {
		if service.Name == k.config.ServiceName {
			for _, endpoint := range service.Endpoints {
				peerID := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
				
				peer := &ports.Peer{
					ID:       peerID,
					Address:  endpoint.IP,
					Port:     int(endpoint.Port),
					Metadata: service.Labels,
				}

				newPeers[peerID] = peer
				k.logger.Debug("discovered kubernetes service endpoint",
					"peerID", peerID,
					"address", endpoint.IP,
					"port", endpoint.Port)
			}
		}
	}

	k.peers = newPeers
	k.logger.Debug("kubernetes discovery completed", "peers", len(newPeers))
}

func (k *KubernetesAdapter) IsInCluster() bool {
	_, err := rest.InClusterConfig()
	return err == nil
}

func (k *KubernetesAdapter) GetNamespace() string {
	return "default"
}

func (k *KubernetesAdapter) performDiscoveryWithBackoff() error {
	if k.watcher == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "kubernetes watcher not available",
			Details: map[string]interface{}{},
		}
	}

	services := k.watcher.GetServices()
	
	k.mu.Lock()
	defer k.mu.Unlock()

	newPeers := make(map[string]*ports.Peer)

	for _, service := range services {
		if service.Name == k.config.ServiceName {
			for _, endpoint := range service.Endpoints {
				peerID := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
				
				peer := &ports.Peer{
					ID:       peerID,
					Address:  endpoint.IP,
					Port:     int(endpoint.Port),
					Metadata: service.Labels,
				}

				newPeers[peerID] = peer
				k.logger.Debug("discovered kubernetes service endpoint",
					"peerID", peerID,
					"address", endpoint.IP,
					"port", endpoint.Port)
			}
		}
	}

	k.peers = newPeers
	k.logger.Debug("kubernetes discovery completed", "peers", len(newPeers))
	return nil
}