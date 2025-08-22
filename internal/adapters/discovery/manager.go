package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery/kubernetes"
	"github.com/eleven-am/graft/internal/adapters/discovery/mdns"
	"github.com/eleven-am/graft/internal/adapters/discovery/static"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

const (
	EnvGraftPeers = "GRAFT_PEERS"
	
	EnvKubernetesServiceHost = "KUBERNETES_SERVICE_HOST"
	EnvKubernetesServicePort = "KUBERNETES_SERVICE_PORT"
	
	KubernetesServiceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount"
	KubernetesConfigPath         = "/etc/kubernetes"
)

type Strategy string

const (
	StrategyAuto       Strategy = "auto"
	StrategyStatic     Strategy = "static"
	StrategyMDNS       Strategy = "mdns"
	StrategyKubernetes Strategy = "kubernetes"
)

type Manager struct {
	mu              sync.RWMutex
	logger          *slog.Logger
	adapter         ports.DiscoveryPort
	strategy        Strategy
	config          *ports.DiscoveryConfig
	serviceInfo     *ports.ServiceInfo
	discoveredPeers map[string]ports.Peer
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewManager(logger *slog.Logger, strategy Strategy) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		logger:          logger.With("component", "discovery", "module", "manager"),
		strategy:        strategy,
		discoveredPeers: make(map[string]ports.Peer),
	}
}

func (m *Manager) Start(ctx context.Context, config ports.DiscoveryConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx != nil {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "discovery manager already started",
			Details: map[string]interface{}{
				"operation": "start",
			},
		}
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.config = &config

	strategy := m.strategy
	if strategy == StrategyAuto {
		strategy = m.detectEnvironment()
	}

	m.logger.Info("initializing discovery adapter",
		"strategy", strategy)

	adapter, err := m.createAdapter(strategy)
	if err != nil {
		m.cancel()
		return err
	}

	if err := adapter.Start(m.ctx, config); err != nil {
		m.cancel()
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to start discovery adapter: %v", err),
			Details: map[string]interface{}{
				"strategy": strategy,
			},
		}
	}

	m.adapter = adapter

	go m.monitorPeers()

	m.logger.Info("discovery manager started",
		"strategy", strategy,
		"service_name", config.ServiceName,
		"service_port", config.ServicePort)

	return nil
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cancel == nil {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "discovery manager not started",
			Details: map[string]interface{}{
				"operation": "stop",
			},
		}
	}

	m.logger.Info("stopping discovery manager")

	if m.adapter != nil {
		if err := m.adapter.Stop(); err != nil {
			m.logger.Error("failed to stop adapter",
				"error", err.Error())
		}
	}

	m.cancel()
	m.ctx = nil
	m.cancel = nil
	m.adapter = nil
	m.config = nil
	m.serviceInfo = nil
	m.discoveredPeers = make(map[string]ports.Peer)

	m.logger.Info("discovery manager stopped")

	return nil
}

func (m *Manager) Advertise(info ports.ServiceInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.adapter == nil {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "discovery manager not started",
			Details: map[string]interface{}{
				"operation": "advertise",
			},
		}
	}

	m.serviceInfo = &info

	if err := m.adapter.Advertise(info); err != nil {
		return err
	}

	m.logger.Info("service advertised through manager",
		"service_id", info.ID,
		"service_name", info.Name)

	return nil
}

func (m *Manager) Discover() ([]ports.Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.adapter == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "discovery manager not started",
			Details: map[string]interface{}{
				"operation": "discover",
			},
		}
	}

	peers := make([]ports.Peer, 0, len(m.discoveredPeers))
	for _, peer := range m.discoveredPeers {
		peers = append(peers, peer)
	}

	return peers, nil
}

func (m *Manager) detectEnvironment() Strategy {
	if m.isKubernetesEnvironment() {
		m.logger.Info("detected Kubernetes environment")
		return StrategyKubernetes
	}

	if os.Getenv(EnvGraftPeers) != "" {
		m.logger.Info("detected static peer configuration")
		return StrategyStatic
	}

	m.logger.Info("defaulting to mDNS discovery")
	return StrategyMDNS
}

func (m *Manager) isKubernetesEnvironment() bool {
	if os.Getenv(EnvKubernetesServiceHost) != "" && os.Getenv(EnvKubernetesServicePort) != "" {
		return true
	}

	if _, err := os.Stat(KubernetesServiceAccountPath); err == nil {
		return true
	}

	if _, err := os.Stat(KubernetesConfigPath); err == nil {
		return true
	}

	return false
}

func (m *Manager) createAdapter(strategy Strategy) (ports.DiscoveryPort, error) {
	switch strategy {
	case StrategyStatic:
		return static.NewStaticAdapter(m.logger), nil
	case StrategyMDNS:
		return mdns.NewMDNSAdapter(m.logger), nil
	case StrategyKubernetes:
		return kubernetes.NewKubernetesAdapter(m.logger), nil
	default:
		return nil, domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: fmt.Sprintf("unknown discovery strategy: %s", strategy),
			Details: map[string]interface{}{
				"strategy": strategy,
			},
		}
	}
}

func (m *Manager) monitorPeers() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		m.mu.RLock()
		ctx := m.ctx
		m.mu.RUnlock()
		
		if ctx == nil {
			return
		}
		
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.refreshPeers()
		}
	}
}

func (m *Manager) refreshPeers() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.adapter == nil {
		return
	}

	peers, err := m.adapter.Discover()
	if err != nil {
		m.logger.Error("failed to refresh peers",
			"error", err.Error())
		return
	}

	newPeers := make(map[string]ports.Peer)
	for _, peer := range peers {
		newPeers[peer.ID] = peer
		
		if _, exists := m.discoveredPeers[peer.ID]; !exists {
			m.logger.Info("new peer discovered",
				"peer_id", peer.ID,
				"address", peer.Address,
				"port", peer.Port)
		}
	}

	for id := range m.discoveredPeers {
		if _, exists := newPeers[id]; !exists {
			m.logger.Info("peer removed",
				"peer_id", id)
		}
	}

	m.discoveredPeers = newPeers
}