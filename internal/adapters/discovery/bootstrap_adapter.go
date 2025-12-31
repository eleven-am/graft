package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/eleven-am/graft/internal/bootstrap"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
)

type BootstrapDiscoveryAdapter struct {
	manager         *Manager
	serviceName     string
	headlessService string
	basePort        int
	replicas        int
	localOrdinal    int
	logger          *slog.Logger

	mu      sync.RWMutex
	started bool
	stopCh  chan struct{}
}

type BootstrapDiscoveryConfig struct {
	Manager         *Manager
	ServiceName     string
	HeadlessService string
	BasePort        int
	Replicas        int
	LocalOrdinal    int
	Logger          *slog.Logger
}

func NewBootstrapDiscoveryAdapter(cfg BootstrapDiscoveryConfig) (*BootstrapDiscoveryAdapter, error) {
	if cfg.Manager == nil {
		return nil, fmt.Errorf("discovery manager is required")
	}
	if cfg.ServiceName == "" {
		return nil, fmt.Errorf("service name is required")
	}
	if cfg.BasePort <= 0 {
		return nil, fmt.Errorf("base port must be positive")
	}
	if cfg.Replicas <= 0 {
		return nil, fmt.Errorf("replicas must be positive")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	headlessService := cfg.HeadlessService

	return &BootstrapDiscoveryAdapter{
		manager:         cfg.Manager,
		serviceName:     cfg.ServiceName,
		headlessService: headlessService,
		basePort:        cfg.BasePort,
		replicas:        cfg.Replicas,
		localOrdinal:    cfg.LocalOrdinal,
		logger:          logger.With("component", "bootstrap_discovery_adapter"),
	}, nil
}

func (a *BootstrapDiscoveryAdapter) AddressForOrdinal(ordinal int) raft.ServerAddress {
	if ordinal < 0 || ordinal >= a.replicas {
		return ""
	}

	peers := a.manager.GetPeers()
	for _, peer := range peers {
		if ordStr, ok := peer.Metadata["ordinal"]; ok {
			if peerOrdinal, err := strconv.Atoi(ordStr); err == nil && peerOrdinal == ordinal {
				return raft.ServerAddress(fmt.Sprintf("%s:%d", peer.Address, a.basePort))
			}
		}
	}

	if a.headlessService != "" {
		hostname := fmt.Sprintf("%s-%d.%s", a.serviceName, ordinal, a.headlessService)
		return raft.ServerAddress(fmt.Sprintf("%s:%d", hostname, a.basePort))
	}

	return ""
}

func (a *BootstrapDiscoveryAdapter) GetHealthyPeers(ctx context.Context) []bootstrap.PeerInfo {
	peers := a.manager.GetPeers()

	var result []bootstrap.PeerInfo
	for _, peer := range peers {
		peerInfo := a.convertPeer(peer)
		if peerInfo.ServerID == "" {
			continue
		}

		if peerInfo.Ordinal == a.localOrdinal {
			continue
		}

		result = append(result, peerInfo)
	}

	return result
}

func (a *BootstrapDiscoveryAdapter) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		return nil
	}

	a.stopCh = make(chan struct{})
	a.started = true

	a.logger.Debug("bootstrap discovery adapter started",
		"service_name", a.serviceName,
		"headless_service", a.headlessService,
		"base_port", a.basePort,
		"replicas", a.replicas,
		"local_ordinal", a.localOrdinal)

	return nil
}

func (a *BootstrapDiscoveryAdapter) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.started {
		return
	}

	close(a.stopCh)
	a.started = false

	a.logger.Debug("bootstrap discovery adapter stopped")
}

func (a *BootstrapDiscoveryAdapter) convertPeer(peer ports.Peer) bootstrap.PeerInfo {
	ordinal := a.extractOrdinal(peer)
	if ordinal < 0 {
		return bootstrap.PeerInfo{}
	}

	serverID := a.serverIDForOrdinal(ordinal)

	port := a.basePort
	address := raft.ServerAddress(fmt.Sprintf("%s:%d", peer.Address, port))

	return bootstrap.PeerInfo{
		ServerID: serverID,
		Address:  address,
		Ordinal:  ordinal,
	}
}

func (a *BootstrapDiscoveryAdapter) extractOrdinal(peer ports.Peer) int {
	if ordStr, ok := peer.Metadata["ordinal"]; ok {
		if ordinal, err := strconv.Atoi(ordStr); err == nil {
			return ordinal
		}
	}

	return -1
}

func (a *BootstrapDiscoveryAdapter) serverIDForOrdinal(ordinal int) raft.ServerID {
	return raft.ServerID(fmt.Sprintf("%s-%d", a.serviceName, ordinal))
}

var _ bootstrap.LifecyclePeerDiscovery = (*BootstrapDiscoveryAdapter)(nil)
