package static

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Adapter struct {
	mu          sync.RWMutex
	logger      *slog.Logger
	peers       []ports.Peer
	config      *ports.DiscoveryConfig
	serviceInfo *ports.ServiceInfo
	ctx         context.Context
	cancel      context.CancelFunc
	health      *HealthChecker
}

func NewStaticAdapter(logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}

	return &Adapter{
		logger: logger.With("component", "discovery", "adapter", "static"),
		peers:  make([]ports.Peer, 0),
		health: NewHealthChecker(logger),
	}
}

func (s *Adapter) Start(ctx context.Context, config ports.DiscoveryConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ctx != nil {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "static discovery already started",
			Details: map[string]interface{}{
				"operation": "start",
			},
		}
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.config = &config

	s.logger.Info("starting static discovery adapter",
		"service_name", config.ServiceName,
		"service_port", config.ServicePort)

	parsedConfig, err := LoadConfig()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to load static configuration: %v", err),
			Details: map[string]interface{}{
				"operation": "load_config",
			},
		}
	}

	s.peers = make([]ports.Peer, 0, len(parsedConfig.Peers))
	for _, peerConfig := range parsedConfig.Peers {
		peer := ports.Peer{
			ID:       peerConfig.ID,
			Address:  peerConfig.Address,
			Port:     peerConfig.Port,
			Metadata: peerConfig.Metadata,
		}
		s.peers = append(s.peers, peer)

		s.logger.Info("added static peer",
			"peer_id", peer.ID,
			"address", peer.Address,
			"port", peer.Port)
	}

	go s.health.Start(s.ctx, s.peers)

	s.logger.Info("static discovery adapter started",
		"peer_count", len(s.peers))

	return nil
}

func (s *Adapter) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel == nil {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "static discovery not started",
			Details: map[string]interface{}{
				"operation": "stop",
			},
		}
	}

	s.logger.Info("stopping static discovery adapter")

	s.cancel()
	s.ctx = nil
	s.cancel = nil
	s.config = nil
	s.serviceInfo = nil
	s.peers = nil

	s.logger.Info("static discovery adapter stopped")

	return nil
}

func (s *Adapter) Advertise(info ports.ServiceInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ctx == nil {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "static discovery not started",
			Details: map[string]interface{}{
				"operation": "advertise",
			},
		}
	}

	s.serviceInfo = &info

	s.logger.Info("service advertised",
		"service_id", info.ID,
		"service_name", info.Name,
		"address", info.Address,
		"port", info.Port)

	return nil
}

func (s *Adapter) Discover() ([]ports.Peer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.ctx == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "static discovery not started",
			Details: map[string]interface{}{
				"operation": "discover",
			},
		}
	}

	healthyPeers := s.health.GetHealthyPeers()

	s.logger.Debug("discovered peers",
		"total_peers", len(s.peers),
		"healthy_peers", len(healthyPeers))

	return healthyPeers, nil
}
