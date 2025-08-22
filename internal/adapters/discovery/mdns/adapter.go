package mdns

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Adapter struct {
	mu          sync.RWMutex
	logger      *slog.Logger
	ctx         context.Context
	cancel      context.CancelFunc
	config      *ports.DiscoveryConfig
	serviceInfo *ports.ServiceInfo
	peers       map[string]*ports.Peer
	broadcaster *Broadcaster
	resolver    Resolver
}

func NewMDNSAdapter(logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}

	return &Adapter{
		logger: logger.With("component", "discovery", "adapter", "mdns"),
		peers:  make(map[string]*ports.Peer),
	}
}

func (m *Adapter) Start(ctx context.Context, config ports.DiscoveryConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx != nil {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "mDNS discovery already started",
			Details: map[string]interface{}{
				"operation": "start",
			},
		}
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.config = &config

	m.logger.Info("starting mDNS discovery adapter",
		"service_name", config.ServiceName,
		"service_port", config.ServicePort)

	m.broadcaster = NewBroadcaster(m.logger, config.ServiceName, config.ServicePort)
	m.resolver = NewResolver(m.logger, config.ServiceName)

	if err := m.broadcaster.Start(); err != nil {
		m.cancel()
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to start mDNS broadcaster: %v", err),
			Details: map[string]interface{}{
				"operation": "start_broadcaster",
			},
		}
	}

	go m.discoveryLoop()

	m.logger.Info("mDNS discovery adapter started")

	return nil
}

func (m *Adapter) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cancel == nil {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "mDNS discovery not started",
			Details: map[string]interface{}{
				"operation": "stop",
			},
		}
	}

	m.logger.Info("stopping mDNS discovery adapter")

	if m.broadcaster != nil {
		m.broadcaster.Stop()
	}

	m.cancel()
	m.ctx = nil
	m.cancel = nil
	m.config = nil
	m.serviceInfo = nil
	m.peers = make(map[string]*ports.Peer)
	m.broadcaster = nil
	m.resolver = nil

	m.logger.Info("mDNS discovery adapter stopped")

	return nil
}

func (m *Adapter) Advertise(info ports.ServiceInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx == nil {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "mDNS discovery not started",
			Details: map[string]interface{}{
				"operation": "advertise",
			},
		}
	}

	m.serviceInfo = &info

	if m.broadcaster != nil {
		if err := m.broadcaster.UpdateService(info); err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: fmt.Sprintf("failed to update mDNS service: %v", err),
				Details: map[string]interface{}{
					"operation": "update_service",
				},
			}
		}
	}

	m.logger.Info("service advertised",
		"service_id", info.ID,
		"service_name", info.Name,
		"address", info.Address,
		"port", info.Port)

	return nil
}

func (m *Adapter) Discover() ([]ports.Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.ctx == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "mDNS discovery not started",
			Details: map[string]interface{}{
				"operation": "discover",
			},
		}
	}

	peers := make([]ports.Peer, 0, len(m.peers))
	for _, peer := range m.peers {
		peers = append(peers, *peer)
	}

	m.logger.Debug("discovered peers",
		"peer_count", len(peers))

	return peers, nil
}

func (m *Adapter) discoveryLoop() {
	m.mu.RLock()
	ctx := m.ctx
	m.mu.RUnlock()
	
	if ctx == nil {
		return
	}
	
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	backoff := time.Second
	maxBackoff := 60 * time.Second
	failureCount := 0

	m.performDiscoveryWithBackoff(&backoff, &failureCount, maxBackoff)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.performDiscoveryWithBackoff(&backoff, &failureCount, maxBackoff)
		}
	}
}

func (m *Adapter) performDiscovery() {
	m.mu.RLock()
	resolver := m.resolver
	m.mu.RUnlock()
	
	if resolver == nil {
		return
	}

	entries, err := resolver.Discover(5 * time.Second)
	if err != nil {
		m.logger.Error("mDNS discovery failed",
			"error", err.Error())
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	newPeers := make(map[string]bool)

	for _, entry := range entries {
		peerID := entry.InfoFields[0]
		if peerID == "" {
			peerID = fmt.Sprintf("%s:%d", entry.AddrV4.String(), entry.Port)
		}

		newPeers[peerID] = true

		if _, exists := m.peers[peerID]; !exists {
			peer := &ports.Peer{
				ID:      peerID,
				Address: entry.AddrV4.String(),
				Port:    entry.Port,
				Metadata: map[string]string{
					"host": entry.Host,
					"name": entry.Name,
				},
			}

			m.peers[peerID] = peer

			m.logger.Info("new peer discovered",
				"peer_id", peerID,
				"address", entry.AddrV4.String(),
				"port", entry.Port)
		}
	}

	for peerID := range m.peers {
		if !newPeers[peerID] {
			delete(m.peers, peerID)
			m.logger.Info("peer removed",
				"peer_id", peerID)
		}
	}
}

func (m *Adapter) performDiscoveryWithBackoff(backoff *time.Duration, failureCount *int, maxBackoff time.Duration) {
	m.mu.RLock()
	resolver := m.resolver
	m.mu.RUnlock()
	
	if resolver == nil {
		return
	}

	entries, err := resolver.Discover(5 * time.Second)
	if err != nil {
		*failureCount++
		*backoff = time.Duration(float64(*backoff) * 1.5)
		if *backoff > maxBackoff {
			*backoff = maxBackoff
		}
		m.logger.Warn("mDNS discovery failed, backing off",
			"error", err.Error(),
			"backoff", *backoff,
			"failures", *failureCount)
		time.Sleep(*backoff)
		return
	}

	if *failureCount > 0 {
		m.logger.Info("mDNS discovery recovered", "previousFailures", *failureCount)
	}
	*failureCount = 0
	*backoff = time.Second

	m.mu.Lock()
	defer m.mu.Unlock()

	newPeers := make(map[string]bool)

	for _, entry := range entries {
		peerID := entry.InfoFields[0]
		if peerID == "" {
			peerID = fmt.Sprintf("%s:%d", entry.AddrV4.String(), entry.Port)
		}

		newPeers[peerID] = true

		if _, exists := m.peers[peerID]; !exists {
			peer := &ports.Peer{
				ID:      peerID,
				Address: entry.AddrV4.String(),
				Port:    entry.Port,
				Metadata: map[string]string{
					"host": entry.Host,
					"name": entry.Name,
				},
			}

			m.peers[peerID] = peer

			m.logger.Info("new peer discovered",
				"peer_id", peerID,
				"address", entry.AddrV4.String(),
				"port", entry.Port)
		}
	}

	for peerID := range m.peers {
		if !newPeers[peerID] {
			delete(m.peers, peerID)
			m.logger.Info("peer removed",
				"peer_id", peerID)
		}
	}
}
