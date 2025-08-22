package static

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type PeerHealth struct {
	Peer      ports.Peer
	Healthy   bool
	LastCheck time.Time
	LastError error
}

type HealthChecker struct {
	mu            sync.RWMutex
	logger        *slog.Logger
	peerHealth    map[string]*PeerHealth
	checkInterval time.Duration
	timeout       time.Duration
}

func NewHealthChecker(logger *slog.Logger) *HealthChecker {
	if logger == nil {
		logger = slog.Default()
	}

	return &HealthChecker{
		logger:        logger.With("component", "discovery", "module", "health"),
		peerHealth:    make(map[string]*PeerHealth),
		checkInterval: 30 * time.Second,
		timeout:       5 * time.Second,
	}
}

func (h *HealthChecker) Start(ctx context.Context, peers []ports.Peer) {
	h.mu.Lock()
	for _, peer := range peers {
		h.peerHealth[peer.ID] = &PeerHealth{
			Peer:      peer,
			Healthy:   false,
			LastCheck: time.Time{},
		}
	}
	h.mu.Unlock()

	backoff := time.Second
	maxBackoff := 60 * time.Second
	failureCount := 0

	h.performHealthCheckWithBackoff(&backoff, &failureCount, maxBackoff)

	ticker := time.NewTicker(h.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("health checker stopped")
			return
		case <-ticker.C:
			h.performHealthCheckWithBackoff(&backoff, &failureCount, maxBackoff)
		}
	}
}



func (h *HealthChecker) GetHealthyPeers() []ports.Peer {
	h.mu.RLock()
	defer h.mu.RUnlock()

	healthyPeers := make([]ports.Peer, 0)
	for _, ph := range h.peerHealth {
		if ph.Healthy {
			healthyPeers = append(healthyPeers, ph.Peer)
		}
	}

	return healthyPeers
}

func (h *HealthChecker) UpdatePeers(peers []ports.Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()

	newHealth := make(map[string]*PeerHealth)
	
	for _, peer := range peers {
		if existing, ok := h.peerHealth[peer.ID]; ok {
			newHealth[peer.ID] = existing
			existing.Peer = peer
		} else {
			newHealth[peer.ID] = &PeerHealth{
				Peer:      peer,
				Healthy:   false,
				LastCheck: time.Time{},
			}
		}
	}

	for id, ph := range h.peerHealth {
		if _, exists := newHealth[id]; !exists {
			h.logger.Info("peer removed from health check",
				"peer_id", id,
				"address", fmt.Sprintf("%s:%d", ph.Peer.Address, ph.Peer.Port))
		}
	}

	h.peerHealth = newHealth
}

func (h *HealthChecker) GetPeerHealth(peerID string) (*PeerHealth, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	ph, ok := h.peerHealth[peerID]
	if !ok {
		return nil, false
	}

	return &PeerHealth{
		Peer:      ph.Peer,
		Healthy:   ph.Healthy,
		LastCheck: ph.LastCheck,
		LastError: ph.LastError,
	}, true
}

func (h *HealthChecker) performHealthCheckWithBackoff(backoff *time.Duration, failureCount *int, maxBackoff time.Duration) {
	h.mu.RLock()
	peers := make([]*PeerHealth, 0, len(h.peerHealth))
	for _, ph := range h.peerHealth {
		peers = append(peers, ph)
	}
	h.mu.RUnlock()

	var wg sync.WaitGroup
	var errorCount int32
	for _, peerHealth := range peers {
		wg.Add(1)
		go func(ph *PeerHealth) {
			defer wg.Done()
			err := h.checkPeerWithError(ph)
			if err != nil {
				atomic.AddInt32(&errorCount, 1)
			}
		}(peerHealth)
	}
	wg.Wait()

	if errorCount > 0 {
		*failureCount++
		*backoff = time.Duration(float64(*backoff) * 1.5)
		if *backoff > maxBackoff {
			*backoff = maxBackoff
		}
		h.logger.Warn("health checks failed, backing off",
			"errorCount", errorCount,
			"backoff", *backoff,
			"failures", *failureCount)
		time.Sleep(*backoff)
	} else {
		if *failureCount > 0 {
			h.logger.Info("health checks recovered", "previousFailures", *failureCount)
		}
		*failureCount = 0
		*backoff = time.Second
	}
}

func (h *HealthChecker) checkPeerWithError(peerHealth *PeerHealth) error {
	address := fmt.Sprintf("%s:%d", peerHealth.Peer.Address, peerHealth.Peer.Port)
	
	conn, err := net.DialTimeout("tcp", address, h.timeout)
	if err != nil {
		h.mu.Lock()
		peerHealth.Healthy = false
		peerHealth.LastError = err
		peerHealth.LastCheck = time.Now()
		h.mu.Unlock()

		if peerHealth.Healthy {
			h.logger.Warn("peer became unhealthy",
				"peer_id", peerHealth.Peer.ID,
				"address", address,
				"error", err.Error())
		}
		return err
	}
	conn.Close()

	h.mu.Lock()
	wasHealthy := peerHealth.Healthy
	peerHealth.Healthy = true
	peerHealth.LastError = nil
	peerHealth.LastCheck = time.Now()
	h.mu.Unlock()

	if !wasHealthy {
		h.logger.Info("peer became healthy",
			"peer_id", peerHealth.Peer.ID,
			"address", address)
	}
	return nil
}