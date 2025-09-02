package circuit_breaker

import (
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/ports"
)

type Provider struct {
	mu       sync.RWMutex
	breakers map[string]ports.CircuitBreaker
	logger   *slog.Logger
}

func NewProvider(logger *slog.Logger) ports.CircuitBreakerProvider {
	if logger == nil {
		logger = slog.Default()
	}

	return &Provider{
		breakers: make(map[string]ports.CircuitBreaker),
		logger:   logger.With("component", "circuit-breaker-provider"),
	}
}

func (p *Provider) GetCircuitBreaker(name string) ports.CircuitBreaker {
	p.mu.RLock()
	breaker, exists := p.breakers[name]
	p.mu.RUnlock()

	if !exists {
		return p.CreateCircuitBreaker(name, ports.CircuitBreakerConfig{})
	}

	return breaker
}

func (p *Provider) CreateCircuitBreaker(name string, config ports.CircuitBreakerConfig) ports.CircuitBreaker {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, exists := p.breakers[name]; exists {
		p.logger.Debug("circuit breaker already exists, returning existing", "name", name)
		return existing
	}

	breaker := NewCircuitBreaker(name, config, p.logger)
	p.breakers[name] = breaker

	p.logger.Info("created circuit breaker",
		"name", name,
		"failure_threshold", config.FailureThreshold,
		"success_threshold", config.SuccessThreshold,
		"timeout", config.Timeout,
		"max_requests", config.MaxRequests,
		"interval", config.Interval)

	return breaker
}

func (p *Provider) GetAllMetrics() map[string]ports.CircuitBreakerMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	metrics := make(map[string]ports.CircuitBreakerMetrics)
	for name, breaker := range p.breakers {
		metrics[name] = breaker.Metrics()
	}

	return metrics
}
