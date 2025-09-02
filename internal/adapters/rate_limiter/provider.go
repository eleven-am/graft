package rate_limiter

import (
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/ports"
)

type Provider struct {
	mu       sync.RWMutex
	limiters map[string]ports.RateLimiter
	logger   *slog.Logger
}

func NewProvider(logger *slog.Logger) ports.RateLimiterProvider {
	if logger == nil {
		logger = slog.Default()
	}

	return &Provider{
		limiters: make(map[string]ports.RateLimiter),
		logger:   logger.With("component", "rate-limiter-provider"),
	}
}

func (p *Provider) GetRateLimiter(name string) ports.RateLimiter {
	p.mu.RLock()
	limiter, exists := p.limiters[name]
	p.mu.RUnlock()

	if !exists {
		return p.CreateRateLimiter(name, ports.RateLimiterConfig{})
	}

	return limiter
}

func (p *Provider) CreateRateLimiter(name string, config ports.RateLimiterConfig) ports.RateLimiter {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, exists := p.limiters[name]; exists {
		p.logger.Debug("rate limiter already exists, returning existing", "name", name)
		return existing
	}

	limiter := NewRateLimiter(name, config, p.logger)
	p.limiters[name] = limiter

	p.logger.Info("created rate limiter",
		"name", name,
		"rps", config.RequestsPerSecond,
		"burst", config.BurstSize,
		"timeout", config.WaitTimeout)

	return limiter
}

func (p *Provider) GetAllMetrics() map[string]map[string]ports.RateLimiterMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	allMetrics := make(map[string]map[string]ports.RateLimiterMetrics)
	for name, limiter := range p.limiters {
		allMetrics[name] = limiter.GlobalMetrics()
	}

	return allMetrics
}
