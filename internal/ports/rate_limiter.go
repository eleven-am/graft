package ports

import (
	"context"
	"time"
)

type RateLimiterConfig struct {
	RequestsPerSecond float64       `json:"requests_per_second" yaml:"requests_per_second"`
	BurstSize         int           `json:"burst_size" yaml:"burst_size"`
	WaitTimeout       time.Duration `json:"wait_timeout" yaml:"wait_timeout"`
	EnableMetrics     bool          `json:"enable_metrics" yaml:"enable_metrics"`
	CleanupInterval   time.Duration `json:"cleanup_interval" yaml:"cleanup_interval"`
	KeyExpiry         time.Duration `json:"key_expiry" yaml:"key_expiry"`
}

type RateLimiterMetrics struct {
	TotalRequests   int64     `json:"total_requests"`
	AllowedRequests int64     `json:"allowed_requests"`
	DeniedRequests  int64     `json:"denied_requests"`
	WaitingRequests int64     `json:"waiting_requests"`
	AvgWaitTime     float64   `json:"avg_wait_time_ms"`
	MaxWaitTime     float64   `json:"max_wait_time_ms"`
	ActiveKeys      int       `json:"active_keys"`
	LastReset       time.Time `json:"last_reset"`
	TokensAvailable float64   `json:"tokens_available"`
	LastRefill      time.Time `json:"last_refill"`
}

type RateLimiter interface {
	Allow(key string) bool
	Wait(ctx context.Context, key string) error
	Reserve(key string) error
	ReserveN(key string, n int) error
	Metrics(key string) RateLimiterMetrics
	Reset(key string)
	SetLimit(key string, requestsPerSecond float64, burstSize int)
	GlobalMetrics() map[string]RateLimiterMetrics
}

type RateLimiterProvider interface {
	GetRateLimiter(name string) RateLimiter
	CreateRateLimiter(name string, config RateLimiterConfig) RateLimiter
	GetAllMetrics() map[string]map[string]RateLimiterMetrics
}
