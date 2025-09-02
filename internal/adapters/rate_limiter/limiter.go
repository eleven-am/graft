package rate_limiter

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
	ErrWaitTimeout       = errors.New("wait timeout exceeded")
)

type bucket struct {
	mu              sync.RWMutex
	tokens          float64
	maxTokens       int
	refillRate      float64
	lastRefill      time.Time
	totalRequests   int64
	allowedRequests int64
	deniedRequests  int64
	waitingRequests int64
	totalWaitTime   int64
	maxWaitTime     int64
	lastActivity    time.Time
}

type rateLimiter struct {
	name    string
	config  ports.RateLimiterConfig
	logger  *slog.Logger
	buckets sync.Map
	cleanup chan struct{}
	done    chan struct{}
}

func NewRateLimiter(name string, config ports.RateLimiterConfig, logger *slog.Logger) ports.RateLimiter {
	if logger == nil {
		logger = slog.Default()
	}

	if config.RequestsPerSecond <= 0 {
		config.RequestsPerSecond = 100
	}
	if config.BurstSize <= 0 {
		config.BurstSize = int(config.RequestsPerSecond)
	}
	if config.WaitTimeout <= 0 {
		config.WaitTimeout = 5 * time.Second
	}
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = 5 * time.Minute
	}
	if config.KeyExpiry <= 0 {
		config.KeyExpiry = 10 * time.Minute
	}

	rl := &rateLimiter{
		name:    name,
		config:  config,
		logger:  logger.With("component", "rate-limiter", "name", name),
		cleanup: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}

	go rl.cleanupExpiredKeys()

	return rl
}

func (rl *rateLimiter) getBucket(key string) *bucket {
	if value, ok := rl.buckets.Load(key); ok {
		return value.(*bucket)
	}

	newBucket := &bucket{
		tokens:       float64(rl.config.BurstSize),
		maxTokens:    rl.config.BurstSize,
		refillRate:   rl.config.RequestsPerSecond,
		lastRefill:   time.Now(),
		lastActivity: time.Now(),
	}

	value, _ := rl.buckets.LoadOrStore(key, newBucket)
	return value.(*bucket)
}

func (rl *rateLimiter) Allow(key string) bool {
	b := rl.getBucket(key)
	return rl.allowN(b, 1)
}

func (rl *rateLimiter) allowN(b *bucket, n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	atomic.AddInt64(&b.totalRequests, int64(n))
	b.lastActivity = time.Now()

	rl.refillBucket(b)

	if b.tokens >= float64(n) {
		b.tokens -= float64(n)
		atomic.AddInt64(&b.allowedRequests, int64(n))
		return true
	}

	atomic.AddInt64(&b.deniedRequests, int64(n))
	return false
}

func (rl *rateLimiter) Wait(ctx context.Context, key string) error {
	b := rl.getBucket(key)
	return rl.waitN(ctx, b, 1)
}

func (rl *rateLimiter) waitN(ctx context.Context, b *bucket, n int) error {
	if rl.allowN(b, n) {
		return nil
	}

	atomic.AddInt64(&b.waitingRequests, 1)
	defer atomic.AddInt64(&b.waitingRequests, -1)

	start := time.Now()
	defer func() {
		waitTime := time.Since(start).Nanoseconds()
		atomic.AddInt64(&b.totalWaitTime, waitTime)

		for {
			current := atomic.LoadInt64(&b.maxWaitTime)
			if waitTime <= current || atomic.CompareAndSwapInt64(&b.maxWaitTime, current, waitTime) {
				break
			}
		}
	}()

	timeout := time.NewTimer(rl.config.WaitTimeout)
	defer timeout.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return ErrWaitTimeout
		case <-ticker.C:
			if rl.allowN(b, n) {
				return nil
			}
		}
	}
}

func (rl *rateLimiter) Reserve(key string) error {
	return rl.ReserveN(key, 1)
}

func (rl *rateLimiter) ReserveN(key string, n int) error {
	b := rl.getBucket(key)

	b.mu.Lock()
	defer b.mu.Unlock()

	atomic.AddInt64(&b.totalRequests, int64(n))
	b.lastActivity = time.Now()

	rl.refillBucket(b)

	if b.tokens >= float64(n) {
		b.tokens -= float64(n)
		atomic.AddInt64(&b.allowedRequests, int64(n))
		return nil
	}

	needed := float64(n) - b.tokens
	waitTime := time.Duration(needed / b.refillRate * float64(time.Second))

	if waitTime > rl.config.WaitTimeout {
		atomic.AddInt64(&b.deniedRequests, int64(n))
		return ErrRateLimitExceeded
	}

	b.tokens -= float64(n)
	atomic.AddInt64(&b.allowedRequests, int64(n))
	return nil
}

func (rl *rateLimiter) refillBucket(b *bucket) {
	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	tokensToAdd := elapsed * b.refillRate

	b.tokens = min(b.tokens+tokensToAdd, float64(b.maxTokens))
	b.lastRefill = now
}

func (rl *rateLimiter) Metrics(key string) ports.RateLimiterMetrics {
	b := rl.getBucket(key)

	b.mu.RLock()
	rl.refillBucket(b)
	metrics := ports.RateLimiterMetrics{
		TotalRequests:   atomic.LoadInt64(&b.totalRequests),
		AllowedRequests: atomic.LoadInt64(&b.allowedRequests),
		DeniedRequests:  atomic.LoadInt64(&b.deniedRequests),
		WaitingRequests: atomic.LoadInt64(&b.waitingRequests),
		TokensAvailable: b.tokens,
		LastRefill:      b.lastRefill,
	}
	b.mu.RUnlock()

	totalRequests := metrics.TotalRequests
	if totalRequests > 0 {
		metrics.AvgWaitTime = float64(atomic.LoadInt64(&b.totalWaitTime)) / float64(totalRequests) / 1e6
	}
	metrics.MaxWaitTime = float64(atomic.LoadInt64(&b.maxWaitTime)) / 1e6

	return metrics
}

func (rl *rateLimiter) Reset(key string) {
	if value, ok := rl.buckets.LoadAndDelete(key); ok {
		b := value.(*bucket)
		rl.logger.Debug("reset rate limiter", "key", key, "requests", atomic.LoadInt64(&b.totalRequests))
	}
}

func (rl *rateLimiter) SetLimit(key string, requestsPerSecond float64, burstSize int) {
	b := rl.getBucket(key)

	b.mu.Lock()
	defer b.mu.Unlock()

	b.refillRate = requestsPerSecond
	b.maxTokens = burstSize
	if b.tokens > float64(burstSize) {
		b.tokens = float64(burstSize)
	}

	rl.logger.Debug("updated rate limit", "key", key, "rps", requestsPerSecond, "burst", burstSize)
}

func (rl *rateLimiter) GlobalMetrics() map[string]ports.RateLimiterMetrics {
	metrics := make(map[string]ports.RateLimiterMetrics)
	activeKeys := 0

	rl.buckets.Range(func(key, value interface{}) bool {
		keyStr := key.(string)
		metrics[keyStr] = rl.Metrics(keyStr)
		activeKeys++
		return true
	})

	return metrics
}

func (rl *rateLimiter) cleanupExpiredKeys() {
	ticker := time.NewTicker(rl.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rl.done:
			return
		case <-ticker.C:
			rl.performCleanup()
		case <-rl.cleanup:
			rl.performCleanup()
		}
	}
}

func (rl *rateLimiter) performCleanup() {
	now := time.Now()
	deleted := 0

	rl.buckets.Range(func(key, value interface{}) bool {
		b := value.(*bucket)
		b.mu.RLock()
		expired := now.Sub(b.lastActivity) > rl.config.KeyExpiry
		b.mu.RUnlock()

		if expired {
			rl.buckets.Delete(key)
			deleted++
		}
		return true
	})

	if deleted > 0 {
		rl.logger.Debug("cleaned up expired keys", "deleted", deleted)
	}
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
