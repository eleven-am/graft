package rate_limiter

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

func TestRateLimiterAllow(t *testing.T) {
	config := ports.RateLimiterConfig{
		RequestsPerSecond: 2,
		BurstSize:         2,
		WaitTimeout:       100 * time.Millisecond,
		EnableMetrics:     true,
		CleanupInterval:   time.Minute,
		KeyExpiry:         time.Minute,
	}

	limiter := NewRateLimiter("test", config, nil)

	if !limiter.Allow("key1") {
		t.Error("Expected first request to be allowed")
	}

	if !limiter.Allow("key1") {
		t.Error("Expected second request to be allowed")
	}

	if limiter.Allow("key1") {
		t.Error("Expected third request to be denied")
	}

	if !limiter.Allow("key2") {
		t.Error("Expected request for different key to be allowed")
	}
}

func TestRateLimiterRefill(t *testing.T) {
	config := ports.RateLimiterConfig{
		RequestsPerSecond: 10,
		BurstSize:         1,
		WaitTimeout:       200 * time.Millisecond,
		EnableMetrics:     true,
		CleanupInterval:   time.Minute,
		KeyExpiry:         time.Minute,
	}

	limiter := NewRateLimiter("test", config, nil)

	if !limiter.Allow("key1") {
		t.Error("Expected first request to be allowed")
	}

	if limiter.Allow("key1") {
		t.Error("Expected second request to be denied")
	}

	time.Sleep(120 * time.Millisecond)

	if !limiter.Allow("key1") {
		t.Error("Expected request to be allowed after refill")
	}
}

func TestRateLimiterWait(t *testing.T) {
	config := ports.RateLimiterConfig{
		RequestsPerSecond: 20,
		BurstSize:         1,
		WaitTimeout:       200 * time.Millisecond,
		EnableMetrics:     true,
		CleanupInterval:   time.Minute,
		KeyExpiry:         time.Minute,
	}

	limiter := NewRateLimiter("test", config, nil)

	if !limiter.Allow("key1") {
		t.Error("Expected first request to be allowed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := limiter.Wait(ctx, "key1")
	if err != nil {
		t.Errorf("Expected Wait to succeed, got error: %v", err)
	}
}

func TestRateLimiterWaitTimeout(t *testing.T) {
	config := ports.RateLimiterConfig{
		RequestsPerSecond: 1,
		BurstSize:         1,
		WaitTimeout:       50 * time.Millisecond,
		EnableMetrics:     true,
		CleanupInterval:   time.Minute,
		KeyExpiry:         time.Minute,
	}

	limiter := NewRateLimiter("test", config, nil)

	if !limiter.Allow("key1") {
		t.Error("Expected first request to be allowed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := limiter.Wait(ctx, "key1")
	if err != ErrWaitTimeout {
		t.Errorf("Expected ErrWaitTimeout, got: %v", err)
	}
}

func TestRateLimiterMetrics(t *testing.T) {
	config := ports.RateLimiterConfig{
		RequestsPerSecond: 10,
		BurstSize:         2,
		WaitTimeout:       100 * time.Millisecond,
		EnableMetrics:     true,
		CleanupInterval:   time.Minute,
		KeyExpiry:         time.Minute,
	}

	limiter := NewRateLimiter("test", config, nil)

	limiter.Allow("key1")
	limiter.Allow("key1")
	limiter.Allow("key1")

	metrics := limiter.Metrics("key1")

	if metrics.TotalRequests != 3 {
		t.Errorf("Expected 3 total requests, got %d", metrics.TotalRequests)
	}

	if metrics.AllowedRequests != 2 {
		t.Errorf("Expected 2 allowed requests, got %d", metrics.AllowedRequests)
	}

	if metrics.DeniedRequests != 1 {
		t.Errorf("Expected 1 denied request, got %d", metrics.DeniedRequests)
	}
}

func TestRateLimiterSetLimit(t *testing.T) {
	config := ports.RateLimiterConfig{
		RequestsPerSecond: 1,
		BurstSize:         1,
		WaitTimeout:       100 * time.Millisecond,
		EnableMetrics:     true,
		CleanupInterval:   time.Minute,
		KeyExpiry:         time.Minute,
	}

	limiter := NewRateLimiter("test", config, nil)

	if !limiter.Allow("key1") {
		t.Error("Expected first request to be allowed")
	}
	if limiter.Allow("key1") {
		t.Error("Expected second request to be denied with initial limit")
	}

	limiter.SetLimit("key1", 10, 5)

	time.Sleep(110 * time.Millisecond)

	if !limiter.Allow("key1") {
		t.Error("Expected request to be allowed after increasing limit")
	}
}
