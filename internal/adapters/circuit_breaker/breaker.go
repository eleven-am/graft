package circuit_breaker

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
	ErrCircuitBreakerOpen    = errors.New("circuit breaker is open")
	ErrCircuitBreakerTimeout = errors.New("circuit breaker request timeout")
	ErrTooManyRequests       = errors.New("too many requests when circuit breaker is half-open")
)

type circuitBreaker struct {
	name   string
	config ports.CircuitBreakerConfig
	logger *slog.Logger

	mu                 sync.RWMutex
	state              ports.CircuitBreakerState
	failureCount       int64
	successCount       int64
	consecutiveSuccess int64
	consecutiveFailure int64
	lastStateChange    time.Time
	nextRetry          time.Time
	totalRequests      int64
	requestsAllowed    int64
	requestsRejected   int64

	halfOpenRequests int64
}

func NewCircuitBreaker(name string, config ports.CircuitBreakerConfig, logger *slog.Logger) ports.CircuitBreaker {
	if logger == nil {
		logger = slog.Default()
	}

	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 5
	}
	if config.SuccessThreshold <= 0 {
		config.SuccessThreshold = 3
	}
	if config.Timeout <= 0 {
		config.Timeout = 60 * time.Second
	}
	if config.MaxRequests <= 0 {
		config.MaxRequests = 1
	}
	if config.Interval <= 0 {
		config.Interval = 10 * time.Second
	}

	return &circuitBreaker{
		name:            name,
		config:          config,
		logger:          logger.With("component", "circuit-breaker", "name", name),
		state:           ports.StateClose,
		lastStateChange: time.Now(),
	}
}

func (cb *circuitBreaker) Execute(ctx context.Context, fn func() error) error {
	return cb.Call(ctx, func(ctx context.Context) error {
		return fn()
	})
}

func (cb *circuitBreaker) Call(ctx context.Context, fn func(context.Context) error) error {
	atomic.AddInt64(&cb.totalRequests, 1)

	if !cb.allowRequest() {
		atomic.AddInt64(&cb.requestsRejected, 1)
		cb.logger.Debug("request rejected", "state", cb.state.String())
		return ErrCircuitBreakerOpen
	}

	atomic.AddInt64(&cb.requestsAllowed, 1)

	timeoutCtx, cancel := context.WithTimeout(ctx, cb.config.Timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- fn(timeoutCtx)
	}()

	select {
	case err := <-done:
		if err != nil {
			cb.onFailure()
			return err
		}
		cb.onSuccess()
		return nil
	case <-timeoutCtx.Done():
		cb.onFailure()
		if timeoutCtx.Err() == context.DeadlineExceeded {
			return ErrCircuitBreakerTimeout
		}
		return timeoutCtx.Err()
	}
}

func (cb *circuitBreaker) allowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if cb.state == ports.StateOpen && now.After(cb.nextRetry) {
		cb.setState(ports.StateHalfOpen)
	}

	switch cb.state {
	case ports.StateClose:
		return true
	case ports.StateOpen:
		return false
	case ports.StateHalfOpen:
		if cb.halfOpenRequests < int64(cb.config.MaxRequests) {
			cb.halfOpenRequests++
			return true
		}
		return false
	default:
		return false
	}
}

func (cb *circuitBreaker) onSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt64(&cb.successCount, 1)
	atomic.AddInt64(&cb.consecutiveSuccess, 1)
	atomic.StoreInt64(&cb.consecutiveFailure, 0)

	if cb.state == ports.StateHalfOpen {
		if cb.consecutiveSuccess >= int64(cb.config.SuccessThreshold) {
			cb.setState(ports.StateClose)
			cb.halfOpenRequests = 0
		}
	}
}

func (cb *circuitBreaker) onFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt64(&cb.failureCount, 1)
	atomic.AddInt64(&cb.consecutiveFailure, 1)
	atomic.StoreInt64(&cb.consecutiveSuccess, 0)

	if cb.state == ports.StateClose {
		if cb.consecutiveFailure >= int64(cb.config.FailureThreshold) {
			cb.setState(ports.StateOpen)
		}
	} else if cb.state == ports.StateHalfOpen {
		cb.setState(ports.StateOpen)
		cb.halfOpenRequests = 0
	}
}

func (cb *circuitBreaker) setState(newState ports.CircuitBreakerState) {
	oldState := cb.state
	if oldState == newState {
		return
	}

	cb.logger.Info("circuit breaker state change",
		"from", oldState.String(),
		"to", newState.String(),
		"consecutive_failures", cb.consecutiveFailure,
		"consecutive_successes", cb.consecutiveSuccess)

	cb.state = newState
	cb.lastStateChange = time.Now()

	switch newState {
	case ports.StateOpen:
		cb.nextRetry = time.Now().Add(cb.config.Interval)
		atomic.StoreInt64(&cb.consecutiveSuccess, 0)
	case ports.StateHalfOpen:
		cb.halfOpenRequests = 0
		atomic.StoreInt64(&cb.consecutiveFailure, 0)
	case ports.StateClose:
		cb.nextRetry = time.Time{}
		atomic.StoreInt64(&cb.consecutiveFailure, 0)
	}

	if cb.config.OnStateChange != nil {
		go cb.config.OnStateChange(cb.name, oldState, newState)
	}
}

func (cb *circuitBreaker) State() ports.CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *circuitBreaker) Metrics() ports.CircuitBreakerMetrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return ports.CircuitBreakerMetrics{
		State:              cb.state,
		FailureCount:       atomic.LoadInt64(&cb.failureCount),
		SuccessCount:       atomic.LoadInt64(&cb.successCount),
		ConsecutiveSuccess: atomic.LoadInt64(&cb.consecutiveSuccess),
		ConsecutiveFailure: atomic.LoadInt64(&cb.consecutiveFailure),
		LastStateChange:    cb.lastStateChange,
		TotalRequests:      atomic.LoadInt64(&cb.totalRequests),
		RequestsAllowed:    atomic.LoadInt64(&cb.requestsAllowed),
		RequestsRejected:   atomic.LoadInt64(&cb.requestsRejected),
	}
}

func (cb *circuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.logger.Info("circuit breaker reset")

	atomic.StoreInt64(&cb.failureCount, 0)
	atomic.StoreInt64(&cb.successCount, 0)
	atomic.StoreInt64(&cb.consecutiveSuccess, 0)
	atomic.StoreInt64(&cb.consecutiveFailure, 0)
	atomic.StoreInt64(&cb.totalRequests, 0)
	atomic.StoreInt64(&cb.requestsAllowed, 0)
	atomic.StoreInt64(&cb.requestsRejected, 0)

	cb.setState(ports.StateClose)
	cb.halfOpenRequests = 0
}

func (cb *circuitBreaker) ForceOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.logger.Info("circuit breaker force opened")
	cb.setState(ports.StateOpen)
}

func (cb *circuitBreaker) ForceClose() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.logger.Info("circuit breaker force closed")
	cb.setState(ports.StateClose)
}
