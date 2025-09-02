package ports

import (
	"context"
	"time"
)

type CircuitBreakerState int

const (
	StateClose CircuitBreakerState = iota
	StateHalfOpen
	StateOpen
)

func (s CircuitBreakerState) String() string {
	switch s {
	case StateClose:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

type CircuitBreakerConfig struct {
	FailureThreshold int           `json:"failure_threshold" yaml:"failure_threshold"`
	SuccessThreshold int           `json:"success_threshold" yaml:"success_threshold"`
	Timeout          time.Duration `json:"timeout" yaml:"timeout"`
	MaxRequests      int           `json:"max_requests" yaml:"max_requests"`
	Interval         time.Duration `json:"interval" yaml:"interval"`
	OnStateChange    func(name string, from, to CircuitBreakerState)
}

type CircuitBreakerMetrics struct {
	State              CircuitBreakerState `json:"state"`
	FailureCount       int64               `json:"failure_count"`
	SuccessCount       int64               `json:"success_count"`
	ConsecutiveSuccess int64               `json:"consecutive_success"`
	ConsecutiveFailure int64               `json:"consecutive_failure"`
	LastStateChange    time.Time           `json:"last_state_change"`
	TotalRequests      int64               `json:"total_requests"`
	RequestsAllowed    int64               `json:"requests_allowed"`
	RequestsRejected   int64               `json:"requests_rejected"`
}

type CircuitBreaker interface {
	Execute(ctx context.Context, fn func() error) error
	Call(ctx context.Context, fn func(context.Context) error) error
	State() CircuitBreakerState
	Metrics() CircuitBreakerMetrics
	Reset()
	ForceOpen()
	ForceClose()
}

type CircuitBreakerProvider interface {
	GetCircuitBreaker(name string) CircuitBreaker
	CreateCircuitBreaker(name string, config CircuitBreakerConfig) CircuitBreaker
	GetAllMetrics() map[string]CircuitBreakerMetrics
}
