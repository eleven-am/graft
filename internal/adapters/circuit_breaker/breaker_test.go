package circuit_breaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

func TestCircuitBreakerClose(t *testing.T) {
	config := ports.CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		MaxRequests:      1,
		Interval:         50 * time.Millisecond,
	}

	cb := NewCircuitBreaker("test", config, nil)

	if cb.State() != ports.StateClose {
		t.Errorf("Expected StateClose, got %v", cb.State())
	}

	err := cb.Execute(context.Background(), func() error {
		return nil
	})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if cb.State() != ports.StateClose {
		t.Errorf("Expected StateClose after success, got %v", cb.State())
	}
}

func TestCircuitBreakerOpenOnFailures(t *testing.T) {
	config := ports.CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		MaxRequests:      1,
		Interval:         50 * time.Millisecond,
	}

	cb := NewCircuitBreaker("test", config, nil)

	for i := 0; i < 3; i++ {
		err := cb.Execute(context.Background(), func() error {
			return errors.New("test error")
		})
		if err == nil {
			t.Error("Expected error from failing function")
		}
	}

	if cb.State() != ports.StateOpen {
		t.Errorf("Expected StateOpen after failures, got %v", cb.State())
	}

	err := cb.Execute(context.Background(), func() error {
		return nil
	})
	if err != ErrCircuitBreakerOpen {
		t.Errorf("Expected ErrCircuitBreakerOpen, got %v", err)
	}
}

func TestCircuitBreakerHalfOpenTransition(t *testing.T) {
	config := ports.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		MaxRequests:      3,
		Interval:         20 * time.Millisecond,
	}

	cb := NewCircuitBreaker("test", config, nil)

	for i := 0; i < 2; i++ {
		cb.Execute(context.Background(), func() error {
			return errors.New("test error")
		})
	}

	if cb.State() != ports.StateOpen {
		t.Errorf("Expected StateOpen, got %v", cb.State())
	}

	time.Sleep(25 * time.Millisecond)

	err := cb.Execute(context.Background(), func() error {
		return nil
	})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = cb.Execute(context.Background(), func() error {
		return nil
	})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if cb.State() != ports.StateClose {
		t.Errorf("Expected StateClose after half-open successes, got %v", cb.State())
	}
}

func TestCircuitBreakerTimeout(t *testing.T) {
	config := ports.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          10 * time.Millisecond,
		MaxRequests:      1,
		Interval:         50 * time.Millisecond,
	}

	cb := NewCircuitBreaker("test", config, nil)

	err := cb.Execute(context.Background(), func() error {
		time.Sleep(20 * time.Millisecond)
		return nil
	})

	if err != ErrCircuitBreakerTimeout {
		t.Errorf("Expected ErrCircuitBreakerTimeout, got %v", err)
	}
}

func TestCircuitBreakerMetrics(t *testing.T) {
	config := ports.CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		MaxRequests:      1,
		Interval:         50 * time.Millisecond,
	}

	cb := NewCircuitBreaker("test", config, nil)

	cb.Execute(context.Background(), func() error { return nil })
	cb.Execute(context.Background(), func() error { return errors.New("fail") })

	metrics := cb.Metrics()

	if metrics.TotalRequests != 2 {
		t.Errorf("Expected 2 total requests, got %d", metrics.TotalRequests)
	}

	if metrics.SuccessCount != 1 {
		t.Errorf("Expected 1 success, got %d", metrics.SuccessCount)
	}

	if metrics.FailureCount != 1 {
		t.Errorf("Expected 1 failure, got %d", metrics.FailureCount)
	}

	if metrics.State != ports.StateClose {
		t.Errorf("Expected StateClose, got %v", metrics.State)
	}
}
