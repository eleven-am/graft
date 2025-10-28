package readiness

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestManagerWaitUntilReadyResetsAfterRegression(t *testing.T) {
	mgr := NewManager()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Initial wait should block until state becomes ready.
	done := make(chan error, 1)
	go func() {
		done <- mgr.WaitUntilReady(ctx)
	}()

	select {
	case <-time.After(20 * time.Millisecond):
	case err := <-done:
		t.Fatalf("expected wait to block, got %v", err)
	}

	mgr.SetState(StateReady)

	if err := <-done; err != nil {
		t.Fatalf("expected wait to succeed after readiness, got %v", err)
	}

	// Regress back to detecting and ensure waits no longer return immediately.
	mgr.SetState(StateDetecting)

	regressionCtx, regressionCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer regressionCancel()

	start := time.Now()
	if err := mgr.WaitUntilReady(regressionCtx); err == nil {
		t.Fatalf("expected wait to block when readiness regressed")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatalf("wait returned too quickly after regression")
	}

	// Transition to ready again should unblock new waits immediately.
	mgr.SetState(StateReady)
	if err := mgr.WaitUntilReady(context.Background()); err != nil {
		t.Fatalf("expected immediate success after readiness restored, got %v", err)
	}
}

func TestManagerWaitUntilReadyImmediateWhenAlreadyReady(t *testing.T) {
	mgr := NewManager()
	mgr.SetState(StateReady)

	if err := mgr.WaitUntilReady(context.Background()); err != nil {
		t.Fatalf("expected immediate success when already ready, got %v", err)
	}
}
