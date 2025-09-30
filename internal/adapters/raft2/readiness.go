package raft2

import (
	"context"
	"sync"

	"github.com/eleven-am/graft/internal/ports"
)

// Readiness provides a concurrency-safe implementation of ports.ReadinessGate
// used by the raft2 controller pipeline. It exposes a simple state machine and
// coordinate waiters via a channel that is closed when the system transitions
// into the ready state.
type Readiness struct {
	mu       sync.RWMutex
	state    ports.ReadinessState
	readyCh  chan struct{}
	listener func(ports.ReadinessState)
}

// NewReadiness constructs a readiness gate starting in the unknown state.
func NewReadiness() *Readiness {
	return &Readiness{
		state:   ports.ReadinessStateUnknown,
		readyCh: make(chan struct{}),
	}
}

// SetState updates the readiness state. When entering the ready state the
// channel is closed to unblock waiters; when leaving ready a new channel is
// created so future callers can wait for the next transition.
func (r *Readiness) SetState(state ports.ReadinessState) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == state {
		return
	}

	wasReady := r.state == ports.ReadinessStateReady
	willBeReady := state == ports.ReadinessStateReady
	if wasReady && !willBeReady {
		r.readyCh = make(chan struct{})
	} else if !wasReady && willBeReady {
		close(r.readyCh)
	}

	r.state = state

	if r.listener != nil {
		go r.listener(state)
	}
}

// State returns the current readiness state.
func (r *Readiness) State() ports.ReadinessState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// IsReady reports whether the gate is currently ready.
func (r *Readiness) IsReady() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state == ports.ReadinessStateReady
}

// WaitUntilReady blocks until the gate transitions to the ready state or the
// provided context is cancelled.
func (r *Readiness) WaitUntilReady(ctx context.Context) error {
	r.mu.RLock()
	state := r.state
	readyCh := r.readyCh
	r.mu.RUnlock()

	if state == ports.ReadinessStateReady {
		return nil
	}

	select {
	case <-readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Readiness) SetListener(listener func(ports.ReadinessState)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listener = listener
}
