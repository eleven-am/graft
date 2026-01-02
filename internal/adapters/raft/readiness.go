package raft

import (
	"context"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type Readiness struct {
	mu       sync.RWMutex
	state    ports.ReadinessState
	listener func(ports.ReadinessState)
}

func NewReadiness() *Readiness {
	return &Readiness{
		state: ports.ReadinessStatePending,
	}
}

func (r *Readiness) SetState(state ports.ReadinessState) {
	r.mu.Lock()
	r.state = state
	listener := r.listener
	r.mu.Unlock()

	if listener != nil {
		listener(state)
	}
}

func (r *Readiness) SetListener(listener func(ports.ReadinessState)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listener = listener
}

func (r *Readiness) State() ports.ReadinessState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Readiness) IsReady() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state == ports.ReadinessStateReady
}

func (r *Readiness) WaitUntilReady(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if r.IsReady() {
				return nil
			}
		}
	}
}

var _ ports.ReadinessGate = (*Readiness)(nil)
