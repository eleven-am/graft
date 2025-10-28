package readiness

import (
	"context"
	"sync"
	"time"
)

type State int

const (
	StateProvisional State = iota
	StateDetecting
	StateReady
)

func (s State) String() string {
	switch s {
	case StateProvisional:
		return "provisional"
	case StateDetecting:
		return "detecting"
	case StateReady:
		return "ready"
	default:
		return "unknown"
	}
}

type Manager struct {
	state    State
	mu       sync.RWMutex
	waitChan chan struct{}
}

func NewManager() *Manager {
	return &Manager{
		state:    StateProvisional,
		waitChan: make(chan struct{}),
	}
}

func (m *Manager) SetState(state State) {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldState := m.state
	m.state = state

	switch {
	case oldState != StateReady && state == StateReady:
		close(m.waitChan)
	case oldState == StateReady && state != StateReady:
		m.waitChan = make(chan struct{})
	}
}

func (m *Manager) GetState() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

func (m *Manager) IsReady() bool {
	return m.GetState() == StateReady
}

func (m *Manager) WaitUntilReady(ctx context.Context) error {
	m.mu.RLock()
	if m.state == StateReady {
		m.mu.RUnlock()
		return nil
	}
	waitChan := m.waitChan
	m.mu.RUnlock()

	select {
	case <-waitChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) WaitUntilReadyTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return m.WaitUntilReady(ctx)
}
