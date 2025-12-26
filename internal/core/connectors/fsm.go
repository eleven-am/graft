package connectors

import (
	"sync"
)

// State represents connector lifecycle states.
type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
	StateError   State = "error"
	StateStopped State = "stopped"
)

// FSM coordinates state transitions for connectors.
type FSM struct {
	mu    sync.RWMutex
	state State
}

func NewFSM() *FSM {
	return &FSM{state: StatePending}
}

func (f *FSM) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state
}

func (f *FSM) Transition(to State) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = to
}
