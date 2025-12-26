package connectors

// FSMAPI exposes the minimal surface for connector lifecycle management.
type FSMAPI interface {
	State() State
	Transition(to State)
}
