package ports

import (
	"context"
)

type Peer struct {
	ID       string
	Address  string
	Port     int
	Metadata map[string]string
}

type NodeInfo = Peer

type Provider interface {
	Start(ctx context.Context, announce NodeInfo) error
	Stop() error
	Snapshot() []Peer
	Events() <-chan Event
	Name() string
}

type Event struct {
	Type EventType
	Peer Peer
}

type EventType int

const (
	PeerAdded EventType = iota
	PeerUpdated
	PeerRemoved
)
