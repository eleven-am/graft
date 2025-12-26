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

// Discovery abstracts the discovery manager surface needed by bootstrap coordination.
type Discovery interface {
	Start(ctx context.Context, host string, port int, grpcPort int, clusterID string) error
	GetPeers() []Peer
	Subscribe() (<-chan Event, func())
}
