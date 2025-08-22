package ports

import (
	"context"
)

type DiscoveryPort interface {
	Start(ctx context.Context, config DiscoveryConfig) error
	Stop() error
	Advertise(info ServiceInfo) error
	Discover() ([]Peer, error)
}

type DiscoveryConfig struct {
	ServiceName string
	ServicePort int
	Namespace   string
	Metadata    map[string]string
}

type ServiceInfo struct {
	ID       string
	Name     string
	Address  string
	Port     int
	Metadata map[string]string
}

type Peer struct {
	ID       string
	Address  string
	Port     int
	Metadata map[string]string
}