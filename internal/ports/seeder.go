package ports

import "context"

type Peer struct {
	ID      string
	Address string
	Port    int
}

type Seeder interface {
	Discover(ctx context.Context) ([]Peer, error)
	Name() string
}

type AdvertisableSeeder interface {
	Seeder
	StartAdvertising(nodeID, addr string, port int) error
	StopAdvertising()
}
