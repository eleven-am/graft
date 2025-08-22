package mdns

import (
	"context"
	"time"

	"github.com/hashicorp/mdns"
)

type Resolver interface {
	Discover(timeout time.Duration) ([]*mdns.ServiceEntry, error)
	Browse(ctx context.Context) (<-chan *mdns.ServiceEntry, error)
}