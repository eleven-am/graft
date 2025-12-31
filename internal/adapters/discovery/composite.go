package discovery

import (
	"context"

	"github.com/eleven-am/graft/internal/ports"
)

type CompositeSeeder struct {
	seeders []ports.Seeder
}

func NewCompositeSeeder(seeders []ports.Seeder) *CompositeSeeder {
	return &CompositeSeeder{seeders: seeders}
}

func (c *CompositeSeeder) Discover(ctx context.Context) ([]ports.Peer, error) {
	seen := make(map[string]bool)
	var result []ports.Peer

	for _, seeder := range c.seeders {
		peers, err := seeder.Discover(ctx)
		if err != nil {
			continue
		}
		for _, p := range peers {
			if !seen[p.ID] {
				seen[p.ID] = true
				result = append(result, p)
			}
		}
	}

	return result, nil
}

func (c *CompositeSeeder) Name() string {
	return "composite"
}
