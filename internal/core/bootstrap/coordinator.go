package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/raft/bootstrap"
	"github.com/eleven-am/graft/internal/readiness"
)

// Coordinator encapsulates bootstrap/discovery orchestration so core.Manager can stay slim.
type Coordinator struct {
	NodeID        string
	BindAddr      string
	HasState      bool
	ClusterID     string
	Discovery     ports.Discovery
	Logger        *slog.Logger
	ExpectedPeers int
}

type Result struct {
	BootstrapSpec bootstrap.Spec
	Peers         []ports.Peer
}

func (c *Coordinator) Start(ctx context.Context, grpcPort int, discoveryTimeout func() time.Duration) (*Result, error) {
	host, portStr, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid bind address: %w", err)
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid bind port: %w", err)
	}

	if c.Discovery != nil {
		clusterID := c.ClusterID
		if clusterID == "" {
			clusterID = c.NodeID
		}
		if err := c.Discovery.Start(ctx, host, p, grpcPort, clusterID); err != nil {
			return nil, err
		}
	}

	var peers []ports.Peer
	if c.HasState {
		peers = filterPeers(c.Discovery, c.NodeID)
		go refreshPeersAsync(ctx, c.Discovery, c.NodeID, discoveryTimeout())
		logPeerMetadata(peers, c.Logger)
	} else {
		var err error
		peers, err = waitForDiscovery(ctx, c.Discovery, c.NodeID, c.ExpectedPeers, discoveryTimeout())
		if err != nil {
			return nil, err
		}
		logPeerMetadata(peers, c.Logger)
	}

	spec := bootstrap.BuildSpec(peers, c.NodeID, c.BindAddr, c.HasState)
	return &Result{BootstrapSpec: spec, Peers: peers}, nil
}

// -------- helpers (mirrors manager local helpers to keep behavior unchanged) --------

func filterPeers(discovery ports.Discovery, selfID string) []ports.Peer {
	if discovery == nil {
		return nil
	}
	peers := discovery.GetPeers()
	filtered := make([]ports.Peer, 0, len(peers))
	for _, p := range peers {
		if p.ID != selfID {
			filtered = append(filtered, p)
		}
	}
	return filtered
}

func waitForDiscovery(ctx context.Context, discovery ports.Discovery, selfID string, expectedStatic int, timeout time.Duration) ([]ports.Peer, error) {
	if discovery == nil {
		return nil, nil
	}

	if timeout <= 0 {
		return filterPeers(discovery, selfID), nil
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	events, unsubscribe := discovery.Subscribe()
	defer unsubscribe()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			if ctx.Err() != nil && errors.Is(timeoutCtx.Err(), context.Canceled) {
				return nil, ctx.Err()
			}
			return filterPeers(discovery, selfID), nil
		case _, ok := <-events:
			if !ok {
				peers := filterPeers(discovery, selfID)
				if len(peers) == 0 {
					return nil, fmt.Errorf("discovery stopped before peers found")
				}
				return peers, nil
			}
			peers := filterPeers(discovery, selfID)
			if expectedStatic > 0 {
				if len(peers) >= expectedStatic {
					return peers, nil
				}
			} else if len(peers) > 0 {
				return peers, nil
			}
		case <-ticker.C:
			peers := filterPeers(discovery, selfID)
			if expectedStatic > 0 {
				if len(peers) >= expectedStatic {
					return peers, nil
				}
			} else if len(peers) > 0 {
				return peers, nil
			}
		}
	}
}

func refreshPeersAsync(ctx context.Context, discovery ports.Discovery, selfID string, timeout time.Duration) {
	if discovery == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		events, unsubscribe := discovery.Subscribe()
		defer unsubscribe()

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-events:
				logPeerMetadata(filterPeers(discovery, selfID), nil)
			case <-ticker.C:
				if peers := filterPeers(discovery, selfID); len(peers) > 0 {
					logPeerMetadata(peers, nil)
					return
				}
			}
		}
	}()
}

func logPeerMetadata(peers []ports.Peer, logger *slog.Logger) {
	if logger == nil {
		return
	}
	readiness.LogPeerMetadata(peers, logger)
}
