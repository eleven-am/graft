package core

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testDiscoveryProvider struct {
	peers  []ports.Peer
	events chan ports.Event
}

func newTestDiscoveryProvider() *testDiscoveryProvider {
	return &testDiscoveryProvider{
		events: make(chan ports.Event, 10),
	}
}

func (p *testDiscoveryProvider) Start(ctx context.Context, announce ports.NodeInfo) error {
	return nil
}

func (p *testDiscoveryProvider) Stop() error {
	close(p.events)
	return nil
}

func (p *testDiscoveryProvider) Snapshot() []ports.Peer {
	return append([]ports.Peer(nil), p.peers...)
}

func (p *testDiscoveryProvider) Events() <-chan ports.Event {
	return p.events
}

func (p *testDiscoveryProvider) Name() string {
	return "test"
}

func (p *testDiscoveryProvider) setPeers(peers []ports.Peer) {
	p.peers = append([]ports.Peer(nil), peers...)
}

func (p *testDiscoveryProvider) emit(event ports.Event) {
	p.events <- event
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestManagerWaitForDiscovery(t *testing.T) {
	logger := newTestLogger()

	t.Run("returns immediately when peers already known", func(t *testing.T) {
		provider := newTestDiscoveryProvider()
		peer := ports.Peer{ID: "peer-initial", Address: "127.0.0.1", Port: 8080}
		provider.setPeers([]ports.Peer{peer})

		manager := discovery.NewManager("test-node", logger)
		require.NoError(t, manager.Add(provider))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		require.NoError(t, manager.Start(ctx, "127.0.0.1", 8080, 9080, "test-cluster"))

		coreManager := &Manager{logger: logger, discovery: manager}
		peers, err := coreManager.waitForDiscovery(ctx, time.Second)
		require.NoError(t, err)
		assert.Len(t, peers, 1)
		assert.Equal(t, peer.ID, peers[0].ID)

		assert.NoError(t, manager.Stop())
	})

	t.Run("returns when discovery emits peers", func(t *testing.T) {
		provider := newTestDiscoveryProvider()

		manager := discovery.NewManager("test-node", logger)
		require.NoError(t, manager.Add(provider))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		require.NoError(t, manager.Start(ctx, "127.0.0.1", 8080, 9080, "test-cluster"))

		coreManager := &Manager{logger: logger, discovery: manager}

		go func() {
			time.Sleep(100 * time.Millisecond)
			peer := ports.Peer{ID: "peer-event", Address: "127.0.0.1", Port: 8081}
			provider.setPeers([]ports.Peer{peer})
			provider.emit(ports.Event{Type: ports.PeerAdded, Peer: peer})
		}()

		peers, err := coreManager.waitForDiscovery(ctx, time.Second)
		require.NoError(t, err)
		assert.Len(t, peers, 1)
		assert.Equal(t, "peer-event", peers[0].ID)

		assert.NoError(t, manager.Stop())
	})

	t.Run("returns error when context cancelled", func(t *testing.T) {
		provider := newTestDiscoveryProvider()

		manager := discovery.NewManager("test-node", logger)
		require.NoError(t, manager.Add(provider))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		require.NoError(t, manager.Start(ctx, "127.0.0.1", 8080, 9080, "test-cluster"))

		coreManager := &Manager{logger: logger, discovery: manager}

		cancel()

		peers, err := coreManager.waitForDiscovery(ctx, time.Second)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, ErrDiscoveryStopped), "unexpected error: %v", err)
		assert.Nil(t, peers)

		assert.NoError(t, manager.Stop())
	})

	t.Run("returns empty result when timeout exceeded", func(t *testing.T) {
		provider := newTestDiscoveryProvider()

		manager := discovery.NewManager("test-node", logger)
		require.NoError(t, manager.Add(provider))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		require.NoError(t, manager.Start(ctx, "127.0.0.1", 8080, 9080, "test-cluster"))

		coreManager := &Manager{logger: logger, discovery: manager}

		peers, err := coreManager.waitForDiscovery(ctx, 50*time.Millisecond)
		assert.NoError(t, err)
		assert.Len(t, peers, 0)

		assert.NoError(t, manager.Stop())
	})

	t.Run("returns error when discovery stops without peers", func(t *testing.T) {
		provider := newTestDiscoveryProvider()

		manager := discovery.NewManager("test-node", logger)
		require.NoError(t, manager.Add(provider))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		require.NoError(t, manager.Start(ctx, "127.0.0.1", 8080, 9080, "test-cluster"))

		coreManager := &Manager{logger: logger, discovery: manager}

		stopErrCh := make(chan error, 1)
		go func() {
			time.Sleep(100 * time.Millisecond)
			stopErrCh <- manager.Stop()
		}()

		peers, err := coreManager.waitForDiscovery(ctx, time.Second)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDiscoveryStopped)
		assert.Nil(t, peers)

		require.NoError(t, <-stopErrCh)
	})
}
