package raft

import (
	"context"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleNodeBootstrap(t *testing.T) {
	config := DefaultRaftConfig("single-node", "127.0.0.1:0", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer func() {
		if adapter != nil && adapter.started {
			adapter.Stop()
			time.Sleep(500 * time.Millisecond)
		}
	}()

	time.Sleep(2 * time.Second)

	assert.True(t, adapter.IsLeader(), "Single node should become leader")

	cmd := domain.NewPutCommand("test-key", []byte("test-value"))
	result, err := adapter.Apply(ctx, cmd)
	require.NoError(t, err)
	assert.True(t, result.Success)

	t.Log("Single node bootstrap successful - can accept commands")
}

func TestBootstrapIdempotency(t *testing.T) {
	dataDir1 := t.TempDir()

	config1 := DefaultRaftConfig("idempotent-test", "127.0.0.1:0", dataDir1)
	adapter1, err := NewAdapter(config1, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter1.Start(ctx, []ports.Peer{})
	require.NoError(t, err)

	time.Sleep(1 * time.Second)
	err = adapter1.Stop()
	require.NoError(t, err)

	adapter1 = nil
	runtime.GC()
	time.Sleep(2 * time.Second)

	dataDir2 := t.TempDir()
	config2 := DefaultRaftConfig("idempotent-test-2", "127.0.0.1:0", dataDir2)
	adapter2, err := NewAdapter(config2, slog.Default())
	require.NoError(t, err)

	err = adapter2.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer func() {
		if adapter2 != nil && adapter2.started {
			adapter2.Stop()
			time.Sleep(500 * time.Millisecond)
		}
	}()

	waitForLeadership(adapter2)
	assert.True(t, adapter2.IsLeader(), "Restarted node should become leader")

	t.Log("Bootstrap idempotency test successful")
}
