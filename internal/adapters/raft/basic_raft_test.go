package raft

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitForLeadership(adapter *Adapter) {
	time.Sleep(2 * time.Second)
	for i := 0; i < 50; i++ {
		if adapter.IsLeader() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func cleanupAdapter(adapter *Adapter) {
	if adapter != nil && adapter.started {
		if err := adapter.Stop(); err != nil {
			fmt.Printf("Warning: failed to stop adapter %s: %v\n", adapter.config.NodeID, err)
		}
		time.Sleep(500 * time.Millisecond)

		if adapter.store != nil {
			if stateDB := adapter.store.StateDB(); stateDB != nil {
				if err := stateDB.Close(); err != nil {
					fmt.Printf("Warning: failed to close state DB for %s: %v\n", adapter.config.NodeID, err)
				}
			}
		}
	}
}

func TestRaftBootstrapVulnerability(t *testing.T) {
	config := DefaultRaftConfig("bootstrap-test", "127.0.0.1:7001", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer cleanupAdapter(adapter)

	waitForLeadership(adapter)

	if !adapter.IsLeader() {
		t.Error("Single node should become leader after bootstrap")
	}

	cmd := domain.NewPutCommand("test", []byte("value"))
	_, err = adapter.Apply(ctx, cmd)
	assert.NoError(t, err, "Leader should accept commands")
}

func TestCommandMarshallingAttacks(t *testing.T) {
	config := DefaultRaftConfig("marshal-test", "127.0.0.1:7002", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer cleanupAdapter(adapter)

	waitForLeadership(adapter)

	maliciousCommands := []*domain.Command{
		{Type: domain.CommandType(255), Key: "", Value: nil},
		{Type: domain.CommandPut, Key: "", Value: make([]byte, 5*1024*1024)},
		{Type: domain.CommandPut, Key: string(make([]byte, 10000)), Value: []byte("test")},
		{Type: domain.CommandDelete, Key: "", Value: nil},
	}

	for i, cmd := range maliciousCommands {
		t.Run(fmt.Sprintf("malicious-cmd-%d", i), func(t *testing.T) {
			_, err := adapter.Apply(ctx, cmd)
			require.Error(t, err, "Malicious command should be rejected")
		})
	}
}

func TestConcurrentConsensusAttack(t *testing.T) {
	config := DefaultRaftConfig("concurrent-test", "127.0.0.1:7003", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer cleanupAdapter(adapter)

	waitForLeadership(adapter)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			cmd := domain.NewPutCommand(fmt.Sprintf("key-%d", id), []byte(fmt.Sprintf("value-%d", id)))
			_, err := adapter.Apply(ctx, cmd)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for range errors {
		errorCount++
	}

	require.Equal(t, 0, errorCount, "Well-formed concurrent operations should not generate errors")
}

func TestLeadershipTransferAttack(t *testing.T) {
	config := DefaultRaftConfig("transfer-test", "127.0.0.1:7004", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)

	waitForLeadership(adapter)
	require.True(t, adapter.IsLeader())

	cmd := domain.NewPutCommand("before-stop", []byte("test"))
	_, err = adapter.Apply(ctx, cmd)
	require.NoError(t, err)

	adapter.Stop()
	time.Sleep(100 * time.Millisecond)

	// Test restart capability
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err, "Adapter should be able to restart after being stopped")
	defer cleanupAdapter(adapter)

	// Verify restart maintains functionality
	waitForLeadership(adapter)
	cmd2 := domain.NewPutCommand("after-restart", []byte("test"))
	_, err = adapter.Apply(ctx, cmd2)
	require.NoError(t, err, "Restarted adapter should accept commands")
}

func TestResourceExhaustionAttack(t *testing.T) {
	config := DefaultRaftConfig("exhaust-test", "127.0.0.1:7005", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer cleanupAdapter(adapter)

	waitForLeadership(adapter)

	var wg sync.WaitGroup
	errors := make(chan error, 200)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			largeData := make([]byte, 2*1024*1024)
			cmd := domain.NewPutCommand(fmt.Sprintf("large-%d", id), largeData)

			_, err := adapter.Apply(ctx, cmd)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for range errors {
		errorCount++
	}

	// Most large operations should be rejected due to size limits
	require.Greater(t, errorCount, 80, "Most oversized operations should be rejected")
}

func TestNilAndCorruptedInput(t *testing.T) {
	config := DefaultRaftConfig("nil-test", "127.0.0.1:7006", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer cleanupAdapter(adapter)

	waitForLeadership(adapter)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Panic on nil command: %v", r)
		}
	}()

	_, err = adapter.Apply(ctx, nil)
	require.Error(t, err, "Nil command should be rejected")

	_, err = adapter.Apply(context.Background(), &domain.Command{})
	require.Error(t, err, "Empty command should be rejected")

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := domain.NewPutCommand("test", []byte("data"))
	_, err = adapter.Apply(cancelledCtx, cmd)
	require.Error(t, err, "Cancelled context should be handled properly")
}
