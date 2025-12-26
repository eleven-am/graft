package core

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

func TestManager_ClusterCommandMethods(t *testing.T) {
	config := domain.NewConfigFromSimple("test-node", "127.0.0.1:9000", t.TempDir(), slog.Default())
	config.Raft.DiscoveryTimeout = 100 * time.Millisecond
	manager := NewWithConfig(config)
	if manager == nil {
		t.Fatal("Failed to create manager")
	}

	ctx := context.Background()
	err := manager.Start(ctx, 0)
	if err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	handler := func(ctx context.Context, from string, params interface{}) error {
		return nil
	}

	err = manager.RegisterCommandHandler("test", handler)
	if err != nil {
		t.Errorf("RegisterCommandHandler failed: %v", err)
	}

	devCmd := &domain.DevCommand{
		Command: "test",
		Params: map[string]interface{}{
			"message": "hello world",
		},
	}

	leaderCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := manager.raftAdapter.WaitForLeader(leaderCtx); err != nil {
		t.Fatalf("leader not established in time: %v", err)
	}

	// Retry BroadcastCommand until it works or timeout is reached
	// Single-node Raft clusters need time to elect themselves as leader
	var lastErr error
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Errorf("BroadcastCommand failed after timeout. Last error: %v", lastErr)
			return
		case <-ticker.C:
			err = manager.BroadcastCommand(ctx, devCmd)
			if err == nil {

				return
			}
			lastErr = err
		}
	}
}
