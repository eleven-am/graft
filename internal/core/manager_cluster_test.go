package core

import (
	"context"
	"log/slog"
	"testing"

	"github.com/eleven-am/graft/internal/domain"
)

func TestManager_ClusterCommandMethods(t *testing.T) {
	manager := New("test-node", "127.0.0.1:0", t.TempDir(), slog.Default())
	if manager == nil {
		t.Fatal("Failed to create manager")
	}

	handler := func(ctx context.Context, from string, params interface{}) error {
		return nil
	}

	err := manager.RegisterCommandHandler("test", handler)
	if err != nil {
		t.Errorf("RegisterCommandHandler failed: %v", err)
	}

	devCmd := &domain.DevCommand{
		Command: "test",
		Params: map[string]interface{}{
			"message": "hello world",
		},
	}

	ctx := context.Background()
	err = manager.BroadcastCommand(ctx, devCmd)
	if err != nil {
		t.Errorf("BroadcastCommand failed: %v", err)
	}
}