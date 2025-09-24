package events

import (
	"context"
	"log/slog"
	"testing"

	"github.com/eleven-am/graft/internal/mocks"
)

func TestEventManager_BasicLifecycle(t *testing.T) {
	storage := &mocks.MockStoragePort{}

	manager := NewManager(storage, "test-node", slog.Default())

	ctx := context.Background()
	err := manager.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	if !manager.running {
		t.Error("Manager should be running")
	}

	storage.AssertExpectations(t)
}

func TestEventManager_PatternMatching(t *testing.T) {
	manager := &Manager{}

	tests := []struct {
		pattern string
		key     string
		matches bool
	}{
		{"*", "anything", true},
		{"workflow:*", "workflow:123", true},
		{"workflow:*", "workflow:123:started", true},
		{"workflow:*", "node:123", false},
		{"workflow:123", "workflow:123", true},
		{"workflow:123", "workflow:456", false},
	}

	for _, tt := range tests {
		result := manager.patternMatches(tt.pattern, tt.key)
		if result != tt.matches {
			t.Errorf("patternMatches(%q, %q) = %v, want %v", tt.pattern, tt.key, result, tt.matches)
		}
	}
}
