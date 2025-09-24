package storage

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/adapters/events"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
)

// Verifies that storage publishes to EventManager hub after Raft apply
func TestAppStorage_PublishToEventManager(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	mockRaft.On("IsLeader").Return(true).Maybe()

	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	ev := events.NewManager(storage, "test-node", slog.New(slog.NewTextHandler(os.Stdout, nil)))
	storage.SetEventManager(ev)
	require.NoError(t, ev.Start(context.Background()))
	defer ev.Stop()

	// Subscribe to workflow: prefix
	ch, unsub, err := ev.SubscribeToChannel("workflow:")
	require.NoError(t, err)
	defer unsub()

	// Mock Raft Apply to return a single event
	key := domain.WorkflowStateKey("testwf")
	mockRaft.On("Apply", mock.Anything, mock.Anything).Return(&domain.CommandResult{
		Success: true,
		Events: []domain.Event{{
			Type:      domain.EventPut,
			Key:       key,
			Version:   1,
			NodeID:    "node-1",
			Timestamp: time.Now(),
		}},
	}, nil)

	// Perform Put (leader path: publishes immediately)
	require.NoError(t, storage.Put(key, []byte("v"), 1))

	select {
	case e := <-ch:
		require.Equal(t, key, e.Key)
		require.Equal(t, int64(1), e.Version)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for event from EventManager")
	}
}

func TestAppStorage_FollowerPublishesAfterTimeout(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	// Simulate follower
	mockRaft.On("IsLeader").Return(false).Maybe()

	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	ev := events.NewManager(storage, "test-node", slog.New(slog.NewTextHandler(os.Stdout, nil)))
	storage.SetEventManager(ev)
	require.NoError(t, ev.Start(context.Background()))
	defer ev.Stop()

	key := domain.WorkflowStateKey("followercase")
	ch2, unsub2, err := ev.SubscribeToChannel("workflow:")
	require.NoError(t, err)
	defer unsub2()

	// Raft Apply returns an event but DB won't reflect version (no real FSM),
	// so waitForVersionsAndBroadcast will time out (~200ms) before publishing
	mockRaft.On("Apply", mock.Anything, mock.Anything).Return(&domain.CommandResult{
		Success: true,
		Events: []domain.Event{{
			Type:      domain.EventPut,
			Key:       key,
			Version:   5,
			NodeID:    "node-follower",
			Timestamp: time.Now(),
		}},
	}, nil)

	start := time.Now()
	go func() { _ = storage.Put(key, []byte("v"), 5) }()

	select {
	case e := <-ch2:
		elapsed := time.Since(start)
		// Expect publish after ~200ms timeout; allow some slack
		if elapsed < 180*time.Millisecond {
			t.Fatalf("event published too early: %v", elapsed)
		}
		require.Equal(t, key, e.Key)
		require.Equal(t, int64(5), e.Version)
	case <-time.After(750 * time.Millisecond):
		t.Fatal("timed out waiting for follower-published event")
	}
}

// helper matchers for testify mock without importing mock directly here
// using testify/mock.Anything in expectations above
