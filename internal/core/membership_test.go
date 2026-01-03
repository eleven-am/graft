package core

import (
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

func TestManager_ClusterMembershipEvents(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "graft-membership-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := New("test-node", "localhost:17222", tempDir, logger)
	if manager == nil {
		t.Skip("Manager creation failed - requires network infrastructure")
	}

	var joinedEvent *NodeJoinedEvent
	var leftEvent *NodeLeftEvent
	var leaderEvent *LeaderChangedEvent
	var wg sync.WaitGroup

	wg.Add(3)

	err = manager.OnNodeJoined(func(event *NodeJoinedEvent) {
		joinedEvent = event
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Failed to register OnNodeJoined: %v", err)
	}

	err = manager.OnNodeLeft(func(event *NodeLeftEvent) {
		leftEvent = event
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Failed to register OnNodeLeft: %v", err)
	}

	err = manager.OnLeaderChanged(func(event *LeaderChangedEvent) {
		leaderEvent = event
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Failed to register OnLeaderChanged: %v", err)
	}

	testJoinedEvent := &domain.NodeJoinedEvent{
		NodeID:   "node-joined-123",
		Address:  "192.168.1.10:7000",
		JoinedAt: time.Now(),
		Metadata: map[string]interface{}{
			"role": "worker",
			"zone": "us-west-1",
		},
	}

	testLeftEvent := &domain.NodeLeftEvent{
		NodeID:  "node-left-456",
		Address: "192.168.1.11:7000",
		LeftAt:  time.Now(),
		Metadata: map[string]interface{}{
			"reason": "graceful_shutdown",
		},
	}

	testLeaderEvent := &domain.LeaderChangedEvent{
		NewLeaderID:   "node-leader-789",
		NewLeaderAddr: "192.168.1.12:7000",
		PreviousID:    "node-old-leader",
		ChangedAt:     time.Now(),
		Metadata: map[string]interface{}{
			"term":   42,
			"reason": "election",
		},
	}

	if eventManager, ok := manager.eventManager.(interface {
		NotifyNodeJoined(*domain.NodeJoinedEvent)
		NotifyNodeLeft(*domain.NodeLeftEvent)
		NotifyLeaderChanged(*domain.LeaderChangedEvent)
	}); ok {
		eventManager.NotifyNodeJoined(testJoinedEvent)
		eventManager.NotifyNodeLeft(testLeftEvent)
		eventManager.NotifyLeaderChanged(testLeaderEvent)
	} else {
		t.Fatal("EventManager doesn't support notification methods")
	}

	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for events")
	}

	if joinedEvent == nil {
		t.Fatal("NodeJoinedEvent was not received")
	}
	if joinedEvent.NodeID != testJoinedEvent.NodeID {
		t.Errorf("Expected NodeID=%s, got=%s", testJoinedEvent.NodeID, joinedEvent.NodeID)
	}
	if joinedEvent.Address != testJoinedEvent.Address {
		t.Errorf("Expected Address=%s, got=%s", testJoinedEvent.Address, joinedEvent.Address)
	}
	if role, ok := joinedEvent.Metadata["role"].(string); !ok || role != "worker" {
		t.Errorf("Expected role=worker, got=%v", joinedEvent.Metadata["role"])
	}

	if leftEvent == nil {
		t.Fatal("NodeLeftEvent was not received")
	}
	if leftEvent.NodeID != testLeftEvent.NodeID {
		t.Errorf("Expected NodeID=%s, got=%s", testLeftEvent.NodeID, leftEvent.NodeID)
	}
	if leftEvent.Address != testLeftEvent.Address {
		t.Errorf("Expected Address=%s, got=%s", testLeftEvent.Address, leftEvent.Address)
	}
	if reason, ok := leftEvent.Metadata["reason"].(string); !ok || reason != "graceful_shutdown" {
		t.Errorf("Expected reason=graceful_shutdown, got=%v", leftEvent.Metadata["reason"])
	}

	if leaderEvent == nil {
		t.Fatal("LeaderChangedEvent was not received")
	}
	if leaderEvent.NewLeaderID != testLeaderEvent.NewLeaderID {
		t.Errorf("Expected NewLeaderID=%s, got=%s", testLeaderEvent.NewLeaderID, leaderEvent.NewLeaderID)
	}
	if leaderEvent.NewLeaderAddr != testLeaderEvent.NewLeaderAddr {
		t.Errorf("Expected NewLeaderAddr=%s, got=%s", testLeaderEvent.NewLeaderAddr, leaderEvent.NewLeaderAddr)
	}
	if leaderEvent.PreviousID != testLeaderEvent.PreviousID {
		t.Errorf("Expected PreviousID=%s, got=%s", testLeaderEvent.PreviousID, leaderEvent.PreviousID)
	}
	if term, ok := leaderEvent.Metadata["term"].(int); !ok || term != 42 {
		t.Errorf("Expected term=42, got=%v", leaderEvent.Metadata["term"])
	}
}

func TestManager_MultipleClusterEventHandlers(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "graft-multi-handlers-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := New("test-node", "localhost:17223", tempDir, logger)
	if manager == nil {
		t.Skip("Manager creation failed - requires network infrastructure")
	}

	var handler1Called, handler2Called, handler3Called bool
	var wg sync.WaitGroup
	wg.Add(3)

	manager.OnNodeJoined(func(event *NodeJoinedEvent) {
		handler1Called = true
		wg.Done()
	})

	manager.OnNodeJoined(func(event *NodeJoinedEvent) {
		handler2Called = true
		wg.Done()
	})

	manager.OnNodeJoined(func(event *NodeJoinedEvent) {
		handler3Called = true
		wg.Done()
	})

	testEvent := &domain.NodeJoinedEvent{
		NodeID:   "multi-handler-node",
		Address:  "192.168.1.99:7000",
		JoinedAt: time.Now(),
		Metadata: make(map[string]interface{}),
	}

	if eventManager, ok := manager.eventManager.(interface {
		NotifyNodeJoined(*domain.NodeJoinedEvent)
	}); ok {
		eventManager.NotifyNodeJoined(testEvent)
	} else {
		t.Fatal("EventManager doesn't support NotifyNodeJoined")
	}

	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for handlers")
	}

	if !handler1Called {
		t.Error("Handler 1 was not called")
	}
	if !handler2Called {
		t.Error("Handler 2 was not called")
	}
	if !handler3Called {
		t.Error("Handler 3 was not called")
	}
}

func TestManager_ClusterEventErrorHandling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "graft-error-handling-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := New("test-node", "localhost:17224", tempDir, logger)
	if manager == nil {
		t.Skip("Manager creation failed - requires network infrastructure")
	}

	var normalHandlerCalled bool
	var wg sync.WaitGroup
	wg.Add(1)

	manager.OnNodeJoined(func(event *NodeJoinedEvent) {
		panic("this handler panics")
	})

	manager.OnNodeJoined(func(event *NodeJoinedEvent) {
		normalHandlerCalled = true
		wg.Done()
	})

	testEvent := &domain.NodeJoinedEvent{
		NodeID:   "panic-test-node",
		Address:  "192.168.1.88:7000",
		JoinedAt: time.Now(),
		Metadata: make(map[string]interface{}),
	}

	if eventManager, ok := manager.eventManager.(interface {
		NotifyNodeJoined(*domain.NodeJoinedEvent)
	}); ok {
		eventManager.NotifyNodeJoined(testEvent)
	} else {
		t.Fatal("EventManager doesn't support NotifyNodeJoined")
	}

	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for normal handler")
	}

	if !normalHandlerCalled {
		t.Error("Normal handler should be called even when other handlers panic")
	}
}

func TestManager_TypeAliases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "graft-type-aliases-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := New("test-node", "localhost:17225", tempDir, logger)
	if manager == nil {
		t.Skip("Manager creation failed - requires network infrastructure")
	}

	var wg sync.WaitGroup
	wg.Add(3)

	manager.OnNodeJoined(func(event *NodeJoinedEvent) {
		if event.NodeID == "" {
			t.Error("NodeID should not be empty")
		}
		wg.Done()
	})

	manager.OnNodeLeft(func(event *NodeLeftEvent) {
		if event.NodeID == "" {
			t.Error("NodeID should not be empty")
		}
		wg.Done()
	})

	manager.OnLeaderChanged(func(event *LeaderChangedEvent) {
		if event.NewLeaderID == "" {
			t.Error("NewLeaderID should not be empty")
		}
		wg.Done()
	})

	if eventManager, ok := manager.eventManager.(interface {
		NotifyNodeJoined(*domain.NodeJoinedEvent)
		NotifyNodeLeft(*domain.NodeLeftEvent)
		NotifyLeaderChanged(*domain.LeaderChangedEvent)
	}); ok {
		eventManager.NotifyNodeJoined(&domain.NodeJoinedEvent{
			NodeID: "type-test-joined", Address: "addr", JoinedAt: time.Now(), Metadata: make(map[string]interface{}),
		})
		eventManager.NotifyNodeLeft(&domain.NodeLeftEvent{
			NodeID: "type-test-left", Address: "addr", LeftAt: time.Now(), Metadata: make(map[string]interface{}),
		})
		eventManager.NotifyLeaderChanged(&domain.LeaderChangedEvent{
			NewLeaderID: "type-test-leader", NewLeaderAddr: "addr", ChangedAt: time.Now(), Metadata: make(map[string]interface{}),
		})
	}

	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for type alias handlers")
	}
}

func TestManager_ClusterEventConcurrency(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "graft-concurrency-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := New("test-node", "localhost:17226", tempDir, logger)
	if manager == nil {
		t.Skip("Manager creation failed - requires network infrastructure")
	}

	const numEvents = 50
	var receivedCount int
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(numEvents * 3)

	manager.OnNodeJoined(func(event *NodeJoinedEvent) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		wg.Done()
	})

	manager.OnNodeLeft(func(event *NodeLeftEvent) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		wg.Done()
	})

	manager.OnLeaderChanged(func(event *LeaderChangedEvent) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		wg.Done()
	})

	if eventManager, ok := manager.eventManager.(interface {
		NotifyNodeJoined(*domain.NodeJoinedEvent)
		NotifyNodeLeft(*domain.NodeLeftEvent)
		NotifyLeaderChanged(*domain.LeaderChangedEvent)
	}); ok {
		for i := 0; i < numEvents; i++ {
			go func(id int) {
				eventManager.NotifyNodeJoined(&domain.NodeJoinedEvent{
					NodeID: "concurrent-joined-" + string(rune(id)), Address: "addr", JoinedAt: time.Now(), Metadata: make(map[string]interface{}),
				})
				eventManager.NotifyNodeLeft(&domain.NodeLeftEvent{
					NodeID: "concurrent-left-" + string(rune(id)), Address: "addr", LeftAt: time.Now(), Metadata: make(map[string]interface{}),
				})
				eventManager.NotifyLeaderChanged(&domain.LeaderChangedEvent{
					NewLeaderID: "concurrent-leader-" + string(rune(id)), NewLeaderAddr: "addr", ChangedAt: time.Now(), Metadata: make(map[string]interface{}),
				})
			}(i)
		}
	}

	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for concurrent events")
	}

	mu.Lock()
	finalCount := receivedCount
	mu.Unlock()

	expectedCount := numEvents * 3
	if finalCount != expectedCount {
		t.Errorf("Expected %d events, got %d", expectedCount, finalCount)
	}
}
