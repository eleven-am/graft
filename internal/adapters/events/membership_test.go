package events

import (
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

func TestManager_OnNodeJoined(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := NewManager(logger)

	var receivedEvent *domain.NodeJoinedEvent
	var wg sync.WaitGroup
	wg.Add(1)

	err := manager.OnNodeJoined(func(event *domain.NodeJoinedEvent) {
		receivedEvent = event
		wg.Done()
	})

	if err != nil {
		t.Fatalf("Expected no error registering handler, got: %v", err)
	}

	testEvent := &domain.NodeJoinedEvent{
		NodeID:   "node-123",
		Address:  "192.168.1.100:7000",
		JoinedAt: time.Now(),
		Metadata: map[string]interface{}{
			"role": "worker",
		},
	}

	manager.NotifyNodeJoined(testEvent)

	wg.Wait()

	if receivedEvent == nil {
		t.Fatal("Handler was not called")
	}

	if receivedEvent.NodeID != testEvent.NodeID {
		t.Errorf("Expected NodeID=%s, got=%s", testEvent.NodeID, receivedEvent.NodeID)
	}

	if receivedEvent.Address != testEvent.Address {
		t.Errorf("Expected Address=%s, got=%s", testEvent.Address, receivedEvent.Address)
	}

	if role, ok := receivedEvent.Metadata["role"].(string); !ok || role != "worker" {
		t.Errorf("Expected metadata role=worker, got=%v", receivedEvent.Metadata["role"])
	}
}

func TestManager_OnNodeLeft(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := NewManager(logger)

	var receivedEvent *domain.NodeLeftEvent
	var wg sync.WaitGroup
	wg.Add(1)

	err := manager.OnNodeLeft(func(event *domain.NodeLeftEvent) {
		receivedEvent = event
		wg.Done()
	})

	if err != nil {
		t.Fatalf("Expected no error registering handler, got: %v", err)
	}

	testEvent := &domain.NodeLeftEvent{
		NodeID:  "node-456",
		Address: "192.168.1.101:7000",
		LeftAt:  time.Now(),
		Metadata: map[string]interface{}{
			"reason": "shutdown",
		},
	}

	manager.NotifyNodeLeft(testEvent)

	wg.Wait()

	if receivedEvent == nil {
		t.Fatal("Handler was not called")
	}

	if receivedEvent.NodeID != testEvent.NodeID {
		t.Errorf("Expected NodeID=%s, got=%s", testEvent.NodeID, receivedEvent.NodeID)
	}

	if receivedEvent.Address != testEvent.Address {
		t.Errorf("Expected Address=%s, got=%s", testEvent.Address, receivedEvent.Address)
	}

	if reason, ok := receivedEvent.Metadata["reason"].(string); !ok || reason != "shutdown" {
		t.Errorf("Expected metadata reason=shutdown, got=%v", receivedEvent.Metadata["reason"])
	}
}

func TestManager_OnLeaderChanged(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := NewManager(logger)

	var receivedEvent *domain.LeaderChangedEvent
	var wg sync.WaitGroup
	wg.Add(1)

	err := manager.OnLeaderChanged(func(event *domain.LeaderChangedEvent) {
		receivedEvent = event
		wg.Done()
	})

	if err != nil {
		t.Fatalf("Expected no error registering handler, got: %v", err)
	}

	testEvent := &domain.LeaderChangedEvent{
		NewLeaderID:   "node-789",
		NewLeaderAddr: "192.168.1.102:7000",
		PreviousID:    "node-456",
		ChangedAt:     time.Now(),
		Metadata: map[string]interface{}{
			"term": 5,
		},
	}

	manager.NotifyLeaderChanged(testEvent)

	wg.Wait()

	if receivedEvent == nil {
		t.Fatal("Handler was not called")
	}

	if receivedEvent.NewLeaderID != testEvent.NewLeaderID {
		t.Errorf("Expected NewLeaderID=%s, got=%s", testEvent.NewLeaderID, receivedEvent.NewLeaderID)
	}

	if receivedEvent.NewLeaderAddr != testEvent.NewLeaderAddr {
		t.Errorf("Expected NewLeaderAddr=%s, got=%s", testEvent.NewLeaderAddr, receivedEvent.NewLeaderAddr)
	}

	if receivedEvent.PreviousID != testEvent.PreviousID {
		t.Errorf("Expected PreviousID=%s, got=%s", testEvent.PreviousID, receivedEvent.PreviousID)
	}

	if term, ok := receivedEvent.Metadata["term"].(int); !ok || term != 5 {
		t.Errorf("Expected metadata term=5, got=%v", receivedEvent.Metadata["term"])
	}
}

func TestManager_MultipleHandlers(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := NewManager(logger)

	var handler1Called, handler2Called bool
	var wg sync.WaitGroup
	wg.Add(2)

	manager.OnNodeJoined(func(event *domain.NodeJoinedEvent) {
		handler1Called = true
		wg.Done()
	})

	manager.OnNodeJoined(func(event *domain.NodeJoinedEvent) {
		handler2Called = true
		wg.Done()
	})

	testEvent := &domain.NodeJoinedEvent{
		NodeID:   "node-multi",
		Address:  "192.168.1.200:7000",
		JoinedAt: time.Now(),
		Metadata: make(map[string]interface{}),
	}

	manager.NotifyNodeJoined(testEvent)

	wg.Wait()

	if !handler1Called {
		t.Error("Handler 1 was not called")
	}

	if !handler2Called {
		t.Error("Handler 2 was not called")
	}
}

func TestManager_ConcurrentNotifications(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := NewManager(logger)

	const numNotifications = 100
	var receivedCount int
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(numNotifications)

	manager.OnNodeJoined(func(event *domain.NodeJoinedEvent) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		wg.Done()
	})

	for i := 0; i < numNotifications; i++ {
		go func(id int) {
			testEvent := &domain.NodeJoinedEvent{
				NodeID:   "concurrent-node-" + string(rune(id)),
				Address:  "192.168.1.100:7000",
				JoinedAt: time.Now(),
				Metadata: make(map[string]interface{}),
			}
			manager.NotifyNodeJoined(testEvent)
		}(i)
	}

	wg.Wait()

	mu.Lock()
	finalCount := receivedCount
	mu.Unlock()

	if finalCount != numNotifications {
		t.Errorf("Expected %d notifications, got %d", numNotifications, finalCount)
	}
}

func TestManager_EventsWithEmptyMetadata(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := NewManager(logger)

	var receivedEvent *domain.NodeJoinedEvent
	var wg sync.WaitGroup
	wg.Add(1)

	manager.OnNodeJoined(func(event *domain.NodeJoinedEvent) {
		receivedEvent = event
		wg.Done()
	})

	testEvent := &domain.NodeJoinedEvent{
		NodeID:   "node-empty-meta",
		Address:  "192.168.1.50:7000",
		JoinedAt: time.Now(),
		Metadata: make(map[string]interface{}),
	}

	manager.NotifyNodeJoined(testEvent)

	wg.Wait()

	if receivedEvent == nil {
		t.Fatal("Handler was not called")
	}

	if receivedEvent.Metadata == nil {
		t.Error("Expected non-nil metadata map")
	}

	if len(receivedEvent.Metadata) != 0 {
		t.Errorf("Expected empty metadata, got %v", receivedEvent.Metadata)
	}
}

func TestManager_EventsWithNilMetadata(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := NewManager(logger)

	var receivedEvent *domain.NodeJoinedEvent
	var wg sync.WaitGroup
	wg.Add(1)

	manager.OnNodeJoined(func(event *domain.NodeJoinedEvent) {
		receivedEvent = event
		wg.Done()
	})

	testEvent := &domain.NodeJoinedEvent{
		NodeID:   "node-nil-meta",
		Address:  "192.168.1.51:7000",
		JoinedAt: time.Now(),
		Metadata: nil,
	}

	manager.NotifyNodeJoined(testEvent)

	wg.Wait()

	if receivedEvent == nil {
		t.Fatal("Handler was not called")
	}

	if receivedEvent.Metadata != nil {
		t.Errorf("Expected nil metadata, got %v", receivedEvent.Metadata)
	}
}

func TestManager_HandlerPanic(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := NewManager(logger)

	var normalHandlerCalled bool
	var wg sync.WaitGroup
	wg.Add(1)

	manager.OnNodeJoined(func(event *domain.NodeJoinedEvent) {
		panic("handler panic")
	})

	manager.OnNodeJoined(func(event *domain.NodeJoinedEvent) {
		normalHandlerCalled = true
		wg.Done()
	})

	testEvent := &domain.NodeJoinedEvent{
		NodeID:   "node-panic",
		Address:  "192.168.1.52:7000",
		JoinedAt: time.Now(),
		Metadata: make(map[string]interface{}),
	}

	manager.NotifyNodeJoined(testEvent)

	wg.Wait()

	if !normalHandlerCalled {
		t.Error("Normal handler should still be called even if another handler panics")
	}
}

func TestManager_EventSequence(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager := NewManager(logger)

	var events []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	manager.OnNodeJoined(func(event *domain.NodeJoinedEvent) {
		mu.Lock()
		events = append(events, "joined:"+event.NodeID)
		mu.Unlock()
		wg.Done()
	})

	manager.OnNodeLeft(func(event *domain.NodeLeftEvent) {
		mu.Lock()
		events = append(events, "left:"+event.NodeID)
		mu.Unlock()
		wg.Done()
	})

	manager.OnLeaderChanged(func(event *domain.LeaderChangedEvent) {
		mu.Lock()
		events = append(events, "leader:"+event.NewLeaderID)
		mu.Unlock()
		wg.Done()
	})

	wg.Add(4)

	manager.NotifyNodeJoined(&domain.NodeJoinedEvent{
		NodeID: "node-1", Address: "addr1", JoinedAt: time.Now(), Metadata: make(map[string]interface{}),
	})

	manager.NotifyNodeJoined(&domain.NodeJoinedEvent{
		NodeID: "node-2", Address: "addr2", JoinedAt: time.Now(), Metadata: make(map[string]interface{}),
	})

	manager.NotifyLeaderChanged(&domain.LeaderChangedEvent{
		NewLeaderID: "node-1", NewLeaderAddr: "addr1", ChangedAt: time.Now(), Metadata: make(map[string]interface{}),
	})

	manager.NotifyNodeLeft(&domain.NodeLeftEvent{
		NodeID: "node-2", Address: "addr2", LeftAt: time.Now(), Metadata: make(map[string]interface{}),
	})

	wg.Wait()

	mu.Lock()
	finalEvents := make([]string, len(events))
	copy(finalEvents, events)
	mu.Unlock()

	expectedEvents := map[string]bool{"joined:node-1": true, "joined:node-2": true, "leader:node-1": true, "left:node-2": true}
	if len(finalEvents) != len(expectedEvents) {
		t.Errorf("Expected %d events, got %d: %v", len(expectedEvents), len(finalEvents), finalEvents)
	}

	for _, event := range finalEvents {
		if !expectedEvents[event] {
			t.Errorf("Unexpected event: %s", event)
		}
	}
}
