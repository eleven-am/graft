package raft

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

type mockEventManagerForNotifications struct {
	*mocks.MockEventManager
	mu                  sync.Mutex
	nodeJoinedEvents    []*domain.NodeJoinedEvent
	nodeLeftEvents      []*domain.NodeLeftEvent
	leaderChangedEvents []*domain.LeaderChangedEvent
}

func newMockEventManagerForNotifications(t *testing.T) *mockEventManagerForNotifications {
	return &mockEventManagerForNotifications{
		MockEventManager: mocks.NewMockEventManager(t),
	}
}

func (m *mockEventManagerForNotifications) NotifyNodeJoined(event *domain.NodeJoinedEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeJoinedEvents = append(m.nodeJoinedEvents, event)
}

func (m *mockEventManagerForNotifications) NotifyNodeLeft(event *domain.NodeLeftEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeLeftEvents = append(m.nodeLeftEvents, event)
}

func (m *mockEventManagerForNotifications) NotifyLeaderChanged(event *domain.LeaderChangedEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderChangedEvents = append(m.leaderChangedEvents, event)
}

func (m *mockEventManagerForNotifications) GetNodeJoinedEvents() []*domain.NodeJoinedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	events := make([]*domain.NodeJoinedEvent, len(m.nodeJoinedEvents))
	copy(events, m.nodeJoinedEvents)
	return events
}

func (m *mockEventManagerForNotifications) GetNodeLeftEvents() []*domain.NodeLeftEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	events := make([]*domain.NodeLeftEvent, len(m.nodeLeftEvents))
	copy(events, m.nodeLeftEvents)
	return events
}

func (m *mockEventManagerForNotifications) GetLeaderChangedEvents() []*domain.LeaderChangedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	events := make([]*domain.LeaderChangedEvent, len(m.leaderChangedEvents))
	copy(events, m.leaderChangedEvents)
	return events
}

func TestNode_handleObservation_PeerJoined(t *testing.T) {
	mockEM := newMockEventManagerForNotifications(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	node := &Node{
		eventManager: mockEM,
		logger:       logger,
	}

	peerObs := raft.Observation{
		Data: raft.PeerObservation{
			Peer: raft.Server{
				ID:      "test-node-123",
				Address: "192.168.1.100:7000",
			},
			Removed: false,
		},
	}

	node.handleObservation(peerObs)

	events := mockEM.GetNodeJoinedEvents()
	assert.Len(t, events, 1)

	event := events[0]
	assert.Equal(t, "test-node-123", event.NodeID)
	assert.Equal(t, "192.168.1.100:7000", event.Address)
	assert.NotNil(t, event.Metadata)
	assert.WithinDuration(t, time.Now(), event.JoinedAt, time.Second)
}

func TestNode_handleObservation_PeerLeft(t *testing.T) {
	mockEM := newMockEventManagerForNotifications(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	node := &Node{
		eventManager: mockEM,
		logger:       logger,
	}

	peerObs := raft.Observation{
		Data: raft.PeerObservation{
			Peer: raft.Server{
				ID:      "test-node-456",
				Address: "192.168.1.101:7000",
			},
			Removed: true,
		},
	}

	node.handleObservation(peerObs)

	events := mockEM.GetNodeLeftEvents()
	assert.Len(t, events, 1)

	event := events[0]
	assert.Equal(t, "test-node-456", event.NodeID)
	assert.Equal(t, "192.168.1.101:7000", event.Address)
	assert.NotNil(t, event.Metadata)
	assert.WithinDuration(t, time.Now(), event.LeftAt, time.Second)
}

func TestNode_handleObservation_LeaderChanged(t *testing.T) {
	mockEM := newMockEventManagerForNotifications(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	node := &Node{
		eventManager: mockEM,
		logger:       logger,
	}

	leaderObs := raft.Observation{
		Data: raft.LeaderObservation{
			LeaderID:   "new-leader-789",
			LeaderAddr: "192.168.1.102:7000",
		},
	}

	node.handleObservation(leaderObs)

	events := mockEM.GetLeaderChangedEvents()
	assert.Len(t, events, 1)

	event := events[0]
	assert.Equal(t, "new-leader-789", event.NewLeaderID)
	assert.Equal(t, "192.168.1.102:7000", event.NewLeaderAddr)
	assert.NotNil(t, event.Metadata)
	assert.WithinDuration(t, time.Now(), event.ChangedAt, time.Second)
}

func TestNode_handleObservation_NilEventManager(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	node := &Node{
		eventManager: nil,
		logger:       logger,
	}

	peerObs := raft.Observation{
		Data: raft.PeerObservation{
			Peer: raft.Server{
				ID:      "test-node",
				Address: "192.168.1.100:7000",
			},
			Removed: false,
		},
	}

	assert.NotPanics(t, func() {
		node.handleObservation(peerObs)
	})
}

func TestNode_handleObservation_UnknownObservationType(t *testing.T) {
	mockEM := newMockEventManagerForNotifications(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	node := &Node{
		eventManager: mockEM,
		logger:       logger,
	}

	unknownObs := raft.Observation{
		Data: "unknown observation type",
	}

	assert.NotPanics(t, func() {
		node.handleObservation(unknownObs)
	})

	assert.Empty(t, mockEM.GetNodeJoinedEvents())
	assert.Empty(t, mockEM.GetNodeLeftEvents())
	assert.Empty(t, mockEM.GetLeaderChangedEvents())
}

func TestNode_processObservations_ContextCancel(t *testing.T) {
	mockEM := newMockEventManagerForNotifications(t)
	node := &Node{
		eventManager: mockEM,
		observerChan: make(chan raft.Observation, 10),
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		node.processObservations(ctx)
		done <- true
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processObservations did not exit after context cancel")
	}
}

func TestNode_handleObservation_EventManagerWithoutNotifyMethods(t *testing.T) {
	mockEM := mocks.NewMockEventManager(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	node := &Node{
		eventManager: mockEM,
		logger:       logger,
	}

	peerObs := raft.Observation{
		Data: raft.PeerObservation{
			Peer: raft.Server{
				ID:      "type-assertion-test",
				Address: "192.168.1.150:7000",
			},
			Removed: false,
		},
	}

	assert.NotPanics(t, func() {
		node.handleObservation(peerObs)
	})
}
