package load_balancer

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/stretchr/testify/mock"
)

func TestLoadBalancer_BasicLifecycle(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}
	
	storage.On("Put", "cluster:load:test-node", mock.Anything, int64(0)).Return(nil)
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "test-node", nil)

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
	events.AssertExpectations(t)
}

func TestLoadBalancer_ShouldExecuteNode(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	kvs := []ports.KeyValueVersion{
		{
			Key:   "cluster:load:node-1",
			Value: []byte(`{"node_id":"node-1","active_workflows":2,"last_updated":1}`),
		},
		{
			Key:   "cluster:load:node-2", 
			Value: []byte(`{"node_id":"node-2","active_workflows":1,"last_updated":1}`),
		},
		{
			Key:   "cluster:load:node-3",
			Value: []byte(`{"node_id":"node-3","active_workflows":3,"last_updated":1}`),
		},
	}
	storage.On("ListByPrefix", "cluster:load:").Return(kvs, nil)

	manager := NewManager(storage, events, "node-1", nil)

	shouldExecute, err := manager.ShouldExecuteNode("node-1", "workflow-123", "test-node")
	if err != nil {
		t.Fatalf("Failed to check should execute: %v", err)
	}

	if shouldExecute {
		t.Error("Node-1 should not execute when node-2 has lower load")
	}

	storage.AssertExpectations(t)
}

func TestLoadBalancer_EventHandling(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	storage.On("Put", "cluster:load:test-node", mock.Anything, int64(0)).Return(nil).Times(3)
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil) 
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "test-node", nil)

	ctx := context.Background()
	err := manager.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	startedEvent := &domain.NodeStartedEvent{
		WorkflowID:  "workflow-123",
		NodeName:    "test-node-name",
		StartedAt:   time.Now(),
		ExecutionID: "exec-123",
		NodeID:      "test-node",
	}
	manager.onNodeStarted(startedEvent)

	if manager.localActiveCount != 1 {
		t.Errorf("Expected local active count to be 1, got %d", manager.localActiveCount)
	}

	completedEvent := &domain.NodeCompletedEvent{
		WorkflowID:  "workflow-123",
		NodeName:    "test-node-name", 
		CompletedAt: time.Now(),
		ExecutionID: "exec-123",
	}
	manager.onNodeCompleted(completedEvent)

	if manager.localActiveCount != 0 {
		t.Errorf("Expected local active count to be 0, got %d", manager.localActiveCount)
	}

	storage.AssertExpectations(t)
	events.AssertExpectations(t)
}