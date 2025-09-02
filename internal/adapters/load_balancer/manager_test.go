package load_balancer

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/mock"
)

func TestLoadBalancer_BasicLifecycle(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	storage.On("Put", "cluster:load:test-node", mock.Anything, int64(0)).Return(nil)
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "test-node", nil, &Config{FailurePolicy: "fail-open"}, nil)

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
			Value: []byte(`{"node_id":"node-1","total_weight":4.0,"execution_units":{"wf1":2.0,"wf2":2.0},"recent_latency_ms":50,"recent_error_rate":0.01,"last_updated":1}`),
		},
		{
			Key:   "cluster:load:node-2",
			Value: []byte(`{"node_id":"node-2","total_weight":2.0,"execution_units":{"wf3":2.0},"recent_latency_ms":30,"recent_error_rate":0.005,"last_updated":1}`),
		},
		{
			Key:   "cluster:load:node-3",
			Value: []byte(`{"node_id":"node-3","total_weight":8.0,"execution_units":{"wf4":8.0},"recent_latency_ms":100,"recent_error_rate":0.02,"last_updated":1}`),
		},
	}
	storage.On("ListByPrefix", "cluster:load:").Return(kvs, nil)

	config := &Config{
		Algorithm:     domain.AlgorithmRoundRobin,
		FailurePolicy: "fail-open",
		ScoreCacheTTL: time.Second,
	}

	manager := NewManager(storage, events, "node-1", nil, config, nil)

	shouldExecute, err := manager.ShouldExecuteNode("node-1", "workflow-123", "test-node")
	if err != nil {
		t.Fatalf("Failed to check should execute: %v", err)
	}

	_ = shouldExecute

	storage.AssertExpectations(t)
}

func TestLoadBalancer_EventHandling(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	events := &mocks.MockEventManager{}

	storage.On("Put", "cluster:load:test-node", mock.Anything, int64(0)).Return(nil).Times(3)
	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(storage, events, "test-node", nil, &Config{FailurePolicy: "fail-open"}, nil)

	ctx := context.Background()
	err := manager.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	startedEvent := &domain.NodeStartedEvent{
		WorkflowID:  "workflow-123",
		NodeName:    "validator-node",
		StartedAt:   time.Now(),
		ExecutionID: "exec-123",
		NodeID:      "test-node",
	}
	manager.onNodeStarted(startedEvent)

	if manager.totalWeight != 1.0 {
		t.Errorf("Expected total weight to be 1.0, got %f", manager.totalWeight)
	}

	if len(manager.executionUnits) != 1 {
		t.Errorf("Expected 1 execution unit, got %d", len(manager.executionUnits))
	}

	completedEvent := &domain.NodeCompletedEvent{
		WorkflowID:  "workflow-123",
		NodeName:    "validator-node",
		CompletedAt: time.Now(),
		Duration:    50 * time.Millisecond,
		ExecutionID: "exec-123",
	}
	manager.onNodeCompleted(completedEvent)

	if manager.totalWeight != 0.0 {
		t.Errorf("Expected total weight to be 0.0, got %f", manager.totalWeight)
	}

	if len(manager.executionUnits) != 0 {
		t.Errorf("Expected 0 execution units, got %d", len(manager.executionUnits))
	}

	storage.AssertExpectations(t)
	events.AssertExpectations(t)
}
