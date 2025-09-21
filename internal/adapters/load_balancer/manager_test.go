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
	events := &mocks.MockEventManager{}

	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(events, "test-node", nil, &Config{FailurePolicy: "fail-open"}, nil)

	ctx := context.Background()
	err := manager.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	if !manager.running {
		t.Error("Manager should be running")
	}

	events.AssertExpectations(t)
}

func TestLoadBalancer_ShouldExecuteNode(t *testing.T) {
	events := &mocks.MockEventManager{}

	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil).Maybe()
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil).Maybe()
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil).Maybe()

	config := &Config{
		Algorithm:     domain.AlgorithmRoundRobin,
		FailurePolicy: "fail-open",
		ScoreCacheTTL: time.Second,
	}

	manager := NewManager(events, "node-1", nil, config, nil)

	// Seed in-memory metrics via ReceiveLoadUpdate (simulating peers)
	_ = manager.ReceiveLoadUpdate(ports.LoadUpdate{NodeID: "node-2", ActiveWorkflows: 1, TotalWeight: 2, RecentLatencyMs: 30, RecentErrorRate: 0.005, Capacity: 10, Timestamp: time.Now().Unix()})
	_ = manager.ReceiveLoadUpdate(ports.LoadUpdate{NodeID: "node-3", ActiveWorkflows: 4, TotalWeight: 8, RecentLatencyMs: 100, RecentErrorRate: 0.02, Capacity: 10, Timestamp: time.Now().Unix()})

	// Decision should be computed without storage
	_, err := manager.ShouldExecuteNode("node-1", "workflow-123", "test-node")
	if err != nil {
		t.Fatalf("Failed to check should execute: %v", err)
	}

	events.AssertExpectations(t)
}

func TestLoadBalancer_EventHandling(t *testing.T) {
	events := &mocks.MockEventManager{}

	events.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
	events.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
	events.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)

	manager := NewManager(events, "test-node", nil, &Config{FailurePolicy: "fail-open"}, nil)

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

	events.AssertExpectations(t)
}
