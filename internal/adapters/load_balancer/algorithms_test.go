package load_balancer

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

func TestRoundRobinStrategy(t *testing.T) {
	logger := slog.Default()
	strategy := NewRoundRobinStrategy(logger)

	nodes := []NodeMetrics{
		{NodeID: "node1", Available: true},
		{NodeID: "node2", Available: true},
		{NodeID: "node3", Available: true},
	}

	ctx := context.Background()
	selections := make([]string, 6)

	for i := 0; i < 6; i++ {
		selected, err := strategy.SelectNode(ctx, nodes, "workflow-1")
		if err != nil {
			t.Fatalf("SelectNode failed: %v", err)
		}
		selections[i] = selected
	}

	expected := []string{"node2", "node3", "node1", "node2", "node3", "node1"}
	for i, expected := range expected {
		if selections[i] != expected {
			t.Errorf("Selection %d: expected %s, got %s", i, expected, selections[i])
		}
	}
}

func TestWeightedRoundRobinStrategy(t *testing.T) {
	logger := slog.Default()
	config := domain.WeightedRoundRobinConfig{
		DefaultWeight: 1,
		NodeWeights: map[string]int{
			"node1": 3,
			"node2": 2,
			"node3": 1,
		},
		SmoothWeighting: true,
	}

	strategy := NewWeightedRoundRobinStrategy(config, logger)

	nodes := []NodeMetrics{
		{NodeID: "node1", Available: true},
		{NodeID: "node2", Available: true},
		{NodeID: "node3", Available: true},
	}

	ctx := context.Background()
	selections := make(map[string]int)

	for i := 0; i < 60; i++ {
		selected, err := strategy.SelectNode(ctx, nodes, "workflow-1")
		if err != nil {
			t.Fatalf("SelectNode failed: %v", err)
		}
		selections[selected]++
	}

	if selections["node1"] <= selections["node2"] || selections["node2"] <= selections["node3"] {
		t.Errorf("Weight distribution incorrect: %v", selections)
	}

	t.Logf("Weight distribution: %v", selections)
}

func TestLeastConnectionsStrategy(t *testing.T) {
	logger := slog.Default()
	strategy := NewLeastConnectionsStrategy(logger)

	nodes := []NodeMetrics{
		{NodeID: "node1", ConnectionCount: 5, Available: true},
		{NodeID: "node2", ConnectionCount: 2, Available: true},
		{NodeID: "node3", ConnectionCount: 8, Available: true},
	}

	ctx := context.Background()
	selected, err := strategy.SelectNode(ctx, nodes, "workflow-1")
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	if selected != "node2" {
		t.Errorf("Expected node2, got %s", selected)
	}
}

func TestAdaptiveStrategy(t *testing.T) {
	logger := slog.Default()
	config := domain.AdaptiveLoadBalancerConfig{
		ResponseTimeWeight:    0.5,
		CpuUsageWeight:        0.2,
		MemoryUsageWeight:     0.1,
		ConnectionCountWeight: 0.1,
		ErrorRateWeight:       0.1,
		AdaptationInterval:    time.Second,
		HistoryWindow:         5 * time.Minute,
	}

	strategy := NewAdaptiveStrategy(config, logger)

	nodes := []NodeMetrics{
		{
			NodeID:       "node1",
			ResponseTime: 100,
			CpuUsage:     50,
			MemoryUsage:  60,
			ErrorRate:    0.01,
			Available:    true,
		},
		{
			NodeID:       "node2",
			ResponseTime: 200,
			CpuUsage:     80,
			MemoryUsage:  90,
			ErrorRate:    0.05,
			Available:    true,
		},
		{
			NodeID:       "node3",
			ResponseTime: 50,
			CpuUsage:     30,
			MemoryUsage:  40,
			ErrorRate:    0.005,
			Available:    true,
		},
	}

	ctx := context.Background()
	selected, err := strategy.SelectNode(ctx, nodes, "workflow-1")
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	if selected != "node3" {
		t.Errorf("Expected node3 (best performer), got %s", selected)
	}

	strategy.UpdateMetrics("node1", nodes[0])

	metrics := strategy.GetAlgorithmMetrics()
	if metrics["algorithm"] != "adaptive" {
		t.Errorf("Expected adaptive algorithm, got %v", metrics["algorithm"])
	}
}

func TestConsistentHashStrategy(t *testing.T) {
	logger := slog.Default()
	strategy := NewConsistentHashStrategy(logger)

	nodes := []NodeMetrics{
		{NodeID: "node1", Available: true},
		{NodeID: "node2", Available: true},
		{NodeID: "node3", Available: true},
	}

	ctx := context.Background()

	selected1, err := strategy.SelectNode(ctx, nodes, "workflow-123")
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	selected2, err := strategy.SelectNode(ctx, nodes, "workflow-123")
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	if selected1 != selected2 {
		t.Errorf("Consistent hash failed: %s != %s for same workflow", selected1, selected2)
	}

	selected3, err := strategy.SelectNode(ctx, nodes, "workflow-456")
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	_ = selected3

	t.Logf("Workflow 123 -> %s", selected1)
	t.Logf("Workflow 456 -> %s", selected3)
}

func TestFilterAvailableNodes(t *testing.T) {
	nodes := []NodeMetrics{
		{NodeID: "node1", Available: true},
		{NodeID: "node2", Available: false},
		{NodeID: "node3", Available: true},
	}

	available := filterAvailableNodes(nodes)

	if len(available) != 2 {
		t.Errorf("Expected 2 available nodes, got %d", len(available))
	}

	expectedAvailable := map[string]bool{"node1": true, "node3": true}
	for _, node := range available {
		if !expectedAvailable[node.NodeID] {
			t.Errorf("Unexpected available node: %s", node.NodeID)
		}
	}
}

func TestStrategyWithNoAvailableNodes(t *testing.T) {
	logger := slog.Default()
	strategy := NewRoundRobinStrategy(logger)

	nodes := []NodeMetrics{
		{NodeID: "node1", Available: false},
		{NodeID: "node2", Available: false},
	}

	ctx := context.Background()
	_, err := strategy.SelectNode(ctx, nodes, "workflow-1")

	if err == nil {
		t.Error("Expected error when no nodes available")
	}

	if !domain.IsDomainError(err) {
		t.Errorf("Expected DomainError, got %T", err)
	}

	if domain.GetErrorCategory(err) != domain.CategoryResource {
		t.Errorf("Expected resource error category, got %v", domain.GetErrorCategory(err))
	}
}
