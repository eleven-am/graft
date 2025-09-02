package load_balancer

import (
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
)

func TestWeightedScoring(t *testing.T) {
	scorer := DefaultScorer

	tests := []struct {
		name     string
		load     ports.NodeLoad
		capacity float64
		expected float64
		delta    float64
	}{
		{
			name: "light_load",
			load: ports.NodeLoad{
				TotalWeight:     2.0,
				RecentLatencyMs: 50,
				RecentErrorRate: 0.01,
				LastUpdated:     time.Now().Unix(),
			},
			capacity: 10.0,
			expected: 0.25,
			delta:    0.01,
		},
		{
			name: "heavy_load",
			load: ports.NodeLoad{
				TotalWeight:     18.0,
				RecentLatencyMs: 200,
				RecentErrorRate: 0.1,
				LastUpdated:     time.Now().Unix(),
			},
			capacity: 10.0,
			expected: 1.67,
			delta:    0.01,
		},
		{
			name: "stale_data",
			load: ports.NodeLoad{
				TotalWeight:     5.0,
				RecentLatencyMs: 50,
				RecentErrorRate: 0.01,
				LastUpdated:     time.Now().Add(-10 * time.Second).Unix(),
			},
			capacity: 10.0,
			expected: 1.46,
			delta:    0.01,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := scorer.CalculateScore(&tt.load, tt.capacity)
			assert.InDelta(t, tt.expected, score, tt.delta)
		})
	}
}

func TestEWMA(t *testing.T) {
	var ewma float64

	ewma = UpdateEWMA(ewma, 100, 0.2)
	assert.Equal(t, 100.0, ewma)

	ewma = UpdateEWMA(ewma, 200, 0.2)
	assert.InDelta(t, 120.0, ewma, 0.01)

	ewma = UpdateEWMA(ewma, 50, 0.2)
	assert.InDelta(t, 106.0, ewma, 0.01)
}

func TestRollingWindow(t *testing.T) {
	window := NewRollingWindow(10)

	for i := 0; i < 10; i++ {
		window.Record(true)
	}
	assert.Equal(t, 0.0, window.GetErrorRate())

	for i := 0; i < 5; i++ {
		window.Record(false)
	}
	assert.Equal(t, 0.5, window.GetErrorRate())

	for i := 0; i < 10; i++ {
		window.Record(true)
	}
	assert.Equal(t, 0.0, window.GetErrorRate())
}

func TestNodeSelection(t *testing.T) {
	scorer := DefaultScorer

	clusterLoad := map[string]*ports.NodeLoad{
		"node-1": {
			NodeID:          "node-1",
			TotalWeight:     4.0,
			RecentLatencyMs: 80,
			RecentErrorRate: 0.02,
			LastUpdated:     time.Now().Unix(),
		},
		"node-2": {
			NodeID:          "node-2",
			TotalWeight:     2.0,
			RecentLatencyMs: 40,
			RecentErrorRate: 0.01,
			LastUpdated:     time.Now().Unix(),
		},
		"node-3": {
			NodeID:          "node-3",
			TotalWeight:     8.0,
			RecentLatencyMs: 120,
			RecentErrorRate: 0.05,
			LastUpdated:     time.Now().Unix(),
		},
	}

	capacities := map[string]float64{
		"node-1": 10.0,
		"node-2": 10.0,
		"node-3": 10.0,
	}

	bestNode, bestScore := scorer.SelectBestNode(clusterLoad, capacities)

	assert.Equal(t, "node-2", bestNode)
	assert.Less(t, bestScore, 1.0)
}

func TestNodeWeights(t *testing.T) {
	tests := []struct {
		nodeName string
		expected float64
	}{
		{"validator-service", 1.0},
		{"ml-training-worker", 8.0},
		{"ocr-processor-node", 6.0},
		{"unknown-node", 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.nodeName, func(t *testing.T) {
			weight := ports.GetNodeWeight(nil, tt.nodeName)
			assert.Equal(t, tt.expected, weight)
		})
	}
}
