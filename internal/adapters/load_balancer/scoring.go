package load_balancer

import (
	"math"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type NodeScorer struct {
	Weights ScoreWeights
	Config  ScoringConfig
}

type ScoreWeights struct {
	Saturation float64
	Latency    float64
	ErrorRate  float64
}

type ScoringConfig struct {
	TargetSLOMs     float64
	StaleDataTTL    time.Duration
	TieBreakEpsilon float64
	DefaultCapacity float64
}

var DefaultScorer = &NodeScorer{
	Weights: ScoreWeights{
		Saturation: 0.7,
		Latency:    0.2,
		ErrorRate:  0.1,
	},
	Config: ScoringConfig{
		TargetSLOMs:     100.0,
		StaleDataTTL:    5 * time.Second,
		TieBreakEpsilon: 0.01,
		DefaultCapacity: 10.0,
	},
}

func (s *NodeScorer) CalculateScore(load *ports.NodeLoad, capacityUnits float64) float64 {
	if capacityUnits <= 0 {
		capacityUnits = s.Config.DefaultCapacity
	}

	totalWeight := sanitizeFloat64(load.TotalWeight, 0.0)
	latencyMs := sanitizeFloat64(load.RecentLatencyMs, 0.0)
	errorRate := sanitizeFloat64(load.RecentErrorRate, 0.0)
	capacityUnits = sanitizeFloat64(capacityUnits, s.Config.DefaultCapacity)

	saturation := totalWeight / capacityUnits
	normalizedLatency := clamp(latencyMs/s.Config.TargetSLOMs, 0, 3)

	stalenessPenalty := 0.0
	age := time.Since(time.Unix(load.LastUpdated, 0))
	if age > s.Config.StaleDataTTL {
		stalenessPenalty = 1.0
	}

	score := s.Weights.Saturation*saturation +
		s.Weights.Latency*normalizedLatency +
		s.Weights.ErrorRate*errorRate +
		stalenessPenalty

	return sanitizeFloat64(score, math.MaxFloat64)
}

func (s *NodeScorer) SelectBestNode(clusterLoad map[string]*ports.NodeLoad,
	capacities map[string]float64) (string, float64) {

	var bestNode string
	bestScore := math.MaxFloat64

	for nodeID, load := range clusterLoad {
		capacity := capacities[nodeID]
		if capacity == 0 {
			capacity = s.Config.DefaultCapacity
		}

		score := s.CalculateScore(load, capacity)

		if score < bestScore {
			bestNode = nodeID
			bestScore = score
		}
	}

	return bestNode, bestScore
}

func clamp(value, min, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func sanitizeFloat64(value, defaultValue float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return defaultValue
	}
	return value
}
