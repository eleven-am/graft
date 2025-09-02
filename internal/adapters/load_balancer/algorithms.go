package load_balancer

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

// LoadBalancingStrategy defines the interface for load balancing algorithms
type LoadBalancingStrategy interface {
	SelectNode(ctx context.Context, nodes []NodeMetrics, workflowID string) (string, error)
	UpdateMetrics(nodeID string, metrics NodeMetrics)
	GetAlgorithmMetrics() map[string]interface{}
}

type NodeMetrics struct {
	NodeID          string    `json:"node_id"`
	ResponseTime    float64   `json:"response_time_ms"`
	CpuUsage        float64   `json:"cpu_usage_percent"`
	MemoryUsage     float64   `json:"memory_usage_percent"`
	ConnectionCount int       `json:"connection_count"`
	ErrorRate       float64   `json:"error_rate"`
	Capacity        float64   `json:"capacity"`
	ActiveWorkflows int       `json:"active_workflows"`
	LastUpdated     time.Time `json:"last_updated"`
	Available       bool      `json:"available"`
}

// RoundRobinStrategy implements simple round-robin load balancing
type RoundRobinStrategy struct {
	mu      sync.RWMutex
	counter uint64
	logger  *slog.Logger
}

func NewRoundRobinStrategy(logger *slog.Logger) *RoundRobinStrategy {
	return &RoundRobinStrategy{
		logger: logger,
	}
}

func (rr *RoundRobinStrategy) SelectNode(ctx context.Context, nodes []NodeMetrics, workflowID string) (string, error) {
	availableNodes := filterAvailableNodes(nodes)
	if len(availableNodes) == 0 {
		return "", domain.NewResourceError("no available nodes", nil)
	}

	index := atomic.AddUint64(&rr.counter, 1) % uint64(len(availableNodes))
	selected := availableNodes[index]

	rr.logger.Debug("round robin selection",
		"selected_node", selected.NodeID,
		"workflow_id", workflowID,
		"index", index,
		"total_nodes", len(availableNodes))

	return selected.NodeID, nil
}

func (rr *RoundRobinStrategy) UpdateMetrics(nodeID string, metrics NodeMetrics) {

}

func (rr *RoundRobinStrategy) GetAlgorithmMetrics() map[string]interface{} {
	return map[string]interface{}{
		"algorithm": "round_robin",
		"counter":   atomic.LoadUint64(&rr.counter),
	}
}

// WeightedRoundRobinStrategy implements weighted round-robin with smooth weighting
type WeightedRoundRobinStrategy struct {
	mu          sync.RWMutex
	config      domain.WeightedRoundRobinConfig
	nodeWeights map[string]*weightedNode
	logger      *slog.Logger
}

type weightedNode struct {
	nodeID          string
	weight          int
	currentWeight   int
	effectiveWeight int
}

func NewWeightedRoundRobinStrategy(config domain.WeightedRoundRobinConfig, logger *slog.Logger) *WeightedRoundRobinStrategy {
	return &WeightedRoundRobinStrategy{
		config:      config,
		nodeWeights: make(map[string]*weightedNode),
		logger:      logger,
	}
}

func (wrr *WeightedRoundRobinStrategy) SelectNode(ctx context.Context, nodes []NodeMetrics, workflowID string) (string, error) {
	availableNodes := filterAvailableNodes(nodes)
	if len(availableNodes) == 0 {
		return "", domain.NewResourceError("no available nodes", nil)
	}

	wrr.mu.Lock()
	defer wrr.mu.Unlock()

	wrr.updateNodeWeights(availableNodes)

	if wrr.config.SmoothWeighting {
		return wrr.selectSmoothWeighted(), nil
	}
	return wrr.selectStandardWeighted(), nil
}

func (wrr *WeightedRoundRobinStrategy) updateNodeWeights(nodes []NodeMetrics) {

	for _, node := range nodes {
		if _, exists := wrr.nodeWeights[node.NodeID]; !exists {
			weight := wrr.config.DefaultWeight
			if nodeWeight, hasCustomWeight := wrr.config.NodeWeights[node.NodeID]; hasCustomWeight {
				weight = nodeWeight
			}

			wrr.nodeWeights[node.NodeID] = &weightedNode{
				nodeID:          node.NodeID,
				weight:          weight,
				currentWeight:   0,
				effectiveWeight: weight,
			}
		}
	}

	availableNodeIDs := make(map[string]bool)
	for _, node := range nodes {
		availableNodeIDs[node.NodeID] = true
	}

	for nodeID := range wrr.nodeWeights {
		if !availableNodeIDs[nodeID] {
			delete(wrr.nodeWeights, nodeID)
		}
	}
}

func (wrr *WeightedRoundRobinStrategy) selectSmoothWeighted() string {
	var selected *weightedNode
	totalWeight := 0

	for _, node := range wrr.nodeWeights {
		node.currentWeight += node.effectiveWeight
		totalWeight += node.effectiveWeight

		if selected == nil || node.currentWeight > selected.currentWeight {
			selected = node
		}
	}

	if selected != nil {
		selected.currentWeight -= totalWeight

		wrr.logger.Debug("smooth weighted round robin selection",
			"selected_node", selected.nodeID,
			"selected_weight", selected.weight,
			"total_weight", totalWeight)

		return selected.nodeID
	}

	for nodeID := range wrr.nodeWeights {
		return nodeID
	}

	return ""
}

func (wrr *WeightedRoundRobinStrategy) selectStandardWeighted() string {

	totalWeight := 0
	for _, node := range wrr.nodeWeights {
		totalWeight += node.weight
	}

	if totalWeight == 0 {

		for nodeID := range wrr.nodeWeights {
			return nodeID
		}
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	target := int(hash) % totalWeight

	current := 0
	for _, node := range wrr.nodeWeights {
		current += node.weight
		if current > target {
			return node.nodeID
		}
	}

	for nodeID := range wrr.nodeWeights {
		return nodeID
	}
	return ""
}

func (wrr *WeightedRoundRobinStrategy) UpdateMetrics(nodeID string, metrics NodeMetrics) {

	wrr.mu.Lock()
	defer wrr.mu.Unlock()

	if node, exists := wrr.nodeWeights[nodeID]; exists {

		if metrics.ErrorRate > 0.1 || metrics.ResponseTime > 1000 {
			node.effectiveWeight = maxInt(1, node.weight/2)
		} else {
			node.effectiveWeight = node.weight
		}
	}
}

func (wrr *WeightedRoundRobinStrategy) GetAlgorithmMetrics() map[string]interface{} {
	wrr.mu.RLock()
	defer wrr.mu.RUnlock()

	weights := make(map[string]interface{})
	for nodeID, node := range wrr.nodeWeights {
		weights[nodeID] = map[string]interface{}{
			"weight":           node.weight,
			"effective_weight": node.effectiveWeight,
			"current_weight":   node.currentWeight,
		}
	}

	return map[string]interface{}{
		"algorithm":      "weighted_round_robin",
		"smooth_weights": wrr.config.SmoothWeighting,
		"node_weights":   weights,
	}
}

// LeastConnectionsStrategy selects nodes with fewest active connections
type LeastConnectionsStrategy struct {
	mu     sync.RWMutex
	logger *slog.Logger
}

func NewLeastConnectionsStrategy(logger *slog.Logger) *LeastConnectionsStrategy {
	return &LeastConnectionsStrategy{
		logger: logger,
	}
}

func (lc *LeastConnectionsStrategy) SelectNode(ctx context.Context, nodes []NodeMetrics, workflowID string) (string, error) {
	availableNodes := filterAvailableNodes(nodes)
	if len(availableNodes) == 0 {
		return "", domain.NewResourceError("no available nodes", nil)
	}

	sort.Slice(availableNodes, func(i, j int) bool {
		if availableNodes[i].ConnectionCount == availableNodes[j].ConnectionCount {
			return availableNodes[i].ActiveWorkflows < availableNodes[j].ActiveWorkflows
		}
		return availableNodes[i].ConnectionCount < availableNodes[j].ConnectionCount
	})

	selected := availableNodes[0]

	lc.logger.Debug("least connections selection",
		"selected_node", selected.NodeID,
		"workflow_id", workflowID,
		"connections", selected.ConnectionCount,
		"active_workflows", selected.ActiveWorkflows)

	return selected.NodeID, nil
}

func (lc *LeastConnectionsStrategy) UpdateMetrics(nodeID string, metrics NodeMetrics) {

}

func (lc *LeastConnectionsStrategy) GetAlgorithmMetrics() map[string]interface{} {
	return map[string]interface{}{
		"algorithm": "least_connections",
	}
}

// AdaptiveStrategy combines multiple factors for intelligent load balancing
type AdaptiveStrategy struct {
	mu          sync.RWMutex
	config      domain.AdaptiveLoadBalancerConfig
	nodeHistory map[string]*nodeHistory
	logger      *slog.Logger
	lastUpdate  time.Time
}

type nodeHistory struct {
	responseTimeHistory []float64
	errorRateHistory    []float64
	timestamps          []time.Time
	score               float64
	lastScoreUpdate     time.Time
}

func NewAdaptiveStrategy(config domain.AdaptiveLoadBalancerConfig, logger *slog.Logger) *AdaptiveStrategy {
	return &AdaptiveStrategy{
		config:      config,
		nodeHistory: make(map[string]*nodeHistory),
		logger:      logger,
		lastUpdate:  time.Now(),
	}
}

func (as *AdaptiveStrategy) SelectNode(ctx context.Context, nodes []NodeMetrics, workflowID string) (string, error) {
	availableNodes := filterAvailableNodes(nodes)
	if len(availableNodes) == 0 {
		return "", domain.NewResourceError("no available nodes", nil)
	}

	as.mu.Lock()
	defer as.mu.Unlock()

	now := time.Now()

	if now.Sub(as.lastUpdate) >= as.config.AdaptationInterval {
		as.updateAllScores(availableNodes, now)
		as.lastUpdate = now
	}

	var bestNode *NodeMetrics
	bestScore := math.Inf(-1)

	for i := range availableNodes {
		node := &availableNodes[i]
		score := as.calculateNodeScore(node, now)

		if score > bestScore {
			bestScore = score
			bestNode = node
		}
	}

	if bestNode == nil {
		return availableNodes[0].NodeID, nil
	}

	as.logger.Debug("adaptive selection",
		"selected_node", bestNode.NodeID,
		"workflow_id", workflowID,
		"score", bestScore,
		"response_time", bestNode.ResponseTime,
		"cpu_usage", bestNode.CpuUsage,
		"error_rate", bestNode.ErrorRate)

	return bestNode.NodeID, nil
}

func (as *AdaptiveStrategy) calculateNodeScore(node *NodeMetrics, now time.Time) float64 {
	history := as.getOrCreateHistory(node.NodeID)

	if now.Sub(history.lastScoreUpdate) < as.config.AdaptationInterval/3 {
		return history.score
	}

	score := 100.0

	if node.ResponseTime > 0 {
		avgResponseTime := as.getAverageResponseTime(history)
		responseTimeFactor := 1.0 / (1.0 + node.ResponseTime/max(avgResponseTime, 1.0))
		score -= (1.0 - responseTimeFactor) * as.config.ResponseTimeWeight * 100
	}

	cpuFactor := 1.0 - (node.CpuUsage / 100.0)
	score -= (1.0 - cpuFactor) * as.config.CpuUsageWeight * 100

	memoryFactor := 1.0 - (node.MemoryUsage / 100.0)
	score -= (1.0 - memoryFactor) * as.config.MemoryUsageWeight * 100

	if node.Capacity > 0 {
		connectionRatio := float64(node.ConnectionCount) / node.Capacity
		score -= connectionRatio * as.config.ConnectionCountWeight * 100
	}

	avgErrorRate := as.getAverageErrorRate(history)
	errorFactor := 1.0 / (1.0 + node.ErrorRate/max(avgErrorRate, 0.01))
	score -= (1.0 - errorFactor) * as.config.ErrorRateWeight * 100

	score = max(score, 0.1)

	history.score = score
	history.lastScoreUpdate = now

	return score
}

func (as *AdaptiveStrategy) getOrCreateHistory(nodeID string) *nodeHistory {
	if history, exists := as.nodeHistory[nodeID]; exists {
		return history
	}

	as.nodeHistory[nodeID] = &nodeHistory{
		responseTimeHistory: make([]float64, 0),
		errorRateHistory:    make([]float64, 0),
		timestamps:          make([]time.Time, 0),
		score:               50.0,
	}
	return as.nodeHistory[nodeID]
}

func (as *AdaptiveStrategy) getAverageResponseTime(history *nodeHistory) float64 {
	if len(history.responseTimeHistory) == 0 {
		return 100.0
	}

	sum := 0.0
	for _, rt := range history.responseTimeHistory {
		sum += rt
	}
	return sum / float64(len(history.responseTimeHistory))
}

func (as *AdaptiveStrategy) getAverageErrorRate(history *nodeHistory) float64 {
	if len(history.errorRateHistory) == 0 {
		return 0.01
	}

	sum := 0.0
	for _, er := range history.errorRateHistory {
		sum += er
	}
	return sum / float64(len(history.errorRateHistory))
}

func (as *AdaptiveStrategy) updateAllScores(nodes []NodeMetrics, now time.Time) {
	for i := range nodes {
		as.UpdateMetrics(nodes[i].NodeID, nodes[i])
	}

	as.cleanupOldHistory(now)
}

func (as *AdaptiveStrategy) UpdateMetrics(nodeID string, metrics NodeMetrics) {
	history := as.getOrCreateHistory(nodeID)
	now := time.Now()

	history.responseTimeHistory = append(history.responseTimeHistory, metrics.ResponseTime)
	history.errorRateHistory = append(history.errorRateHistory, metrics.ErrorRate)
	history.timestamps = append(history.timestamps, now)

	cutoff := now.Add(-as.config.HistoryWindow)
	as.trimHistoryToWindow(history, cutoff)
}

func (as *AdaptiveStrategy) trimHistoryToWindow(history *nodeHistory, cutoff time.Time) {

	start := 0
	for i, ts := range history.timestamps {
		if ts.After(cutoff) {
			start = i
			break
		}
	}

	if start > 0 {
		history.responseTimeHistory = history.responseTimeHistory[start:]
		history.errorRateHistory = history.errorRateHistory[start:]
		history.timestamps = history.timestamps[start:]
	}
}

func (as *AdaptiveStrategy) cleanupOldHistory(now time.Time) {
	cutoff := now.Add(-as.config.HistoryWindow * 2)

	for nodeID, history := range as.nodeHistory {
		as.trimHistoryToWindow(history, cutoff)

		if len(history.timestamps) == 0 || history.timestamps[len(history.timestamps)-1].Before(cutoff) {
			delete(as.nodeHistory, nodeID)
		}
	}
}

func (as *AdaptiveStrategy) GetAlgorithmMetrics() map[string]interface{} {
	as.mu.RLock()
	defer as.mu.RUnlock()

	nodeScores := make(map[string]interface{})
	for nodeID, history := range as.nodeHistory {
		nodeScores[nodeID] = map[string]interface{}{
			"score":             history.score,
			"avg_response_time": as.getAverageResponseTime(history),
			"avg_error_rate":    as.getAverageErrorRate(history),
			"history_points":    len(history.timestamps),
		}
	}

	return map[string]interface{}{
		"algorithm":           "adaptive",
		"adaptation_interval": as.config.AdaptationInterval,
		"history_window":      as.config.HistoryWindow,
		"node_scores":         nodeScores,
		"weights": map[string]float64{
			"response_time":    as.config.ResponseTimeWeight,
			"cpu_usage":        as.config.CpuUsageWeight,
			"memory_usage":     as.config.MemoryUsageWeight,
			"connection_count": as.config.ConnectionCountWeight,
			"error_rate":       as.config.ErrorRateWeight,
		},
	}
}

// ConsistentHashStrategy implements consistent hashing for workflow affinity
type ConsistentHashStrategy struct {
	mu     sync.RWMutex
	ring   []hashNode
	logger *slog.Logger
}

type hashNode struct {
	hash   uint32
	nodeID string
}

func NewConsistentHashStrategy(logger *slog.Logger) *ConsistentHashStrategy {
	return &ConsistentHashStrategy{
		ring:   make([]hashNode, 0),
		logger: logger,
	}
}

func (ch *ConsistentHashStrategy) SelectNode(ctx context.Context, nodes []NodeMetrics, workflowID string) (string, error) {
	availableNodes := filterAvailableNodes(nodes)
	if len(availableNodes) == 0 {
		return "", domain.NewResourceError("no available nodes", nil)
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.rebuildRing(availableNodes)

	if len(ch.ring) == 0 {
		return availableNodes[0].NodeID, nil
	}

	workflowHash := ch.hashString(workflowID)

	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i].hash >= workflowHash
	})

	if idx >= len(ch.ring) {
		idx = 0
	}

	selected := ch.ring[idx].nodeID

	ch.logger.Debug("consistent hash selection",
		"selected_node", selected,
		"workflow_id", workflowID,
		"workflow_hash", workflowHash,
		"node_hash", ch.ring[idx].hash)

	return selected, nil
}

func (ch *ConsistentHashStrategy) rebuildRing(nodes []NodeMetrics) {
	ch.ring = ch.ring[:0]

	virtualNodes := 150

	for _, node := range nodes {
		for i := 0; i < virtualNodes; i++ {
			virtualNodeID := fmt.Sprintf("%s-%d", node.NodeID, i)
			hash := ch.hashString(virtualNodeID)
			ch.ring = append(ch.ring, hashNode{
				hash:   hash,
				nodeID: node.NodeID,
			})
		}
	}

	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i].hash < ch.ring[j].hash
	})
}

func (ch *ConsistentHashStrategy) hashString(s string) uint32 {
	hash := sha256.Sum256([]byte(s))
	return uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
}

func (ch *ConsistentHashStrategy) UpdateMetrics(nodeID string, metrics NodeMetrics) {

}

func (ch *ConsistentHashStrategy) GetAlgorithmMetrics() map[string]interface{} {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	return map[string]interface{}{
		"algorithm":     "consistent_hash",
		"ring_size":     len(ch.ring),
		"virtual_nodes": 150,
	}
}

// Utility functions
func filterAvailableNodes(nodes []NodeMetrics) []NodeMetrics {
	available := make([]NodeMetrics, 0, len(nodes))
	for _, node := range nodes {
		if node.Available {
			available = append(available, node)
		}
	}
	return available
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
