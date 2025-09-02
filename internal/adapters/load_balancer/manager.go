package load_balancer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Manager struct {
	storage        ports.StoragePort
	events         ports.EventManager
	nodeID         string
	logger         *slog.Logger
	clusterManager ports.ClusterManager

	mu       sync.RWMutex
	running  bool
	draining bool
	ctx      context.Context
	cancel   context.CancelFunc

	executionUnits  map[string]float64
	totalWeight     float64
	recentLatencyMs float64
	errorWindow     *RollingWindow
	nodeCapacities  map[string]float64
	nodeWeights     map[string]float64

	scoreCache    map[string]scoreCacheEntry
	scoreCacheTTL time.Duration
}

type scoreCacheEntry struct {
	score     float64
	timestamp int64
}

func NewManager(storage ports.StoragePort, events ports.EventManager, nodeID string, clusterManager ports.ClusterManager, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		storage:        storage,
		events:         events,
		nodeID:         nodeID,
		clusterManager: clusterManager,
		logger:         logger.With("component", "load-balancer"),
		executionUnits: make(map[string]float64),
		errorWindow:    NewRollingWindow(100),
		nodeCapacities: make(map[string]float64),
		nodeWeights:    make(map[string]float64),
		scoreCache:     make(map[string]scoreCacheEntry),
		scoreCacheTTL:  1 * time.Second,
	}
}

func (m *Manager) SetNodeWeights(weights map[string]float64) {
	m.mu.Lock()
	for k, v := range weights {
		m.nodeWeights[k] = v
	}
	m.mu.Unlock()
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return domain.NewDiscoveryError("load-balancer", "start", domain.ErrAlreadyStarted)
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.running = true

	if err := m.subscribeToEvents(); err != nil {
		m.cancel()
		m.running = false
		return domain.NewDiscoveryError("load-balancer", "subscribe", err)
	}

	if err := m.initializeNodeLoad(); err != nil {
		m.cancel()
		m.running = false
		return domain.NewDiscoveryError("load-balancer", "initialize", err)
	}

	if m.clusterManager != nil {
		go m.monitorClusterHealth()
	}

	return nil
}

func (m *Manager) monitorClusterHealth() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			health := m.clusterManager.GetClusterHealth()

			if !health.IsHealthy {
				m.logger.Warn("cluster unhealthy",
					"healthy_nodes", health.HealthyNodes,
					"total_nodes", health.TotalNodes,
					"minimum_nodes", health.MinimumNodes,
					"unhealthy_nodes", health.UnhealthyNodes)

				m.mu.Lock()
				for cacheKey := range m.scoreCache {
					delete(m.scoreCache, cacheKey)
				}
				m.mu.Unlock()
			} else if len(health.UnhealthyNodes) > 0 {
				m.logger.Info("cluster stable with some unhealthy nodes",
					"healthy_nodes", health.HealthyNodes,
					"unhealthy_nodes", health.UnhealthyNodes)

				m.cleanupUnhealthyNodeCache(health.UnhealthyNodes)
			}
		}
	}
}

func (m *Manager) cleanupUnhealthyNodeCache(unhealthyNodes []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for cacheKey := range m.scoreCache {
		for _, unhealthyNode := range unhealthyNodes {
			if len(cacheKey) > len(unhealthyNode) && cacheKey[:len(unhealthyNode)] == unhealthyNode {
				delete(m.scoreCache, cacheKey)
				break
			}
		}
	}
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return domain.NewDiscoveryError("load-balancer", "stop", domain.ErrNotStarted)
	}

	m.cancel()
	m.running = false

	return nil
}

func (m *Manager) subscribeToEvents() error {
	if err := m.events.OnNodeStarted(m.onNodeStarted); err != nil {
		return domain.NewDiscoveryError("load-balancer", "subscribe-started", err)
	}

	if err := m.events.OnNodeCompleted(m.onNodeCompleted); err != nil {
		return domain.NewDiscoveryError("load-balancer", "subscribe-completed", err)
	}

	if err := m.events.OnNodeError(m.onNodeError); err != nil {
		return domain.NewDiscoveryError("load-balancer", "subscribe-error", err)
	}

	return nil
}

func (m *Manager) initializeNodeLoad() error {
	load := &ports.NodeLoad{
		NodeID:          m.nodeID,
		TotalWeight:     0,
		ExecutionUnits:  make(map[string]float64),
		RecentLatencyMs: 0,
		RecentErrorRate: 0,
		LastUpdated:     time.Now().Unix(),
	}

	if err := m.updateNodeLoad(load); err != nil {
		if domain.IsNotFound(err) {
			m.logger.Info("storage not writable on follower; skipping initial load write", "node_id", m.nodeID)
			return nil
		}
		return err
	}
	return nil
}

func (m *Manager) onNodeStarted(event *domain.NodeStartedEvent) {
	if event.NodeID != m.nodeID {
		return
	}

	m.mu.Lock()
	weight := 0.0
	if w, ok := m.nodeWeights[event.NodeName]; ok {
		weight = w
	} else {
		weight = ports.GetNodeWeight(nil, event.NodeName)
	}
	m.executionUnits[event.WorkflowID] = weight
	m.totalWeight += weight

	executionUnitsCopy := make(map[string]float64, len(m.executionUnits))
	for k, v := range m.executionUnits {
		executionUnitsCopy[k] = v
	}

	load := &ports.NodeLoad{
		NodeID:          m.nodeID,
		TotalWeight:     m.totalWeight,
		ExecutionUnits:  executionUnitsCopy,
		RecentLatencyMs: m.recentLatencyMs,
		RecentErrorRate: m.errorWindow.GetErrorRate(),
		LastUpdated:     time.Now().Unix(),
	}
	m.mu.Unlock()

	if err := m.updateNodeLoad(load); err != nil {
		m.logger.Error("failed to update node load on start", "error", err,
			"workflow_id", event.WorkflowID,
			"weight", weight)
	}

	m.mu.Lock()
	m.cleanupScoreCacheIfNeeded()
	m.mu.Unlock()
}

func (m *Manager) onNodeCompleted(event *domain.NodeCompletedEvent) {
	m.mu.Lock()

	if weight, exists := m.executionUnits[event.WorkflowID]; exists {
		m.totalWeight -= weight
		delete(m.executionUnits, event.WorkflowID)
	}

	latencyMs := float64(event.Duration.Milliseconds())
	if latencyMs > 0 {
		m.recentLatencyMs = UpdateEWMA(m.recentLatencyMs, latencyMs, 0.2)
	}

	m.errorWindow.Record(true)

	executionUnitsCopy := make(map[string]float64, len(m.executionUnits))
	for k, v := range m.executionUnits {
		executionUnitsCopy[k] = v
	}

	load := &ports.NodeLoad{
		NodeID:          m.nodeID,
		TotalWeight:     m.totalWeight,
		ExecutionUnits:  executionUnitsCopy,
		RecentLatencyMs: m.recentLatencyMs,
		RecentErrorRate: m.errorWindow.GetErrorRate(),
		LastUpdated:     time.Now().Unix(),
	}
	m.mu.Unlock()

	if err := m.updateNodeLoad(load); err != nil {
		m.logger.Error("failed to update load in storage", "error", err)
	}
}

func (m *Manager) onNodeError(event *domain.NodeErrorEvent) {
	m.mu.Lock()

	if weight, exists := m.executionUnits[event.WorkflowID]; exists {
		m.totalWeight -= weight
		delete(m.executionUnits, event.WorkflowID)
	}

	m.errorWindow.Record(false)

	executionUnitsCopy := make(map[string]float64, len(m.executionUnits))
	for k, v := range m.executionUnits {
		executionUnitsCopy[k] = v
	}

	load := &ports.NodeLoad{
		NodeID:          m.nodeID,
		TotalWeight:     m.totalWeight,
		ExecutionUnits:  executionUnitsCopy,
		RecentLatencyMs: m.recentLatencyMs,
		RecentErrorRate: m.errorWindow.GetErrorRate(),
		LastUpdated:     time.Now().Unix(),
	}
	m.mu.Unlock()

	if err := m.updateNodeLoad(load); err != nil {
		m.logger.Error("failed to update load in storage", "error", err)
	}
}

func (m *Manager) updateNodeLoad(load *ports.NodeLoad) error {
	data, err := json.Marshal(load)
	if err != nil {
		return domain.ErrInvalidInput
	}

	key := fmt.Sprintf("cluster:load:%s", load.NodeID)
	return m.storage.Put(key, data, 0)
}

func (m *Manager) ShouldExecuteNode(nodeID string, workflowID string, nodeName string) (bool, error) {
	m.mu.RLock()
	isDraining := m.draining
	m.mu.RUnlock()

	if isDraining {
		m.logger.Info("node is draining, rejecting new work", "node_id", m.nodeID)
		return false, nil
	}

	if m.clusterManager != nil {
		health := m.clusterManager.GetClusterHealth()

		if !health.IsHealthy {
			m.logger.Warn("cluster unhealthy, applying fail-safe behavior",
				"healthy_nodes", health.HealthyNodes,
				"minimum_nodes", health.MinimumNodes,
				"cluster_viable", health.IsMinimumViable)

			if !health.IsMinimumViable {
				m.logger.Error("cluster below minimum viable nodes, defaulting to execute",
					"healthy_nodes", health.HealthyNodes,
					"minimum_nodes", health.MinimumNodes)
				return true, nil
			}
		}

		if !m.clusterManager.IsNodeActive(m.nodeID) {
			m.logger.Warn("current node not active in cluster, defaulting to execute", "node_id", m.nodeID)
			return true, nil
		}
	}

	clusterLoad, err := m.GetClusterLoad()
	if err != nil {
		m.logger.Warn("failed to get cluster load, defaulting to execute", "error", err)
		return true, nil
	}

	if len(clusterLoad) == 0 {
		m.logger.Warn("no valid cluster load data found, defaulting to execute",
			"reason", "empty_cluster_load")
		return true, nil
	}

	filteredLoad := m.filterActiveNodes(clusterLoad)
	if len(filteredLoad) == 0 {
		if len(clusterLoad) > 0 {
			m.logger.Warn("no active nodes reported; falling back to all nodes",
				"total_nodes", len(clusterLoad))
			filteredLoad = clusterLoad
		} else {
			m.logger.Warn("no cluster load data available; allowing local execution")
			return true, nil
		}
	}

	capacities := m.getNodeCapacities()
	scorer := DefaultScorer
	bestNode, bestScore := scorer.SelectBestNode(filteredLoad, capacities)

	if bestNode == "" {
		var lexMin string
		for nodeID := range filteredLoad {
			if lexMin == "" || nodeID < lexMin {
				lexMin = nodeID
			}
		}
		bestNode = lexMin
		m.logger.Warn("no best node selected; using lexicographic tie-breaker",
			"selected_node", bestNode, "score", bestScore)
	}

	shouldExecute := bestNode == m.nodeID
	return shouldExecute, nil
}

func (m *Manager) StartDraining() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return domain.NewDiscoveryError("load-balancer", "drain", domain.ErrNotStarted)
	}

	if m.draining {
		return nil
	}

	m.draining = true
	m.logger.Info("started draining node", "node_id", m.nodeID)
	return nil
}

func (m *Manager) StopDraining() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.draining {
		return nil
	}

	m.draining = false
	m.logger.Info("stopped draining node", "node_id", m.nodeID)
	return nil
}

func (m *Manager) IsDraining() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.draining
}

func (m *Manager) WaitForDraining(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.mu.RLock()
			totalWork := m.totalWeight
			m.mu.RUnlock()

			if totalWork == 0 {
				m.logger.Info("node fully drained", "node_id", m.nodeID)
				return nil
			}

			m.logger.Debug("waiting for work to complete",
				"node_id", m.nodeID,
				"remaining_work", totalWork)
		}
	}
}

func (m *Manager) filterActiveNodes(clusterLoad map[string]*ports.NodeLoad) map[string]*ports.NodeLoad {
	if m.clusterManager == nil {
		return clusterLoad
	}

	filteredLoad := make(map[string]*ports.NodeLoad)
	activeNodes := m.clusterManager.GetActiveNodes()

	for _, activeNodeID := range activeNodes {
		if load, exists := clusterLoad[activeNodeID]; exists {
			filteredLoad[activeNodeID] = load
		}
	}

	return filteredLoad
}

func (m *Manager) getNodeCapacities() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	capacities := make(map[string]float64)
	for nodeID, capacity := range m.nodeCapacities {
		capacities[nodeID] = capacity
	}

	if len(capacities) == 0 {
		capacities[m.nodeID] = 10.0
	}

	return capacities
}

func (m *Manager) selectBestNodeWithCache(clusterLoad map[string]*ports.NodeLoad, capacities map[string]float64, scorer *NodeScorer) (string, float64) {
	if len(clusterLoad) == 1 {
		for nodeID := range clusterLoad {
			return nodeID, 0
		}
	}

	now := time.Now().Unix()
	var bestNode string
	bestScore := math.MaxFloat64

	for nodeID, load := range clusterLoad {
		cacheKey := fmt.Sprintf("%s:%d", nodeID, load.LastUpdated)

		var score float64
		if entry, exists := m.scoreCache[cacheKey]; exists && (now-entry.timestamp) < int64(m.scoreCacheTTL.Seconds()) {
			score = entry.score
		} else {
			capacity := capacities[nodeID]
			if capacity == 0 {
				capacity = scorer.Config.DefaultCapacity
			}
			score = scorer.CalculateScore(load, capacity)

			m.scoreCache[cacheKey] = scoreCacheEntry{
				score:     score,
				timestamp: now,
			}

			if len(m.scoreCache) > 1000 {
				m.cleanupScoreCache(now)
			}
		}

		if score < bestScore {
			bestNode = nodeID
			bestScore = score
		}
	}

	return bestNode, bestScore
}

func (m *Manager) cleanupScoreCache(now int64) {
	for key, entry := range m.scoreCache {
		if (now - entry.timestamp) > int64(m.scoreCacheTTL.Seconds()) {
			delete(m.scoreCache, key)
		}
	}
}

func (m *Manager) cleanupScoreCacheIfNeeded() {
	if len(m.scoreCache) > 100 {
		now := time.Now().Unix()
		m.cleanupScoreCache(now)
	}
}

func (m *Manager) GetClusterLoad() (map[string]*ports.NodeLoad, error) {
	keys, err := m.storage.ListByPrefix("cluster:load:")
	if err != nil {
		return nil, domain.ErrConnection
	}

	clusterLoad := make(map[string]*ports.NodeLoad)
	for _, kv := range keys {
		var load ports.NodeLoad
		if err := json.Unmarshal(kv.Value, &load); err != nil {
			m.logger.Warn("failed to unmarshal node load", "key", kv.Key, "error", err)
			continue
		}

		if load.ExecutionUnits == nil {
			load.ExecutionUnits = make(map[string]float64)
		}

		clusterLoad[load.NodeID] = &load
	}

	return clusterLoad, nil
}

func (m *Manager) GetNodeLoad(nodeID string) (*ports.NodeLoad, error) {
	key := fmt.Sprintf("cluster:load:%s", nodeID)
	data, _, exists, err := m.storage.Get(key)
	if err != nil {
		return nil, domain.ErrConnection
	}
	if !exists {
		return &ports.NodeLoad{
			NodeID:          nodeID,
			TotalWeight:     0,
			ExecutionUnits:  make(map[string]float64),
			RecentLatencyMs: 0,
			RecentErrorRate: 0,
			LastUpdated:     0,
		}, nil
	}

	var load ports.NodeLoad
	if err := json.Unmarshal(data, &load); err != nil {
		return nil, domain.ErrInvalidInput
	}

	if load.ExecutionUnits == nil {
		load.ExecutionUnits = make(map[string]float64)
	}

	return &load, nil
}
