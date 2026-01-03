package load_balancer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	rmetrics "runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type NodeMetrics struct {
	NodeID          string
	ResponseTime    float64
	CpuUsage        float64
	LastUpdated     time.Time
	MemoryUsage     float64
	ConnectionCount int
	ErrorRate       float64
	Pressure        float64
	ActiveWorkflows int
	Available       bool
}

type Config struct {
	FailurePolicy      string        `json:"failure_policy"` // "fail-open" or "fail-closed"
	PublishInterval    time.Duration `json:"publish_interval"`
	PublishDebounce    time.Duration `json:"publish_debounce"`
	AvailabilityWindow time.Duration `json:"availability_window"`
}

type Manager struct {
	events         ports.EventManager
	nodeID         string
	logger         *slog.Logger
	clusterManager ports.ClusterManager
	config         *Config

	mu       sync.RWMutex
	running  bool
	draining bool
	ctx      context.Context
	cancel   context.CancelFunc

	executionUnits  map[string]float64
	totalWeight     float64
	recentLatencyMs float64
	errorWindow     *RollingWindow
	// Minimal mode: no strategy/score caches
	nodeMetrics map[string]NodeMetrics

	// Gossip-based telemetry publishing
	broadcaster     func(msg []byte)
	publishDebounce time.Duration
	publishInterval    time.Duration
	availabilityWindow time.Duration
	publishCh          chan struct{}

	// Local pressure sampling (CPU and memory)
	lastCPUSampleSeconds float64
	lastCPUSampleTime    time.Time
	cpuSampleMu          sync.Mutex

	// test hook: when set, used to compute local pressure instead of sampling
	pressureFn func() float64
}

// score caches removed in minimal mode

func NewManager(events ports.EventManager, nodeID string, clusterManager ports.ClusterManager, config *Config, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	if config == nil {
		config = &Config{FailurePolicy: "fail-open"}
	}

	if config.FailurePolicy != "fail-open" && config.FailurePolicy != "fail-closed" {
		config.FailurePolicy = "fail-open"
	}

	manager := &Manager{
		events:         events,
		nodeID:         nodeID,
		clusterManager: clusterManager,
		config:         config,
		logger:         logger.With("component", "load-balancer"),
		executionUnits: make(map[string]float64),
		errorWindow:    NewRollingWindow(100),
		nodeMetrics:    make(map[string]NodeMetrics),

		publishInterval:    2 * time.Second,
		publishDebounce:    250 * time.Millisecond,
		availabilityWindow: 30 * time.Second,
		publishCh:          make(chan struct{}, 1),
	}

	if config.PublishInterval > 0 {
		manager.publishInterval = config.PublishInterval
	}
	if config.PublishDebounce > 0 {
		manager.publishDebounce = config.PublishDebounce
	}
	if config.AvailabilityWindow > 0 {
		manager.availabilityWindow = config.AvailabilityWindow
	}

	return manager
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

	go m.publisherLoop()

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
			} else if len(health.UnhealthyNodes) > 0 {
				m.logger.Info("cluster stable with some unhealthy nodes",
					"healthy_nodes", health.HealthyNodes,
					"unhealthy_nodes", health.UnhealthyNodes)
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
	return nil
}

func (m *Manager) onNodeStarted(event *domain.NodeStartedEvent) {
	if event.NodeID != m.nodeID {
		return
	}

	m.mu.Lock()

	const unit = 1.0
	m.executionUnits[event.WorkflowID] = unit
	m.totalWeight += unit

	m.mu.Unlock()

	m.requestPublish()
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

	m.mu.Unlock()

	m.requestPublish()
}

func (m *Manager) onNodeError(event *domain.NodeErrorEvent) {
	m.mu.Lock()

	if weight, exists := m.executionUnits[event.WorkflowID]; exists {
		m.totalWeight -= weight
		delete(m.executionUnits, event.WorkflowID)
	}

	m.errorWindow.Record(false)

	m.mu.Unlock()

	m.requestPublish()
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
				m.logger.Error("cluster below minimum viable nodes",
					"healthy_nodes", health.HealthyNodes,
					"minimum_nodes", health.MinimumNodes)
				return m.handleFailurePolicy("cluster below minimum viable nodes", nil), nil
			}
		}

		if !m.clusterManager.IsNodeActive(m.nodeID) {
			m.logger.Info("current node not active in cluster", "node_id", m.nodeID)
			return m.handleFailurePolicy("current node not active in cluster", nil), nil
		}
	}

	clusterLoad := m.getInMemoryClusterLoad()

	if len(clusterLoad) == 0 {
		return m.handleFailurePolicy("no valid cluster load data found", nil), nil
	}

	filteredLoad := m.filterActiveNodes(clusterLoad)
	if len(filteredLoad) == 0 {
		return m.handleFailurePolicy(fmt.Sprintf("no active nodes in cluster load data (total_nodes: %d)", len(clusterLoad)), nil), nil
	}

	type candidate struct {
		id       string
		active   int
		pressure float64
	}

	cands := make([]candidate, 0, len(filteredLoad))
	for id, load := range filteredLoad {
		if load == nil {
			continue
		}
		active := int(load.TotalWeight)
		c := candidate{id: id, active: active}

		if id != m.nodeID {
			m.mu.RLock()
			if nm, ok := m.nodeMetrics[id]; ok {
				c.pressure = nm.Pressure
			}
			m.mu.RUnlock()
		}
		cands = append(cands, c)
	}

	if len(cands) == 0 {
		return m.handleFailurePolicy("no candidates available", nil), nil
	}

	localPressure := m.collectLocalPressure()

	minActive := math.MaxInt32
	for _, c := range cands {
		if c.active < minActive {
			minActive = c.active
		}
	}

	winners := make([]candidate, 0, len(cands))
	for _, c := range cands {
		if c.active == minActive {
			if c.id == m.nodeID {
				c.pressure = localPressure
			}
			winners = append(winners, c)
		}
	}

	if len(winners) == 1 {
		return winners[0].id == m.nodeID, nil
	}

	selected := winners[0]
	for _, c := range winners[1:] {
		if c.pressure < selected.pressure {
			selected = c
		} else if c.pressure == selected.pressure {
			if strings.Compare(c.id, selected.id) < 0 {
				selected = c
			}
		}
	}

	return selected.id == m.nodeID, nil
}

func (m *Manager) RegisterConnectorLoad(connectorID string, weight float64) error {
	connectorID = strings.TrimSpace(connectorID)
	if connectorID == "" {
		return domain.NewValidationError("connector id cannot be empty", nil,
			domain.WithComponent("load-balancer"))
	}

	if weight <= 0 {
		weight = 1
	}

	key := connectorWorkloadKey(connectorID)

	m.mu.Lock()
	if existing, exists := m.executionUnits[key]; exists {
		if existing != weight {
			m.totalWeight -= existing
			if m.totalWeight < 0 {
				m.totalWeight = 0
			}
			m.executionUnits[key] = weight
			m.totalWeight += weight
		}
		m.mu.Unlock()
		return nil
	}

	m.executionUnits[key] = weight
	m.totalWeight += weight
	m.mu.Unlock()

	m.requestPublish()
	return nil
}

func (m *Manager) DeregisterConnectorLoad(connectorID string) error {
	connectorID = strings.TrimSpace(connectorID)
	if connectorID == "" {
		return domain.NewValidationError("connector id cannot be empty", nil,
			domain.WithComponent("load-balancer"))
	}

	key := connectorWorkloadKey(connectorID)

	m.mu.Lock()
	if weight, exists := m.executionUnits[key]; exists {
		delete(m.executionUnits, key)
		m.totalWeight -= weight
		if m.totalWeight < 0 {
			m.totalWeight = 0
		}
		m.mu.Unlock()
		m.requestPublish()
		return nil
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) getInMemoryClusterLoad() map[string]*ports.NodeLoad {
	m.mu.RLock()
	defer m.mu.RUnlock()

	clusterLoad := make(map[string]*ports.NodeLoad)

	clusterLoad[m.nodeID] = &ports.NodeLoad{
		NodeID:          m.nodeID,
		TotalWeight:     m.totalWeight,
		ExecutionUnits:  make(map[string]float64),
		RecentLatencyMs: m.recentLatencyMs,
		RecentErrorRate: m.errorWindow.GetErrorRate(),
		LastUpdated:     time.Now().Unix(),
	}

	for k, v := range m.executionUnits {
		clusterLoad[m.nodeID].ExecutionUnits[k] = v
	}

	now := time.Now()

	nodeMetricsCopy := make(map[string]NodeMetrics)
	for nodeID, metrics := range m.nodeMetrics {
		nodeMetricsCopy[nodeID] = metrics
	}

	for nodeID, metrics := range nodeMetricsCopy {

		if nodeID == m.nodeID {
			continue
		}

		window := m.availabilityWindow
		if window <= 0 {
			window = 30 * time.Second
		}
		if now.Sub(metrics.LastUpdated) > window {
			continue
		}

		clusterLoad[nodeID] = &ports.NodeLoad{
			NodeID:          nodeID,
			TotalWeight:     float64(metrics.ActiveWorkflows),
			ExecutionUnits:  make(map[string]float64),
			RecentLatencyMs: metrics.ResponseTime,
			RecentErrorRate: metrics.ErrorRate,
			LastUpdated:     metrics.LastUpdated.Unix(),
		}
	}

	return clusterLoad
}

// GetClusterLoad returns in-memory cluster load data (interface compatibility)
// (compat methods removed) Cluster load is internal-only and derived from in-memory telemetry.

// ReceiveLoadUpdate processes incoming load updates from other nodes
func (m *Manager) ReceiveLoadUpdate(update ports.LoadUpdate) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.nodeMetrics == nil {
		m.nodeMetrics = make(map[string]NodeMetrics)
	}

	m.nodeMetrics[update.NodeID] = NodeMetrics{
		NodeID:          update.NodeID,
		ResponseTime:    update.RecentLatencyMs,
		CpuUsage:        0,
		MemoryUsage:     0,
		ConnectionCount: update.ActiveWorkflows,
		ErrorRate:       update.RecentErrorRate,
		Pressure:        update.Pressure,
		ActiveWorkflows: update.ActiveWorkflows,
		LastUpdated:     time.Now(),
		Available:       true,
	}

	m.logger.Debug("received load update",
		"node_id", update.NodeID,
		"active_workflows", update.ActiveWorkflows,
		"total_weight", update.TotalWeight,
		"latency_ms", update.RecentLatencyMs,
		"error_rate", update.RecentErrorRate,
		"pressure", update.Pressure)

	m.cleanupStaleNodeMetrics()

	return nil
}

// cleanupStaleNodeMetrics removes outdated node metrics entries
func (m *Manager) cleanupStaleNodeMetrics() {
	now := time.Now()
	window := m.availabilityWindow
	if window <= 0 {
		window = 30 * time.Second
	}
	for nodeID, metrics := range m.nodeMetrics {

		if now.Sub(metrics.LastUpdated) > window {
			delete(m.nodeMetrics, nodeID)
			m.logger.Debug("removed stale node metrics", "node_id", nodeID)
		}
	}
}

func (m *Manager) SetBroadcaster(fn func(msg []byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broadcaster = fn
}

func (m *Manager) HandleBroadcast(from string, msg []byte) {
	var update ports.LoadUpdate
	if err := json.Unmarshal(msg, &update); err != nil {
		m.logger.Debug("invalid load broadcast", "from", from, "error", err)
		return
	}
	update.NodeID = from
	m.ReceiveLoadUpdate(update)
}

// requestPublish triggers a debounced publish of local metrics.
func (m *Manager) requestPublish() {
	select {
	case m.publishCh <- struct{}{}:
	default:
	}
}

func (m *Manager) publisherLoop() {

	interval := m.publishInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.publishLocalMetrics()
		case <-m.publishCh:

			debounce := m.publishDebounce
			if debounce <= 0 {
				debounce = 250 * time.Millisecond
			}
			time.Sleep(debounce)
			m.publishLocalMetrics()
		}
	}
}

func (m *Manager) publishLocalMetrics() {
	m.mu.RLock()
	broadcaster := m.broadcaster
	baseCtx := m.ctx
	update := ports.LoadUpdate{
		NodeID:          m.nodeID,
		ActiveWorkflows: int(len(m.executionUnits)),
		TotalWeight:     m.totalWeight,
		RecentLatencyMs: m.recentLatencyMs,
		RecentErrorRate: m.errorWindow.GetErrorRate(),
		Pressure:        m.collectLocalPressure(),
		Timestamp:       time.Now().Unix(),
	}
	m.mu.RUnlock()

	if baseCtx != nil && baseCtx.Err() != nil {
		return
	}

	if broadcaster == nil {
		return
	}

	data, err := json.Marshal(update)
	if err == nil {
		broadcaster(data)
	}
}

func (m *Manager) convertToNodeMetrics(clusterLoad map[string]*ports.NodeLoad) []NodeMetrics {
	if len(clusterLoad) == 0 {
		return nil
	}

	metrics := make([]NodeMetrics, 0, len(clusterLoad))
	for _, load := range clusterLoad {
		if load == nil {
			continue
		}

		active := len(load.ExecutionUnits)
		if active == 0 && load.TotalWeight > 0 {
			active = int(math.Round(load.TotalWeight))
		}

		lastUpdated := time.Unix(load.LastUpdated, 0)
		available := load.LastUpdated > 0

		metrics = append(metrics, NodeMetrics{
			NodeID:          load.NodeID,
			ResponseTime:    load.RecentLatencyMs,
			CpuUsage:        0,
			MemoryUsage:     0,
			ConnectionCount: active,
			ErrorRate:       load.RecentErrorRate,
			Pressure:        load.TotalWeight,
			ActiveWorkflows: active,
			LastUpdated:     lastUpdated,
			Available:       available,
		})
	}

	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].NodeID < metrics[j].NodeID
	})

	return metrics
}

func (m *Manager) handleFailurePolicy(reason string, err error) bool {
	switch m.config.FailurePolicy {
	case "fail-closed":
		m.logger.Warn("load balancer failing closed", "reason", reason, "error", err)
		return false
	case "fail-open":
		fallthrough
	default:
		m.logger.Warn("load balancer failing open", "reason", reason, "error", err)
		return true
	}
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

// Note: score caches and capacity helpers are removed in minimal mode.

func (m *Manager) GetLoadBalancingMetrics() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := map[string]interface{}{
		"failure_policy": m.config.FailurePolicy,
		"running":        m.running,
		"draining":       m.draining,
	}

	if len(m.nodeMetrics) > 0 {
		nodeCount := len(m.nodeMetrics)
		availableNodes := 0
		totalResponseTime := 0.0
		totalErrorRate := 0.0

		for _, node := range m.nodeMetrics {
			if node.Available {
				availableNodes++
			}
			totalResponseTime += node.ResponseTime
			totalErrorRate += node.ErrorRate
		}

		metrics["node_summary"] = map[string]interface{}{
			"total_nodes":       nodeCount,
			"available_nodes":   availableNodes,
			"avg_response_time": totalResponseTime / float64(nodeCount),
			"avg_error_rate":    totalErrorRate / float64(nodeCount),
		}
	}

	return metrics
}

// collectLocalPressure computes a simple pressure value by sampling CPU and memory.
// Returns a value in [0,2] (cpu [0,1] + mem [0,1]).
func (m *Manager) collectLocalPressure() float64 {
	if m.pressureFn != nil {
		return m.pressureFn()
	}
	cpu := m.sampleCPUUsage()
	mem := m.sampleMemPressure()
	if cpu < 0 {
		cpu = 0
	} else if cpu > 1 {
		cpu = 1
	}
	if mem < 0 {
		mem = 0
	} else if mem > 1 {
		mem = 1
	}
	p := cpu + mem
	if p < 0 {
		p = 0
	}
	if p > 2 {
		p = 2
	}
	return p
}

func connectorWorkloadKey(connectorID string) string {
	return "connector::" + connectorID
}

// sampleCPUUsage estimates CPU usage as process CPU seconds over wall time since last sample.
func (m *Manager) sampleCPUUsage() float64 {
	m.cpuSampleMu.Lock()
	defer m.cpuSampleMu.Unlock()

	samples := []rmetrics.Sample{{Name: "/process/cpu:seconds"}}
	rmetrics.Read(samples)

	var cpuSeconds float64
	if samples[0].Value.Kind() == rmetrics.KindFloat64 {
		cpuSeconds = samples[0].Value.Float64()
	} else {

		cpuSeconds = float64(runtime.NumGoroutine()) * 0.001
	}
	now := time.Now()

	if m.lastCPUSampleTime.IsZero() {
		m.lastCPUSampleSeconds = cpuSeconds
		m.lastCPUSampleTime = now
		return 0
	}

	deltaCPU := cpuSeconds - m.lastCPUSampleSeconds
	deltaWall := now.Sub(m.lastCPUSampleTime).Seconds()
	if deltaWall <= 0 {
		return 0
	}

	m.lastCPUSampleSeconds = cpuSeconds
	m.lastCPUSampleTime = now

	usage := deltaCPU / deltaWall
	if usage < 0 {
		usage = 0
	} else if usage > 1 {
		usage = 1
	}
	return usage
}

// sampleMemPressure estimates memory pressure using Alloc vs GOMEMLIMIT (bytes), if present.
func (m *Manager) sampleMemPressure() float64 {
	limitStr := os.Getenv("GOMEMLIMIT")
	if limitStr == "" {
		return 0
	}
	limitBytes, ok := parseBytes(limitStr)
	if !ok || limitBytes == 0 {
		return 0
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	alloc := ms.Alloc
	p := float64(alloc) / float64(limitBytes)
	if p < 0 {
		p = 0
	} else if p > 1 {
		p = 1
	}
	return p
}

func (m *Manager) OnPeerJoin(nodeID, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.nodeMetrics == nil {
		m.nodeMetrics = make(map[string]NodeMetrics)
	}

	m.nodeMetrics[nodeID] = NodeMetrics{
		NodeID:      nodeID,
		LastUpdated: time.Now(),
		Available:   true,
	}

	m.logger.Info("peer joined cluster", "node_id", nodeID, "address", address)
}

func (m *Manager) OnPeerLeave(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.nodeMetrics, nodeID)
	m.logger.Info("peer left cluster", "node_id", nodeID)
}

func parseBytes(s string) (uint64, bool) {
	s = strings.TrimSpace(strings.ToLower(s))
	mul := uint64(1)
	switch {
	case strings.HasSuffix(s, "kib"):
		mul = 1024
		s = strings.TrimSuffix(s, "kib")
	case strings.HasSuffix(s, "mib"):
		mul = 1024 * 1024
		s = strings.TrimSuffix(s, "mib")
	case strings.HasSuffix(s, "gib"):
		mul = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "gib")
	case strings.HasSuffix(s, "kb"):
		mul = 1000
		s = strings.TrimSuffix(s, "kb")
	case strings.HasSuffix(s, "mb"):
		mul = 1000 * 1000
		s = strings.TrimSuffix(s, "mb")
	case strings.HasSuffix(s, "gb"):
		mul = 1000 * 1000 * 1000
		s = strings.TrimSuffix(s, "gb")
	case strings.HasSuffix(s, "b"):
		mul = 1
		s = strings.TrimSuffix(s, "b")
	}
	s = strings.TrimSpace(s)
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n * mul, true
}
