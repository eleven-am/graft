package cluster

import (
	"context"
	"fmt"
	workflowEngine "github.com/eleven-am/graft/internal/adapters/engine"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Cluster struct {
	config ClusterConfig
	logger *slog.Logger

	discovery       ports.DiscoveryPort
	transport       ports.TransportPort
	storage         ports.StoragePort
	queue           ports.QueuePort
	nodeRegistry    ports.NodeRegistryPort
	resourceManager ports.ResourceManagerPort
	workflowEngine  ports.WorkflowEnginePort
	semaphore       ports.SemaphorePort

	nodeID    string
	isRunning bool
	isLeader  bool
	isStopped bool
	mu        sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	completionHandlers []ports.CompletionHandler
	errorHandlers      []ports.ErrorHandler

	healthMonitor *HealthMonitor
}

type HealthMonitor struct {
	config          HealthConfig
	logger          *slog.Logger
	mu              sync.RWMutex
	componentStates map[string]*ComponentHealthState
	overallHealthy  bool
	lastHealthCheck time.Time
}

type ComponentHealthState struct {
	Name                string
	Healthy             bool
	LastCheck           time.Time
	LastError           error
	ConsecutiveFailures int
	Config              ComponentCheckConfig
}

func New(config ClusterConfig, logger *slog.Logger) (*Cluster, error) {
	if err := ValidateClusterConfig(config); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = slog.Default()
	}

	cluster := &Cluster{
		config:             config,
		logger:             logger.With("component", "cluster", "node_id", config.NodeID),
		nodeID:             config.NodeID,
		completionHandlers: make([]ports.CompletionHandler, 0),
		errorHandlers:      make([]ports.ErrorHandler, 0),
		healthMonitor:      newHealthMonitor(config.Health, logger),
	}

	factory, err := NewComponentFactory(config, logger)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create component factory",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if err := cluster.initializeComponents(factory); err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to initialize components",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return cluster, nil
}

func (c *Cluster) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isRunning {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "cluster already running",
			Details: map[string]interface{}{
				"node_id": c.nodeID,
			},
		}
	}

	if c.isStopped {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "cluster has been stopped and cannot be restarted without re-initialization",
			Details: map[string]interface{}{
				"node_id": c.nodeID,
			},
		}
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	c.logger.Info("starting cluster components",
		"node_id", c.nodeID,
		"service_name", c.config.ServiceName,
		"service_port", c.config.ServicePort,
	)

	if err := c.connectComponents(); err != nil {
		c.cancel()
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to connect components",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if err := c.startComponents(); err != nil {
		c.cancel()
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to start components",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if err := c.advertiseService(); err != nil {
		c.cancel()
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "failed to advertise service during startup",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	c.isRunning = true
	c.wg.Add(1)
	go c.monitorClusterHealth()

	c.logger.Info("cluster started successfully",
		"node_id", c.nodeID,
		"components", "all",
	)

	return nil
}

func (c *Cluster) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isRunning {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "cluster not running",
			Details: map[string]interface{}{
				"node_id": c.nodeID,
			},
		}
	}

	c.logger.Info("stopping cluster",
		"node_id", c.nodeID,
	)

	c.cancel()
	c.wg.Wait()

	if err := c.stopComponents(); err != nil {
		c.logger.Error("failed to stop some components",
			"error", err.Error(),
		)
		return err
	}

	c.isRunning = false
	c.isStopped = true
	c.logger.Info("cluster stopped successfully",
		"node_id", c.nodeID,
	)
	return nil
}

func (c *Cluster) RegisterNode(node ports.NodePort) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.isRunning {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "cluster not running",
			Details: map[string]interface{}{
				"node_id": c.nodeID,
			},
		}
	}

	if c.nodeRegistry == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "node registry not available",
		}
	}

	return c.nodeRegistry.RegisterNode(node)
}

func (c *Cluster) ProcessTrigger(trigger ports.WorkflowTrigger) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.isRunning {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "cluster not running",
		}
	}

	if c.workflowEngine == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "workflow engine not available",
		}
	}

	return c.workflowEngine.ProcessTrigger(trigger)
}

func (c *Cluster) GetWorkflowStatus(workflowID string) (*ports.WorkflowStatus, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.isRunning {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "cluster not running",
		}
	}

	if c.workflowEngine == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "workflow engine not available",
		}
	}

	return c.workflowEngine.GetWorkflowStatus(workflowID)
}

func (c *Cluster) OnComplete(handler ports.CompletionHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.completionHandlers = append(c.completionHandlers, handler)
	c.updateLifecycleHandlers()
}

func (c *Cluster) OnError(handler ports.ErrorHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errorHandlers = append(c.errorHandlers, handler)
	c.updateLifecycleHandlers()
}

func (c *Cluster) updateLifecycleHandlers() {
	if engine, ok := c.workflowEngine.(*workflowEngine.Engine); ok {
		engine.RegisterLifecycleHandlers(c.completionHandlers, c.errorHandlers)
		c.logger.Debug("updated lifecycle handlers",
			"completion_handlers", len(c.completionHandlers),
			"error_handlers", len(c.errorHandlers),
		)
	}
}

func (c *Cluster) GetClusterInfo() ports.ClusterInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info := ports.ClusterInfo{
		NodeID:          c.nodeID,
		RegisteredNodes: []string{},
		ActiveWorkflows: 0,
		IsLeader:        c.isLeader,
		ClusterMembers:  []ports.ClusterMember{},
	}

	if c.nodeRegistry != nil {
		info.RegisteredNodes = c.nodeRegistry.ListNodes()
	}

	if c.resourceManager != nil {
		info.ResourceLimits = c.config.Resources
		info.ExecutionStats = c.resourceManager.GetExecutionStats()
	}

	if c.workflowEngine != nil {
		info.EngineMetrics = c.workflowEngine.GetExecutionMetrics()
		info.ActiveWorkflows = info.EngineMetrics.ActiveWorkflows
	}

	if c.discovery != nil {
		if peers, err := c.discovery.Discover(); err == nil {
			for _, peer := range peers {
				member := ports.ClusterMember{
					NodeID:   peer.ID,
					Address:  peer.Address,
					Status:   "active",
					IsLeader: peer.ID == c.nodeID && c.isLeader,
				}
				if peer.Port > 0 {
					member.Address = fmt.Sprintf("%s:%d", peer.Address, peer.Port)
				}
				info.ClusterMembers = append(info.ClusterMembers, member)
			}
		}
	}

	return info
}

func (c *Cluster) GetHealthStatus() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.healthMonitor == nil {
		return map[string]interface{}{
			"health_monitoring": "disabled",
		}
	}

	return c.healthMonitor.GetHealthStatus()
}

func (c *Cluster) initializeComponents(factory *ComponentFactory) error {
	var err error

	c.discovery, err = factory.CreateDiscovery()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create discovery component",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	c.transport, err = factory.CreateTransport()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create transport component",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	c.storage, err = factory.CreateStorage()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "FATAL: storage is required for workflow persistence and distributed coordination",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	c.queue, err = factory.CreateQueueWithStorage(c.storage)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "FATAL: queue is required for distributed workflow execution",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	c.nodeRegistry, err = factory.CreateNodeRegistry()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create node registry",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	c.resourceManager, err = factory.CreateResourceManager()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create resource manager",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	c.workflowEngine, err = factory.CreateWorkflowEngine()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create workflow engine",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	c.semaphore, err = factory.CreateSemaphore(c.storage)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create semaphore component",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return nil
}

func (c *Cluster) startComponents() error {
	discoveryConfig := convertDiscoveryConfig(c.config.Discovery)
	transportConfig := convertTransportConfig(c.config.Transport)

	startOrder := []struct {
		name      string
		startFunc func() error
	}{
		{"discovery", func() error { return c.discovery.Start(c.ctx, discoveryConfig) }},
		{"transport", func() error { return c.transport.Start(c.ctx, transportConfig) }},
		{"workflow_engine", func() error { return c.workflowEngine.Start(c.ctx) }},
	}

	for _, component := range startOrder {
		c.logger.Debug("starting component", "component", component.name)
		if err := component.startFunc(); err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: fmt.Sprintf("failed to start %s component", component.name),
				Details: map[string]interface{}{
					"component": component.name,
					"error":     err.Error(),
				},
			}
		}
		c.logger.Debug("component started successfully", "component", component.name)
	}

	return nil
}

func (c *Cluster) connectComponents() error {
	if engine, ok := c.workflowEngine.(*workflowEngine.Engine); ok {
		c.logger.Debug("injecting workflow engine dependencies")
		if c.nodeRegistry != nil {
			engine.SetNodeRegistry(c.nodeRegistry)
			c.logger.Debug("injected node registry")
		}
		if c.resourceManager != nil {
			engine.SetResourceManager(c.resourceManager)
			c.logger.Debug("injected resource manager")
		}
		if c.storage != nil {
			engine.SetStorage(c.storage)
			c.logger.Debug("injected storage")
		} else {
			c.logger.Debug("storage not available for injection")
		}
		if c.queue != nil {
			engine.SetQueue(c.queue)
			c.logger.Debug("injected queue")
		} else {
			c.logger.Debug("queue not available for injection")
		}
		if c.transport != nil {
			engine.SetTransport(c.transport)
			c.logger.Debug("injected transport")
		}
		if c.discovery != nil {
			engine.SetDiscovery(c.discovery)
			c.logger.Debug("injected discovery")
		}
		if c.semaphore != nil {
			engine.SetSemaphore(c.semaphore)
			c.logger.Debug("injected semaphore")
		}

		if len(c.completionHandlers) > 0 || len(c.errorHandlers) > 0 {
			engine.RegisterLifecycleHandlers(c.completionHandlers, c.errorHandlers)
			c.logger.Debug("injected lifecycle handlers",
				"completion_handlers", len(c.completionHandlers),
				"error_handlers", len(c.errorHandlers),
			)
		}

		c.logger.Info("workflow engine dependencies injected successfully")
	} else {
		c.logger.Error("could not cast workflow engine for dependency injection")
	}

	return nil
}

func (c *Cluster) advertiseService() error {
	address := c.config.AdvertiseAddress
	if address == "" {
		address = DefaultLocalhostAddress
	}

	serviceInfo := ports.ServiceInfo{
		ID:       c.nodeID,
		Name:     c.config.ServiceName,
		Address:  address,
		Port:     c.config.ServicePort,
		Metadata: make(map[string]string),
	}

	if err := c.discovery.Advertise(serviceInfo); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "failed to advertise service through discovery",
			Details: map[string]interface{}{
				"service_id":   serviceInfo.ID,
				"service_name": serviceInfo.Name,
				"error":        err.Error(),
			},
		}
	}

	c.logger.Debug("service advertised successfully", "service_id", c.nodeID)
	return nil
}

func (c *Cluster) stopComponents() error {
	var lastErr error

	stopOrder := []struct {
		name     string
		stopFunc func() error
	}{
		{"workflow_engine", func() error { return c.workflowEngine.Stop() }},
		{"transport", func() error { return c.transport.Stop() }},
		{"discovery", func() error { return c.discovery.Stop() }},
	}

	for _, component := range stopOrder {
		c.logger.Debug("stopping component", "component", component.name)
		if err := component.stopFunc(); err != nil {
			c.logger.Error("failed to stop component",
				"component", component.name,
				"error", err.Error(),
			)
			lastErr = err
		} else {
			c.logger.Debug("component stopped successfully", "component", component.name)
		}
	}

	return lastErr
}

func (c *Cluster) monitorClusterHealth() {
	defer c.wg.Done()

	if !c.config.Health.Enabled {
		c.logger.Info("health monitoring disabled")
		return
	}

	c.healthMonitor.start(c.ctx, c)
}

func newHealthMonitor(config HealthConfig, logger *slog.Logger) *HealthMonitor {
	componentStates := make(map[string]*ComponentHealthState)

	components := map[string]ComponentCheckConfig{
		"discovery":        config.Components.Discovery,
		"transport":        config.Components.Transport,
		"storage":          config.Components.Storage,
		"queue":            config.Components.Queue,
		"node_registry":    config.Components.NodeRegistry,
		"resource_manager": config.Components.ResourceManager,
		"workflow_engine":  config.Components.WorkflowEngine,
	}

	for name, componentConfig := range components {
		componentStates[name] = &ComponentHealthState{
			Name:                name,
			Healthy:             true,
			LastCheck:           time.Time{},
			LastError:           nil,
			ConsecutiveFailures: 0,
			Config:              componentConfig,
		}
	}

	return &HealthMonitor{
		config:          config,
		logger:          logger.With("component", "health-monitor"),
		componentStates: componentStates,
		overallHealthy:  true,
		lastHealthCheck: time.Time{},
	}
}

func (h *HealthMonitor) start(ctx context.Context, cluster *Cluster) {
	ticker := time.NewTicker(h.config.CheckInterval)
	defer ticker.Stop()

	h.logger.Info("health monitoring started",
		"check_interval", h.config.CheckInterval,
		"timeout", h.config.Timeout,
		"max_failures", h.config.MaxConsecutiveFailures)

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("health monitoring stopped")
			return
		case <-ticker.C:
			h.performHealthCheck(ctx, cluster)
		}
	}
}

func (h *HealthMonitor) performHealthCheck(ctx context.Context, cluster *Cluster) {
	checkCtx, cancel := context.WithTimeout(ctx, h.config.Timeout)
	defer cancel()

	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastHealthCheck = time.Now()
	overallHealthy := true

	cluster.mu.RLock()
	components := map[string]interface{}{
		"discovery":        cluster.discovery,
		"transport":        cluster.transport,
		"storage":          cluster.storage,
		"queue":            cluster.queue,
		"node_registry":    cluster.nodeRegistry,
		"resource_manager": cluster.resourceManager,
		"workflow_engine":  cluster.workflowEngine,
	}
	isRunning := cluster.isRunning
	cluster.mu.RUnlock()

	if !isRunning {
		h.logger.Debug("cluster not running, skipping health check")
		return
	}

	for name, component := range components {
		state := h.componentStates[name]
		if !state.Config.Enabled {
			continue
		}

		state.LastCheck = time.Now()
		healthy := h.checkComponent(checkCtx, name, component, state)

		if healthy {
			if !state.Healthy {
				h.logger.Info("component recovered", "component", name, "previous_failures", state.ConsecutiveFailures)
			}
			state.Healthy = true
			state.ConsecutiveFailures = 0
			state.LastError = nil
		} else {
			state.Healthy = false
			state.ConsecutiveFailures++
			overallHealthy = false

			if state.ConsecutiveFailures >= state.Config.MaxConsecutiveFailures {
				h.logger.Error("component health check failed - max consecutive failures reached",
					"component", name,
					"consecutive_failures", state.ConsecutiveFailures,
					"max_failures", state.Config.MaxConsecutiveFailures,
					"last_error", state.LastError)
			} else {
				h.logger.Warn("component health check failed",
					"component", name,
					"consecutive_failures", state.ConsecutiveFailures,
					"max_failures", state.Config.MaxConsecutiveFailures,
					"last_error", state.LastError)
			}
		}
	}

	if h.overallHealthy != overallHealthy {
		if overallHealthy {
			h.logger.Info("cluster health restored")
		} else {
			h.logger.Error("cluster health degraded")
		}
	}

	h.overallHealthy = overallHealthy

	if overallHealthy {
		h.logger.Debug("cluster health check passed")
	} else {
		h.logger.Error("cluster health check failed")
	}
}

func (h *HealthMonitor) checkComponent(ctx context.Context, name string, component interface{}, state *ComponentHealthState) bool {
	if component == nil {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "component not initialized",
		}
		return false
	}

	switch name {
	case "storage":
		return h.checkStorageHealth(ctx, component, state)
	case "queue":
		return h.checkQueueHealth(ctx, component, state)
	case "transport":
		return h.checkTransportHealth(ctx, component, state)
	case "discovery":
		return h.checkDiscoveryHealth(ctx, component, state)
	case "workflow_engine":
		return h.checkWorkflowEngineHealth(ctx, component, state)
	case "resource_manager":
		return h.checkResourceManagerHealth(ctx, component, state)
	case "node_registry":
		return h.checkNodeRegistryHealth(ctx, component, state)
	default:
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "unknown component type",
		}
		return false
	}
}

func (h *HealthMonitor) checkStorageHealth(ctx context.Context, component interface{}, state *ComponentHealthState) bool {
	storage, ok := component.(ports.StoragePort)
	if !ok {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "invalid storage component type",
		}
		return false
	}

	testKey := "__health_check_" + time.Now().Format("20060102150405")
	testValue := []byte("health_check")

	if err := storage.Put(ctx, testKey, testValue); err != nil {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "storage put operation failed",
			Details: map[string]interface{}{"error": err.Error()},
		}
		return false
	}

	if _, err := storage.Get(ctx, testKey); err != nil {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "storage get operation failed",
			Details: map[string]interface{}{"error": err.Error()},
		}
		return false
	}

	if err := storage.Delete(ctx, testKey); err != nil {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "storage delete operation failed",
			Details: map[string]interface{}{"error": err.Error()},
		}
		return false
	}

	return true
}

func (h *HealthMonitor) checkQueueHealth(ctx context.Context, component interface{}, state *ComponentHealthState) bool {
	queue, ok := component.(ports.QueuePort)
	if !ok {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "invalid queue component type",
		}
		return false
	}

	isEmpty, err := queue.IsEmpty(ctx)
	if err != nil {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "queue empty check failed",
			Details: map[string]interface{}{"error": err.Error()},
		}
		return false
	}

	pendingItems, err := queue.GetPendingItems(ctx)
	if err != nil {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "queue pending items check failed",
			Details: map[string]interface{}{"error": err.Error()},
		}
		return false
	}

	h.logger.Debug("queue health check passed", "is_empty", isEmpty, "pending_count", len(pendingItems))
	return true
}

func (h *HealthMonitor) checkTransportHealth(ctx context.Context, component interface{}, state *ComponentHealthState) bool {
	transport, ok := component.(ports.TransportPort)
	if !ok {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "invalid transport component type",
		}
		return false
	}

	leader, err := transport.GetLeader(ctx)
	if err != nil {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "transport get leader failed",
			Details: map[string]interface{}{"error": err.Error()},
		}
		return false
	}

	h.logger.Debug("transport health check passed", "leader", func() string {
		if leader != nil {
			return leader.NodeID
		}
		return "none"
	}())
	return true
}

func (h *HealthMonitor) checkDiscoveryHealth(ctx context.Context, component interface{}, state *ComponentHealthState) bool {
	discovery, ok := component.(ports.DiscoveryPort)
	if !ok {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "invalid discovery component type",
		}
		return false
	}

	peers, err := discovery.Discover()
	if err != nil {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "discovery operation failed",
			Details: map[string]interface{}{"error": err.Error()},
		}
		return false
	}

	h.logger.Debug("discovery health check passed", "peer_count", len(peers))
	return true
}

func (h *HealthMonitor) checkWorkflowEngineHealth(ctx context.Context, component interface{}, state *ComponentHealthState) bool {
	engine, ok := component.(ports.WorkflowEnginePort)
	if !ok {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "invalid workflow engine component type",
		}
		return false
	}

	metrics := engine.GetExecutionMetrics()
	h.logger.Debug("workflow engine health check passed",
		"active_workflows", metrics.ActiveWorkflows,
		"total_workflows", metrics.TotalWorkflows)
	return true
}

func (h *HealthMonitor) checkResourceManagerHealth(ctx context.Context, component interface{}, state *ComponentHealthState) bool {
	resourceManager, ok := component.(ports.ResourceManagerPort)
	if !ok {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "invalid resource manager component type",
		}
		return false
	}

	stats := resourceManager.GetExecutionStats()
	h.logger.Debug("resource manager health check passed",
		"total_executing", stats.TotalExecuting,
		"total_capacity", stats.TotalCapacity,
		"available_slots", stats.AvailableSlots)
	return true
}

func (h *HealthMonitor) checkNodeRegistryHealth(ctx context.Context, component interface{}, state *ComponentHealthState) bool {
	nodeRegistry, ok := component.(ports.NodeRegistryPort)
	if !ok {
		state.LastError = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "invalid node registry component type",
		}
		return false
	}

	nodes := nodeRegistry.ListNodes()
	h.logger.Debug("node registry health check passed", "registered_nodes", len(nodes))
	return true
}

func (h *HealthMonitor) GetHealthStatus() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	status := make(map[string]interface{})
	status["overall_healthy"] = h.overallHealthy
	status["last_check"] = h.lastHealthCheck
	status["check_interval"] = h.config.CheckInterval
	status["timeout"] = h.config.Timeout

	components := make(map[string]interface{})
	for name, state := range h.componentStates {
		components[name] = map[string]interface{}{
			"healthy":              state.Healthy,
			"last_check":           state.LastCheck,
			"consecutive_failures": state.ConsecutiveFailures,
			"last_error": func() string {
				if state.LastError != nil {
					return state.LastError.Error()
				}
				return ""
			}(),
		}
	}
	status["components"] = components

	return status
}

func convertDiscoveryConfig(config DiscoveryConfig) ports.DiscoveryConfig {
	return ports.DiscoveryConfig{
		ServiceName: config.ServiceName,
		ServicePort: config.ServicePort,
		Metadata:    config.Metadata,
	}
}

func convertTransportConfig(config TransportConfig) ports.TransportConfig {
	var tlsConfig *ports.TLSConfig
	if config.TLS.Enabled {
		tlsConfig = &ports.TLSConfig{
			Enabled:  config.TLS.Enabled,
			CertFile: config.TLS.CertFile,
			KeyFile:  config.TLS.KeyFile,
			CAFile:   config.TLS.CAFile,
		}
	}

	return ports.TransportConfig{
		BindAddress: config.ListenAddress,
		BindPort:    config.ListenPort,
		TLS:         tlsConfig,
	}
}
