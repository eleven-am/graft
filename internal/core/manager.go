package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/eleven-am/graft/internal/adapters/circuit_breaker"
	"github.com/eleven-am/graft/internal/adapters/cluster"
	"github.com/eleven-am/graft/internal/adapters/discovery"
	"github.com/eleven-am/graft/internal/adapters/engine"
	"github.com/eleven-am/graft/internal/adapters/events"
	"github.com/eleven-am/graft/internal/adapters/load_balancer"
	"github.com/eleven-am/graft/internal/adapters/node_registry"
	"github.com/eleven-am/graft/internal/adapters/observability"
	"github.com/eleven-am/graft/internal/adapters/queue"
	"github.com/eleven-am/graft/internal/adapters/raft"
	"github.com/eleven-am/graft/internal/adapters/rate_limiter"
	"github.com/eleven-am/graft/internal/adapters/storage"
	"github.com/eleven-am/graft/internal/adapters/tracing"
	"github.com/eleven-am/graft/internal/adapters/transport"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"

	badger "github.com/dgraph-io/badger/v3"
)

type Manager struct {
	engine          ports.EnginePort
	storage         ports.StoragePort
	queue           ports.QueuePort
	nodeRegistry    ports.NodeRegistryPort
	transport       ports.TransportPort
	discovery       *discovery.Manager
	raftAdapter     ports.RaftNode
	eventManager    ports.EventManager
	loadBalancer    ports.LoadBalancer
	clusterManager  ports.ClusterManager
	observability   *observability.Server
	circuitBreakers ports.CircuitBreakerProvider
	rateLimiters    ports.RateLimiterProvider
	tracing         ports.TracingProvider

	config *domain.Config
	logger *slog.Logger
	nodeID string

	ctx    context.Context
	cancel context.CancelFunc
}

type managerProviders struct {
	newEventManager   func(*slog.Logger) ports.EventManager
	newRaftStorage    func(dataDir string, logger *slog.Logger) (*raft.Storage, error)
	newRaftNode       func(cfg *raft.Config, storage *raft.Storage, events ports.EventManager, logger *slog.Logger) (ports.RaftNode, error)
	newAppStorage     func(raft ports.RaftNode, db *badger.DB, logger *slog.Logger) ports.StoragePort
	newNodeRegistry   func(*slog.Logger) ports.NodeRegistryPort
	newClusterManager func(ports.RaftNode, int, *slog.Logger) ports.ClusterManager
	newLoadBalancer   func(ports.StoragePort, ports.EventManager, string, ports.ClusterManager, *load_balancer.Config, *slog.Logger) ports.LoadBalancer
	newQueue          func(name string, storage ports.StoragePort, events ports.EventManager, logger *slog.Logger) ports.QueuePort
	newEngine         func(domain.EngineConfig, string, ports.NodeRegistryPort, ports.QueuePort, ports.StoragePort, ports.EventManager, ports.LoadBalancer, *slog.Logger) ports.EnginePort
	newTransport      func(*slog.Logger, domain.TransportConfig) ports.TransportPort
}

func defaultProviders() managerProviders {
	return managerProviders{
		newEventManager: func(l *slog.Logger) ports.EventManager { return events.NewManager(l) },
		newRaftStorage:  func(dataDir string, l *slog.Logger) (*raft.Storage, error) { return raft.NewStorage(dataDir, l) },
		newRaftNode: func(cfg *raft.Config, st *raft.Storage, ev ports.EventManager, l *slog.Logger) (ports.RaftNode, error) {
			return raft.NewNode(cfg, st, ev, l)
		},
		newAppStorage: func(r ports.RaftNode, db *badger.DB, l *slog.Logger) ports.StoragePort {
			return storage.NewAppStorage(r, db, l)
		},
		newNodeRegistry: func(l *slog.Logger) ports.NodeRegistryPort { return node_registry.NewManager(l) },
		newClusterManager: func(r ports.RaftNode, min int, l *slog.Logger) ports.ClusterManager {
			return cluster.NewRaftClusterManager(r, min, l)
		},
		newLoadBalancer: func(s ports.StoragePort, ev ports.EventManager, nodeID string, cm ports.ClusterManager, cfg *load_balancer.Config, l *slog.Logger) ports.LoadBalancer {
			return load_balancer.NewManager(s, ev, nodeID, cm, cfg, l)
		},
		newQueue: func(name string, s ports.StoragePort, ev ports.EventManager, l *slog.Logger) ports.QueuePort {
			return queue.NewQueue(name, s, ev, l)
		},
		newEngine: func(cfg domain.EngineConfig, nodeID string, nr ports.NodeRegistryPort, q ports.QueuePort, s ports.StoragePort, ev ports.EventManager, lb ports.LoadBalancer, l *slog.Logger) ports.EnginePort {
			return engine.NewEngine(cfg, nodeID, nr, q, s, ev, lb, l)
		},
		newTransport: func(l *slog.Logger, cfg domain.TransportConfig) ports.TransportPort {
			return transport.NewGRPCTransport(l, cfg)
		},
	}
}

type ClusterMetrics struct {
	TotalWorkflows     int `json:"total_workflows"`
	ActiveWorkflows    int `json:"active_workflows"`
	CompletedWorkflows int `json:"completed_workflows"`
	FailedWorkflows    int `json:"failed_workflows"`
	NodesExecuted      int `json:"nodes_executed"`
}

type ClusterInfo struct {
	NodeID   string         `json:"node_id"`
	Status   string         `json:"status"`
	IsLeader bool           `json:"is_leader"`
	Peers    []string       `json:"peers"`
	Metrics  ClusterMetrics `json:"metrics"`
}

type WorkflowStartedEvent = domain.WorkflowStartedEvent
type WorkflowCompletedEvent = domain.WorkflowCompletedEvent
type WorkflowErrorEvent = domain.WorkflowErrorEvent
type WorkflowPausedEvent = domain.WorkflowPausedEvent
type WorkflowResumedEvent = domain.WorkflowResumedEvent
type NodeStartedEvent = domain.NodeStartedEvent
type NodeCompletedEvent = domain.NodeCompletedEvent
type NodeErrorEvent = domain.NodeErrorEvent
type CommandHandler = domain.CommandHandler
type DevCommand = domain.DevCommand
type NodeJoinedEvent = domain.NodeJoinedEvent
type NodeLeftEvent = domain.NodeLeftEvent
type LeaderChangedEvent = domain.LeaderChangedEvent

func New(nodeID, bindAddr, dataDir string, logger *slog.Logger) *Manager {
	config := domain.NewConfigFromSimple(nodeID, bindAddr, dataDir, logger)
	return NewWithConfig(config)
}

func NewWithConfig(config *domain.Config) *Manager {
	if err := config.Validate(); err != nil {
		config.Logger.Error("invalid configuration", "error", err)
		return nil
	}

	logger := config.Logger.With("component", "graft", "node_id", config.NodeID)

	var cleanup []func() error
	defer func() {
		if len(cleanup) > 0 {
			for i := len(cleanup) - 1; i >= 0; i-- {
				if err := cleanup[i](); err != nil {
					logger.Error("cleanup failed", "error", err)
				}
			}
		}
	}()

	discoveryManager := createDiscoveryManager(config, logger)

	raftConfig := raft.DefaultRaftConfig(config.NodeID, config.ClusterID, config.BindAddr, config.DataDir, config.Cluster.Policy)

	prov := defaultProviders()

	raftStorage, err := prov.newRaftStorage(config.DataDir, logger)
	if err != nil {
		logger.Error("failed to create raft storage", "error", err)
		return nil
	}
	cleanup = append(cleanup, raftStorage.Close)

	eventManager := prov.newEventManager(logger)
	cleanup = append(cleanup, func() error {
		if eventManager != nil {
			return eventManager.Stop()
		}
		return nil
	})

	raftAdapter, err := prov.newRaftNode(raftConfig, raftStorage, eventManager, logger)
	if err != nil {
		logger.Error("failed to create raft node", "error", err)
		return nil
	}
	cleanup = append(cleanup, raftAdapter.Stop)

	appStorage := prov.newAppStorage(raftAdapter, raftStorage.StateDB(), logger)

	if setter, ok := eventManager.(interface {
		SetStorage(ports.StoragePort, string)
	}); ok {
		setter.SetStorage(appStorage, config.NodeID)
	}

	nodeRegistryManager := prov.newNodeRegistry(logger)

	clusterManager := prov.newClusterManager(raftAdapter, 1, logger)

	loadBalancerConfig := &load_balancer.Config{
		ScoreCacheTTL:   config.LoadBalancer.ScoreCacheTTL,
		FailurePolicy:   config.LoadBalancer.FailurePolicy,
		DefaultCapacity: config.LoadBalancer.DefaultCapacity,
		Algorithm:       config.LoadBalancer.Algorithm,
		WeightedConfig:  config.LoadBalancer.WeightedConfig,
		AdaptiveConfig:  config.LoadBalancer.AdaptiveConfig,
	}
	loadBalancerManager := prov.newLoadBalancer(appStorage, eventManager, config.NodeID, clusterManager, loadBalancerConfig, logger)
	cleanup = append(cleanup, func() error {
		if loadBalancerManager != nil {
			return loadBalancerManager.Stop()
		}
		return nil
	})

	queueAdapter := prov.newQueue("main", appStorage, eventManager, logger)
	cleanup = append(cleanup, queueAdapter.Close)

	engineAdapter := prov.newEngine(config.Engine, config.NodeID, nodeRegistryManager, queueAdapter, appStorage, eventManager, loadBalancerManager, logger)
	cleanup = append(cleanup, func() error {
		if engineAdapter != nil {
			return engineAdapter.Stop()
		}
		return nil
	})

	var circuitBreakerProvider ports.CircuitBreakerProvider
	if config.CircuitBreaker.Enabled {
		circuitBreakerProvider = circuit_breaker.NewProvider(logger)
	}

	var rateLimiterProvider ports.RateLimiterProvider
	if config.RateLimiter.Enabled {
		rateLimiterProvider = rate_limiter.NewProvider(logger)
	}

	var tracingProvider ports.TracingProvider
	if config.Tracing.Enabled {
		tracingProvider = tracing.NewTracingProvider(config.Tracing, logger)
		cleanup = append(cleanup, func() error {
			if tracingProvider != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return tracingProvider.Shutdown(ctx)
			}
			return nil
		})
	}

	manager := &Manager{
		config:          config,
		logger:          logger,
		nodeID:          config.NodeID,
		discovery:       discoveryManager,
		raftAdapter:     raftAdapter,
		storage:         appStorage,
		eventManager:    eventManager,
		nodeRegistry:    nodeRegistryManager,
		queue:           queueAdapter,
		engine:          engineAdapter,
		loadBalancer:    loadBalancerManager,
		clusterManager:  clusterManager,
		circuitBreakers: circuitBreakerProvider,
		rateLimiters:    rateLimiterProvider,
		tracing:         tracingProvider,
	}

	if config.Observability.Enabled {
		manager.observability = observability.NewServer(config.Observability.Port, manager, manager, logger)
	}

	cleanup = nil
	return manager
}

func createDiscoveryManager(config *domain.Config, logger *slog.Logger) *discovery.Manager {
	manager := discovery.NewManager(config.NodeID, logger)

	for _, discoveryConfig := range config.Discovery {
		switch discoveryConfig.Type {
		case domain.DiscoveryMDNS:
			if discoveryConfig.MDNS != nil {
				manager.MDNS(discoveryConfig.MDNS.Service, discoveryConfig.MDNS.Domain)
			} else {
				manager.MDNS("", "")
			}
		case domain.DiscoveryStatic:
			if len(discoveryConfig.Static) > 0 {
				peers := make([]ports.Peer, len(discoveryConfig.Static))
				for i, staticPeer := range discoveryConfig.Static {
					peers[i] = ports.Peer{
						ID:       staticPeer.ID,
						Address:  staticPeer.Address,
						Port:     staticPeer.Port,
						Metadata: staticPeer.Metadata,
					}
				}
				manager.Static(peers)
			}
		}
	}

	return manager
}

func (m *Manager) Discovery() *discovery.Manager {
	return m.discovery
}

func (m *Manager) MDNS(service, domain string) *Manager {
	m.discovery.MDNS(service, domain)
	return m
}

func (m *Manager) AddProvider(provider ports.Provider) *Manager {
	m.discovery.Add(provider)
	return m
}

func (m *Manager) Static(peers []ports.Peer) *Manager {
	m.discovery.Static(peers)
	return m
}

func (m *Manager) Start(ctx context.Context, grpcPort int) error {
	m.ctx, m.cancel = context.WithCancel(ctx)

	host, portStr, err := net.SplitHostPort(m.config.BindAddr)
	if err != nil {
		return fmt.Errorf("invalid bind address: %w", err)
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid bind port: %w", err)
	}
	if err := m.discovery.Start(m.ctx, host, p); err != nil {
		return fmt.Errorf("failed to start discovery: %w", err)
	}

	existingPeers, err := m.waitForDiscovery(ctx, m.config.Raft.DiscoveryTimeout)
	if err != nil {
		return fmt.Errorf("discovery wait failed: %w", err)
	}

	if len(existingPeers) == 0 && !m.shouldBootstrap(existingPeers, &m.config.Raft) {

		m.logger.Info("deferring bootstrap decision, will wait for other nodes")

		extendedTimeout := m.config.Raft.DiscoveryTimeout * 3
		existingPeers, err = m.waitForDiscovery(ctx, extendedTimeout)
		if err != nil {
			return fmt.Errorf("extended discovery wait failed: %w", err)
		}

		if len(existingPeers) == 0 {
			m.logger.Warn("no peers found after extended wait, proceeding with bootstrap")
		}
	}

	if err := m.raftAdapter.Start(ctx, existingPeers); err != nil {
		return fmt.Errorf("failed to start raft node: %w", err)
	}

	if err := m.loadBalancer.Start(m.ctx); err != nil {
		return fmt.Errorf("failed to start load balancer: %w", err)
	}

	if err := m.engine.Start(m.ctx); err != nil {
		return fmt.Errorf("failed to start engine: %w", err)
	}

	if err := m.eventManager.Start(m.ctx); err != nil {
		return fmt.Errorf("failed to start event manager: %w", err)
	}

	if m.observability != nil {
		go func() {
			if err := m.observability.Start(m.ctx); err != nil {
				m.logger.Error("observability server failed", "error", err)
			}
		}()
	}

	m.transport = transport.NewGRPCTransport(m.logger, m.config.Transport)
	m.transport.RegisterRaft(m.raftAdapter)

	if err := m.transport.Start(m.ctx, m.config.BindAddr, grpcPort); err != nil {
		return err
	}

	if len(existingPeers) > 0 {
		joinCtx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
		defer cancel()
		for _, peer := range existingPeers {
			if peer.ID == m.nodeID {
				continue
			}
			peerAddr := net.JoinHostPort(peer.Address, strconv.Itoa(peer.Port))
			req := &ports.JoinRequest{NodeID: m.nodeID, Address: host, Port: grpcPort, Metadata: map[string]string{"cluster_id": m.config.ClusterID}}
			resp, err := m.transport.SendJoinRequest(joinCtx, peerAddr, req)
			if err != nil {
				m.logger.Warn("join request failed", "peer", peerAddr, "error", err)
				continue
			}
			if resp != nil && resp.Accepted {
				m.logger.Info("successfully joined cluster via RPC", "peer", peerAddr)
				break
			} else {
				m.logger.Warn("join request not accepted", "peer", peerAddr, "message", func() string {
					if resp != nil {
						return resp.Message
					}
					return ""
				}())
			}
		}
	}

	return nil
}

func (m *Manager) Stop() error {
	if m.cancel != nil {
		m.cancel()
	}

	if m.discovery != nil {
		m.discovery.Stop()
	}

	if m.engine != nil {
		m.engine.Stop()
	}

	if m.loadBalancer != nil {
		m.loadBalancer.Stop()
	}

	if m.queue != nil {
		m.queue.Close()
	}

	if m.eventManager != nil {
		m.eventManager.Stop()
	}

	if m.transport != nil {
		m.transport.Stop()
	}

	return nil
}

func (m *Manager) RegisterNode(node interface{}) error {

	return m.nodeRegistry.RegisterNode(node)
}

func (m *Manager) UnregisterNode(nodeName string) error {
	return m.nodeRegistry.UnregisterNode(nodeName)
}

func (m *Manager) StartWorkflow(trigger WorkflowTrigger) error {
	if m.engine == nil {
		return fmt.Errorf("engine not started")
	}
	internalTrigger, err := trigger.toInternal()
	if err != nil {
		return err
	}
	return m.engine.ProcessTrigger(internalTrigger)
}

func (m *Manager) GetWorkflowStatus(workflowID string) (*WorkflowStatus, error) {
	if m.engine == nil {
		return nil, fmt.Errorf("engine not started")
	}
	internalStatus, err := m.engine.GetWorkflowStatus(workflowID)
	if err != nil {
		return nil, err
	}

	publicStatus, err := workflowStatusFromInternal(*internalStatus)
	if err != nil {
		return nil, err
	}

	return &publicStatus, nil
}

func (m *Manager) PauseWorkflow(ctx context.Context, workflowID string) error {
	if m.engine == nil {
		return fmt.Errorf("engine not started")
	}
	return m.engine.PauseWorkflow(ctx, workflowID)
}

func (m *Manager) ResumeWorkflow(ctx context.Context, workflowID string) error {
	if m.engine == nil {
		return fmt.Errorf("engine not started")
	}
	return m.engine.ResumeWorkflow(ctx, workflowID)
}

func (m *Manager) StopWorkflow(ctx context.Context, workflowID string) error {
	if m.engine == nil {
		return fmt.Errorf("engine not started")
	}
	return m.engine.StopWorkflow(ctx, workflowID)
}

func (m *Manager) GetClusterInfo() ClusterInfo {
	info := ClusterInfo{
		NodeID:   m.nodeID,
		Status:   "stopped",
		IsLeader: false,
		Peers:    []string{},
		Metrics:  ClusterMetrics{},
	}

	if m.raftAdapter != nil {
		raftInfo := m.raftAdapter.GetClusterInfo()
		info.IsLeader = (raftInfo.Leader != nil && raftInfo.Leader.ID == m.nodeID)

		for _, member := range raftInfo.Members {
			if member.ID != m.nodeID {
				info.Peers = append(info.Peers, member.ID)
			}
		}
		info.Status = "running"
	} else if m.discovery != nil {
		peers := m.discovery.GetPeers()
		peerIDs := make([]string, len(peers))
		for i, peer := range peers {
			peerIDs[i] = peer.ID
		}
		info.Peers = peerIDs
		info.Status = "running"
	}

	if m.storage != nil {
		metrics := m.calculateClusterMetrics()
		info.Metrics = metrics
	}

	return info
}

func (m *Manager) OnWorkflowStarted(handler func(*WorkflowStartedEvent)) error {
	return m.eventManager.OnWorkflowStarted(handler)
}

func (m *Manager) OnWorkflowCompleted(handler func(*WorkflowCompletedEvent)) error {
	return m.eventManager.OnWorkflowCompleted(handler)
}

func (m *Manager) OnWorkflowFailed(handler func(*WorkflowErrorEvent)) error {
	return m.eventManager.OnWorkflowFailed(handler)
}

func (m *Manager) OnWorkflowPaused(handler func(*WorkflowPausedEvent)) error {
	return m.eventManager.OnWorkflowPaused(handler)
}

func (m *Manager) OnWorkflowResumed(handler func(*WorkflowResumedEvent)) error {
	return m.eventManager.OnWorkflowResumed(handler)
}

func (m *Manager) OnNodeStarted(handler func(*NodeStartedEvent)) error {
	return m.eventManager.OnNodeStarted(handler)
}

func (m *Manager) OnNodeCompleted(handler func(*NodeCompletedEvent)) error {
	return m.eventManager.OnNodeCompleted(handler)
}

func (m *Manager) OnNodeError(handler func(*NodeErrorEvent)) error {
	return m.eventManager.OnNodeError(handler)
}

func (m *Manager) Subscribe(pattern string, handler func(string, interface{})) error {
	return m.eventManager.Subscribe(pattern, handler)
}

func (m *Manager) Unsubscribe(pattern string) error {
	return m.eventManager.Unsubscribe(pattern)
}

func (m *Manager) BroadcastCommand(ctx context.Context, devCmd *DevCommand) error {
	return m.eventManager.BroadcastCommand(ctx, devCmd)
}

func (m *Manager) RegisterCommandHandler(cmdName string, handler CommandHandler) error {
	return m.eventManager.RegisterCommandHandler(cmdName, handler)
}

func (m *Manager) OnNodeJoined(handler func(event *NodeJoinedEvent)) error {
	return m.eventManager.OnNodeJoined(handler)
}

func (m *Manager) OnNodeLeft(handler func(event *NodeLeftEvent)) error {
	return m.eventManager.OnNodeLeft(handler)
}

func (m *Manager) OnLeaderChanged(handler func(event *LeaderChangedEvent)) error {
	return m.eventManager.OnLeaderChanged(handler)
}

func (m *Manager) SubscribeToWorkflowState(workflowID string) (<-chan *WorkflowStatus, func(), error) {
	if m.engine == nil {
		return nil, nil, fmt.Errorf("engine not started")
	}

	statusChan := make(chan *WorkflowStatus, 10)
	pattern := fmt.Sprintf("workflow:state:%s", workflowID)

	err := m.eventManager.Subscribe(pattern, func(key string, data interface{}) {
		internalStatus, err := m.engine.GetWorkflowStatus(workflowID)
		if err != nil {
			m.logger.Error("failed to get workflow status for subscription",
				"workflow_id", workflowID,
				"error", err)
			return
		}

		publicStatus, err := workflowStatusFromInternal(*internalStatus)
		if err != nil {
			m.logger.Error("failed to convert workflow status for subscription",
				"workflow_id", workflowID,
				"error", err)
			return
		}

		select {
		case statusChan <- &publicStatus:
		default:
			m.logger.Warn("workflow status channel full, dropping update",
				"workflow_id", workflowID)
		}
	})

	if err != nil {
		close(statusChan)
		return nil, nil, err
	}

	unsubscribe := func() {
		m.eventManager.Unsubscribe(pattern)
		close(statusChan)
	}

	return statusChan, unsubscribe, nil
}

func (m *Manager) calculateClusterMetrics() ClusterMetrics {
	totalWorkflows := 0
	activeWorkflows := 0
	completedWorkflows := 0
	failedWorkflows := 0
	nodesExecuted := 0

	if workflowItems, err := m.storage.ListByPrefix("workflow:state:"); err == nil {
		totalWorkflows = len(workflowItems)
		for _, item := range workflowItems {
			var workflow domain.WorkflowInstance
			if err := json.Unmarshal(item.Value, &workflow); err == nil {
				switch workflow.Status {
				case domain.WorkflowStateRunning:
					activeWorkflows++
				case domain.WorkflowStateCompleted:
					completedWorkflows++
				case domain.WorkflowStateFailed:
					failedWorkflows++
				}
			}
		}
	}

	if executionItems, err := m.storage.ListByPrefix("workflow:execution:"); err == nil {
		nodesExecuted = len(executionItems)
	}

	return ClusterMetrics{
		TotalWorkflows:     totalWorkflows,
		ActiveWorkflows:    activeWorkflows,
		CompletedWorkflows: completedWorkflows,
		FailedWorkflows:    failedWorkflows,
		NodesExecuted:      nodesExecuted,
	}
}

// waitForDiscovery waits for discovery to stabilize and find peers
func (m *Manager) waitForDiscovery(ctx context.Context, timeout time.Duration) ([]ports.Peer, error) {
	if m.discovery == nil {
		return nil, nil
	}

	m.logger.Info("waiting for discovery to find peers", "timeout", timeout)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			peers := m.discovery.GetPeers()
			m.logger.Info("discovery timeout reached", "peers_found", len(peers))
			return peers, nil
		case <-ticker.C:
			peers := m.discovery.GetPeers()
			if len(peers) > 0 {
				m.logger.Info("discovery found peers", "peer_count", len(peers))
				return peers, nil
			}
		}
	}
}

// shouldBootstrap determines if this node should bootstrap based on configuration and peers
func (m *Manager) shouldBootstrap(peers []ports.Peer, raftConfig *domain.RaftConfig) bool {

	if raftConfig.ForceBootstrap {
		m.logger.Info("force bootstrap enabled, will bootstrap")
		return true
	}

	if len(peers) > 0 {
		m.logger.Info("peers found, will attempt to join cluster", "peer_count", len(peers))
		return false
	}

	expectedNodes := raftConfig.ExpectedNodes
	if len(expectedNodes) == 0 {

		expectedNodes = []string{m.nodeID}
		m.logger.Info("no expected nodes configured, will bootstrap as single node", "node_id", m.nodeID)
		return true
	}

	nodeInExpected := false
	for _, nodeID := range expectedNodes {
		if nodeID == m.nodeID {
			nodeInExpected = true
			break
		}
	}

	if !nodeInExpected {
		m.logger.Info("node not in expected nodes list, will not bootstrap", "node_id", m.nodeID, "expected_nodes", expectedNodes)
		return false
	}

	sort.Strings(expectedNodes)
	shouldBootstrap := expectedNodes[0] == m.nodeID

	if shouldBootstrap {
		m.logger.Info("lexicographic decision: will bootstrap as leader", "node_id", m.nodeID, "expected_nodes", expectedNodes)
	} else {
		m.logger.Info("lexicographic decision: will wait for other node to bootstrap", "node_id", m.nodeID, "lowest_expected", expectedNodes[0])
	}

	return shouldBootstrap
}

type WorkflowTrigger struct {
	WorkflowID   string            `json:"workflow_id"`
	InitialNodes []NodeConfig      `json:"initial_nodes"`
	InitialState interface{}       `json:"initial_state"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type NodeConfig struct {
	Name   string      `json:"name"`
	Config interface{} `json:"config"`
}

type WorkflowStatus struct {
	WorkflowID     string              `json:"workflow_id"`
	Status         WorkflowState       `json:"status"`
	CurrentState   interface{}         `json:"current_state"`
	StartedAt      time.Time           `json:"started_at"`
	CompletedAt    *time.Time          `json:"completed_at,omitempty"`
	ExecutedNodes  []ExecutedNodeData  `json:"executed_nodes"`
	ExecutingNodes []ExecutingNodeData `json:"executing_nodes"`
	PendingNodes   []NodeConfig        `json:"pending_nodes"`
	LastError      *string             `json:"last_error,omitempty"`
}

type ExecutedNodeData struct {
	NodeName   string        `json:"node_name"`
	ExecutedAt time.Time     `json:"executed_at"`
	Duration   time.Duration `json:"duration"`
	Status     string        `json:"status"`
	Config     interface{}   `json:"config"`
	Results    interface{}   `json:"results"`
	Error      *string       `json:"error,omitempty"`
}

type ExecutingNodeData struct {
	NodeName  string      `json:"node_name"`
	StartedAt time.Time   `json:"started_at"`
	ClaimID   string      `json:"claim_id"`
	Config    interface{} `json:"config,omitempty"`
}

type WorkflowState string

const (
	WorkflowStateRunning   WorkflowState = "running"
	WorkflowStateCompleted WorkflowState = "completed"
	WorkflowStateFailed    WorkflowState = "failed"
	WorkflowStatePaused    WorkflowState = "paused"
)

func (w WorkflowTrigger) toInternal() (domain.WorkflowTrigger, error) {
	initialState, err := marshalToRawMessage(w.InitialState)
	if err != nil {
		return domain.WorkflowTrigger{}, fmt.Errorf("failed to marshal initial state: %w", err)
	}

	nodes := make([]domain.NodeConfig, len(w.InitialNodes))
	for i, node := range w.InitialNodes {
		internalNode, err := node.toInternal()
		if err != nil {
			return domain.WorkflowTrigger{}, fmt.Errorf("failed to convert node %s: %w", node.Name, err)
		}
		nodes[i] = internalNode
	}

	return domain.WorkflowTrigger{
		WorkflowID:   w.WorkflowID,
		InitialNodes: nodes,
		InitialState: initialState,
		Metadata:     w.Metadata,
	}, nil
}

func (n NodeConfig) toInternal() (domain.NodeConfig, error) {
	config, err := marshalToRawMessage(n.Config)
	if err != nil {
		return domain.NodeConfig{}, fmt.Errorf("failed to marshal config: %w", err)
	}

	return domain.NodeConfig{
		Name:   n.Name,
		Config: config,
	}, nil
}

func workflowStatusFromInternal(w domain.WorkflowStatus) (WorkflowStatus, error) {
	var currentState interface{}
	if len(w.CurrentState) > 0 {
		if err := json.Unmarshal(w.CurrentState, &currentState); err != nil {
			return WorkflowStatus{}, fmt.Errorf("failed to unmarshal current state: %w", err)
		}
	}

	executedNodes := make([]ExecutedNodeData, len(w.ExecutedNodes))
	for i, node := range w.ExecutedNodes {
		publicNode, err := executedNodeDataFromInternal(node)
		if err != nil {
			return WorkflowStatus{}, fmt.Errorf("failed to convert executed node: %w", err)
		}
		executedNodes[i] = publicNode
	}

	executingNodes := make([]ExecutingNodeData, len(w.ExecutingNodes))
	for i, node := range w.ExecutingNodes {
		publicNode, err := executingNodeDataFromInternal(node)
		if err != nil {
			return WorkflowStatus{}, fmt.Errorf("failed to convert executing node: %w", err)
		}
		executingNodes[i] = publicNode
	}

	pendingNodes := make([]NodeConfig, len(w.PendingNodes))
	for i, node := range w.PendingNodes {
		publicNode, err := nodeConfigFromInternal(node)
		if err != nil {
			return WorkflowStatus{}, fmt.Errorf("failed to convert pending node: %w", err)
		}
		pendingNodes[i] = publicNode
	}

	return WorkflowStatus{
		WorkflowID:     w.WorkflowID,
		Status:         WorkflowState(w.Status),
		CurrentState:   currentState,
		StartedAt:      w.StartedAt,
		CompletedAt:    w.CompletedAt,
		ExecutedNodes:  executedNodes,
		ExecutingNodes: executingNodes,
		PendingNodes:   pendingNodes,
		LastError:      w.LastError,
	}, nil
}

func executedNodeDataFromInternal(e domain.ExecutedNodeData) (ExecutedNodeData, error) {
	var config interface{}
	if len(e.Config) > 0 {
		if err := json.Unmarshal(e.Config, &config); err != nil {
			return ExecutedNodeData{}, fmt.Errorf("failed to unmarshal config: %w", err)
		}
	}

	var results interface{}
	if len(e.Results) > 0 {
		if err := json.Unmarshal(e.Results, &results); err != nil {
			return ExecutedNodeData{}, fmt.Errorf("failed to unmarshal results: %w", err)
		}
	}

	return ExecutedNodeData{
		NodeName:   e.NodeName,
		ExecutedAt: e.ExecutedAt,
		Duration:   e.Duration,
		Status:     e.Status,
		Config:     config,
		Results:    results,
		Error:      e.Error,
	}, nil
}

func executingNodeDataFromInternal(e domain.ExecutingNodeData) (ExecutingNodeData, error) {
	var config interface{}
	if len(e.Config) > 0 {
		if err := json.Unmarshal(e.Config, &config); err != nil {
			return ExecutingNodeData{}, fmt.Errorf("failed to unmarshal config: %w", err)
		}
	}

	return ExecutingNodeData{
		NodeName:  e.NodeName,
		StartedAt: e.StartedAt,
		ClaimID:   e.ClaimID,
		Config:    config,
	}, nil
}

func nodeConfigFromInternal(n domain.NodeConfig) (NodeConfig, error) {
	var config interface{}
	if len(n.Config) > 0 {
		if err := json.Unmarshal(n.Config, &config); err != nil {
			return NodeConfig{}, fmt.Errorf("failed to unmarshal config: %w", err)
		}
	}

	return NodeConfig{
		Name:   n.Name,
		Config: config,
	}, nil
}

func marshalToRawMessage(v interface{}) (json.RawMessage, error) {
	if v == nil {
		return json.RawMessage("null"), nil
	}

	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func (m *Manager) GetHealth() ports.HealthStatus {
	health := ports.HealthStatus{
		Healthy: true,
		Details: make(map[string]interface{}),
	}

	if m.engine == nil {
		health.Healthy = false
		health.Error = "engine not initialized"
		return health
	}

	if m.raftAdapter != nil {
		raftHealth := m.raftAdapter.GetHealth()
		if !raftHealth.Healthy {
			health.Healthy = false
			health.Error = fmt.Sprintf("raft unhealthy: %s", raftHealth.Error)
		}
		health.Details["raft"] = raftHealth
	}

	if m.queue != nil {
		queueSize, err := m.queue.Size()
		if err != nil {
			health.Details["queue_error"] = err.Error()
		} else {
			health.Details["queue_size"] = queueSize
		}
	}

	if m.storage != nil {
		health.Details["storage"] = "connected"
	}

	info := m.GetClusterInfo()
	health.Details["cluster"] = map[string]interface{}{
		"node_id":   info.NodeID,
		"is_leader": info.IsLeader,
		"status":    info.Status,
		"peers":     len(info.Peers),
	}

	return health
}

func (m *Manager) GetMetrics() ports.SystemMetrics {
	metrics := ports.SystemMetrics{}

	if m.engine != nil {
		metrics.Engine = m.engine.GetMetrics()
	}

	metrics.Cluster = m.calculateClusterMetrics()

	if m.raftAdapter != nil {
		metrics.Raft = m.raftAdapter.GetMetrics()
	}

	if m.queue != nil {
		queueSize, _ := m.queue.Size()
		dlqSize, _ := m.queue.GetDeadLetterSize()
		metrics.Queue = map[string]interface{}{
			"size":             queueSize,
			"dead_letter_size": dlqSize,
		}
	}

	if m.storage != nil {
		metrics.Storage = map[string]interface{}{
			"status": "connected",
		}
	}

	if m.circuitBreakers != nil {
		cbMetrics := m.circuitBreakers.GetAllMetrics()
		if len(cbMetrics) > 0 {
			metrics.CircuitBreaker = cbMetrics
		}
	}

	if m.rateLimiters != nil {
		rlMetrics := m.rateLimiters.GetAllMetrics()
		if len(rlMetrics) > 0 {
			metrics.RateLimiter = rlMetrics
		}
	}

	if m.tracing != nil {
		if tracingImpl, ok := m.tracing.(*tracing.TracingProvider); ok {
			metrics.Tracing = tracingImpl.GetMetrics()
		}
	}

	return metrics
}

func (m *Manager) GetCircuitBreakerProvider() ports.CircuitBreakerProvider {
	return m.circuitBreakers
}

func (m *Manager) GetRateLimiterProvider() ports.RateLimiterProvider {
	return m.rateLimiters
}
