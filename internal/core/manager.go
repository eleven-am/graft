package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	bootstrapadapter "github.com/eleven-am/graft/internal/adapters/bootstrap"
	"github.com/eleven-am/graft/internal/adapters/circuit_breaker"
	"github.com/eleven-am/graft/internal/adapters/cluster"
	"github.com/eleven-am/graft/internal/adapters/connector_registry"
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
	"github.com/eleven-am/graft/internal/bootstrap"
	"github.com/eleven-am/graft/internal/core/connectors"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/helpers/metadata"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/readiness"

	"github.com/dgraph-io/badger/v3"
)

type Manager struct {
	engine            ports.EnginePort
	storage           ports.StoragePort
	queue             ports.QueuePort
	nodeRegistry      ports.NodeRegistryPort
	connectorRegistry ports.ConnectorRegistryPort
	transport         ports.TransportPort
	discovery         *discovery.Manager
	raftAdapter       ports.RaftNode
	eventManager      ports.EventManager
	loadBalancer      ports.LoadBalancer
	clusterManager    ports.ClusterManager
	observability     *observability.Server
	circuitBreakers   ports.CircuitBreakerProvider
	rateLimiters      ports.RateLimiterProvider
	tracing           ports.TracingProvider
	bootstrapper      *bootstrap.Bootstrapper

	config                 *domain.Config
	logger                 *slog.Logger
	nodeID                 string
	grpcPort               int
	expectedStaticPeers    int
	hasPersistedRaftState  bool
	readinessManager       *readiness.Manager
	connectorFSM           connectors.FSMAPI
	workflowIntakeMu       sync.RWMutex
	workflowIntakeOk       bool
	connectorMu            sync.RWMutex
	connectors             map[string]*connectorHandle
	connectorObserversOnce sync.Once
	connectorWatchersOnce  sync.Once
	connectorWatcherMu     sync.Mutex
	connectorWatcherUnsub  func()
	connectorRandMu        sync.Mutex
	connectorRand          *rand.Rand
	watchers               *connectors.WatcherGroup
	watchersCtx            context.Context
	watchersCancel         context.CancelFunc
	leaseManager           ports.LeaseManagerPort

	ctx    context.Context
	cancel context.CancelFunc
}

type managerProviders struct {
	newEventManager      func(ports.StoragePort, string, *slog.Logger) ports.EventManager
	newRaftStorage       func(dataDir string, logger *slog.Logger) (*raft.Storage, error)
	newRaftNode          func(cfg *raft.Config, storage *raft.Storage, events ports.EventManager, appTransport ports.TransportPort, logger *slog.Logger) (ports.RaftNode, error)
	newAppStorage        func(raft ports.RaftNode, db *badger.DB, logger *slog.Logger) ports.StoragePort
	newNodeRegistry      func(*slog.Logger) ports.NodeRegistryPort
	newConnectorRegistry func(*slog.Logger) ports.ConnectorRegistryPort
	newClusterManager    func(ports.RaftNode, int, *slog.Logger) ports.ClusterManager
	newLoadBalancer      func(ports.EventManager, string, ports.ClusterManager, *load_balancer.Config, *slog.Logger) ports.LoadBalancer
	newQueue             func(name string, storage ports.StoragePort, events ports.EventManager, leaseManager ports.LeaseManagerPort, nodeID string, claimTTL time.Duration, logger *slog.Logger) ports.QueuePort
	newEngine            func(domain.EngineConfig, string, ports.NodeRegistryPort, ports.QueuePort, ports.StoragePort, ports.EventManager, ports.LoadBalancer, *slog.Logger) ports.EnginePort
	newTransport         func(*slog.Logger, domain.TransportConfig) ports.TransportPort
	newLeaseManager      func(ports.StoragePort, *slog.Logger) ports.LeaseManagerPort
}

func defaultProviders() managerProviders {
	return managerProviders{
		newEventManager: func(storage ports.StoragePort, nodeID string, l *slog.Logger) ports.EventManager {
			return events.NewManager(storage, nodeID, l)
		},
		newRaftStorage: func(dataDir string, l *slog.Logger) (*raft.Storage, error) {
			return raft.NewStorage(raft.StorageConfig{DataDir: filepath.Join(dataDir, "raft")}, l)
		},
		newRaftNode: func(cfg *raft.Config, st *raft.Storage, ev ports.EventManager, t ports.TransportPort, l *slog.Logger) (ports.RaftNode, error) {
			return raft.NewNode(cfg, st, ev, t, l)
		},
		newAppStorage: func(r ports.RaftNode, db *badger.DB, l *slog.Logger) ports.StoragePort {
			return storage.NewAppStorage(r, db, l)
		},
		newNodeRegistry:      func(l *slog.Logger) ports.NodeRegistryPort { return node_registry.NewManager(l) },
		newConnectorRegistry: func(l *slog.Logger) ports.ConnectorRegistryPort { return connector_registry.NewManager(l) },
		newClusterManager: func(r ports.RaftNode, min int, l *slog.Logger) ports.ClusterManager {
			return cluster.NewRaftClusterManager(r, min, l)
		},
		newLoadBalancer: func(ev ports.EventManager, nodeID string, cm ports.ClusterManager, cfg *load_balancer.Config, l *slog.Logger) ports.LoadBalancer {
			return load_balancer.NewManager(ev, nodeID, cm, cfg, l)
		},
		newQueue: func(name string, s ports.StoragePort, ev ports.EventManager, leaseManager ports.LeaseManagerPort, nodeID string, claimTTL time.Duration, l *slog.Logger) ports.QueuePort {
			return queue.NewQueue(name, s, ev, leaseManager, nodeID, claimTTL, l)
		},
		newEngine: func(cfg domain.EngineConfig, nodeID string, nr ports.NodeRegistryPort, q ports.QueuePort, s ports.StoragePort, ev ports.EventManager, lb ports.LoadBalancer, l *slog.Logger) ports.EnginePort {
			return engine.NewEngine(cfg, nodeID, nr, q, s, ev, lb, l)
		},
		newTransport: func(l *slog.Logger, cfg domain.TransportConfig) ports.TransportPort {
			return transport.NewGRPCTransport(l, cfg)
		},
		newLeaseManager: func(s ports.StoragePort, l *slog.Logger) ports.LeaseManagerPort {
			return storage.NewLeaseManager(s, l)
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

var ErrDiscoveryStopped = errors.New("discovery stopped before finding any peers")

const ()

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

	raftConfig := raft.DefaultRaftConfig(config.NodeID, config.Cluster.ID, config.BindAddr, config.DataDir, config.Cluster.Policy)

	prov := defaultProviders()

	raftStorage, err := prov.newRaftStorage(config.DataDir, logger)
	if err != nil {
		logger.Error("failed to create raft storage", "error", err)
		return nil
	}
	cleanup = append(cleanup, raftStorage.Close)
	persistedState := raftStorage.HasExistingState()
	if persistedState {
		logger.Info("detected persisted raft state", "data_dir", config.DataDir)
	}

	appTransport := prov.newTransport(logger, config.Transport)

	tempEventManager := prov.newEventManager(nil, config.NodeID, logger)

	raftAdapter, err := prov.newRaftNode(raftConfig, raftStorage, tempEventManager, appTransport, logger)
	if err != nil {
		logger.Error("failed to create raft node", "error", err)
		return nil
	}
	cleanup = append(cleanup, raftAdapter.Stop)

	appStorage := prov.newAppStorage(raftAdapter, raftStorage.StateDB(), logger)

	eventManager := prov.newEventManager(appStorage, config.NodeID, logger)
	cleanup = append(cleanup, func() error {
		if eventManager != nil {
			return eventManager.Stop()
		}
		return nil
	})

	if updater, ok := raftAdapter.(interface{ SetEventManager(ports.EventManager) }); ok {
		updater.SetEventManager(eventManager)
	}

	if s, ok := appStorage.(interface{ SetEventManager(ports.EventManager) }); ok {
		s.SetEventManager(eventManager)
	}

	nodeRegistryManager := prov.newNodeRegistry(logger)
	connectorRegistryManager := prov.newConnectorRegistry(logger)

	clusterManager := prov.newClusterManager(raftAdapter, 1, logger)

	loadBalancerManager := prov.newLoadBalancer(eventManager, config.NodeID, clusterManager, nil, logger)

	if lb, ok := loadBalancerManager.(interface {
		SetTransport(ports.TransportPort)
		SetPeerAddrProvider(func() []string)
	}); ok {
		lb.SetTransport(appTransport)
		lb.SetPeerAddrProvider(func() []string {
			peers := discoveryManager.GetPeers()
			addrs := make([]string, 0, len(peers))
			for _, p := range peers {
				grpcPortStr := p.Metadata["grpc_port"]
				if grpcPortStr == "" {
					continue
				}
				addrs = append(addrs, net.JoinHostPort(p.Address, grpcPortStr))
			}
			return addrs
		})
	}
	cleanup = append(cleanup, func() error {
		if loadBalancerManager != nil {
			return loadBalancerManager.Stop()
		}
		return nil
	})

	leaseManager := prov.newLeaseManager(appStorage, logger)

	queueAdapter := prov.newQueue("main", appStorage, eventManager, leaseManager, config.NodeID, 0, logger)
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
				return tracingProvider.Shutdown()
			}
			return nil
		})
	}

	manager := &Manager{
		config:                config,
		logger:                logger,
		nodeID:                config.NodeID,
		connectorFSM:          connectors.NewFSM(),
		watchers:              connectors.NewWatcherGroup(),
		discovery:             discoveryManager,
		raftAdapter:           raftAdapter,
		storage:               appStorage,
		eventManager:          eventManager,
		nodeRegistry:          nodeRegistryManager,
		connectorRegistry:     connectorRegistryManager,
		queue:                 queueAdapter,
		engine:                engineAdapter,
		loadBalancer:          loadBalancerManager,
		clusterManager:        clusterManager,
		circuitBreakers:       circuitBreakerProvider,
		rateLimiters:          rateLimiterProvider,
		tracing:               tracingProvider,
		transport:             appTransport,
		readinessManager:      readiness.NewManager(),
		workflowIntakeOk:      true,
		connectors:            make(map[string]*connectorHandle),
		leaseManager:          leaseManager,
		hasPersistedRaftState: persistedState,
	}

	expectedStaticPeers := 0
	for _, discoveryConfig := range config.Discovery {
		if discoveryConfig.Type == domain.DiscoveryStatic {
			for _, peer := range discoveryConfig.Static {
				if peer.ID != config.NodeID {
					expectedStaticPeers++
				}
			}
		}
	}
	manager.expectedStaticPeers = expectedStaticPeers

	if manager.raftAdapter != nil {
		manager.raftAdapter.SetConnectorLeaseCleaner(manager)
	}

	if config.Observability.Enabled {
		manager.observability = observability.NewServer(0, manager, manager, logger)
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
				manager.MDNS(discoveryConfig.MDNS.Service, discoveryConfig.MDNS.Domain, discoveryConfig.MDNS.DisableIPv6)
			} else {
				manager.MDNS("", "", true)
			}
		case domain.DiscoveryStatic:
			if len(discoveryConfig.Static) > 0 {
				peers := make([]ports.Peer, len(discoveryConfig.Static))
				for i, staticPeer := range discoveryConfig.Static {
					metadata := staticPeer.Metadata
					if metadata == nil {
						metadata = make(map[string]string)
					}
					if _, exists := metadata["grpc_port"]; !exists {
						metadata["grpc_port"] = strconv.Itoa(staticPeer.Port)
					}

					peers[i] = ports.Peer{
						ID:       staticPeer.ID,
						Address:  staticPeer.Address,
						Port:     staticPeer.Port,
						Metadata: metadata,
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

func (m *Manager) MDNS(service, domain string, disableIPv6 bool) *Manager {
	m.discovery.MDNS(service, domain, disableIPv6)
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

	if err := m.startBootstrap(ctx, grpcPort); err != nil {
		return err
	}

	if err := m.loadBalancer.Start(m.ctx); err != nil {
		return domain.NewDomainErrorWithCategory(
			domain.CategoryResource,
			"failed to start load balancer",
			err,
			domain.WithComponent("core.Manager.Start"),
		)
	}

	if err := m.engine.Start(m.ctx); err != nil {
		return domain.NewWorkflowError(
			"failed to start engine",
			err,
			domain.WithComponent("core.Manager.Start"),
		)
	}

	if err := m.eventManager.Start(m.ctx); err != nil {
		return domain.NewWorkflowError(
			"failed to start event manager",
			err,
			domain.WithComponent("core.Manager.Start"),
		)
	}

	m.initConnectorObservers()
	m.startConnectorWatchers()

	if m.observability != nil {
		go func() {
			if err := m.observability.Start(m.ctx); err != nil {
				m.logger.Error("observability server failed", "error", err)
			}
		}()
	}

	m.transport.RegisterRaft(m.raftAdapter)
	if sink, ok := m.loadBalancer.(ports.LoadSink); ok {
		m.transport.RegisterLoadSink(sink)
	}
	m.grpcPort = grpcPort

	if err := m.transport.Start(m.ctx, m.config.BindAddr, grpcPort); err != nil {
		return err
	}

	if m.watchers != nil {
		wctx, wcancel := context.WithCancel(m.ctx)
		m.watchersCtx = wctx
		m.watchersCancel = wcancel
		m.watchers.Add(func() { m.watchForSeniorPeers(wctx) }, wcancel)
		m.watchers.Add(func() { m.reconcilePeersLoop(wctx) }, nil)
		m.watchers.Start(wctx)
	} else {
		go m.watchForSeniorPeers(m.ctx)
		go m.reconcilePeersLoop(m.ctx)
	}

	return nil
}

func (m *Manager) Stop() error {
	if m.cancel != nil {
		m.cancel()
	}

	if m.watchersCancel != nil {
		m.watchersCancel()
	}
	m.stopConnectorWatchers()
	m.stopAllConnectors()
	if m.connectorFSM != nil {
		m.connectorFSM.Transition(connectors.StateStopped)
	}

	if m.watchers != nil {
		m.watchers.Stop()
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

	if m.raftAdapter != nil {
		if err := m.raftAdapter.Stop(); err != nil {
			m.logger.Error("failed to stop raft adapter", "error", err)
		}
	}

	if m.bootstrapper != nil {
		if err := m.bootstrapper.Stop(); err != nil {
			m.logger.Error("failed to stop bootstrapper", "error", err)
		}
	}

	return nil
}

func (m *Manager) startBootstrap(ctx context.Context, grpcPort int) error {
	m.logger.Info("starting with new bootstrap system",
		"service_name", m.config.Bootstrap.ServiceName,
		"ordinal", m.config.Bootstrap.Ordinal,
		"replicas", m.config.Bootstrap.Replicas)

	if m.discovery != nil {
		host, portStr, err := net.SplitHostPort(m.config.BindAddr)
		if err != nil {
			host = m.config.BindAddr
			portStr = "7222"
		}
		port, _ := strconv.Atoi(portStr)

		if err := m.discovery.Start(ctx, host, port, grpcPort, m.config.Cluster.ID); err != nil {
			return domain.NewDomainErrorWithCategory(
				domain.CategoryDiscovery,
				"failed to start discovery manager",
				err,
				domain.WithComponent("core.Manager.Start"),
			)
		}
		m.logger.Info("discovery manager started", "address", host, "port", port, "grpc_port", grpcPort)
	}

	factory, err := bootstrapadapter.NewFactory(bootstrapadapter.FactoryDeps{
		Config:           m.config,
		DiscoveryManager: m.discovery,
		Logger:           m.logger,
	})
	if err != nil {
		return domain.NewDomainErrorWithCategory(
			domain.CategoryDiscovery,
			"failed to create bootstrap factory",
			err,
			domain.WithComponent("core.Manager.Start"),
		)
	}

	bootstrapper, err := factory.CreateBootstrapper()
	if err != nil {
		return domain.NewDomainErrorWithCategory(
			domain.CategoryDiscovery,
			"failed to create bootstrapper",
			err,
			domain.WithComponent("core.Manager.Start"),
		)
	}

	m.bootstrapper = bootstrapper

	if err := m.bootstrapper.Start(m.ctx); err != nil {
		return domain.NewDomainErrorWithCategory(
			domain.CategoryDiscovery,
			"failed to start bootstrapper",
			err,
			domain.WithComponent("core.Manager.Start"),
		)
	}

	readyTimeout := m.config.Bootstrap.ReadyTimeout
	if readyTimeout <= 0 {
		readyTimeout = 60 * time.Second
	}

	m.logger.Info("waiting for bootstrapper to become ready",
		"timeout", readyTimeout,
		"ordinal", m.config.Bootstrap.Ordinal)

	select {
	case <-m.bootstrapper.Ready():
		if err := m.bootstrapper.ReadyError(); err != nil {
			return domain.NewDomainErrorWithCategory(
				domain.CategoryDiscovery,
				"bootstrapper ready with error",
				err,
				domain.WithComponent("core.Manager.Start"),
			)
		}
	case <-time.After(readyTimeout):
		return domain.NewDomainErrorWithCategory(
			domain.CategoryDiscovery,
			"bootstrapper ready timeout",
			fmt.Errorf("timed out waiting for bootstrapper after %v", readyTimeout),
			domain.WithComponent("core.Manager.Start"),
		)
	case <-ctx.Done():
		return ctx.Err()
	}

	resolvedOrdinal := m.bootstrapper.GetOrdinal()
	m.logger.Info("bootstrapper ready, starting raft adapter",
		"state", m.bootstrapper.CurrentState(),
		"resolved_ordinal", resolvedOrdinal,
		"config_ordinal", m.config.Bootstrap.Ordinal)

	isOrdinalZero := resolvedOrdinal == 0
	var peers []domain.RaftPeerSpec

	var discoveredPeers []ports.Peer
	if m.discovery != nil {
		discoveredPeers = m.discovery.GetPeers()
	}

	m.logger.Debug("cluster formation state",
		"resolved_ordinal", resolvedOrdinal,
		"config_ordinal", m.config.Bootstrap.Ordinal,
		"is_ordinal_zero", isOrdinalZero,
		"headless_service", m.config.Bootstrap.HeadlessService,
		"service_name", m.config.Bootstrap.ServiceName,
		"discovered_peers_count", len(discoveredPeers))

	for i, p := range discoveredPeers {
		m.logger.Debug("discovered peer",
			"index", i,
			"peer_id", p.ID,
			"peer_address", p.Address,
			"peer_port", p.Port,
			"peer_metadata", p.Metadata)
	}

	if !isOrdinalZero {
		var leaderAddr string
		var leaderID string
		var grpcPort string
		var foundFromDiscovery bool

		for _, p := range discoveredPeers {
			if ordStr, ok := p.Metadata["ordinal"]; ok && ordStr == "0" {
				leaderAddr = fmt.Sprintf("%s:%d", p.Address, p.Port)
				leaderID = fmt.Sprintf("%s-0", m.config.Bootstrap.ServiceName)
				grpcPort = p.Metadata["grpc_port"]
				foundFromDiscovery = true
				m.logger.Debug("found ordinal-0 via discovery",
					"peer_id", p.ID,
					"peer_address", p.Address,
					"peer_port", p.Port,
					"grpc_port", grpcPort,
					"leader_addr", leaderAddr)
				break
			}
		}

		if !foundFromDiscovery {
			headlessSvc := m.config.Bootstrap.HeadlessService
			if headlessSvc == "" {
				headlessSvc = m.config.Bootstrap.ServiceName
			}
			leaderAddr = fmt.Sprintf("%s-0.%s:%d",
				m.config.Bootstrap.ServiceName,
				headlessSvc,
				m.config.Bootstrap.BasePort)
			leaderID = fmt.Sprintf("%s-0", m.config.Bootstrap.ServiceName)
			grpcPort = strconv.Itoa(m.grpcPort)
			m.logger.Debug("using K8s-style leader address (discovery fallback)",
				"leader_addr", leaderAddr,
				"grpc_port", grpcPort)
		}

		peerMeta := make(map[string]string)
		if grpcPort != "" {
			peerMeta["grpc_port"] = grpcPort
		}
		peers = []domain.RaftPeerSpec{{
			ID:       leaderID,
			Address:  leaderAddr,
			Metadata: peerMeta,
		}}
		m.logger.Debug("constructed peer for joining",
			"leader_addr", leaderAddr,
			"leader_id", leaderID,
			"grpc_port", grpcPort,
			"from_discovery", foundFromDiscovery)
	}

	if err := m.raftAdapter.Start(ctx, peers, isOrdinalZero); err != nil {
		m.logger.Error("raft adapter start failed (new bootstrap)",
			"node_id", m.nodeID,
			"resolved_ordinal", resolvedOrdinal,
			"is_bootstrap_leader", isOrdinalZero,
			"error", err)
		return domain.NewRaftError(
			"failed to start raft node",
			err,
			domain.WithComponent("core.Manager.Start"),
		)
	}

	m.logger.Info("raft adapter started (new bootstrap)",
		"node_id", m.nodeID,
		"resolved_ordinal", resolvedOrdinal,
		"is_bootstrap_leader", isOrdinalZero,
		"state", m.bootstrapper.CurrentState())

	m.readinessManager.SetState(readiness.StateReady)
	m.resumeWorkflowIntake()

	return nil
}

// convertPeersToPorts converts raft peer specs back to ports.Peer for the adapter.
func convertPeersToPorts(peers []domain.RaftPeerSpec) []ports.Peer {
	if len(peers) == 0 {
		return nil
	}
	out := make([]ports.Peer, 0, len(peers))
	for _, p := range peers {
		host, portStr, err := net.SplitHostPort(p.Address)
		if err != nil {
			out = append(out, ports.Peer{ID: p.ID, Address: p.Address, Port: 0, Metadata: p.Metadata})
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			port = 0
		}
		out = append(out, ports.Peer{ID: p.ID, Address: host, Port: port, Metadata: p.Metadata})
	}
	return out
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

	if !m.isWorkflowIntakeAllowed() {
		return fmt.Errorf("workflow intake paused during bootstrap handoff")
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
		leadershipInfo := m.raftAdapter.GetLeadershipInfo()
		info.IsLeader = leadershipInfo.State == ports.RaftLeadershipLeader && leadershipInfo.LeaderID == m.nodeID

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

// Subscribe wraps SubscribeToChannel by deriving a prefix from the pattern and invoking the handler
// for matching keys. Prefer SubscribeToChannel for lifecycle control.
func (m *Manager) Subscribe(pattern string, handler func(string, interface{})) error {
	prefix := pattern
	if strings.HasSuffix(pattern, "*") {
		prefix = strings.TrimSuffix(pattern, "*")
	}
	ch, _, err := m.eventManager.SubscribeToChannel(prefix)
	if err != nil {
		return err
	}
	go func() {
		for ev := range ch {
			if pattern == "*" || strings.HasPrefix(ev.Key, prefix) || ev.Key == pattern {
				handler(ev.Key, nil)
			}
		}
	}()
	return nil
}

// Unsubscribe is not supported via pattern; use SubscribeToChannel and the returned unsubscribe function.
func (m *Manager) Unsubscribe(pattern string) error { return nil }

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
	pattern := domain.WorkflowStateKey(workflowID)

	ch, unsub, err := m.eventManager.SubscribeToChannel(pattern)
	if err != nil {
		close(statusChan)
		return nil, nil, err
	}

	go func() {
		for range ch {
			internalStatus, err := m.engine.GetWorkflowStatus(workflowID)
			if err != nil {
				m.logger.Error("failed to get workflow status for subscription",
					"workflow_id", workflowID,
					"error", err)
				continue
			}

			publicStatus, err := workflowStatusFromInternal(*internalStatus)
			if err != nil {
				m.logger.Error("failed to convert workflow status for subscription",
					"workflow_id", workflowID,
					"error", err)
				continue
			}

			select {
			case statusChan <- &publicStatus:
			default:
				m.logger.Warn("workflow status channel full, dropping update",
					"workflow_id", workflowID)
			}
		}
	}()

	unsubscribe := func() {
		unsub()
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

	if workflowItems, err := m.storage.ListByPrefix(domain.WorkflowStatePrefix); err == nil {
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

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	events, unsubscribe := m.discovery.Subscribe()
	defer unsubscribe()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			if ctx.Err() != nil && errors.Is(timeoutCtx.Err(), context.Canceled) {
				return nil, ctx.Err()
			}
			peers := filterPeers(m.discovery.GetPeers(), m.nodeID)
			m.logger.Info("discovery phase complete", "peers_found", len(peers), "reason", "timeout")
			return peers, nil
		case _, ok := <-events:
			if !ok {
				peers := filterPeers(m.discovery.GetPeers(), m.nodeID)
				if len(peers) == 0 {
					return nil, fmt.Errorf("discovery stopped before peers found: %w", ErrDiscoveryStopped)
				}
				m.logger.Info("discovery phase complete", "peers_found", len(peers), "reason", "events_closed")
				return peers, nil
			}
			peers := filterPeers(m.discovery.GetPeers(), m.nodeID)
			if m.expectedStaticPeers > 0 {
				if len(peers) >= m.expectedStaticPeers {
					m.logger.Info("discovery phase complete", "peers_found", len(peers), "reason", "expected_peers")
					return peers, nil
				}
			} else if len(peers) > 0 {
				m.logger.Info("discovery phase complete", "peers_found", len(peers), "reason", "peers_detected")
				return peers, nil
			}
		case <-ticker.C:
			peers := filterPeers(m.discovery.GetPeers(), m.nodeID)
			if m.expectedStaticPeers > 0 {
				if len(peers) >= m.expectedStaticPeers {
					m.logger.Info("discovery phase complete", "peers_found", len(peers), "reason", "expected_peers")
					return peers, nil
				}
			} else if len(peers) > 0 {
				m.logger.Info("discovery phase complete", "peers_found", len(peers), "reason", "peers_detected")
				return peers, nil
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("discovery wait aborted: %w", ctx.Err())
		}
	}
}

func (m *Manager) refreshPeersAsync(ctx context.Context, timeout time.Duration) {

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

	health.Details["readiness"] = map[string]interface{}{
		"state":  m.readinessManager.GetState().String(),
		"ready":  m.readinessManager.IsReady(),
		"intake": m.isWorkflowIntakeAllowed(),
	}

	if m.raftAdapter != nil {
		bootID, timestamp := m.raftAdapter.GetBootMetadata()
		health.Details["bootstrap"] = map[string]interface{}{
			"provisional": m.raftAdapter.IsProvisional(),
			"boot_id":     bootID,
			"timestamp":   timestamp,
		}
	}

	if raftDetails, ok := health.Details["raft"].(ports.HealthStatus); ok {
		if !raftDetails.Healthy {
			health.Healthy = false
			if health.Error == "" {
				health.Error = raftDetails.Error
			}
		}
	}

	if m.raftAdapter != nil {
		leadership := m.raftAdapter.GetLeadershipInfo()

		if m.expectedStaticPeers > 1 && len(info.Peers) == 0 {
			health.Healthy = false
			if health.Error == "" {
				health.Error = "stale single-node raft state detected"
			}
		}

		if leadership.LeaderID == "" && (health.Error == "" && health.Healthy) {
			health.Healthy = false
			health.Error = "raft has no leader"

			if readinessDetails, ok := health.Details["readiness"].(map[string]interface{}); ok {
				readinessDetails["ready"] = false
				readinessDetails["intake"] = false
			}
		}
	}

	return health
}

func (m *Manager) GetRaftStatus() ports.RaftStatus {
	if m.raftAdapter == nil {
		return ports.RaftStatus{}
	}
	return m.raftAdapter.GetRaftStatus()
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
		if tracingImpl, ok := m.tracing.(*tracing.Provider); ok {
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

func (m *Manager) WaitUntilReady(ctx context.Context) error {
	return m.readinessManager.WaitUntilReady(ctx)
}

func (m *Manager) IsReady() bool {
	return m.readinessManager.IsReady()
}

func (m *Manager) GetReadinessState() string {
	return m.readinessManager.GetState().String()
}

func (m *Manager) pauseWorkflowIntake() {
	m.workflowIntakeMu.Lock()
	defer m.workflowIntakeMu.Unlock()
	m.workflowIntakeOk = false
	m.logger.Info("workflow intake paused for bootstrap handoff")
}

func (m *Manager) resumeWorkflowIntake() {
	m.workflowIntakeMu.Lock()
	defer m.workflowIntakeMu.Unlock()
	m.workflowIntakeOk = true
	m.logger.Info("workflow intake resumed")
}

func (m *Manager) isWorkflowIntakeAllowed() bool {
	m.workflowIntakeMu.RLock()
	defer m.workflowIntakeMu.RUnlock()
	return m.workflowIntakeOk
}

func (m *Manager) watchForSeniorPeers(ctx context.Context) {
	if !m.raftAdapter.IsProvisional() {
		m.logger.Debug("node not provisional, skipping senior peer detection")
		return
	}

	m.logger.Info("starting senior peer detection for provisional leader")

	discoveryEvents, unsubscribe := m.discovery.Subscribe()
	defer unsubscribe()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-discoveryEvents:
			if !ok {
				return
			}
			m.handleDiscoveryEvent(event)
		case <-ticker.C:
			m.checkForSeniorPeers()
		}
	}
}

// reconcilePeersLoop periodically removes raft members that are no longer present in discovery.
// This keeps the raft configuration aligned with the active cluster view even if removal events are missed.
func (m *Manager) reconcilePeersLoop(ctx context.Context) {
	if m.discovery == nil || m.raftAdapter == nil {
		return
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peers := m.discovery.GetPeers()
			peerSet := make(map[string]struct{}, len(peers))
			for _, p := range peers {
				peerSet[p.ID] = struct{}{}
			}

			cluster := m.raftAdapter.GetClusterInfo()
			for _, member := range cluster.Members {
				if member.ID == m.nodeID {
					continue
				}
				if _, ok := peerSet[member.ID]; !ok {
					if err := m.raftAdapter.RemoveServer(member.ID); err != nil {
						m.logger.Warn("failed to remove missing peer from raft config", "peer_id", member.ID, "error", err)
					} else {
						m.logger.Info("removed missing peer from raft config", "peer_id", member.ID)
					}
				}
			}
		}
	}
}

func (m *Manager) handleDiscoveryEvent(event ports.Event) {
	switch event.Type {
	case ports.PeerAdded, ports.PeerUpdated:
		peers := m.discovery.GetPeers()
		seniorPeer := readiness.FindSeniorPeer(m.nodeID, peers, m.logger)

		if seniorPeer != nil {
			m.logger.Info("discovered senior peer via event", "senior_peer", seniorPeer.ID)
			m.initiateDemotion(*seniorPeer)
		}
	case ports.PeerRemoved:
		if m.raftAdapter == nil {
			return
		}
		peerID := event.Peer.ID
		if peerID == "" || peerID == m.nodeID {
			return
		}
		if err := m.raftAdapter.RemoveServer(peerID); err != nil {
			m.logger.Warn("failed to remove peer from raft config", "peer_id", peerID, "error", err)
		}
	}
}

func (m *Manager) checkForSeniorPeers() {
	if !m.raftAdapter.IsProvisional() {
		return
	}

	peers := m.discovery.GetPeers()
	seniorPeer := readiness.FindSeniorPeer(m.nodeID, peers, m.logger)

	if seniorPeer != nil {
		m.logger.Info("discovered senior peer via periodic check", "senior_peer", seniorPeer.ID)
		m.initiateDemotion(*seniorPeer)
	}
}

func (m *Manager) initiateDemotion(seniorPeer ports.Peer) {
	if !m.raftAdapter.IsProvisional() {
		m.logger.Debug("node no longer provisional, skipping demotion")
		return
	}

	m.logger.Info("initiating demotion and join to senior peer",
		"senior_peer_id", seniorPeer.ID,
		"senior_address", seniorPeer.Address,
		"senior_port", seniorPeer.Port)

	m.readinessManager.SetState(readiness.StateDetecting)
	m.pauseWorkflowIntake()

	demotionCtx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	if err := m.raftAdapter.DemoteAndJoin(demotionCtx, seniorPeer); err != nil {
		m.logger.Error("demotion failed", "error", err, "senior_peer", seniorPeer.ID)
		m.resumeWorkflowIntake()
		return
	}

	m.logger.Info("demotion successful - node joined cluster", "senior_peer", seniorPeer.ID)

	joinCtx, cancelJoin := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancelJoin()
	if m.joinPeers(joinCtx, []ports.Peer{seniorPeer}) {
		m.logger.Info("successfully joined senior peer after demotion", "senior_peer", seniorPeer.ID)
	} else {
		m.logger.Warn("join request after demotion not accepted", "senior_peer", seniorPeer.ID)
	}
	m.readinessManager.SetState(readiness.StateReady)
	m.resumeWorkflowIntake()
}

func (m *Manager) joinPeers(ctx context.Context, peers []ports.Peer) bool {
	host, raftPortStr, err := net.SplitHostPort(m.config.BindAddr)
	if err != nil {
		host = m.config.BindAddr
		raftPortStr = "7222"
	}

	raftPort, err := strconv.Atoi(raftPortStr)
	if err != nil {
		m.logger.Error("invalid raft port in bind address", "bind_addr", m.config.BindAddr, "port_str", raftPortStr)
		return false
	}

	bootMetadata := metadata.GetGlobalBootstrapMetadata()
	joinMetadata := metadata.ExtendMetadata(map[string]string{
		"cluster_id": m.config.Cluster.ID,
	}, bootMetadata)

	succeeded := false
	for _, peer := range peers {
		if peer.ID == m.nodeID {
			continue
		}

		grpcPortStr, ok := peer.Metadata["grpc_port"]
		if !ok {
			m.logger.Warn("peer missing grpc_port metadata", "peer_id", peer.ID)
			continue
		}
		grpcPort, err := strconv.Atoi(grpcPortStr)
		if err != nil {
			m.logger.Warn("invalid grpc_port in peer metadata", "peer_id", peer.ID, "value", grpcPortStr)
			continue
		}

		peerAddr := net.JoinHostPort(peer.Address, strconv.Itoa(grpcPort))

		m.logger.Debug("sending join request",
			"peer_addr", peerAddr,
			"node_id", m.nodeID,
			"raft_address", host,
			"raft_port", raftPort,
			"grpc_port", m.grpcPort)

		req := &ports.JoinRequest{
			NodeID:   m.nodeID,
			Address:  host,
			Port:     raftPort,
			Metadata: joinMetadata,
		}

		resp, err := m.transport.SendJoinRequest(ctx, peerAddr, req)
		if err != nil {
			m.logger.Warn("join request failed", "peer", peerAddr, "error", err)
			continue
		}

		if resp != nil && resp.Accepted {
			m.logger.Info("successfully joined cluster via RPC", "peer", peerAddr)
			succeeded = true
			break
		}

		message := "no response"
		if resp != nil {
			message = resp.Message
		}
		m.logger.Warn("join request not accepted", "peer", peerAddr, "message", message)
	}

	return succeeded
}

func (m *Manager) joinWithRetry(peers []ports.Peer) bool {
	joinWindow := m.discoveryTimeout()
	deadline := time.Now().Add(joinWindow)

	ctxDeadline, hasCtx := m.ctx.Deadline()
	if hasCtx && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	baseAttempts := m.config.Raft.MaxJoinAttempts
	if baseAttempts <= 0 {
		baseAttempts = 100
	}

	attemptsMade := 0
	backoff := 500 * time.Millisecond
	maxBackoff := 2 * time.Second
	lastErrTime := time.Now()

	for attempt := 1; attempt <= baseAttempts || m.timeToExtendJoin(deadline); attempt++ {
		attemptsMade = attempt

		if m.ctx.Err() != nil {
			return false
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		joinTimeout := 10 * time.Second
		if remaining < joinTimeout {
			joinTimeout = remaining
		}

		joinCtx, cancel := context.WithTimeout(m.ctx, joinTimeout)
		success := m.joinPeers(joinCtx, peers)
		cancel()

		if success {
			m.logger.Info("successfully joined cluster", "attempt", attempt)
			return true
		}

		now := time.Now()
		elapsedSinceLastErr := now.Sub(lastErrTime)
		lastErrTime = now

		m.logger.Warn("join attempt failed, retrying",
			"attempt", attempt,
			"backoff", backoff,
			"remaining_time", remaining)

		wait := backoff
		if wait > remaining {
			wait = remaining
		}

		if wait > 0 {
			select {
			case <-time.After(wait):
			case <-m.ctx.Done():
				return false
			}
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		if elapsedSinceLastErr >= 2*time.Second && deadline.Before(now.Add(5*time.Second)) {
			deadline = now.Add(5 * time.Second)
			m.logger.Warn("extending join window due to repeated failures",
				"new_deadline", deadline.Format(time.RFC3339Nano))
		}
	}

	m.logger.Warn("join retries exhausted",
		"attempts", attemptsMade,
		"max_attempts", baseAttempts,
		"elapsed", joinWindow)
	return false
}

func (m *Manager) discoveryTimeout() time.Duration {
	if m == nil || m.config == nil {
		return 30 * time.Second
	}

	timeout := m.config.Raft.DiscoveryTimeout
	if timeout <= 0 {
		return 30 * time.Second
	}
	return timeout
}

func (m *Manager) timeToExtendJoin(deadline time.Time) bool {
	if deadline.IsZero() {
		return true
	}
	return time.Now().Before(deadline)
}

func (m *Manager) waitForSelfLeadership(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			isLeader := m.raftAdapter.IsLeader()
			leaderAddr := m.raftAdapter.LeaderAddr()

			if isLeader {
				m.logger.Info("self-leadership established", "node_id", m.nodeID, "leader_addr", leaderAddr)
				return nil
			}
		}
	}
}

func (m *Manager) awaitLeadership(timeout time.Duration, role string) {
	if m == nil {
		return
	}

	leaderCtx, cancel := context.WithTimeout(m.ctx, timeout)
	defer cancel()

	if err := m.waitForSelfLeadership(leaderCtx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			m.logger.Warn("timed out waiting for raft leadership",
				"role", role,
				"error", err)
		} else {
			m.logger.Warn("failed waiting for raft leadership",
				"role", role,
				"error", err)
		}
		return
	}

	m.logger.Info("raft leadership established",
		"role", role,
		"node_id", m.nodeID,
		"leader_addr", m.raftAdapter.LeaderAddr())
	m.readinessManager.SetState(readiness.StateReady)
	m.resumeWorkflowIntake()
}

func filterPeers(peers []ports.Peer, selfID string) []ports.Peer {
	if len(peers) == 0 {
		return nil
	}
	result := make([]ports.Peer, 0, len(peers))
	for _, p := range peers {
		if p.ID == selfID {
			continue
		}
		result = append(result, p)
	}
	return result
}
