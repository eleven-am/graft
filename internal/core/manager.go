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
	seeder            ports.Seeder
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
	newRaftStorage       func(dataDir string, inMemory bool, logger *slog.Logger) (*raft.Storage, error)
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
		newRaftStorage: func(dataDir string, inMemory bool, l *slog.Logger) (*raft.Storage, error) {
			return raft.NewStorage(raft.StorageConfig{DataDir: filepath.Join(dataDir, "raft"), InMemory: inMemory}, l)
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

	seeder := createSeeder(config, logger)

	raftConfig := raft.DefaultRaftConfig(config.NodeID, config.Cluster.ID, config.BindAddr, config.DataDir, config.Cluster.Policy)
	raftConfig.IgnoreExistingState = config.Bootstrap.IgnoreExistingState
	raftConfig.InMemoryStorage = config.Bootstrap.InMemoryStorage

	prov := defaultProviders()

	raftStorage, err := prov.newRaftStorage(config.DataDir, config.Bootstrap.InMemoryStorage, logger)
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
			if raftAdapter == nil {
				return nil
			}
			clusterInfo := raftAdapter.GetClusterInfo()
			addrs := make([]string, 0, len(clusterInfo.Members))
			for _, member := range clusterInfo.Members {
				if member.ID == config.NodeID {
					continue
				}
				addrs = append(addrs, member.Address)
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
		seeder:                seeder,
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

func createSeeder(config *domain.Config, logger *slog.Logger) ports.Seeder {
	var seederList []ports.Seeder

	for _, discoveryConfig := range config.Discovery {
		switch discoveryConfig.Type {
		case domain.DiscoveryMDNS:
			service := "_graft._tcp"
			domainStr := "local."
			if discoveryConfig.MDNS != nil {
				if discoveryConfig.MDNS.Service != "" {
					service = discoveryConfig.MDNS.Service
				}
				if discoveryConfig.MDNS.Domain != "" {
					domainStr = discoveryConfig.MDNS.Domain
				}
			}
			seederList = append(seederList, discovery.NewMDNSSeeder(service, domainStr))
		case domain.DiscoveryStatic:
			if len(discoveryConfig.Static) > 0 {
				peers := make([]ports.Peer, len(discoveryConfig.Static))
				for i, staticPeer := range discoveryConfig.Static {
					peers[i] = ports.Peer{
						ID:      staticPeer.ID,
						Address: staticPeer.Address,
						Port:    staticPeer.Port,
					}
				}
				seederList = append(seederList, discovery.NewStaticSeeder(peers))
			}
		case domain.DiscoveryDNS:
			if discoveryConfig.DNS != nil && discoveryConfig.DNS.Hostname != "" {
				seederList = append(seederList, discovery.NewDNSSeeder(discoveryConfig.DNS.Hostname, discoveryConfig.DNS.Port, discoveryConfig.DNS.Service))
			}
		}
	}

	if len(seederList) == 0 {
		return discovery.NewStaticSeeder(nil)
	}
	if len(seederList) == 1 {
		return seederList[0]
	}
	return discovery.NewCompositeSeeder(seederList)
}

func (m *Manager) Seeder() ports.Seeder {
	return m.seeder
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
		m.watchers.Start(wctx)
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

	if advertiser, ok := m.seeder.(ports.AdvertisableSeeder); ok {
		advertiser.StopAdvertising()
	}

	return nil
}

func (m *Manager) startBootstrap(ctx context.Context, grpcPort int) error {
	m.logger.Info("starting with new bootstrap system",
		"service_name", m.config.Bootstrap.ServiceName,
		"ordinal", m.config.Bootstrap.Ordinal,
		"replicas", m.config.Bootstrap.Replicas)

	if advertiser, ok := m.seeder.(ports.AdvertisableSeeder); ok {
		raftPort := m.config.Bootstrap.BasePort
		if _, portStr, err := net.SplitHostPort(m.config.BindAddr); err == nil {
			if p, _ := strconv.Atoi(portStr); p > 0 {
				raftPort = p
			}
		}

		if err := advertiser.StartAdvertising(m.nodeID, m.config.BindAddr, raftPort); err != nil {
			m.logger.Warn("failed to start seeder advertising", "error", err)
		} else {
			m.logger.Info("started seeder advertising", "node_id", m.nodeID, "port", raftPort)
		}
	}

	bootstrapConfig := m.buildBootstrapConfig(grpcPort)
	metaStore := bootstrap.NewFileMetaStore(m.config.DataDir, m.logger)
	stateMachine, err := bootstrap.NewStateMachine(bootstrap.StateMachineDeps{
		MetaStore: metaStore,
		Logger:    m.logger,
	})
	if err != nil {
		return domain.NewDomainErrorWithCategory(
			domain.CategoryDiscovery,
			"failed to create state machine",
			err,
			domain.WithComponent("core.Manager.startBootstrap"),
		)
	}

	m.bootstrapper = bootstrap.NewBootstrapper(bootstrap.BootstrapperDeps{
		Config:       bootstrapConfig,
		MetaStore:    metaStore,
		StateMachine: stateMachine,
		Seeder:       m.seeder,
		Logger:       m.logger,
	})

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

	isClusterInitiator := m.bootstrapper.IsClusterInitiator()
	serverID := m.bootstrapper.GetServerID()
	m.logger.Info("bootstrapper ready, starting raft adapter",
		"state", m.bootstrapper.CurrentState(),
		"server_id", serverID,
		"is_cluster_initiator", isClusterInitiator)

	var peers []domain.RaftPeerSpec
	discoveredPeers := m.bootstrapper.GetDiscoveredPeers()

	m.logger.Debug("cluster formation state",
		"server_id", serverID,
		"is_cluster_initiator", isClusterInitiator,
		"headless_service", m.config.Bootstrap.HeadlessService,
		"service_name", m.config.Bootstrap.ServiceName,
		"discovered_peers_count", len(discoveredPeers))

	for i, p := range discoveredPeers {
		m.logger.Debug("discovered peer",
			"index", i,
			"peer_id", p.ID,
			"peer_address", p.Address,
			"peer_port", p.Port)
	}

	if isClusterInitiator {
		for _, p := range discoveredPeers {
			peerAddr := fmt.Sprintf("%s:%d", p.Address, p.Port)
			peers = append(peers, domain.RaftPeerSpec{
				ID:      p.ID,
				Address: peerAddr,
			})
			m.logger.Debug("adding discovered peer to initial cluster",
				"peer_id", p.ID,
				"peer_address", peerAddr)
		}
	} else {
		lowestPeer := findLowestServerIDPeer(discoveredPeers, string(serverID))
		if lowestPeer != nil {
			peerAddr := fmt.Sprintf("%s:%d", lowestPeer.Address, lowestPeer.Port)

			m.logger.Debug("found lowest ServerID peer to join",
				"peer_id", lowestPeer.ID,
				"peer_address", peerAddr)

			peers = []domain.RaftPeerSpec{{
				ID:      lowestPeer.ID,
				Address: peerAddr,
			}}
		} else {
			m.logger.Warn("no peers found to join, will wait for cluster initiator")
		}
	}

	if err := m.raftAdapter.Start(ctx, peers, isClusterInitiator); err != nil {
		m.logger.Error("raft adapter start failed (new bootstrap)",
			"node_id", m.nodeID,
			"server_id", serverID,
			"is_cluster_initiator", isClusterInitiator,
			"error", err)
		return domain.NewRaftError(
			"failed to start raft node",
			err,
			domain.WithComponent("core.Manager.Start"),
		)
	}

	m.logger.Info("raft adapter started (new bootstrap)",
		"node_id", m.nodeID,
		"server_id", serverID,
		"is_cluster_initiator", isClusterInitiator,
		"state", m.bootstrapper.CurrentState())

	m.readinessManager.SetState(readiness.StateReady)
	m.resumeWorkflowIntake()

	return nil
}

func convertPeersToPorts(peers []domain.RaftPeerSpec) []ports.Peer {
	if len(peers) == 0 {
		return nil
	}
	out := make([]ports.Peer, 0, len(peers))
	for _, p := range peers {
		host, portStr, err := net.SplitHostPort(p.Address)
		if err != nil {
			out = append(out, ports.Peer{ID: p.ID, Address: p.Address, Port: 0})
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			port = 0
		}
		out = append(out, ports.Peer{ID: p.ID, Address: host, Port: port})
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

		peerAddr := net.JoinHostPort(peer.Address, strconv.Itoa(peer.Port))

		m.logger.Debug("sending join request",
			"peer_addr", peerAddr,
			"node_id", m.nodeID,
			"raft_address", host,
			"raft_port", raftPort)

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

func findLowestServerIDPeer(peers []ports.Peer, selfID string) *ports.Peer {
	var lowest *ports.Peer
	for i := range peers {
		p := &peers[i]
		if p.ID == selfID {
			continue
		}
		if lowest == nil || p.ID < lowest.ID {
			lowest = p
		}
	}
	return lowest
}

func (m *Manager) buildBootstrapConfig(grpcPort int) *bootstrap.BootstrapConfig {
	raftPort := m.config.Bootstrap.BasePort
	if _, portStr, err := net.SplitHostPort(m.config.BindAddr); err == nil {
		if p, _ := strconv.Atoi(portStr); p > 0 {
			raftPort = p
		}
	}

	return &bootstrap.BootstrapConfig{
		ServiceName:       m.config.Bootstrap.ServiceName,
		DataDir:           m.config.DataDir,
		Ordinal:           m.config.Bootstrap.Ordinal,
		ServerID:          m.nodeID,
		ExpectedNodes:     m.config.Bootstrap.Replicas,
		MinQuorum:         (m.config.Bootstrap.Replicas / 2) + 1,
		RaftPort:          raftPort,
		JoinPort:          grpcPort,
		BindAddr:          m.config.BindAddr,
		HeadlessService:   m.config.Bootstrap.HeadlessService,
		BootstrapTimeout:  60 * time.Second,
		JoinTimeout:       30 * time.Second,
		LeaderWaitTimeout: 30 * time.Second,
	}
}
