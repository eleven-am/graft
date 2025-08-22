package cluster

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dgraph-io/badger/v3/options"
	"github.com/eleven-am/graft/internal/adapters/discovery"
	workflowEngine "github.com/eleven-am/graft/internal/adapters/engine"
	grpcTransport "github.com/eleven-am/graft/internal/adapters/grpc"
	resourceManager "github.com/eleven-am/graft/internal/adapters/manager"
	memoryRegistry "github.com/eleven-am/graft/internal/adapters/memory"
	queue2 "github.com/eleven-am/graft/internal/adapters/queue"
	raftimpl2 "github.com/eleven-am/graft/internal/adapters/raftimpl"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type ComponentFactory struct {
	config ClusterConfig
	logger *slog.Logger
}

func NewComponentFactory(config ClusterConfig, logger *slog.Logger) (*ComponentFactory, error) {
	if logger == nil {
		logger = slog.Default()
	}

	return &ComponentFactory{
		config: config,
		logger: logger.With("component", "factory"),
	}, nil
}

func (f *ComponentFactory) CreateDiscovery() (ports.DiscoveryPort, error) {
	f.logger.Debug("creating discovery component", "strategy", f.config.Discovery.Strategy)

	strategy := discovery.Strategy(f.config.Discovery.Strategy)
	if strategy == "" {
		strategy = discovery.StrategyAuto
	}

	manager := discovery.NewManager(f.logger, strategy)
	return manager, nil
}

func (f *ComponentFactory) CreateTransport() (ports.TransportPort, error) {
	f.logger.Debug("creating transport component", "type", "grpc")

	transport := grpcTransport.NewGRPCTransport(f.logger)
	return transport, nil
}

func (f *ComponentFactory) CreateStorage() (ports.StoragePort, error) {
	f.logger.Debug("creating storage component", "type", "raft")

	f.logger.Debug("setting up badger store configuration")
	storeConfig := &raftimpl2.StoreConfig{
		DataDir:            f.config.Storage.DataDir,
		RetainSnapshots:    f.config.Storage.SnapshotRetention,
		SnapshotThreshold:  uint64(f.config.Storage.SnapshotThreshold),
		Compression:        options.ZSTD,
		ValueLogFileSize:   64 << 20,
		NumMemtables:       3,
		NumLevelZeroTables: 2,
		NumCompactors:      2,
	}

	f.logger.Debug("creating badger store", "data_dir", storeConfig.DataDir)
	store, err := raftimpl2.NewStore(storeConfig, f.logger)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create storage store",
			Details: map[string]interface{}{
				"data_dir": storeConfig.DataDir,
				"error":    err.Error(),
			},
		}
	}
	f.logger.Debug("badger store created successfully")

	f.logger.Debug("creating FSM")
	fsm := raftimpl2.NewGraftFSM(store.StateDB(), f.logger)
	f.logger.Debug("FSM created successfully")

	f.logger.Debug("creating raft config")
	raftConfig := raftimpl2.DefaultRaftConfig(
		f.config.NodeID,
		fmt.Sprintf("%s:%d", f.config.Storage.ListenAddress, f.config.Storage.ListenPort),
		f.config.Storage.DataDir,
	)
	f.logger.Debug("raft config created successfully")

	f.logger.Debug("creating raft node")
	raftNode, err := raftimpl2.NewRaftNode(raftConfig, store, fsm, f.logger)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create raft node",
			Details: map[string]interface{}{
				"node_id": f.config.NodeID,
				"address": fmt.Sprintf("%s:%d", f.config.Storage.ListenAddress, f.config.Storage.ListenPort),
				"error":   err.Error(),
			},
		}
	}

	transport, err := f.CreateTransport()
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to initialize transport for storage",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	adapter := raftimpl2.NewStorageAdapter(raftNode, store.StateDB(), transport, f.logger)

	f.logger.Debug("waiting for raft leadership before storage ready")
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		f.logger.Warn("raft leadership not established within timeout, continuing anyway", "timeout", "10s", "error", err.Error())
	} else {
		f.logger.Debug("raft leadership established successfully")
	}

	f.logger.Info("storage component created successfully", "data_dir", storeConfig.DataDir)
	return adapter, nil
}

func (f *ComponentFactory) CreateQueue() (ports.QueuePort, error) {
	f.logger.Debug("creating queue component", "type", "badger")

	f.logger.Warn("queue creation deferred until storage is available")
	return nil, domain.Error{
		Type:    domain.ErrorTypeUnavailable,
		Message: "queue creation deferred until storage is available",
		Details: map[string]interface{}{
			"reason": "queue requires storage dependency",
		},
	}
}

func (f *ComponentFactory) CreateQueueWithStorage(storage ports.StoragePort) (ports.QueuePort, error) {
	f.logger.Debug("creating queue component with storage", "type", "badger")

	config := queue2.Config{
		MaxSize:        f.config.Queue.MaxSize,
		EnableMetrics:  true,
		CleanupEnabled: true,
		CleanupTTL:     time.Hour,
		NodeID:         f.config.NodeID,
	}

	queue, err := queue2.NewUnifiedBadgerQueue(storage, config)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create unified badger queue",
			Details: map[string]interface{}{
				"max_size": f.config.Queue.MaxSize,
				"error":    err.Error(),
			},
		}
	}

	f.logger.Info("queue component created successfully", "type", "badger")
	return queue, nil
}

func (f *ComponentFactory) CreateNodeRegistry() (ports.NodeRegistryPort, error) {
	f.logger.Debug("creating node registry component", "type", "memory")

	registry := memoryRegistry.NewMemoryNodeRegistry(f.logger)
	return registry, nil
}

func (f *ComponentFactory) CreateResourceManager() (ports.ResourceManagerPort, error) {
	f.logger.Debug("creating resource manager component")

	manager := resourceManager.NewResourceManager(f.config.Resources, f.logger)
	return manager, nil
}

func (f *ComponentFactory) CreateWorkflowEngine() (ports.WorkflowEnginePort, error) {
	f.logger.Debug("creating workflow engine component")

	engineConfig := workflowEngine.Config{
		MaxConcurrentWorkflows: f.config.Engine.MaxConcurrentWorkflows,
		NodeExecutionTimeout:   f.config.Engine.NodeExecutionTimeout,
		StateUpdateInterval:    f.config.Engine.StateUpdateInterval,
		RetryAttempts:          f.config.Engine.RetryAttempts,
		RetryBackoff:           f.config.Engine.RetryBackoff,
	}

	engine := workflowEngine.NewEngine(engineConfig, f.logger)

	f.logger.Debug("workflow engine created, dependencies will be injected during cluster initialization")

	return engine, nil
}

type MockComponentFactory struct {
	config ClusterConfig
	logger *slog.Logger
	mocks  ComponentMocks
}

type ComponentMocks struct {
	Discovery       ports.DiscoveryPort
	Transport       ports.TransportPort
	Storage         ports.StoragePort
	Queue           ports.QueuePort
	NodeRegistry    ports.NodeRegistryPort
	ResourceManager ports.ResourceManagerPort
	WorkflowEngine  ports.WorkflowEnginePort
}

func NewMockComponentFactory(config ClusterConfig, logger *slog.Logger, mocks ComponentMocks) *MockComponentFactory {
	if logger == nil {
		logger = slog.Default()
	}

	return &MockComponentFactory{
		config: config,
		logger: logger.With("component", "mock_factory"),
		mocks:  mocks,
	}
}

func (f *MockComponentFactory) CreateDiscovery() (ports.DiscoveryPort, error) {
	if f.mocks.Discovery != nil {
		return f.mocks.Discovery, nil
	}
	return nil, domain.NewConfigurationError("mock_discovery", "mock discovery component not provided", "provide discovery mock in ComponentMocks")
}

func (f *MockComponentFactory) CreateTransport() (ports.TransportPort, error) {
	if f.mocks.Transport != nil {
		return f.mocks.Transport, nil
	}
	return nil, domain.NewConfigurationError("mock_transport", "mock transport component not provided", "provide transport mock in ComponentMocks")
}

func (f *MockComponentFactory) CreateStorage() (ports.StoragePort, error) {
	if f.mocks.Storage != nil {
		return f.mocks.Storage, nil
	}
	return nil, domain.NewConfigurationError("mock_storage", "mock storage component not provided", "provide storage mock in ComponentMocks")
}

func (f *MockComponentFactory) CreateQueue() (ports.QueuePort, error) {
	if f.mocks.Queue != nil {
		return f.mocks.Queue, nil
	}
	return nil, domain.NewConfigurationError("mock_queue", "mock queue component not provided", "provide queue mock in ComponentMocks")
}

func (f *MockComponentFactory) CreateNodeRegistry() (ports.NodeRegistryPort, error) {
	if f.mocks.NodeRegistry != nil {
		return f.mocks.NodeRegistry, nil
	}
	return nil, domain.NewConfigurationError("mock_node_registry", "mock node registry component not provided", "provide node registry mock in ComponentMocks")
}

func (f *MockComponentFactory) CreateResourceManager() (ports.ResourceManagerPort, error) {
	if f.mocks.ResourceManager != nil {
		return f.mocks.ResourceManager, nil
	}
	return nil, domain.NewConfigurationError("mock_resource_manager", "mock resource manager component not provided", "provide resource manager mock in ComponentMocks")
}

func (f *MockComponentFactory) CreateWorkflowEngine() (ports.WorkflowEnginePort, error) {
	if f.mocks.WorkflowEngine != nil {
		return f.mocks.WorkflowEngine, nil
	}
	return nil, domain.NewConfigurationError("mock_workflow_engine", "mock workflow engine component not provided", "provide workflow engine mock in ComponentMocks")
}
