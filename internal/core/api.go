package core

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery"
	"github.com/eleven-am/graft/internal/adapters/node_registry"
	"github.com/eleven-am/graft/internal/adapters/queue"
	"github.com/eleven-am/graft/internal/adapters/raft"
	"github.com/eleven-am/graft/internal/adapters/resource_manager"
	"github.com/eleven-am/graft/internal/adapters/semaphore"
	"github.com/eleven-am/graft/internal/adapters/storage"
	"github.com/eleven-am/graft/internal/adapters/transport"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Manager struct {
	orchestrator       *Orchestrator
	engine             ports.EnginePort
	transport          ports.TransportPort
	discovery          ports.DiscoveryManager
	raftAdapter        ports.RaftPort
	logger             *slog.Logger
	config             *ports.ResourceConfig
	registry           ports.NodeRegistryPort
	nodeID             string
	address            string
	raftPort           int
	completionHandlers []ports.CompletionHandler
	errorHandlers      []ports.ErrorHandler
}

type ClusterInfo struct {
	NodeID   string        `json:"node_id"`
	Status   string        `json:"status"`
	IsLeader bool          `json:"is_leader"`
	Peers    []string      `json:"peers"`
	Metrics  EngineMetrics `json:"metrics"`
}

type EngineMetrics struct {
	TotalWorkflows     int64 `json:"total_workflows"`
	ActiveWorkflows    int64 `json:"active_workflows"`
	CompletedWorkflows int64 `json:"completed_workflows"`
	FailedWorkflows    int64 `json:"failed_workflows"`
	NodesExecuted      int64 `json:"nodes_executed"`
}

type CompletionHandler func(ctx context.Context, data domain.WorkflowCompletionData) error
type ErrorHandler func(ctx context.Context, data domain.WorkflowErrorData) error

func extractAddress(bindAddr string) string {
	for i := len(bindAddr) - 1; i >= 0; i-- {
		if bindAddr[i] == ':' {
			return bindAddr[:i]
		}
	}
	return bindAddr
}

func extractPort(bindAddr string) int {
	for i := len(bindAddr) - 1; i >= 0; i-- {
		if bindAddr[i] == ':' {
			portStr := bindAddr[i+1:]
			port := 0
			for _, r := range portStr {
				if r >= '0' && r <= '9' {
					port = port*10 + int(r-'0')
				} else {
					return 8080
				}
			}
			return port
		}
	}
	return 8080
}

func New(nodeId, bindAddr, dataDir string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	logger = logger.With("component", "graft", "node_id", nodeId)
	raftConfig := raft.DefaultRaftConfig(nodeId, bindAddr, dataDir)
	raftAdapter, err := raft.NewAdapter(raftConfig, logger)
	if err != nil {
		logger.Error("failed to create raft adapter", "error", err)
		return nil
	}

	d := discovery.NewManager(nodeId, logger)
	nodeRegistryAdapter := node_registry.NewAdapter(logger)
	return &Manager{
		logger:      logger,
		raftAdapter: raftAdapter,
		nodeID:      nodeId,
		address:     extractAddress(bindAddr),
		raftPort:    extractPort(bindAddr),
		discovery:   d,
		registry:    nodeRegistryAdapter,
	}
}

func (m *Manager) Discovery() ports.DiscoveryManager {
	return m.discovery
}

func (m *Manager) Start(ctx context.Context, grpcPort int) error {
	orchestratorConfig := OrchestratorConfig{
		ShutdownTimeout: 30 * time.Second,
		StartupTimeout:  30 * time.Second,
		GracePeriod:     2 * time.Second,
	}

	transportAdapter := transport.NewGRPCTransport(m.logger)
	storageAdapter := storage.NewAdapter(m.raftAdapter, transportAdapter, m.nodeID, m.logger)

	readyQueueAdapter := queue.NewAdapter(storageAdapter, m.nodeID, ports.QueueTypeReady, m.logger)
	pendingQueueAdapter := queue.NewAdapter(storageAdapter, m.nodeID, ports.QueueTypePending, m.logger)

	resourceConfig := &ports.ResourceConfig{
		MaxConcurrentTotal:   100,
		DefaultPerTypeLimit:  10, // Allow up to 10 nodes per type by default
		MaxConcurrentPerType: map[string]int{},
		HealthThresholds:     ports.HealthConfig{},
	}
	resourceManagerAdapter := resource_manager.NewAdapter(*resourceConfig, m.logger)
	semaphoreAdapter := semaphore.NewAdapter(storageAdapter, m.logger)

	workflowEngineAdapter := createWorkflowEngine(
		storageAdapter,
		readyQueueAdapter,
		pendingQueueAdapter,
		m.registry,
		resourceManagerAdapter,
		semaphoreAdapter,
		m.raftAdapter,
		m.logger,
	)

	m.engine = workflowEngineAdapter
	m.orchestrator = NewOrchestrator(
		m.logger,
		workflowEngineAdapter,
		transportAdapter,
		m.discovery,
		m.raftAdapter,
		orchestratorConfig,
		m.address,
		m.nodeID,
		m.raftPort,
	)

	if len(m.completionHandlers) > 0 || len(m.errorHandlers) > 0 {
		m.engine.RegisterLifecycleHandlers(m.completionHandlers, m.errorHandlers)
	}

	m.config = resourceConfig

	return m.orchestrator.Startup(ctx, grpcPort)
}

func (m *Manager) Stop(ctx context.Context) error {
	return m.orchestrator.Shutdown(ctx)
}

func (m *Manager) RegisterNode(node interface{}) error {
	wrapper, err := NewNodeWrapper(node)
	if err != nil {
		return err
	}

	return m.registry.RegisterNode(wrapper)
}

func (m *Manager) UnregisterNode(nodeName string) error {
	return m.registry.UnregisterNode(nodeName)
}

func (m *Manager) StartWorkflow(trigger ports.WorkflowTrigger) error {
	if m.orchestrator == nil {
		return domain.NewWorkflowError(trigger.WorkflowID, "start", domain.ErrNotStarted)
	}
	return m.orchestrator.workflowEngine.ProcessTrigger(trigger)
}

func (m *Manager) GetWorkflowState(workflowID string) (*ports.WorkflowStatus, error) {
	if m.orchestrator == nil {
		return nil, domain.NewWorkflowError(workflowID, "get_state", domain.ErrNotStarted)
	}
	return m.orchestrator.workflowEngine.GetWorkflowStatus(workflowID)
}

func (m *Manager) GetClusterInfo() ClusterInfo {
	info := ClusterInfo{
		NodeID:   m.nodeID,
		Status:   "stopped",
		IsLeader: false,
		Peers:    []string{},
		Metrics:  EngineMetrics{},
	}

	if m.orchestrator != nil {
		info.Status = "running"

		if m.raftAdapter != nil {
			info.IsLeader = m.raftAdapter.IsLeader()
		}

		if m.orchestrator.workflowEngine != nil {
			metrics := m.orchestrator.workflowEngine.GetExecutionMetrics()
			info.Metrics = EngineMetrics{
				TotalWorkflows:     metrics.TotalWorkflows,
				ActiveWorkflows:    metrics.ActiveWorkflows,
				CompletedWorkflows: metrics.CompletedWorkflows,
				FailedWorkflows:    metrics.FailedWorkflows,
				NodesExecuted:      metrics.NodesExecuted,
			}
		}
	}

	return info
}

func (m *Manager) OnComplete(handler CompletionHandler) {
	m.completionHandlers = append(m.completionHandlers, ports.CompletionHandler(handler))
	if m.engine != nil {
		m.engine.RegisterLifecycleHandlers(m.completionHandlers, m.errorHandlers)
	}
	m.logger.Debug("completion handler registered")
}

func (m *Manager) OnError(handler ErrorHandler) {
	m.errorHandlers = append(m.errorHandlers, ports.ErrorHandler(handler))
	if m.engine != nil {
		m.engine.RegisterLifecycleHandlers(m.completionHandlers, m.errorHandlers)
	}
	m.logger.Debug("error handler registered")
}

func (m *Manager) PauseWorkflow(ctx context.Context, workflowID string) error {
	if m.orchestrator == nil {
		return domain.NewWorkflowError(workflowID, "pause", domain.ErrNotStarted)
	}
	return m.orchestrator.workflowEngine.PauseWorkflow(ctx, workflowID)
}

func (m *Manager) ResumeWorkflow(ctx context.Context, workflowID string) error {
	if m.orchestrator == nil {
		return domain.NewWorkflowError(workflowID, "resume", domain.ErrNotStarted)
	}
	return m.orchestrator.workflowEngine.ResumeWorkflow(ctx, workflowID)
}

func (m *Manager) StopWorkflow(ctx context.Context, workflowID string) error {
	if m.orchestrator == nil {
		return domain.NewWorkflowError(workflowID, "stop", domain.ErrNotStarted)
	}
	return m.orchestrator.workflowEngine.StopWorkflow(ctx, workflowID)
}
