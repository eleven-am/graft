package graft

import (
	"context"
	"log/slog"

	"github.com/eleven-am/graft/internal/core"
	"github.com/eleven-am/graft/internal/domain"
)

// Manager wraps the internal core.Manager to provide a user-friendly API
type Manager struct {
	internal *core.Manager
}

// ClusterInfo represents cluster information
type ClusterInfo = core.ClusterInfo

// Event types
type WorkflowStartedEvent = core.WorkflowStartedEvent
type WorkflowCompletedEvent = core.WorkflowCompletedEvent
type WorkflowErrorEvent = core.WorkflowErrorEvent
type WorkflowPausedEvent = core.WorkflowPausedEvent
type WorkflowResumedEvent = core.WorkflowResumedEvent
type NodeStartedEvent = core.NodeStartedEvent
type NodeCompletedEvent = core.NodeCompletedEvent
type NodeErrorEvent = core.NodeErrorEvent

// WorkflowContext represents workflow execution context
type WorkflowContext = domain.WorkflowContext
type WorkflowInstance = domain.WorkflowInstance
type NodeExecutionStatus = domain.NodeExecutionStatus

// New creates a new Graft manager
func New(nodeID, bindAddr, dataDir string, logger *slog.Logger) *Manager {
	return &Manager{
		internal: core.New(nodeID, bindAddr, dataDir, logger),
	}
}

// NewWithConfig creates a new Graft manager using comprehensive configuration
func NewWithConfig(config *domain.Config) *Manager {
	return &Manager{
		internal: core.NewWithConfig(config),
	}
}

// Discovery returns the discovery manager for service discovery configuration
func (m *Manager) Discovery() interface{} {
	return m.internal.Discovery()
}

// MDNS configures MDNS-based service discovery
func (m *Manager) MDNS(args ...string) *Manager {
	m.internal.MDNS(args...)
	return m
}

// Kubernetes configures Kubernetes-based service discovery
func (m *Manager) Kubernetes(args ...string) *Manager {
	m.internal.Kubernetes(args...)
	return m
}

// Static configures static peer-based service discovery
func (m *Manager) Static(peers interface{}) *Manager {
	// Note: This would need proper conversion if we exposed peer configuration
	return m
}

// Start starts the Graft manager
func (m *Manager) Start(ctx context.Context, grpcPort int) error {
	return m.internal.Start(ctx, grpcPort)
}

// Stop stops the Graft manager
func (m *Manager) Stop() error {
	return m.internal.Stop()
}

// RegisterNode registers a workflow node with the system
func (m *Manager) RegisterNode(node interface{}) error {
	return m.internal.RegisterNode(node)
}

// UnregisterNode removes a node from the registry
func (m *Manager) UnregisterNode(nodeName string) error {
	return m.internal.UnregisterNode(nodeName)
}

// StartWorkflow starts a new workflow with the given trigger
func (m *Manager) StartWorkflow(trigger WorkflowTrigger) error {
	internalTrigger, err := trigger.toInternal()
	if err != nil {
		return err
	}

	return m.internal.StartWorkflow(internalTrigger)
}

// GetWorkflowStatus returns the current status of a workflow
func (m *Manager) GetWorkflowStatus(workflowID string) (*WorkflowStatus, error) {
	internalStatus, err := m.internal.GetWorkflowStatus(workflowID)
	if err != nil {
		return nil, err
	}

	publicStatus, err := workflowStatusFromInternal(*internalStatus)
	if err != nil {
		return nil, err
	}

	return &publicStatus, nil
}

// PauseWorkflow pauses a running workflow
func (m *Manager) PauseWorkflow(ctx context.Context, workflowID string) error {
	return m.internal.PauseWorkflow(ctx, workflowID)
}

// ResumeWorkflow resumes a paused workflow
func (m *Manager) ResumeWorkflow(ctx context.Context, workflowID string) error {
	return m.internal.ResumeWorkflow(ctx, workflowID)
}

// StopWorkflow stops a workflow (marks it as failed)
func (m *Manager) StopWorkflow(ctx context.Context, workflowID string) error {
	return m.internal.StopWorkflow(ctx, workflowID)
}

// GetClusterInfo returns information about the cluster state
func (m *Manager) GetClusterInfo() ClusterInfo {
	return m.internal.GetClusterInfo()
}

// Event subscription methods
func (m *Manager) OnWorkflowStarted(handler func(*WorkflowStartedEvent)) error {
	return m.internal.OnWorkflowStarted(handler)
}

func (m *Manager) OnWorkflowCompleted(handler func(*WorkflowCompletedEvent)) error {
	return m.internal.OnWorkflowCompleted(handler)
}

func (m *Manager) OnWorkflowFailed(handler func(*WorkflowErrorEvent)) error {
	return m.internal.OnWorkflowFailed(handler)
}

func (m *Manager) OnWorkflowPaused(handler func(*WorkflowPausedEvent)) error {
	return m.internal.OnWorkflowPaused(handler)
}

func (m *Manager) OnWorkflowResumed(handler func(*WorkflowResumedEvent)) error {
	return m.internal.OnWorkflowResumed(handler)
}

func (m *Manager) OnNodeStarted(handler func(*NodeStartedEvent)) error {
	return m.internal.OnNodeStarted(handler)
}

func (m *Manager) OnNodeCompleted(handler func(*NodeCompletedEvent)) error {
	return m.internal.OnNodeCompleted(handler)
}

func (m *Manager) OnNodeError(handler func(*NodeErrorEvent)) error {
	return m.internal.OnNodeError(handler)
}

func (m *Manager) Subscribe(pattern string, handler func(string, interface{})) error {
	return m.internal.Subscribe(pattern, handler)
}

func (m *Manager) Unsubscribe(pattern string) error {
	return m.internal.Unsubscribe(pattern)
}

// SubscribeToWorkflowState subscribes to workflow state changes
func (m *Manager) SubscribeToWorkflowState(workflowID string) (<-chan *WorkflowStatus, func(), error) {
	internalChan, cleanup, err := m.internal.SubscribeToWorkflowState(workflowID)
	if err != nil {
		return nil, nil, err
	}

	// Create a channel that converts internal status to public status
	publicChan := make(chan *WorkflowStatus, 1)

	go func() {
		defer close(publicChan)
		for internalStatus := range internalChan {
			if publicStatus, err := workflowStatusFromInternal(*internalStatus); err == nil {
				publicChan <- &publicStatus
			}
		}
	}()

	return publicChan, cleanup, nil
}
