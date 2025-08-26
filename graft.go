// Package graft provides a distributed workflow orchestration engine for Go applications.
//
// Graft allows you to build complex workflows by composing individual nodes that can be
// distributed across multiple machines. It provides features like:
//   - Dynamic workflow execution with conditional branching
//   - Distributed execution across cluster nodes
//   - State persistence and recovery
//   - Event-driven architecture with pub/sub capabilities
//   - Service discovery (mDNS, Kubernetes, static)
//   - Built-in clustering with Raft consensus
//
// Basic usage:
//
//	manager := graft.New("node1", "localhost:8080", "./data", logger)
//	manager.RegisterNode(&MyWorkflowNode{})
//	manager.Start(context.Background(), 8080)
//
//	trigger := graft.WorkflowTrigger{
//	    WorkflowID:   "my-workflow-123",
//	    InitialNodes: []graft.NodeConfig{{Name: "MyWorkflowNode"}},
//	    InitialState: map[string]interface{}{"input": "data"},
//	}
//	manager.StartWorkflow(trigger)
package graft

import (
	"context"
	"log/slog"

	"github.com/eleven-am/graft/internal/core"
	"github.com/eleven-am/graft/internal/domain"
)

// Manager is the main orchestration engine that manages workflow execution,
// node registration, cluster coordination, and service discovery.
type Manager = core.Manager

// ClusterInfo contains information about the current cluster state including
// node membership, leadership status, and cluster health metrics.
type ClusterInfo = core.ClusterInfo

// ClusterMetrics provides statistical information about workflow execution
// across the cluster, including counts of active, completed, and failed workflows.
type ClusterMetrics = core.ClusterMetrics

// WorkflowTrigger defines the initial parameters for starting a new workflow,
// including the workflow ID, initial nodes to execute, starting state, and metadata.
type WorkflowTrigger = core.WorkflowTrigger

// NodeConfig specifies the configuration for a workflow node, including
// its name and any node-specific configuration parameters.
type NodeConfig = core.NodeConfig

// NextNode represents a node that should be executed next in the workflow,
// along with its configuration, priority, delay, and idempotency settings.
type NextNode = core.NextNode

// NodeResult contains the output from a node execution, including any updated
// global state and the list of next nodes to execute.
type NodeResult = core.NodeResult

// WorkflowStatus represents the current state of a workflow execution,
// including its status, current state, executed nodes, and any errors.
type WorkflowStatus = core.WorkflowStatus

// ExecutedNodeData contains information about a node that has been executed,
// including its execution time, duration, status, configuration, and results.
type ExecutedNodeData = core.ExecutedNodeData

// WorkflowState represents the possible states of a workflow during execution.
type WorkflowState = core.WorkflowState

// Node defines the interface that all workflow nodes must implement.
// Nodes contain the business logic that gets executed as part of workflows.
type Node = core.Node

// Event types for workflow lifecycle monitoring

// WorkflowStartedEvent is emitted when a workflow begins execution.
type WorkflowStartedEvent = core.WorkflowStartedEvent

// WorkflowCompletedEvent is emitted when a workflow completes successfully.
type WorkflowCompletedEvent = core.WorkflowCompletedEvent

// WorkflowErrorEvent is emitted when a workflow encounters an error and fails.
type WorkflowErrorEvent = core.WorkflowErrorEvent

// WorkflowPausedEvent is emitted when a workflow is paused.
type WorkflowPausedEvent = core.WorkflowPausedEvent

// WorkflowResumedEvent is emitted when a paused workflow is resumed.
type WorkflowResumedEvent = core.WorkflowResumedEvent

// NodeStartedEvent is emitted when an individual node begins execution.
type NodeStartedEvent = core.NodeStartedEvent

// NodeCompletedEvent is emitted when an individual node completes execution.
type NodeCompletedEvent = core.NodeCompletedEvent

// NodeErrorEvent is emitted when an individual node encounters an error.
type NodeErrorEvent = core.NodeErrorEvent

// Context types for workflow execution

// WorkflowContext provides metadata about the current workflow execution
// that is available to nodes during their execution.
type WorkflowContext = domain.WorkflowContext

// WorkflowInstance represents a specific instance of a workflow execution.
type WorkflowInstance = domain.WorkflowInstance

// NodeExecutionStatus indicates the current execution status of a node.
type NodeExecutionStatus = domain.NodeExecutionStatus

// Workflow state constants
const (
	// WorkflowStateRunning indicates the workflow is currently executing.
	WorkflowStateRunning = core.WorkflowStateRunning

	// WorkflowStateCompleted indicates the workflow has finished successfully.
	WorkflowStateCompleted = core.WorkflowStateCompleted

	// WorkflowStateFailed indicates the workflow has failed due to an error.
	WorkflowStateFailed = core.WorkflowStateFailed

	// WorkflowStatePaused indicates the workflow execution has been paused.
	WorkflowStatePaused = core.WorkflowStatePaused
)

// New creates a new Graft manager with the specified configuration.
//
// Parameters:
//   - nodeID: Unique identifier for this node in the cluster
//   - bindAddr: Address to bind the node's network services to
//   - dataDir: Directory path where persistent data will be stored
//   - logger: Structured logger instance for system logging
//
// The manager must be started with Start() before it can process workflows.
//
// Example:
//
//	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
//	manager := graft.New("node-1", "0.0.0.0:8080", "./data", logger)
func New(nodeID, bindAddr, dataDir string, logger *slog.Logger) *Manager {
	return core.New(nodeID, bindAddr, dataDir, logger)
}

// NewWithConfig creates a new Graft manager using a comprehensive configuration object.
//
// This constructor provides more control over the manager's behavior by accepting
// a full configuration that can specify advanced settings for clustering, storage,
// networking, and other system parameters.
//
// Parameters:
//   - config: Complete configuration object with all manager settings
//
// Example:
//
//	config := &domain.Config{
//	    NodeID:   "node-1",
//	    BindAddr: "0.0.0.0:8080",
//	    DataDir:  "./data",
//	    Logger:   logger,
//	    // ... other configuration options
//	}
//	manager := graft.NewWithConfig(config)
func NewWithConfig(config *domain.Config) *Manager {
	return core.NewWithConfig(config)
}

// GetWorkflowContext extracts workflow metadata from the execution context during node execution.
//
// This function should be called within a node's Execute method to access information
// about the current workflow execution, such as the workflow ID, node name, and other
// execution metadata.
//
// Parameters:
//   - ctx: The context passed to the node's Execute method
//
// Returns:
//   - *WorkflowContext: Metadata about the current workflow execution
//   - bool: True if workflow context was found, false otherwise
//
// Example usage in a node:
//
//	func (n *MyNode) Execute(ctx context.Context, state interface{}, config interface{}) (*graft.NodeResult, error) {
//	    if workflowCtx, ok := graft.GetWorkflowContext(ctx); ok {
//	        log.Printf("Executing node %s in workflow %s", workflowCtx.NodeName, workflowCtx.WorkflowID)
//	    }
//	    // ... node execution logic
//	    return &graft.NodeResult{}, nil
//	}
func GetWorkflowContext(ctx context.Context) (*WorkflowContext, bool) {
	return domain.GetWorkflowContext(ctx)
}
