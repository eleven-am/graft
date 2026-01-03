// Package graft provides a distributed workflow orchestration engine for Go applications.
//
// Graft allows you to build complex workflows by composing individual nodes that can be
// distributed across multiple machines. It provides features like:
//   - Dynamic workflow execution with conditional branching
//   - Distributed execution across cluster nodes
//   - State persistence and recovery
//   - Event-driven architecture with pub/sub capabilities
//   - Service discovery (mDNS, static; external providers supported)
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
	"github.com/eleven-am/graft/internal/helpers/metadata"
	"github.com/eleven-am/graft/internal/ports"
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
type NextNode = ports.NextNode

// NodeResult contains the output from a node execution, including any updated
// global state and the list of next nodes to execute.
type NodeResult = ports.NodeResult

// WorkflowStatus represents the current state of a workflow execution,
// including its status, current state, executed nodes, and any errors.
type WorkflowStatus = core.WorkflowStatus

// ExecutedNodeData contains information about a node that has been executed,
// including its execution time, duration, status, configuration, and results.
type ExecutedNodeData = core.ExecutedNodeData

// ExecutingNodeData contains information about a node that is currently being executed,
// including its start time, claim ID, and configuration.
type ExecutingNodeData = core.ExecutingNodeData

// WorkflowState represents the possible states of a workflow during execution.
type WorkflowState = core.WorkflowState

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

// Cluster membership events for monitoring cluster topology changes

// NodeJoinedEvent is emitted when a new node joins the cluster.
type NodeJoinedEvent = core.NodeJoinedEvent

// NodeLeftEvent is emitted when a node leaves the cluster.
type NodeLeftEvent = core.NodeLeftEvent

// LeaderChangedEvent is emitted when cluster leadership changes.
type LeaderChangedEvent = core.LeaderChangedEvent

// Cross-node developer messaging types

// DevCommand represents a developer command that can be broadcast across cluster nodes.
type DevCommand = core.DevCommand

// CommandHandler defines the signature for handling developer commands.
type CommandHandler = core.CommandHandler

// Context types for workflow execution

// WorkflowContext provides metadata about the current workflow execution
// that is available to nodes during their execution.
type WorkflowContext = domain.WorkflowContext

// WorkflowInstance represents a specific instance of a workflow execution.
type WorkflowInstance = domain.WorkflowInstance

// NodeExecutionStatus indicates the current execution status of a node.
type NodeExecutionStatus = domain.NodeExecutionStatus

// ConnectorConfig defines the configuration interface for connectors.
type ConnectorConfig = ports.ConnectorConfig

// ConnectorFactory builds a connector instance from a serialized configuration payload.
type ConnectorFactory = ports.ConnectorFactory

// ConnectorPort represents a long-lived connector instance managed by Graft.
type ConnectorPort = ports.ConnectorPort

// ConnectorRegistrationError describes failures when registering connector factories.
type ConnectorRegistrationError = ports.ConnectorRegistrationError

// ConnectorState represents the runtime state of a connector handle.
type ConnectorState = ports.ConnectorState

const (
	// ConnectorStatePending indicates the connector is waiting to acquire a lease.
	ConnectorStatePending = ports.ConnectorStatePending
	// ConnectorStateRunning indicates the connector is currently executing.
	ConnectorStateRunning = ports.ConnectorStateRunning
	// ConnectorStateError indicates the connector terminated due to an error.
	ConnectorStateError = ports.ConnectorStateError
)

// ConnectorStatus describes the runtime details for a connector handle managed by Graft.
type ConnectorStatus = ports.ConnectorStatus

// ConnectorLeaseStatus provides additional information about the distributed lease managing a connector handle.
type ConnectorLeaseStatus = ports.ConnectorLeaseStatus

// ConnectorStatusReporter can be implemented by connectors that wish to expose custom metadata.
type ConnectorStatusReporter = ports.ConnectorStatusReporter

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

func New(nodeID, dataDir string, logger *slog.Logger) *Manager {
	return core.New(nodeID, dataDir, logger)
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

// ResetBootstrapMetadataForTesting forces regeneration of bootstrap metadata.
//
// This helper is intended for in-process integration tests where multiple managers
// run inside the same binary and would otherwise share bootstrap identifiers.
// Do not use in production code.
func ResetBootstrapMetadataForTesting() {
	metadata.ResetGlobalBootstrapMetadata()
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

// WrapHandler creates a type-safe command handler wrapper that automatically converts
// interface{} parameters to the specified concrete type T.
//
// This wrapper enables you to write strongly-typed command handlers while still
// conforming to the generic CommandHandler interface expected by the command system.
//
// Parameters:
//   - handler: A typed handler function that accepts parameters of type T
//
// Returns:
//   - CommandHandler: A generic handler that can be registered with the command system
//
// Example usage:
//
//	type DeployParams struct {
//	    Service  string `json:"service"`
//	    Version  string `json:"version"`
//	    Replicas int    `json:"replicas"`
//	}
//
//	handler := graft.WrapHandler(func(ctx context.Context, from string, params DeployParams) error {
//	    return deployService(params.Service, params.Version, params.Replicas)
//	})
//
//	manager.RegisterCommandHandler("deploy", handler)
func WrapHandler[T any](handler func(ctx context.Context, from string, params T) error) CommandHandler {
	return core.WrapHandler(handler)
}
