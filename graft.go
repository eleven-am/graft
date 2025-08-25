package graft

import (
	"context"
	"log/slog"

	"github.com/eleven-am/graft/internal/core"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

// Re-export core types for convenience
type Manager = core.Manager
type ClusterInfo = core.ClusterInfo
type WorkflowTrigger = ports.WorkflowTrigger
type WorkflowStatus = ports.WorkflowStatus
type NodeConfig = ports.NodeConfig
type NodeResult = ports.NodeResult
type NextNode = ports.NextNode

// Re-export domain types
type WorkflowCompletionData = domain.WorkflowCompletionData
type WorkflowErrorData = domain.WorkflowErrorData
type ExecutedNodeData = domain.ExecutedNodeData
type CheckpointData = domain.CheckpointData
type WorkflowContext = domain.WorkflowContext
type ClusterBasicInfo = domain.ClusterBasicInfo

// Handler types
type CompletionHandler = ports.CompletionHandler
type ErrorHandler = ports.ErrorHandler

// New creates a new Graft manager with the core API
//
// Example usage:
//
//	manager := graft.New("node-1", "localhost:7000", "./data", myLogger)
//	manager.Discovery().MDNS()  // Configure discovery
//	manager.Start(ctx, "localhost:8080")  // Start the system
func New(nodeID, bindAddr, dataDir string, logger *slog.Logger) *Manager {
	return core.New(nodeID, bindAddr, dataDir, logger)
}

// NewWithConfig creates a new Graft manager using comprehensive configuration
//
// This constructor provides full control over all aspects of the Graft system,
// including discovery methods, resource limits, engine settings, and more.
//
// Example usage:
//
//	config := graft.NewConfigBuilder("node-1", "localhost:7000", "./data").
//		WithMDNS("_graft._tcp", "local.", "").
//		WithKubernetes("graft-service", "default").
//		WithResourceLimits(200, 20, map[string]int{"heavy": 5}).
//		WithEngineSettings(100, 10*time.Minute, 5).
//		Build()
//
//	manager := graft.NewWithConfig(config)
//	manager.Start(ctx, 8080)
func NewWithConfig(config *Config) *Manager {
	return core.NewWithConfig(config)
}

// GetWorkflowContext extracts workflow metadata from the context during node execution.
// Nodes can call this function to access workflow-level information like workflow ID,
// execution metadata, cluster state, and more.
//
// Example usage in a node:
//
//	func (n *MyNode) Execute(ctx context.Context, state MyState, config MyConfig) (graft.NodeResult, error) {
//	    if workflowCtx, ok := graft.GetWorkflowContext(ctx); ok {
//	        logger.Info("Executing node", "workflow_id", workflowCtx.WorkflowID, "node", workflowCtx.NodeName)
//	    }
//	    // ... rest of node logic
//	}
func GetWorkflowContext(ctx context.Context) (*WorkflowContext, bool) {
	return domain.GetWorkflowContext(ctx)
}
