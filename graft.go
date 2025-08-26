package graft

import (
	"context"

	"github.com/eleven-am/graft/internal/domain"
)

// GetWorkflowContext extracts workflow metadata from the context during node execution.
// Nodes can call this function to access workflow-level information like workflow ID,
// execution metadata, cluster state, and more.
//
// Example usage in a node:
//
//	func (n *MyNode) Execute(ctx context.Context, state MyState, config MyConfig) (*graft.NodeResult, error) {
//	    if workflowCtx, ok := graft.GetWorkflowContext(ctx); ok {
//	        logger.Info("Executing node", "workflow_id", workflowCtx.WorkflowID, "node", workflowCtx.NodeName)
//	    }
//	    // ... rest of node logic
//	}
func GetWorkflowContext(ctx context.Context) (*WorkflowContext, bool) {
	return domain.GetWorkflowContext(ctx)
}
