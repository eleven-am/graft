package engine

import (
	"context"
	"github.com/eleven-am/graft/internal/domain"
)

// StateManagerInterface defines the contract for workflow state management
type StateManagerInterface interface {
	// SaveWorkflowState saves a workflow instance's state
	SaveWorkflowState(ctx context.Context, workflow *domain.WorkflowInstance) error

	// LoadWorkflowState loads a workflow instance by ID
	LoadWorkflowState(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error)

	// UpdateWorkflowState atomically updates a workflow's state
	UpdateWorkflowState(ctx context.Context, workflowID string, updateFn func(*domain.WorkflowInstance) error) error
}

// Ensure StateManager implements the interface
var _ StateManagerInterface = (*StateManager)(nil)

// Ensure OptimizedStateManager implements the interface
var _ StateManagerInterface = (*OptimizedStateManager)(nil)
