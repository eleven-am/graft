package ports

import (
	"context"

	"github.com/eleven-am/graft/internal/domain"
)

type EnginePort interface {
	Start(ctx context.Context) error
	Stop() error

	ProcessTrigger(trigger domain.WorkflowTrigger) error
	GetWorkflowStatus(workflowID string) (*domain.WorkflowStatus, error)

	PauseWorkflow(ctx context.Context, workflowID string) error
	ResumeWorkflow(ctx context.Context, workflowID string) error
	StopWorkflow(ctx context.Context, workflowID string) error
}

type StateManagerPort interface {
	SaveWorkflowState(ctx context.Context, workflow *domain.WorkflowInstance) error
	LoadWorkflowState(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error)
	UpdateWorkflowState(ctx context.Context, workflowID string, updateFn func(*domain.WorkflowInstance) error) error
}

type ExecutorPort interface {
	ExecuteNode(ctx context.Context, workflowID, nodeName string, config interface{}) error
}
