package domain

import "context"

type contextKey string

const WorkflowContextKey contextKey = "graft:workflow_context"

func WithWorkflowContext(ctx context.Context, workflowCtx *WorkflowContext) context.Context {
	return context.WithValue(ctx, WorkflowContextKey, workflowCtx)
}

func GetWorkflowContext(ctx context.Context) (*WorkflowContext, bool) {
	workflowCtx, ok := ctx.Value(WorkflowContextKey).(*WorkflowContext)
	return workflowCtx, ok
}
