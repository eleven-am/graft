package engine

import "github.com/eleven-am/graft/internal/domain"

const (
	engineComponent       = "engine.Engine"
	executorComponent     = "engine.Executor"
	stateManagerComponent = "engine.StateManager"
)

func newWorkflowError(component, message string, cause error, opts ...domain.ErrorOption) *domain.DomainError {
	merged := []domain.ErrorOption{domain.WithComponent(component)}
	if len(opts) > 0 {
		merged = append(merged, opts...)
	}
	return domain.NewWorkflowError(message, cause, merged...)
}

func newWorkflowStorageError(component, message string, cause error, opts ...domain.ErrorOption) *domain.DomainError {
	merged := []domain.ErrorOption{domain.WithComponent(component)}
	if len(opts) > 0 {
		merged = append(merged, opts...)
	}
	return domain.NewStorageError(message, cause, merged...)
}

func errorLogAttrs(err error) []any {
	if err == nil {
		return nil
	}

	attrs := []any{
		"error", err,
		"error_category", domain.GetErrorCategory(err).String(),
		"error_severity", domain.GetErrorSeverity(err).String(),
		"error_retryable", domain.IsRetryableError(err),
		"error_user_facing", domain.IsUserFacingError(err),
	}

	if ctx := domain.GetErrorContext(err); ctx != nil {
		if ctx.Component != "" {
			attrs = append(attrs, "error_component", ctx.Component)
		}
		if ctx.Operation != "" {
			attrs = append(attrs, "error_operation", ctx.Operation)
		}
		if ctx.WorkflowID != "" {
			attrs = append(attrs, "workflow_id", ctx.WorkflowID)
		}
		if ctx.NodeID != "" {
			attrs = append(attrs, "node_id", ctx.NodeID)
		}
		if ctx.RequestID != "" {
			attrs = append(attrs, "request_id", ctx.RequestID)
		}
		if len(ctx.Details) > 0 {
			attrs = append(attrs, "error_details", ctx.Details)
		}
	}

	return attrs
}
