package domain

import (
	"context"
	"time"
)

type contextKey string

const WorkflowContextKey contextKey = "graft:workflow_context"

type WorkflowContext struct {
	WorkflowID   string            `json:"workflow_id"`
	NodeName     string            `json:"node_name"`
	ExecutionID  string            `json:"execution_id"`
	StartedAt    time.Time         `json:"started_at"`
	Metadata     map[string]string `json:"metadata"`
	ClusterInfo  ClusterBasicInfo  `json:"cluster_info"`
	PreviousNode *string           `json:"previous_node,omitempty"`
	RetryCount   int               `json:"retry_count"`
	Priority     int               `json:"priority"`
}

type ClusterBasicInfo struct {
	NodeID   string `json:"node_id"`
	IsLeader bool   `json:"is_leader"`
	Status   string `json:"status"`
}

func WithWorkflowContext(ctx context.Context, workflowCtx *WorkflowContext) context.Context {
	return context.WithValue(ctx, WorkflowContextKey, workflowCtx)
}

func GetWorkflowContext(ctx context.Context) (*WorkflowContext, bool) {
	workflowCtx, ok := ctx.Value(WorkflowContextKey).(*WorkflowContext)
	return workflowCtx, ok
}
