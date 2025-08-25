package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type RecoverableExecutor struct {
	logger         *slog.Logger
	metricsTracker *MetricsTracker
}

func NewRecoverableExecutor(logger *slog.Logger, metricsTracker *MetricsTracker) *RecoverableExecutor {
	return &RecoverableExecutor{
		logger:         logger.With("component", "recoverable-executor"),
		metricsTracker: metricsTracker,
	}
}

func (re *RecoverableExecutor) ExecuteWithRecovery(
	ctx context.Context,
	node ports.NodePort,
	currentState interface{},
	config interface{},
	item *ports.QueueItem,
	workflowCtx *domain.WorkflowContext,
) (result interface{}, nextNodes []ports.NextNode, err error) {
	startTime := time.Now()

	defer func() {
		if r := recover(); r != nil {
			duration := time.Since(startTime)
			panicErr := domain.NewPanicError(item.WorkflowID, item.NodeName, r)

			if re.metricsTracker != nil {
				re.metricsTracker.RecordPanic(duration)
				re.metricsTracker.RecordNodeExecution(duration, false)
			}

			re.logger.Error("node execution panicked",
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"panic_value", r,
				"duration", duration,
				"stack_trace", panicErr.StackTrace,
			)

			err = panicErr
			result = nil
			nextNodes = nil
		}
	}()

	re.logger.Debug("executing node with recovery protection",
		"workflow_id", item.WorkflowID,
		"node_name", item.NodeName,
	)

	enrichedCtx := ctx
	if workflowCtx != nil {
		enrichedCtx = domain.WithWorkflowContext(ctx, workflowCtx)
	}

	nodeResult, err := node.Execute(enrichedCtx, currentState, config)
	if err == nil {
		result = nodeResult.GlobalState
		nextNodes = nodeResult.NextNodes
	}

	if err == nil {
		duration := time.Since(startTime)
		if re.metricsTracker != nil {
			re.metricsTracker.RecordNodeExecution(duration, true)
		}
		re.logger.Debug("node execution completed successfully",
			"workflow_id", item.WorkflowID,
			"node_name", item.NodeName,
			"duration", duration,
		)
	}

	// Record failed execution metrics if error occurred
	if err != nil && re.metricsTracker != nil {
		duration := time.Since(startTime)
		re.metricsTracker.RecordNodeExecution(duration, false)
	}

	return result, nextNodes, err
}

func (re *RecoverableExecutor) HandlePanic(workflowID, nodeID string, panicValue interface{}) error {
	panicErr := domain.NewPanicError(workflowID, nodeID, panicValue)

	re.logger.Error("handling panic for workflow node",
		"workflow_id", workflowID,
		"node_id", nodeID,
		"panic_value", panicValue,
		"recovered_at", panicErr.RecoveredAt,
	)

	return panicErr
}

func (re *RecoverableExecutor) MarkNodeFailed(ctx context.Context, item *ports.QueueItem, reason string) *domain.ExecutedNodeData {
	return &domain.ExecutedNodeData{
		NodeName:   item.NodeName,
		ExecutedAt: time.Now(),
		Duration:   0,
		Status:     string(ports.NodeExecutionStatusPanicFailed),
		Config:     item.Config,
		Results:    nil,
		Error:      &reason,
	}
}
