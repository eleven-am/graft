package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type PendingEvaluator interface {
	EvaluatePendingNodes(ctx context.Context, workflowID string, currentState interface{}) error
	CheckNodeReadiness(node *ports.PendingNode, state interface{}, config interface{}) bool
	MovePendingToReady(ctx context.Context, items []ports.QueueItem) error
}

type pendingEvaluator struct {
	engine         *Engine
	logger         *slog.Logger
	mu             sync.RWMutex
	evaluationLock sync.Map
}

func NewPendingEvaluator(engine *Engine, logger *slog.Logger) PendingEvaluator {
	return &pendingEvaluator{
		engine: engine,
		logger: logger.With("component", "pending_evaluator"),
	}
}

func (pe *pendingEvaluator) EvaluatePendingNodes(ctx context.Context, workflowID string, currentState interface{}) error {
	if workflowID == "" {
		return domain.NewValidationError("workflow_id", "workflow ID cannot be empty")
	}

	lockKey := workflowID
	if _, loaded := pe.evaluationLock.LoadOrStore(lockKey, true); loaded {
		pe.logger.Debug("evaluation already in progress for workflow", "workflow_id", workflowID)
		return nil
	}
	defer pe.evaluationLock.Delete(lockKey)

	startTime := time.Now()
	pe.logger.Debug("starting pending node evaluation", "workflow_id", workflowID)

	if pe.engine.pendingQueue == nil {
		return domain.NewValidationError("pending_queue", "pending queue not available")
	}

	pendingItems, err := pe.engine.pendingQueue.GetItems(ctx)
	if err != nil {
		pe.logger.Error("failed to get pending items",
			"workflow_id", workflowID,
			"error", err.Error(),
		)
		return err
	}

	var readyItems []ports.QueueItem
	for _, item := range pendingItems {
		if item.WorkflowID != workflowID {
			continue
		}

		pendingNode := &ports.PendingNode{
			NodeName: item.NodeName,
			Config:   item.Config,
			QueuedAt: item.EnqueuedAt,
			Priority: item.Priority,
		}

		if pe.CheckNodeReadiness(pendingNode, currentState, item.Config) {
			readyItems = append(readyItems, item)
			pe.logger.Debug("node became ready",
				"workflow_id", workflowID,
				"node_name", item.NodeName,
				"item_id", item.ID,
			)
		}
	}

	if len(readyItems) > 0 {
		if err := pe.MovePendingToReady(ctx, readyItems); err != nil {
			pe.logger.Error("failed to move items to ready queue",
				"workflow_id", workflowID,
				"ready_count", len(readyItems),
				"error", err.Error(),
			)
			return err
		}

		pe.logger.Debug("evaluation completed",
			"workflow_id", workflowID,
			"nodes_made_ready", len(readyItems),
			"evaluation_duration", time.Since(startTime),
		)
	} else {
		pe.logger.Debug("evaluation completed with no ready nodes",
			"workflow_id", workflowID,
			"evaluation_duration", time.Since(startTime),
		)
	}

	return nil
}

func (pe *pendingEvaluator) CheckNodeReadiness(node *ports.PendingNode, state interface{}, config interface{}) bool {
	if node == nil {
		pe.logger.Debug("node is nil, not ready")
		return false
	}

	registeredNode, err := pe.engine.nodeRegistry.GetNode(node.NodeName)
	if err != nil {
		pe.logger.Debug("node not found in registry",
			"node_name", node.NodeName,
			"error", err.Error(),
		)
		return false
	}

	canStart := registeredNode.CanStart(context.Background(), state, config)

	pe.logger.Debug("node readiness check",
		"node_name", node.NodeName,
		"can_start", canStart,
	)

	return canStart
}

func (pe *pendingEvaluator) MovePendingToReady(ctx context.Context, items []ports.QueueItem) error {
	if len(items) == 0 {
		return nil
	}

	if pe.engine.readyQueue == nil {
		return domain.NewValidationError("ready_queue", "ready queue not available")
	}

	for _, item := range items {
		if err := pe.engine.pendingQueue.RemoveItem(ctx, item.ID); err != nil {
			pe.logger.Error("failed to remove item from pending queue",
				"item_id", item.ID,
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"error", err.Error(),
			)
			return err
		}

		if err := pe.engine.readyQueue.Enqueue(ctx, item); err != nil {
			pe.logger.Error("failed to enqueue item to ready queue",
				"item_id", item.ID,
				"workflow_id", item.WorkflowID,
				"node_name", item.NodeName,
				"error", err.Error(),
			)
			return err
		}

		pe.logger.Debug("moved item to ready queue",
			"item_id", item.ID,
			"workflow_id", item.WorkflowID,
			"node_name", item.NodeName,
		)
	}

	pe.logger.Debug("batch move to ready completed",
		"items_moved", len(items),
	)

	return nil
}

func (pe *pendingEvaluator) evaluateConditions(state interface{}, conditions map[string]interface{}) bool {
	if conditions == nil || len(conditions) == 0 {
		return true
	}

	stateMap, err := pe.convertToMap(state)
	if err != nil {
		pe.logger.Debug("failed to convert state to map for condition evaluation",
			"error", err.Error(),
		)
		return false
	}

	for key, expectedValue := range conditions {
		actualValue, exists := stateMap[key]
		if !exists {
			pe.logger.Debug("condition key not found in state",
				"key", key,
			)
			return false
		}

		if !pe.valuesEqual(actualValue, expectedValue) {
			pe.logger.Debug("condition not met",
				"key", key,
				"expected", expectedValue,
				"actual", actualValue,
			)
			return false
		}
	}

	return true
}

func (pe *pendingEvaluator) convertToMap(value interface{}) (map[string]interface{}, error) {
	if value == nil {
		return make(map[string]interface{}), nil
	}

	if m, ok := value.(map[string]interface{}); ok {
		return m, nil
	}

	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (pe *pendingEvaluator) valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	aBytes, err := json.Marshal(a)
	if err != nil {
		return false
	}

	bBytes, err := json.Marshal(b)
	if err != nil {
		return false
	}

	return string(aBytes) == string(bBytes)
}
