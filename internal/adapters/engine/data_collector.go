package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type WorkflowDataCollector struct {
	storage      ports.StoragePort
	queue        ports.QueuePort
	nodeRegistry ports.NodeRegistryPort
}

func NewWorkflowDataCollector(storage ports.StoragePort, queue ports.QueuePort, nodeRegistry ports.NodeRegistryPort) *WorkflowDataCollector {
	return &WorkflowDataCollector{
		storage:      storage,
		queue:        queue,
		nodeRegistry: nodeRegistry,
	}
}

func (wdc *WorkflowDataCollector) CollectWorkflowData(ctx context.Context, workflow *WorkflowInstance) (*domain.WorkflowCompletionData, error) {
	workflow.mu.RLock()
	workflowID := workflow.ID
	finalState := workflow.CurrentState
	status := string(workflow.Status)
	startedAt := workflow.StartedAt
	completedAt := time.Now()
	if workflow.CompletedAt != nil {
		completedAt = *workflow.CompletedAt
	}
	metadata := workflow.Metadata
	workflow.mu.RUnlock()

	data := &domain.WorkflowCompletionData{
		WorkflowID:  workflowID,
		FinalState:  finalState,
		Status:      status,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		Duration:    completedAt.Sub(startedAt),
		Metadata:    metadata,
	}

	var err error

	data.ExecutedNodes, err = wdc.collectExecutedNodes(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect executed nodes: %w", err)
	}

	data.Checkpoints, err = wdc.collectCheckpoints(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect checkpoints: %w", err)
	}

	data.QueueItems, err = wdc.collectQueueItems(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect queue items: %w", err)
	}

	data.IdempotencyKeys, err = wdc.collectIdempotencyKeys(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect idempotency keys: %w", err)
	}

	data.ClaimsData, err = wdc.collectClaimsData(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect claims data: %w", err)
	}

	return data, nil
}

func (wdc *WorkflowDataCollector) collectExecutedNodes(ctx context.Context, workflowID string) ([]domain.ExecutedNodeData, error) {
	prefix := fmt.Sprintf("workflow:execution:%s:", workflowID)

	items, err := wdc.storage.List(ctx, prefix)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list executed nodes",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	result := make([]domain.ExecutedNodeData, 0, len(items))

	for _, item := range items {
		var nodeData map[string]interface{}
		if err := json.Unmarshal(item.Value, &nodeData); err != nil {
			continue
		}

		executedAtStr, ok := nodeData["executed_at"].(string)
		if !ok {
			continue
		}

		executedAt, err := time.Parse(time.RFC3339Nano, executedAtStr)
		if err != nil {
			continue
		}

		durationNanos, ok := nodeData["duration"].(float64)
		if !ok {
			continue
		}

		duration := time.Duration(int64(durationNanos))

		nodeResult := domain.ExecutedNodeData{
			NodeName:   nodeData["node_name"].(string),
			ExecutedAt: executedAt,
			Duration:   duration,
			Status:     nodeData["status"].(string),
			Config:     nodeData["config"],
			Results:    nodeData["results"],
		}

		if errorMsg, exists := nodeData["error"].(string); exists {
			nodeResult.Error = &errorMsg
		}

		result = append(result, nodeResult)
	}

	return result, nil
}

func (wdc *WorkflowDataCollector) collectCheckpoints(ctx context.Context, workflowID string) ([]domain.CheckpointData, error) {
	prefix := fmt.Sprintf("workflow:checkpoint:%s:", workflowID)

	items, err := wdc.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list checkpoints: %w", err)
	}

	result := make([]domain.CheckpointData, 0, len(items))

	for _, item := range items {
		var state interface{}
		if err := json.Unmarshal(item.Value, &state); err != nil {
			continue
		}

		timestampStr := strings.TrimPrefix(item.Key, prefix)
		timestamp, err := time.Parse("20060102150405", timestampStr)
		if err != nil {
			continue
		}

		result = append(result, domain.CheckpointData{
			Timestamp:  timestamp,
			State:      state,
			StorageKey: item.Key,
		})
	}

	return result, nil
}

func (wdc *WorkflowDataCollector) collectQueueItems(ctx context.Context, workflowID string) ([]domain.QueueItemData, error) {
	result := make([]domain.QueueItemData, 0)

	pendingItems, err := wdc.queue.GetPendingItems(ctx)
	if err == nil {
		for _, item := range pendingItems {
			if item.WorkflowID == workflowID {
				result = append(result, domain.QueueItemData{
					ID:         item.ID,
					NodeName:   item.NodeName,
					Config:     item.Config,
					Priority:   item.Priority,
					EnqueuedAt: item.EnqueuedAt,
					QueueType:  "pending",
				})
			}
		}
	}

	return result, nil
}

func (wdc *WorkflowDataCollector) collectIdempotencyKeys(ctx context.Context, workflowID string) ([]domain.IdempotencyKeyData, error) {
	prefix := fmt.Sprintf("workflow:idempotency:%s:", workflowID)

	items, err := wdc.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list idempotency keys: %w", err)
	}

	result := make([]domain.IdempotencyKeyData, 0, len(items))

	for _, item := range items {
		var keyData map[string]interface{}
		if err := json.Unmarshal(item.Value, &keyData); err != nil {
			continue
		}

		claimedAtStr, ok := keyData["claimed_at"].(string)
		if !ok {
			continue
		}

		claimedAt, err := time.Parse(time.RFC3339, claimedAtStr)
		if err != nil {
			continue
		}

		key := strings.TrimPrefix(item.Key, prefix)

		result = append(result, domain.IdempotencyKeyData{
			Key:        key,
			ClaimedAt:  claimedAt,
			StorageKey: item.Key,
		})
	}

	return result, nil
}

func (wdc *WorkflowDataCollector) collectClaimsData(ctx context.Context, workflowID string) ([]domain.ClaimData, error) {
	prefix := fmt.Sprintf("claim:%s:", workflowID)

	items, err := wdc.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list claims: %w", err)
	}

	result := make([]domain.ClaimData, 0, len(items))

	for _, item := range items {
		var claimData map[string]interface{}
		if err := json.Unmarshal(item.Value, &claimData); err != nil {
			continue
		}

		claimedAtStr, ok := claimData["claimed_at"].(string)
		if !ok {
			continue
		}

		claimedAt, err := time.Parse(time.RFC3339, claimedAtStr)
		if err != nil {
			continue
		}

		nodeName := strings.TrimPrefix(item.Key, prefix)

		result = append(result, domain.ClaimData{
			NodeName:  nodeName,
			ClaimedAt: claimedAt,
			Data:      claimData,
		})
	}

	return result, nil
}
