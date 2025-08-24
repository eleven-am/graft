package engine

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type WorkflowCleaner struct {
	storage      ports.StoragePort
	readyQueue   ports.QueuePort
	pendingQueue ports.QueuePort
	logger       *slog.Logger
}

func NewWorkflowCleaner(storage ports.StoragePort, readyQueue ports.QueuePort, pendingQueue ports.QueuePort, logger *slog.Logger) *WorkflowCleaner {
	return &WorkflowCleaner{
		storage:      storage,
		readyQueue:   readyQueue,
		pendingQueue: pendingQueue,
		logger:       logger,
	}
}

func (wc *WorkflowCleaner) NukeWorkflow(ctx context.Context, workflowID string) error {
	wc.logger.Debug("starting nuclear cleanup for workflow", "workflow_id", workflowID)

	var aggregatedErrors []error

	storageErrors := wc.cleanupStorage(ctx, workflowID)
	aggregatedErrors = append(aggregatedErrors, storageErrors...)

	queueErrors := wc.cleanupQueue(ctx, workflowID)
	aggregatedErrors = append(aggregatedErrors, queueErrors...)

	if len(aggregatedErrors) > 0 {
		wc.logger.Error("workflow cleanup completed with errors",
			"workflow_id", workflowID,
			"error_count", len(aggregatedErrors),
		)
		return fmt.Errorf("cleanup failed with %d errors: %v", len(aggregatedErrors), aggregatedErrors)
	}

	wc.logger.Debug("workflow completely nuked", "workflow_id", workflowID)
	return nil
}

func (wc *WorkflowCleaner) cleanupStorage(ctx context.Context, workflowID string) []error {
	var errors []error

	storagePrefixes := []string{
		fmt.Sprintf("workflow:state:%s", workflowID),
		fmt.Sprintf("workflow:checkpoint:%s:", workflowID),
		fmt.Sprintf("workflow:execution:%s:", workflowID),
		fmt.Sprintf("workflow:executed_node:%s:", workflowID),
		fmt.Sprintf("workflow:idempotency:%s:", workflowID),
		fmt.Sprintf("claim:%s:", workflowID),
	}

	for _, prefix := range storagePrefixes {
		if err := wc.deleteByPrefix(ctx, prefix); err != nil {
			wc.logger.Error("failed to delete storage keys with prefix",
				"workflow_id", workflowID,
				"prefix", prefix,
				"error", err.Error(),
			)
			errors = append(errors, fmt.Errorf("storage cleanup failed for prefix %s: %w", prefix, err))
		} else {
			wc.logger.Debug("cleaned storage prefix", "workflow_id", workflowID, "prefix", prefix)
		}
	}

	return errors
}

func (wc *WorkflowCleaner) deleteByPrefix(ctx context.Context, prefix string) error {
	if wc.storage == nil {
		return fmt.Errorf("storage not available")
	}

	if strings.HasSuffix(prefix, ":") {
		keyValues, err := wc.storage.List(ctx, prefix)
		if err != nil {
			if domain.IsNotFoundError(err) {
				return nil
			}
			return err
		}

		for _, kv := range keyValues {
			if err := wc.storage.Delete(ctx, kv.Key); err != nil {
				wc.logger.Warn("failed to delete key", "key", kv.Key, "error", err.Error())
			}
		}
	} else {
		if err := wc.storage.Delete(ctx, prefix); err != nil {
			if !domain.IsNotFoundError(err) {
				return err
			}
		}
	}

	return nil
}

func (wc *WorkflowCleaner) cleanupQueue(ctx context.Context, workflowID string) []error {
	var errors []error

	if err := wc.removeQueueItems(ctx, workflowID); err != nil {
		wc.logger.Error("failed to cleanup queue items",
			"workflow_id", workflowID,
			"error", err.Error(),
		)
		errors = append(errors, fmt.Errorf("queue cleanup failed: %w", err))
	} else {
		wc.logger.Debug("cleaned queue items", "workflow_id", workflowID)
	}

	return errors
}

func (wc *WorkflowCleaner) removeQueueItems(ctx context.Context, workflowID string) error {
	queues := []struct {
		queue ports.QueuePort
		name  string
	}{
		{wc.readyQueue, "ready"},
		{wc.pendingQueue, "pending"},
	}

	for _, q := range queues {
		if q.queue == nil {
			wc.logger.Debug("queue not available for cleanup",
				"workflow_id", workflowID,
				"queue_type", q.name,
			)
			continue
		}

		items, err := q.queue.GetItems(ctx)
		if err != nil {
			if domain.IsNotFoundError(err) {
				continue
			}
			wc.logger.Warn("failed to get queue items",
				"workflow_id", workflowID,
				"queue_type", q.name,
				"error", err.Error(),
			)
			continue
		}

		for _, item := range items {
			if item.WorkflowID == workflowID {
				if err := q.queue.RemoveItem(ctx, item.ID); err != nil {
					wc.logger.Warn("failed to remove queue item",
						"workflow_id", workflowID,
						"queue_type", q.name,
						"item_id", item.ID,
						"error", err.Error(),
					)
				} else {
					wc.logger.Debug("removed queue item",
						"workflow_id", workflowID,
						"queue_type", q.name,
						"item_id", item.ID,
					)
				}
			}
		}
	}

	return nil
}

func (wc *WorkflowCleaner) NukeWorkflows(ctx context.Context, workflowIDs []string) map[string]error {
	results := make(map[string]error)

	for _, workflowID := range workflowIDs {
		if err := wc.NukeWorkflow(ctx, workflowID); err != nil {
			results[workflowID] = err
		}
	}

	wc.logger.Debug("batch workflow cleanup completed",
		"total_workflows", len(workflowIDs),
		"failed_count", len(results),
	)

	return results
}
