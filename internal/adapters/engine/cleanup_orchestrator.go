package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/eleven-am/graft/internal/adapters/raftimpl"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type CleanupOptions struct {
	PreserveState   bool          `json:"preserve_state"`
	PreserveAudit   bool          `json:"preserve_audit"`
	RetentionPeriod time.Duration `json:"retention_period"`
	ArchiveLocation string        `json:"archive_location"`
	Force           bool          `json:"force"`
}

type CleanupOrchestrator struct {
	storage         ports.StoragePort
	queue           ports.QueuePort
	queueCleaner    QueueCleaner
	archivalManager *ArchivalManager
	logger          *slog.Logger
	metricsTracker  *CleanupMetricsTracker
}

type QueueCleaner interface {
	RemoveWorkflowItems(ctx context.Context, workflowID string) error
	RemovePendingItems(ctx context.Context, workflowID string) error
	ReleaseWorkflowClaims(ctx context.Context, workflowID string) error
	GetWorkflowItemCount(ctx context.Context, workflowID string) int
	RemoveAllWorkflowData(ctx context.Context, workflowID string) error
	BatchRemoveWorkflows(ctx context.Context, workflowIDs []string) error
	GetOrphanedItems(ctx context.Context) ([]ports.QueueItem, error)
}

func NewCleanupOrchestrator(storage ports.StoragePort, queue ports.QueuePort, queueCleaner QueueCleaner, archivalManager *ArchivalManager, logger *slog.Logger) *CleanupOrchestrator {
	return &CleanupOrchestrator{
		storage:         storage,
		queue:           queue,
		queueCleaner:    queueCleaner,
		archivalManager: archivalManager,
		logger:          logger.With("component", "cleanup_orchestrator"),
		metricsTracker:  NewCleanupMetricsTracker(),
	}
}

func (co *CleanupOrchestrator) CleanupWorkflow(ctx context.Context, workflowID string, options CleanupOptions) error {
	co.logger.Info("starting workflow cleanup",
		"workflow_id", workflowID,
		"preserve_state", options.PreserveState,
		"preserve_audit", options.PreserveAudit,
		"force", options.Force)

	startTime := time.Now()
	co.metricsTracker.RecordCleanupStart()

	operations := co.buildCleanupOperations(options)
	if len(operations) == 0 {
		co.logger.Info("no cleanup operations needed",
			"workflow_id", workflowID)
		return nil
	}

	if !options.Force {
		if err := co.validateCleanupSafety(ctx, workflowID); err != nil {
			co.logger.Error("cleanup safety check failed",
				"workflow_id", workflowID,
				"error", err)
			co.metricsTracker.RecordCleanupFailure(time.Since(startTime))
			return err
		}
	}

	if co.archivalManager != nil && !options.PreserveState {
		if err := co.createPreCleanupBackup(ctx, workflowID); err != nil {
			co.logger.Error("failed to create pre-cleanup backup",
				"workflow_id", workflowID,
				"error", err)
			if !options.Force {
				co.metricsTracker.RecordCleanupFailure(time.Since(startTime))
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "pre-cleanup backup failed",
					Details: map[string]interface{}{
						"workflow_id": workflowID,
						"error":       err.Error(),
					},
				}
			}
			co.logger.Warn("continuing cleanup without backup due to force flag",
				"workflow_id", workflowID)
		}
	}

	results := make(map[string]string)
	var cleanupError error

	for _, op := range operations {
		if err := co.executeCleanupOperation(ctx, workflowID, op, results); err != nil {
			cleanupError = domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: fmt.Sprintf("cleanup operation %s/%s failed", op.Target, op.Action),
				Details: map[string]interface{}{
					"target": op.Target,
					"action": op.Action,
					"error":  err.Error(),
				},
			}
			break
		}
	}

	duration := time.Since(startTime)

	if cleanupError != nil {
		co.logger.Error("workflow cleanup failed",
			"workflow_id", workflowID,
			"duration", duration,
			"error", cleanupError)
		co.metricsTracker.RecordCleanupFailure(duration)

		if err := co.rollbackCleanup(ctx, workflowID, operations); err != nil {
			co.logger.Error("cleanup rollback failed",
				"workflow_id", workflowID,
				"error", err)
		}

		return cleanupError
	}

	co.logger.Info("workflow cleanup completed successfully",
		"workflow_id", workflowID,
		"duration", duration,
		"operations", len(operations))
	co.metricsTracker.RecordCleanupSuccess(duration, results)

	return nil
}

func (co *CleanupOrchestrator) CleanupBatch(ctx context.Context, workflowIDs []string, options CleanupOptions) error {
	co.logger.Info("starting batch cleanup",
		"workflow_count", len(workflowIDs))

	var failedWorkflows []string

	for _, workflowID := range workflowIDs {
		if err := co.CleanupWorkflow(ctx, workflowID, options); err != nil {
			co.logger.Error("batch cleanup failed for workflow",
				"workflow_id", workflowID,
				"error", err)
			failedWorkflows = append(failedWorkflows, workflowID)
		}
	}

	if len(failedWorkflows) > 0 {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("batch cleanup failed for %d workflows", len(failedWorkflows)),
			Details: map[string]interface{}{
				"failed_count":     len(failedWorkflows),
				"failed_workflows": failedWorkflows,
			},
		}
	}

	co.logger.Info("batch cleanup completed successfully",
		"workflow_count", len(workflowIDs))

	return nil
}

func (co *CleanupOrchestrator) ScheduleCleanup(workflowID string, delay time.Duration, options CleanupOptions) error {
	co.logger.Info("scheduling workflow cleanup",
		"workflow_id", workflowID,
		"delay", delay)

	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		<-timer.C

		ctx := context.Background()
		if err := co.CleanupWorkflow(ctx, workflowID, options); err != nil {
			co.logger.Error("scheduled cleanup failed",
				"workflow_id", workflowID,
				"error", err)
		}
	}()

	return nil
}

func (co *CleanupOrchestrator) GetCleanupStatus(ctx context.Context, workflowID string) (*raftimpl.CleanupStatus, error) {
	statusKey := fmt.Sprintf("cleanup:status:%s", workflowID)
	data, err := co.storage.Get(ctx, statusKey)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get cleanup status",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	var status raftimpl.CleanupStatus
	if err := unmarshalJSON(data, &status); err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal cleanup status",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	return &status, nil
}

func (co *CleanupOrchestrator) GetMetrics() CleanupMetrics {
	return co.metricsTracker.GetMetrics()
}

func (co *CleanupOrchestrator) buildCleanupOperations(options CleanupOptions) []raftimpl.CleanupOp {
	var operations []raftimpl.CleanupOp

	if !options.PreserveState {
		action := "delete"
		if options.ArchiveLocation != "" {
			action = "archive"
		}
		operations = append(operations, raftimpl.CleanupOp{
			Target: "state",
			Action: action,
		})
	}

	operations = append(operations, raftimpl.CleanupOp{
		Target: "queue",
		Action: "delete",
	})

	operations = append(operations, raftimpl.CleanupOp{
		Target: "claims",
		Action: "delete",
	})

	operations = append(operations, raftimpl.CleanupOp{
		Target: "idempotency",
		Action: "delete",
	})

	if !options.PreserveAudit {
		action := "delete"
		if options.ArchiveLocation != "" {
			action = "archive"
		}
		operations = append(operations, raftimpl.CleanupOp{
			Target: "audit",
			Action: action,
		})
	}

	return operations
}

func (co *CleanupOrchestrator) validateCleanupSafety(ctx context.Context, workflowID string) error {
	co.logger.Debug("validating cleanup safety",
		"workflow_id", workflowID)

	if workflowID == "" {
		return domain.NewValidationError("workflow_id", "cannot be empty")
	}

	stateKey := fmt.Sprintf("workflow:state:%s", workflowID)
	data, err := co.storage.Get(ctx, stateKey)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	var workflowData map[string]interface{}
	if err := unmarshalJSON(data, &workflowData); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	status, ok := workflowData["status"].(string)
	if !ok {
		return domain.NewValidationError("workflow_status", "invalid format")
	}

	const (
		StatusRunning   = "running"
		StatusPaused    = "paused"
		StatusStopping  = "stopping"
		StatusCompleted = "completed"
		StatusFailed    = "failed"
		StatusCancelled = "cancelled"
	)

	switch status {
	case StatusRunning, StatusPaused, StatusStopping:
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "cannot cleanup active workflow",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"status":      status,
				"reason":      "workflow must be completed or failed before cleanup",
			},
		}
	case StatusCompleted, StatusFailed, StatusCancelled:
		co.logger.Debug("workflow is in cleanable state",
			"workflow_id", workflowID,
			"status", status)
	default:
		co.logger.Warn("unknown workflow status during cleanup validation",
			"workflow_id", workflowID,
			"status", status)
	}

	itemCount := co.queueCleaner.GetWorkflowItemCount(ctx, workflowID)
	if itemCount > 0 {
		co.logger.Warn("workflow has pending queue items",
			"workflow_id", workflowID,
			"item_count", itemCount)

		if itemCount > 100 {
			return domain.Error{
				Type:    domain.ErrorTypeConflict,
				Message: "workflow has too many pending items for safe cleanup",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
					"item_count":  itemCount,
					"max_allowed": 100,
				},
			}
		}
	}

	lastActivity, ok := workflowData["last_activity"].(string)
	if ok {
		if lastActivityTime, err := time.Parse(time.RFC3339, lastActivity); err == nil {
			if time.Since(lastActivityTime) < 5*time.Minute {
				return domain.Error{
					Type:    domain.ErrorTypeConflict,
					Message: "workflow had recent activity, cleanup may be unsafe",
					Details: map[string]interface{}{
						"workflow_id":   workflowID,
						"last_activity": lastActivityTime.Format(time.RFC3339),
						"min_idle_time": "5m",
					},
				}
			}
		}
	}

	if claimsData, ok := workflowData["active_claims"]; ok {
		if claims, ok := claimsData.([]interface{}); ok && len(claims) > 0 {
			return domain.Error{
				Type:    domain.ErrorTypeConflict,
				Message: "workflow has active claims, cleanup may cause resource leaks",
				Details: map[string]interface{}{
					"workflow_id":   workflowID,
					"active_claims": len(claims),
				},
			}
		}
	}

	co.logger.Debug("cleanup safety validation passed",
		"workflow_id", workflowID,
		"status", status,
		"pending_items", itemCount)

	return nil
}

func (co *CleanupOrchestrator) executeCleanupOperation(ctx context.Context, workflowID string, op raftimpl.CleanupOp, results map[string]string) error {
	co.logger.Debug("executing cleanup operation",
		"workflow_id", workflowID,
		"target", op.Target,
		"action", op.Action)

	switch op.Target {
	case "queue":
		return co.queueCleaner.RemoveAllWorkflowData(ctx, workflowID)
	case "idempotency":
		return co.cleanupIdempotencyKeys(ctx, workflowID)
	case "state", "claims", "audit":
		cmd := raftimpl.NewCleanupCommand(workflowID, []raftimpl.CleanupOp{op})
		data, err := cmd.Marshal()
		if err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to marshal cleanup command",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
					"target":      op.Target,
					"action":      op.Action,
					"error":       err.Error(),
				},
			}
		}
		operations := []ports.Operation{
			{Type: ports.OpPut, Key: fmt.Sprintf("cleanup:%s", workflowID), Value: data},
		}
		return co.storage.Batch(ctx, operations)
	default:
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "unknown cleanup target",
			Details: map[string]interface{}{
				"target": op.Target,
			},
		}
	}
}

func (co *CleanupOrchestrator) cleanupIdempotencyKeys(ctx context.Context, workflowID string) error {
	prefix := fmt.Sprintf("workflow:idempotency:%s:", workflowID)

	items, err := co.storage.List(ctx, prefix)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list idempotency keys for cleanup",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	if len(items) == 0 {
		co.logger.Debug("no idempotency keys found for cleanup",
			"workflow_id", workflowID)
		return nil
	}

	var operations []ports.Operation
	for _, item := range items {
		operations = append(operations, ports.Operation{
			Type: ports.OpDelete,
			Key:  item.Key,
		})
	}

	if err := co.storage.Batch(ctx, operations); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to delete idempotency keys",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"key_count":   len(items),
				"error":       err.Error(),
			},
		}
	}

	co.logger.Debug("idempotency keys cleaned up",
		"workflow_id", workflowID,
		"deleted_keys", len(items))

	return nil
}

func (co *CleanupOrchestrator) rollbackCleanup(ctx context.Context, workflowID string, operations []raftimpl.CleanupOp) error {
	co.logger.Info("attempting cleanup rollback",
		"workflow_id", workflowID,
		"operations_count", len(operations))

	var rollbackErrors []error
	var restoredTargets []string

	for i := len(operations) - 1; i >= 0; i-- {
		op := operations[i]
		co.logger.Debug("processing rollback operation",
			"workflow_id", workflowID,
			"target", op.Target,
			"action", op.Action,
			"operation_index", i)

		switch op.Action {
		case "archive":
			if err := co.restoreFromArchive(ctx, workflowID, op.Target); err != nil {
				co.logger.Error("failed to restore from archive during rollback",
					"workflow_id", workflowID,
					"target", op.Target,
					"error", err)
				rollbackErrors = append(rollbackErrors, domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to restore from archive",
					Details: map[string]interface{}{
						"target": op.Target,
						"error":  err.Error(),
					},
				})
			} else {
				restoredTargets = append(restoredTargets, op.Target)
			}
		case "delete":
			co.logger.Warn("cannot rollback delete operation - data permanently lost",
				"workflow_id", workflowID,
				"target", op.Target)
			rollbackErrors = append(rollbackErrors, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "cannot rollback delete operation",
				Details: map[string]interface{}{
					"target": op.Target,
					"reason": "data permanently lost",
				},
			})
		case "compress":
			co.logger.Debug("skipping rollback for compress operation",
				"workflow_id", workflowID,
				"target", op.Target)
		default:
			co.logger.Warn("unknown operation action during rollback",
				"workflow_id", workflowID,
				"target", op.Target,
				"action", op.Action)
		}
	}

	if len(rollbackErrors) > 0 {
		co.logger.Error("cleanup rollback completed with errors",
			"workflow_id", workflowID,
			"error_count", len(rollbackErrors),
			"restored_targets", restoredTargets)
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "cleanup rollback failed for multiple operations",
			Details: map[string]interface{}{
				"workflow_id":  workflowID,
				"failed_count": len(rollbackErrors),
				"errors":       rollbackErrors,
			},
		}
	}

	co.logger.Info("cleanup rollback completed successfully",
		"workflow_id", workflowID,
		"restored_targets", restoredTargets)
	return nil
}

func (co *CleanupOrchestrator) restoreFromArchive(ctx context.Context, workflowID string, target string) error {
	co.logger.Info("restoring from archive during rollback",
		"workflow_id", workflowID,
		"target", target)

	if co.archivalManager == nil {
		co.logger.Error("archival manager not available for restoration",
			"workflow_id", workflowID,
			"target", target)
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "archival manager not available for restoration",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"target":      target,
			},
		}
	}

	if err := co.archivalManager.RestoreWorkflow(ctx, workflowID, nil, co.storage); err != nil {
		co.logger.Error("failed to restore workflow from archive",
			"workflow_id", workflowID,
			"target", target,
			"error", err)
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to restore workflow from archive",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"target":      target,
				"error":       err.Error(),
			},
		}
	}

	co.logger.Info("successfully restored workflow from archive",
		"workflow_id", workflowID,
		"target", target)
	return nil
}

func (co *CleanupOrchestrator) createPreCleanupBackup(ctx context.Context, workflowID string) error {
	co.logger.Info("creating pre-cleanup backup",
		"workflow_id", workflowID)

	stateKey := fmt.Sprintf("workflow:state:%s", workflowID)
	stateData, err := co.storage.Get(ctx, stateKey)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get workflow state for backup",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	var workflowState interface{}
	if err := json.Unmarshal(stateData, &workflowState); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal workflow state for backup",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	queueItemsKey := fmt.Sprintf("queue:workflow:%s", workflowID)
	queueData, _ := co.storage.Get(ctx, queueItemsKey)
	var queueItems []ports.QueueItem
	if queueData != nil {
		json.Unmarshal(queueData, &queueItems)
	}

	claimsKey := fmt.Sprintf("claims:%s", workflowID)
	claimsData, _ := co.storage.Get(ctx, claimsKey)
	var claims []interface{}
	if claimsData != nil {
		json.Unmarshal(claimsData, &claims)
	}

	auditKey := fmt.Sprintf("audit:%s", workflowID)
	auditData, _ := co.storage.Get(ctx, auditKey)
	var auditLogs []interface{}
	if auditData != nil {
		json.Unmarshal(auditData, &auditLogs)
	}

	archive := WorkflowArchive{
		WorkflowID: workflowID,
		State:      workflowState,
		QueueItems: queueItems,
		Claims:     claims,
		AuditLogs:  auditLogs,
		Metadata: map[string]interface{}{
			"backup_type": "pre_cleanup",
			"created_by":  "cleanup_orchestrator",
		},
	}

	if err := co.archivalManager.ArchiveWorkflow(ctx, workflowID, archive); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create archive backup",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	co.logger.Info("pre-cleanup backup created successfully",
		"workflow_id", workflowID)
	return nil
}

func unmarshalJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
