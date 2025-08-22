package raftimpl

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/hashicorp/raft"
)

type GraftFSM struct {
	db     *badger.DB
	logger *slog.Logger
	mu     sync.RWMutex
}

func NewGraftFSM(db *badger.DB, logger *slog.Logger) *GraftFSM {
	return &GraftFSM{
		db:     db,
		logger: logger.With("component", "fsm"),
	}
}

func (f *GraftFSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	cmd, err := UnmarshalCommand(log.Data)
	if err != nil {
		f.logger.Error("failed to unmarshal command",
			"error", err,
			"term", log.Term,
			"index", log.Index)
		return &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal command: %v", err),
		}
	}

	f.logger.Debug("applying command",
		"command_type", cmd.Type.String(),
		"key", cmd.Key,
		"term", log.Term,
		"index", log.Index)

	switch cmd.Type {
	case CommandPut:
		return f.applyPut(cmd)
	case CommandDelete:
		return f.applyDelete(cmd)
	case CommandBatch:
		return f.applyBatch(cmd)
	case CommandStateUpdate:
		return f.applyStateUpdate(cmd)
	case CommandQueueOperation:
		return f.applyQueueOperation(cmd)
	case CommandClaimWork:
		return f.applyClaimWork(cmd)
	case CommandReleaseClaim:
		return f.applyReleaseClaim(cmd)
	case CommandCleanupWorkflow:
		return f.applyCleanup(cmd)
	default:
		return &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("unknown command type: %v", cmd.Type),
		}
	}
}

func (f *GraftFSM) applyPut(cmd *Command) interface{} {
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(cmd.Key), cmd.Value)
	})

	if err != nil {
		f.logger.Error("failed to apply put command",
			"key", cmd.Key,
			"error", err)
		return &CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &CommandResult{Success: true}
}

func (f *GraftFSM) applyDelete(cmd *Command) interface{} {
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(cmd.Key))
	})

	if err != nil {
		f.logger.Error("failed to apply delete command",
			"key", cmd.Key,
			"error", err)
		return &CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &CommandResult{Success: true}
}

func (f *GraftFSM) applyBatch(cmd *Command) interface{} {
	err := f.db.Update(func(txn *badger.Txn) error {
		for _, op := range cmd.Batch {
			switch op.Type {
			case CommandPut:
				if err := txn.Set([]byte(op.Key), op.Value); err != nil {
					return domain.Error{
						Type:    domain.ErrorTypeInternal,
						Message: "batch put operation failed",
						Details: map[string]interface{}{
							"key":   op.Key,
							"error": err.Error(),
						},
					}
				}
			case CommandDelete:
				if err := txn.Delete([]byte(op.Key)); err != nil {
					return domain.Error{
						Type:    domain.ErrorTypeInternal,
						Message: "batch delete operation failed",
						Details: map[string]interface{}{
							"key":   op.Key,
							"error": err.Error(),
						},
					}
				}
			default:
				return domain.Error{
					Type:    domain.ErrorTypeValidation,
					Message: "invalid batch operation type",
					Details: map[string]interface{}{
						"operation_type": op.Type.String(),
						"key":            op.Key,
					},
				}
			}
		}
		return nil
	})

	if err != nil {
		f.logger.Error("failed to apply batch command",
			"operations", len(cmd.Batch),
			"error", err)
		return &CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &CommandResult{Success: true}
}

func (f *GraftFSM) applyStateUpdate(cmd *Command) interface{} {
	key := fmt.Sprintf("state:%s", cmd.Key)
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), cmd.Value)
	})

	if err != nil {
		f.logger.Error("failed to apply state update",
			"key", cmd.Key,
			"error", err)
		return &CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	f.logger.Info("state update applied",
		"key", cmd.Key,
		"timestamp", cmd.Timestamp)

	return &CommandResult{Success: true}
}

func (f *GraftFSM) applyQueueOperation(cmd *Command) interface{} {
	key := fmt.Sprintf("queue:%s", cmd.Key)
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), cmd.Value)
	})

	if err != nil {
		f.logger.Error("failed to apply queue operation",
			"key", cmd.Key,
			"error", err)
		return &CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &CommandResult{Success: true}
}

func (f *GraftFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	f.logger.Info("creating FSM snapshot")

	return &FSMSnapshot{
		db:     f.db,
		logger: f.logger,
	}, nil
}

func (f *GraftFSM) Restore(snapshot io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logger.Info("restoring FSM from snapshot")

	defer snapshot.Close()

	decoder := json.NewDecoder(snapshot)

	var snapshotData struct {
		Version int               `json:"version"`
		Data    map[string][]byte `json:"data"`
	}

	if err := decoder.Decode(&snapshotData); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to decode snapshot",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if snapshotData.Version != 1 {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "unsupported snapshot version",
			Details: map[string]interface{}{
				"version":           snapshotData.Version,
				"supported_version": 1,
			},
		}
	}

	err := f.db.DropAll()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to clear database during restore",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	err = f.db.Update(func(txn *badger.Txn) error {
		for key, value := range snapshotData.Data {
			if err := txn.Set([]byte(key), value); err != nil {
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to restore key from snapshot",
					Details: map[string]interface{}{
						"key":   key,
						"error": err.Error(),
					},
				}
			}
		}
		return nil
	})

	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to restore data from snapshot",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	f.logger.Info("FSM restored from snapshot",
		"keys_restored", len(snapshotData.Data))

	return nil
}

func (f *GraftFSM) applyClaimWork(cmd *Command) interface{} {
	claimCmd, err := UnmarshalClaimWorkCommand(cmd.Value)
	if err != nil {
		f.logger.Error("failed to unmarshal claim work command",
			"error", err)
		return &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal claim work command: %v", err),
		}
	}

	if err := claimCmd.Validate(); err != nil {
		f.logger.Error("invalid claim work command",
			"error", err,
			"work_item_id", claimCmd.WorkItemID,
			"node_id", claimCmd.NodeID)
		return &CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	claimKey := cmd.Key
	var existingClaim *domain.ClaimEntity

	err = f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(claimKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}

		claimData, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		existingClaim = &domain.ClaimEntity{}
		return json.Unmarshal(claimData, existingClaim)
	})

	if err != nil {
		f.logger.Error("failed to check existing claim",
			"error", err,
			"claim_key", claimKey)
		return &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to check existing claim: %v", err),
		}
	}

	if existingClaim != nil && existingClaim.IsActive() {
		f.logger.Debug("claim conflict detected",
			"work_item_id", claimCmd.WorkItemID,
			"existing_node_id", existingClaim.NodeID,
			"requesting_node_id", claimCmd.NodeID)
		return &CommandResult{
			Success: false,
			Error:   domain.NewClaimConflictError(claimCmd.WorkItemID, existingClaim.NodeID, claimCmd.NodeID).Error(),
		}
	}

	newClaim := domain.NewClaim(claimCmd.WorkItemID, claimCmd.NodeID, claimCmd.Duration)
	claimData, err := json.Marshal(newClaim)
	if err != nil {
		f.logger.Error("failed to marshal claim",
			"error", err,
			"work_item_id", claimCmd.WorkItemID)
		return &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to marshal claim: %v", err),
		}
	}

	err = f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(claimKey), claimData)
	})

	if err != nil {
		f.logger.Error("failed to store claim",
			"error", err,
			"work_item_id", claimCmd.WorkItemID,
			"node_id", claimCmd.NodeID)
		return &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to store claim: %v", err),
		}
	}

	f.logger.Info("work item claimed successfully",
		"work_item_id", claimCmd.WorkItemID,
		"node_id", claimCmd.NodeID,
		"expires_at", newClaim.ExpiresAt)

	return &CommandResult{Success: true}
}

func (f *GraftFSM) applyReleaseClaim(cmd *Command) interface{} {
	releaseCmd, err := UnmarshalReleaseClaimCommand(cmd.Value)
	if err != nil {
		f.logger.Error("failed to unmarshal release claim command",
			"error", err)
		return &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal release claim command: %v", err),
		}
	}

	if err := releaseCmd.Validate(); err != nil {
		f.logger.Error("invalid release claim command",
			"error", err,
			"work_item_id", releaseCmd.WorkItemID,
			"node_id", releaseCmd.NodeID)
		return &CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	claimKey := cmd.Key
	var existingClaim *domain.ClaimEntity

	err = f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(claimKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return domain.NewClaimNotFoundError(releaseCmd.WorkItemID, releaseCmd.NodeID)
			}
			return err
		}

		claimData, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		existingClaim = &domain.ClaimEntity{}
		return json.Unmarshal(claimData, existingClaim)
	})

	if err != nil {
		f.logger.Error("failed to get existing claim",
			"error", err,
			"work_item_id", releaseCmd.WorkItemID,
			"node_id", releaseCmd.NodeID)
		return &CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	if existingClaim.NodeID != releaseCmd.NodeID {
		f.logger.Error("claim release denied: not owned by requesting node",
			"work_item_id", releaseCmd.WorkItemID,
			"claim_owner", existingClaim.NodeID,
			"requesting_node", releaseCmd.NodeID)
		return &CommandResult{
			Success: false,
			Error:   domain.NewClaimNotFoundError(releaseCmd.WorkItemID, releaseCmd.NodeID).Error(),
		}
	}

	if existingClaim.IsExpired() {
		f.logger.Debug("claim already expired",
			"work_item_id", releaseCmd.WorkItemID,
			"node_id", releaseCmd.NodeID,
			"expired_at", existingClaim.ExpiresAt)
	}

	err = f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(claimKey))
	})

	if err != nil {
		f.logger.Error("failed to delete claim",
			"error", err,
			"work_item_id", releaseCmd.WorkItemID,
			"node_id", releaseCmd.NodeID)
		return &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to delete claim: %v", err),
		}
	}

	f.logger.Info("claim released successfully",
		"work_item_id", releaseCmd.WorkItemID,
		"node_id", releaseCmd.NodeID)

	return &CommandResult{Success: true}
}

func (f *GraftFSM) applyCleanup(cmd *Command) interface{} {
	cleanupCmd, err := UnmarshalWorkflowCleanupCommand(cmd.Value)
	if err != nil {
		f.logger.Error("failed to unmarshal cleanup command",
			"error", err)
		return &CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal cleanup command: %v", err),
		}
	}

	if err := cleanupCmd.Validate(); err != nil {
		f.logger.Error("invalid cleanup command",
			"error", err,
			"workflow_id", cleanupCmd.WorkflowID)
		return &CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	f.logger.Info("starting workflow cleanup",
		"workflow_id", cleanupCmd.WorkflowID,
		"operations", len(cleanupCmd.Operations))

	status := &CleanupStatus{
		WorkflowID: cleanupCmd.WorkflowID,
		Status:     "running",
		StartedAt:  time.Now(),
		Operations: cleanupCmd.Operations,
		Results:    make(map[string]string),
	}

	var cleanupError error
	for _, op := range cleanupCmd.Operations {
		if err := f.executeCleanupOperation(op, cleanupCmd.WorkflowID, status); err != nil {
			cleanupError = err
			break
		}
	}

	if cleanupError != nil {
		status.Status = "failed"
		errorStr := cleanupError.Error()
		status.Error = &errorStr
		f.logger.Error("cleanup operation failed",
			"workflow_id", cleanupCmd.WorkflowID,
			"error", cleanupError)
	} else {
		status.Status = "completed"
		f.logger.Info("workflow cleanup completed successfully",
			"workflow_id", cleanupCmd.WorkflowID,
			"operations_completed", len(cleanupCmd.Operations))
	}

	now := time.Now()
	status.CompletedAt = &now

	statusKey := fmt.Sprintf("cleanup:status:%s", cleanupCmd.WorkflowID)
	statusData, _ := json.Marshal(status)
	if err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(statusKey), statusData)
	}); err != nil {
		f.logger.Error("failed to store cleanup status",
			"workflow_id", cleanupCmd.WorkflowID,
			"error", err)
	}

	if cleanupError != nil {
		return &CommandResult{
			Success: false,
			Error:   cleanupError.Error(),
		}
	}

	return &CommandResult{Success: true}
}

func (f *GraftFSM) executeCleanupOperation(op CleanupOp, workflowID string, status *CleanupStatus) error {
	f.logger.Debug("executing cleanup operation",
		"workflow_id", workflowID,
		"target", op.Target,
		"action", op.Action)

	switch op.Target {
	case "state":
		return f.cleanupWorkflowState(workflowID, op.Action, status)
	case "queue":
		return f.cleanupQueueItems(workflowID, op.Action, status)
	case "claims":
		return f.cleanupWorkflowClaims(workflowID, op.Action, status)
	case "audit":
		return f.cleanupAuditLogs(workflowID, op.Action, status)
	default:
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "unknown cleanup target",
			Details: map[string]interface{}{
				"target":      op.Target,
				"workflow_id": workflowID,
			},
		}
	}
}

func (f *GraftFSM) cleanupWorkflowState(workflowID string, action string, status *CleanupStatus) error {
	prefix := fmt.Sprintf("workflow:state:%s", workflowID)

	switch action {
	case "delete":
		return f.deleteKeysWithPrefix(prefix, status, "state")
	case "archive":
		return f.archiveKeysWithPrefix(prefix, status, "state")
	default:
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "unsupported action for state cleanup",
			Details: map[string]interface{}{
				"action":      action,
				"workflow_id": workflowID,
				"target":      "state",
			},
		}
	}
}

func (f *GraftFSM) cleanupQueueItems(workflowID string, action string, status *CleanupStatus) error {
	prefix := fmt.Sprintf("queue:%s", workflowID)

	switch action {
	case "delete":
		return f.deleteKeysWithPrefix(prefix, status, "queue")
	case "archive":
		return f.archiveKeysWithPrefix(prefix, status, "queue")
	default:
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "unsupported action for queue cleanup",
			Details: map[string]interface{}{
				"action":      action,
				"workflow_id": workflowID,
				"target":      "queue",
			},
		}
	}
}

func (f *GraftFSM) cleanupWorkflowClaims(workflowID string, action string, status *CleanupStatus) error {
	var keysToDelete []string

	err := f.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte("claim:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())

			value, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}

			var claim domain.ClaimEntity
			if err := json.Unmarshal(value, &claim); err != nil {
				continue
			}

			if strings.Contains(key, workflowID) {
				keysToDelete = append(keysToDelete, key)
			}
		}

		return nil
	})

	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to scan claims during cleanup",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	switch action {
	case "delete":
		for _, key := range keysToDelete {
			if err := f.db.Update(func(txn *badger.Txn) error {
				return txn.Delete([]byte(key))
			}); err != nil {
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to delete claim during cleanup",
					Details: map[string]interface{}{
						"claim_key":   key,
						"workflow_id": workflowID,
						"error":       err.Error(),
					},
				}
			}
		}
		status.Results["claims"] = fmt.Sprintf("deleted %d claims", len(keysToDelete))
	case "archive":
		status.Results["claims"] = fmt.Sprintf("archived %d claims", len(keysToDelete))
	default:
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "unsupported action for claims cleanup",
			Details: map[string]interface{}{
				"action":      action,
				"workflow_id": workflowID,
				"target":      "claims",
			},
		}
	}

	return nil
}

func (f *GraftFSM) cleanupAuditLogs(workflowID string, action string, status *CleanupStatus) error {
	prefix := fmt.Sprintf("audit:%s", workflowID)

	switch action {
	case "delete":
		return f.deleteKeysWithPrefix(prefix, status, "audit")
	case "archive":
		return f.archiveKeysWithPrefix(prefix, status, "audit")
	default:
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "unsupported action for audit cleanup",
			Details: map[string]interface{}{
				"action":      action,
				"workflow_id": workflowID,
				"target":      "audit",
			},
		}
	}
}

func (f *GraftFSM) deleteKeysWithPrefix(prefix string, status *CleanupStatus, target string) error {
	var deletedCount int

	err := f.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefixBytes := []byte(prefix)
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			key := it.Item().Key()
			if err := txn.Delete(key); err != nil {
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to delete key during cleanup",
					Details: map[string]interface{}{
						"key":    string(key),
						"target": target,
						"error":  err.Error(),
					},
				}
			}
			deletedCount++
		}

		return nil
	})

	if err != nil {
		return err
	}

	status.Results[target] = fmt.Sprintf("deleted %d items", deletedCount)
	return nil
}

func (f *GraftFSM) archiveKeysWithPrefix(prefix string, status *CleanupStatus, target string) error {
	var archivedCount int
	archivePrefix := fmt.Sprintf("archive:%d:", time.Now().Unix())

	err := f.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefixBytes := []byte(prefix)
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			item := it.Item()
			key := string(item.Key())

			value, err := item.ValueCopy(nil)
			if err != nil {
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to copy value for archival",
					Details: map[string]interface{}{
						"key":    key,
						"target": target,
						"error":  err.Error(),
					},
				}
			}

			archiveKey := archivePrefix + key
			if err := txn.Set([]byte(archiveKey), value); err != nil {
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to archive key",
					Details: map[string]interface{}{
						"key":         key,
						"archive_key": archivePrefix + key,
						"target":      target,
						"error":       err.Error(),
					},
				}
			}

			if err := txn.Delete(item.Key()); err != nil {
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to delete original key after archival",
					Details: map[string]interface{}{
						"key":    key,
						"target": target,
						"error":  err.Error(),
					},
				}
			}

			archivedCount++
		}

		return nil
	})

	if err != nil {
		return err
	}

	status.Results[target] = fmt.Sprintf("archived %d items", archivedCount)
	return nil
}
