package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/hashicorp/raft"
)

type GraftFSM struct {
	db     *badger.DB
	logger *slog.Logger
}

func NewGraftFSM(db *badger.DB, logger *slog.Logger) *GraftFSM {
	if logger == nil {
		logger = slog.Default()
	}

	return &GraftFSM{
		db:     db,
		logger: logger.With("component", "fsm"),
	}
}

func (f *GraftFSM) Apply(logEntry *raft.Log) interface{} {
	f.logger.Debug("applying raft log entry", "index", logEntry.Index, "term", logEntry.Term)

	var cmd domain.Command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		f.logger.Error("failed to unmarshal command", "error", err, "data", string(logEntry.Data))
		return &domain.CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal command: %v", err),
		}
	}

	f.logger.Debug("applying command", "type", cmd.Type.String(), "key", cmd.Key)

	if cmd.IdempotencyKey != nil {
		if f.checkIdempotency(*cmd.IdempotencyKey) {
			f.logger.Debug("command already processed", "idempotency_key", *cmd.IdempotencyKey)
			return &domain.CommandResult{
				Success: true,
				Error:   "command already processed (idempotent)",
			}
		}
	}

	var result *domain.CommandResult
	switch cmd.Type {
	case domain.CommandPut:
		result = f.applyPut(cmd.Key, cmd.Value)
	case domain.CommandDelete:
		result = f.applyDelete(cmd.Key)
	case domain.CommandBatch:
		result = f.applyBatch(cmd.Batch)
	case domain.CommandAtomicBatch:
		result = f.applyAtomicBatch(cmd.Batch)
	case domain.CommandStateUpdate:
		result = f.applyStateUpdate(cmd.Key, cmd.Value)
	case domain.CommandQueueOperation:
		result = f.applyQueueOperation(cmd.Key, cmd.Value)
	case domain.CommandCleanupWorkflow:
		result = f.applyCleanupWorkflow(cmd.Key)
	case domain.CommandCompleteWorkflowPurge:
		result = f.applyCompleteWorkflowPurge(cmd.Key)
	case domain.CommandResourceLock:
		result = f.applyResourceLock(cmd.Key, cmd.Value)
	case domain.CommandResourceUnlock:
		result = f.applyResourceUnlock(cmd.Key)
	default:
		f.logger.Error("unknown command type", "type", cmd.Type)
		result = &domain.CommandResult{
			Success: false,
			Error:   fmt.Sprintf("unknown command type: %v", cmd.Type),
		}
	}

	if result.Success && cmd.IdempotencyKey != nil {
		if err := f.recordIdempotency(*cmd.IdempotencyKey); err != nil {
			f.logger.Error("failed to record idempotency", "error", err)
		}
	}

	f.logger.Debug("command applied", "success", result.Success, "error", result.Error)
	return result
}

func (f *GraftFSM) applyPut(key string, value []byte) *domain.CommandResult {
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) applyDelete(key string) *domain.CommandResult {
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) applyBatch(batch []domain.BatchOp) *domain.CommandResult {
	err := f.db.Update(func(txn *badger.Txn) error {
		for _, op := range batch {
			switch op.Type {
			case domain.CommandPut:
				if err := txn.Set([]byte(op.Key), op.Value); err != nil {
					return err
				}
			case domain.CommandDelete:
				if err := txn.Delete([]byte(op.Key)); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported batch operation: %v", op.Type)
			}
		}
		return nil
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) applyStateUpdate(key string, value []byte) *domain.CommandResult {
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("state:"+key), value)
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) applyQueueOperation(key string, value []byte) *domain.CommandResult {
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("queue:"+key), value)
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) applyCleanupWorkflow(key string) *domain.CommandResult {
	err := f.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte("workflow:" + key)
		var keysToDelete [][]byte

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			keysToDelete = append(keysToDelete, item.KeyCopy(nil))
		}

		for _, k := range keysToDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) applyResourceLock(key string, value []byte) *domain.CommandResult {
	err := f.db.Update(func(txn *badger.Txn) error {
		lockKey := []byte("lock:" + key)

		if _, err := txn.Get(lockKey); err == nil {
			return fmt.Errorf("resource already locked: %s", key)
		} else if err != badger.ErrKeyNotFound {
			return err
		}

		return txn.Set(lockKey, value)
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) applyResourceUnlock(key string) *domain.CommandResult {
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte("lock:" + key))
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Info("creating FSM snapshot")
	return &GraftSnapshot{
		db:     f.db,
		logger: f.logger,
	}, nil
}

func (f *GraftFSM) Restore(reader io.ReadCloser) error {
	f.logger.Info("restoring FSM from snapshot")
	defer reader.Close()

	return f.db.Load(reader, 1000)
}

type GraftSnapshot struct {
	db     *badger.DB
	logger *slog.Logger
}

func (s *GraftSnapshot) Persist(sink raft.SnapshotSink) error {
	s.logger.Info("persisting snapshot")
	defer sink.Close()

	_, err := s.db.Backup(sink, 0)
	if err != nil {
		s.logger.Error("failed to backup database", "error", err)
		sink.Cancel()
		return err
	}

	s.logger.Info("snapshot persisted successfully")
	return nil
}

func (s *GraftSnapshot) Release() {
	s.logger.Debug("releasing snapshot")
}

func (f *GraftFSM) applyAtomicBatch(batch []domain.BatchOp) *domain.CommandResult {
	f.logger.Debug("applying atomic batch", "operations", len(batch))

	err := f.db.Update(func(txn *badger.Txn) error {
		for i, op := range batch {
			f.logger.Debug("processing atomic batch operation", "index", i, "type", op.Type, "key", op.Key)

			switch op.Type {
			case domain.CommandPut:
				if err := txn.Set([]byte(op.Key), op.Value); err != nil {
					f.logger.Error("atomic batch put failed", "index", i, "key", op.Key, "error", err)
					return fmt.Errorf("atomic batch operation %d (PUT %s) failed: %v", i, op.Key, err)
				}
			case domain.CommandDelete:
				if err := txn.Delete([]byte(op.Key)); err != nil {
					f.logger.Error("atomic batch delete failed", "index", i, "key", op.Key, "error", err)
					return fmt.Errorf("atomic batch operation %d (DELETE %s) failed: %v", i, op.Key, err)
				}
			default:
				f.logger.Error("unsupported atomic batch operation", "type", op.Type, "index", i)
				return fmt.Errorf("atomic batch operation %d: unsupported type %v", i, op.Type)
			}
		}

		f.logger.Debug("atomic batch completed successfully", "operations", len(batch))
		return nil
	})

	if err != nil {
		f.logger.Error("atomic batch transaction failed", "error", err, "operations", len(batch))
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) applyCompleteWorkflowPurge(workflowID string) *domain.CommandResult {
	f.logger.Info("applying complete workflow purge", "workflow_id", workflowID)

	err := f.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefixes := []string{
			"workflow:" + workflowID,
			"state:" + workflowID,
			"queue:" + workflowID,
			"lock:" + workflowID,
			"node:" + workflowID,
			"claim:" + workflowID,
		}

		var totalDeleted int
		for _, prefix := range prefixes {
			prefixBytes := []byte(prefix)
			var keysToDelete [][]byte

			for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
				item := it.Item()
				keysToDelete = append(keysToDelete, item.KeyCopy(nil))
			}

			for _, key := range keysToDelete {
				if err := txn.Delete(key); err != nil {
					f.logger.Error("failed to delete key during purge", "key", string(key), "error", err)
					return fmt.Errorf("failed to delete key %s: %v", string(key), err)
				}
				totalDeleted++
			}

			f.logger.Debug("purged prefix", "prefix", prefix, "keys_deleted", len(keysToDelete))
		}

		f.logger.Info("complete workflow purge successful", "workflow_id", workflowID, "total_deleted", totalDeleted)
		return nil
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success: true,
	}
}

func (f *GraftFSM) checkIdempotency(key string) bool {
	idempotencyKey := []byte("idempotency:" + key)

	err := f.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(idempotencyKey)
		return err
	})

	exists := err == nil
	if exists {
		f.logger.Debug("idempotency key found", "key", key)
	}

	return exists
}

func (f *GraftFSM) recordIdempotency(key string) error {
	idempotencyKey := []byte("idempotency:" + key)

	return f.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(idempotencyKey, []byte("processed"))
		entry = entry.WithTTL(24 * 60 * 60 * 1000000000)
		return txn.SetEntry(entry)
	})
}
