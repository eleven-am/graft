package queue

import (
	"context"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type QueueCleaner struct {
	db *badger.DB
}

func NewQueueCleaner(db *badger.DB) *QueueCleaner {
	return &QueueCleaner{
		db: db,
	}
}

func (qc *QueueCleaner) RemoveWorkflowItems(ctx context.Context, workflowID string) error {
	readyPrefix := fmt.Sprintf("ready:%s:", workflowID)
	if err := qc.removeItemsWithPrefix(readyPrefix); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to remove ready items",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return nil
}

func (qc *QueueCleaner) RemovePendingItems(ctx context.Context, workflowID string) error {
	pendingPrefix := fmt.Sprintf("pending:%s:", workflowID)
	if err := qc.removeItemsWithPrefix(pendingPrefix); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to remove pending items",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return nil
}

func (qc *QueueCleaner) ReleaseWorkflowClaims(ctx context.Context, workflowID string) error {
	var keysToDelete []string

	err := qc.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte("claim:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())

			if strings.Contains(key, workflowID) {
				keysToDelete = append(keysToDelete, key)
			}
		}

		return nil
	})

	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to scan claims",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return qc.db.Update(func(txn *badger.Txn) error {
		for _, key := range keysToDelete {
			if err := txn.Delete([]byte(key)); err != nil {
				return domain.Error{
					Type:    domain.ErrorTypeInternal,
					Message: "failed to delete claim",
					Details: map[string]interface{}{
						"claim_key": key,
						"error":     err.Error(),
					},
				}
			}
		}
		return nil
	})
}

func (qc *QueueCleaner) GetWorkflowItemCount(ctx context.Context, workflowID string) int {
	var count int

	readyPrefix := fmt.Sprintf("ready:%s:", workflowID)
	pendingPrefix := fmt.Sprintf("pending:%s:", workflowID)

	qc.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		readyPrefixBytes := []byte(readyPrefix)
		for it.Seek(readyPrefixBytes); it.ValidForPrefix(readyPrefixBytes); it.Next() {
			count++
		}

		pendingPrefixBytes := []byte(pendingPrefix)
		for it.Seek(pendingPrefixBytes); it.ValidForPrefix(pendingPrefixBytes); it.Next() {
			count++
		}

		return nil
	})

	return count
}

func (qc *QueueCleaner) RemoveAllWorkflowData(ctx context.Context, workflowID string) error {
	if err := qc.RemoveWorkflowItems(ctx, workflowID); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to remove workflow items",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	if err := qc.RemovePendingItems(ctx, workflowID); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to remove pending items",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if err := qc.ReleaseWorkflowClaims(ctx, workflowID); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to release workflow claims",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	return nil
}

func (qc *QueueCleaner) BatchRemoveWorkflows(ctx context.Context, workflowIDs []string) error {
	for _, workflowID := range workflowIDs {
		if err := qc.RemoveAllWorkflowData(ctx, workflowID); err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to cleanup workflow",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
					"error":       err.Error(),
				},
			}
		}
	}
	return nil
}

func (qc *QueueCleaner) GetOrphanedItems(ctx context.Context) ([]ports.QueueItem, error) {
	var orphanedItems []ports.QueueItem

	err := qc.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		readyPrefix := []byte("ready:")
		for it.Seek(readyPrefix); it.ValidForPrefix(readyPrefix); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}

			var queueItem ports.QueueItem
			if err := deserializeQueueItem(value, &queueItem); err != nil {
				continue
			}

			if qc.isOrphaned(&queueItem) {
				orphanedItems = append(orphanedItems, queueItem)
			}
		}

		pendingPrefix := []byte("pending:")
		for it.Seek(pendingPrefix); it.ValidForPrefix(pendingPrefix); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}

			var queueItem ports.QueueItem
			if err := deserializeQueueItem(value, &queueItem); err != nil {
				continue
			}

			if qc.isOrphaned(&queueItem) {
				orphanedItems = append(orphanedItems, queueItem)
			}
		}

		return nil
	})

	return orphanedItems, err
}

func (qc *QueueCleaner) removeItemsWithPrefix(prefix string) error {
	var keysToDelete [][]byte

	err := qc.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefixBytes := []byte(prefix)
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysToDelete = append(keysToDelete, key)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return qc.db.Update(func(txn *badger.Txn) error {
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (qc *QueueCleaner) isOrphaned(item *ports.QueueItem) bool {
	stateKey := fmt.Sprintf("workflow:state:%s", item.WorkflowID)

	err := qc.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(stateKey))
		return err
	})

	return err == badger.ErrKeyNotFound
}

func deserializeQueueItem(data []byte, item *ports.QueueItem) error {
	deserializedItem, err := deserializeItem(data)
	if err != nil {
		return err
	}
	*item = *deserializedItem
	return nil
}
