package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestCleanerDB(t *testing.T) (*badger.DB, func()) {
	opts := badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

func TestQueueCleaner_RemoveWorkflowItems(t *testing.T) {
	db, cleanup := setupTestCleanerDB(t)
	defer cleanup()

	cleaner := NewQueueCleaner(db)
	workflowID := "test-workflow-123"

	err := db.Update(func(txn *badger.Txn) error {
		readyKey := fmt.Sprintf("ready:%s:item1", workflowID)
		readyKey2 := fmt.Sprintf("ready:%s:item2", workflowID)
		otherKey := "ready:other-workflow:item1"

		if err := txn.Set([]byte(readyKey), []byte("test-data")); err != nil {
			return err
		}
		if err := txn.Set([]byte(readyKey2), []byte("test-data")); err != nil {
			return err
		}
		return txn.Set([]byte(otherKey), []byte("test-data"))
	})
	require.NoError(t, err)

	err = cleaner.RemoveWorkflowItems(context.Background(), workflowID)
	assert.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		readyKey := fmt.Sprintf("ready:%s:item1", workflowID)
		readyKey2 := fmt.Sprintf("ready:%s:item2", workflowID)
		otherKey := "ready:other-workflow:item1"

		_, err1 := txn.Get([]byte(readyKey))
		_, err2 := txn.Get([]byte(readyKey2))
		_, err3 := txn.Get([]byte(otherKey))

		assert.Equal(t, badger.ErrKeyNotFound, err1)
		assert.Equal(t, badger.ErrKeyNotFound, err2)
		assert.NoError(t, err3)

		return nil
	})
	require.NoError(t, err)
}

func TestQueueCleaner_RemovePendingItems(t *testing.T) {
	db, cleanup := setupTestCleanerDB(t)
	defer cleanup()

	cleaner := NewQueueCleaner(db)
	workflowID := "test-workflow-456"

	err := db.Update(func(txn *badger.Txn) error {
		pendingKey := fmt.Sprintf("pending:%s:item1", workflowID)
		pendingKey2 := fmt.Sprintf("pending:%s:item2", workflowID)
		otherKey := "pending:other-workflow:item1"

		if err := txn.Set([]byte(pendingKey), []byte("test-data")); err != nil {
			return err
		}
		if err := txn.Set([]byte(pendingKey2), []byte("test-data")); err != nil {
			return err
		}
		return txn.Set([]byte(otherKey), []byte("test-data"))
	})
	require.NoError(t, err)

	err = cleaner.RemovePendingItems(context.Background(), workflowID)
	assert.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		pendingKey := fmt.Sprintf("pending:%s:item1", workflowID)
		pendingKey2 := fmt.Sprintf("pending:%s:item2", workflowID)
		otherKey := "pending:other-workflow:item1"

		_, err1 := txn.Get([]byte(pendingKey))
		_, err2 := txn.Get([]byte(pendingKey2))
		_, err3 := txn.Get([]byte(otherKey))

		assert.Equal(t, badger.ErrKeyNotFound, err1)
		assert.Equal(t, badger.ErrKeyNotFound, err2)
		assert.NoError(t, err3)

		return nil
	})
	require.NoError(t, err)
}

func TestQueueCleaner_ReleaseWorkflowClaims(t *testing.T) {
	db, cleanup := setupTestCleanerDB(t)
	defer cleanup()

	cleaner := NewQueueCleaner(db)
	workflowID := "test-workflow-789"

	err := db.Update(func(txn *badger.Txn) error {
		claimKey := fmt.Sprintf("claim:%s:node1", workflowID)
		claimKey2 := fmt.Sprintf("claim:%s:node2", workflowID)
		otherClaimKey := "claim:other-workflow:node1"

		if err := txn.Set([]byte(claimKey), []byte("claim-data")); err != nil {
			return err
		}
		if err := txn.Set([]byte(claimKey2), []byte("claim-data")); err != nil {
			return err
		}
		return txn.Set([]byte(otherClaimKey), []byte("claim-data"))
	})
	require.NoError(t, err)

	err = cleaner.ReleaseWorkflowClaims(context.Background(), workflowID)
	assert.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		claimKey := fmt.Sprintf("claim:%s:node1", workflowID)
		claimKey2 := fmt.Sprintf("claim:%s:node2", workflowID)
		otherClaimKey := "claim:other-workflow:node1"

		_, err1 := txn.Get([]byte(claimKey))
		_, err2 := txn.Get([]byte(claimKey2))
		_, err3 := txn.Get([]byte(otherClaimKey))

		assert.Equal(t, badger.ErrKeyNotFound, err1)
		assert.Equal(t, badger.ErrKeyNotFound, err2)
		assert.NoError(t, err3)

		return nil
	})
	require.NoError(t, err)
}

func TestQueueCleaner_GetWorkflowItemCount(t *testing.T) {
	db, cleanup := setupTestCleanerDB(t)
	defer cleanup()

	cleaner := NewQueueCleaner(db)
	workflowID := "test-workflow-count"

	err := db.Update(func(txn *badger.Txn) error {
		readyKey1 := fmt.Sprintf("ready:%s:item1", workflowID)
		readyKey2 := fmt.Sprintf("ready:%s:item2", workflowID)
		pendingKey1 := fmt.Sprintf("pending:%s:item1", workflowID)
		otherKey := "ready:other-workflow:item1"

		if err := txn.Set([]byte(readyKey1), []byte("data")); err != nil {
			return err
		}
		if err := txn.Set([]byte(readyKey2), []byte("data")); err != nil {
			return err
		}
		if err := txn.Set([]byte(pendingKey1), []byte("data")); err != nil {
			return err
		}
		return txn.Set([]byte(otherKey), []byte("data"))
	})
	require.NoError(t, err)

	count := cleaner.GetWorkflowItemCount(context.Background(), workflowID)
	assert.Equal(t, 3, count)
}

func TestQueueCleaner_RemoveAllWorkflowData(t *testing.T) {
	db, cleanup := setupTestCleanerDB(t)
	defer cleanup()

	cleaner := NewQueueCleaner(db)
	workflowID := "test-workflow-all"

	err := db.Update(func(txn *badger.Txn) error {
		readyKey := fmt.Sprintf("ready:%s:item1", workflowID)
		pendingKey := fmt.Sprintf("pending:%s:item1", workflowID)
		claimKey := fmt.Sprintf("claim:%s:node1", workflowID)

		if err := txn.Set([]byte(readyKey), []byte("data")); err != nil {
			return err
		}
		if err := txn.Set([]byte(pendingKey), []byte("data")); err != nil {
			return err
		}
		return txn.Set([]byte(claimKey), []byte("data"))
	})
	require.NoError(t, err)

	err = cleaner.RemoveAllWorkflowData(context.Background(), workflowID)
	assert.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		readyKey := fmt.Sprintf("ready:%s:item1", workflowID)
		pendingKey := fmt.Sprintf("pending:%s:item1", workflowID)
		claimKey := fmt.Sprintf("claim:%s:node1", workflowID)

		_, err1 := txn.Get([]byte(readyKey))
		_, err2 := txn.Get([]byte(pendingKey))
		_, err3 := txn.Get([]byte(claimKey))

		assert.Equal(t, badger.ErrKeyNotFound, err1)
		assert.Equal(t, badger.ErrKeyNotFound, err2)
		assert.Equal(t, badger.ErrKeyNotFound, err3)

		return nil
	})
	require.NoError(t, err)
}

func TestQueueCleaner_BatchRemoveWorkflows(t *testing.T) {
	db, cleanup := setupTestCleanerDB(t)
	defer cleanup()

	cleaner := NewQueueCleaner(db)
	workflowIDs := []string{"workflow-1", "workflow-2", "workflow-3"}

	for _, workflowID := range workflowIDs {
		err := db.Update(func(txn *badger.Txn) error {
			readyKey := fmt.Sprintf("ready:%s:item", workflowID)
			pendingKey := fmt.Sprintf("pending:%s:item", workflowID)

			if err := txn.Set([]byte(readyKey), []byte("data")); err != nil {
				return err
			}
			return txn.Set([]byte(pendingKey), []byte("data"))
		})
		require.NoError(t, err)
	}

	err := cleaner.BatchRemoveWorkflows(context.Background(), workflowIDs)
	assert.NoError(t, err)

	for _, workflowID := range workflowIDs {
		count := cleaner.GetWorkflowItemCount(context.Background(), workflowID)
		assert.Equal(t, 0, count)
	}
}

func TestQueueCleaner_GetOrphanedItems(t *testing.T) {
	db, cleanup := setupTestCleanerDB(t)
	defer cleanup()

	cleaner := NewQueueCleaner(db)

	workflowWithState := "workflow-with-state"
	orphanedWorkflow := "orphaned-workflow"

	err := db.Update(func(txn *badger.Txn) error {
		stateKey := fmt.Sprintf("workflow:state:%s", workflowWithState)
		readyKey1 := fmt.Sprintf("ready:%s:item1", workflowWithState)
		readyKey2 := fmt.Sprintf("ready:%s:item1", orphanedWorkflow)

		queueItem1 := ports.QueueItem{
			ID:         "item1",
			WorkflowID: workflowWithState,
			NodeName:   "test-node",
			EnqueuedAt: time.Now(),
		}

		queueItem2 := ports.QueueItem{
			ID:         "item2",
			WorkflowID: orphanedWorkflow,
			NodeName:   "test-node",
			EnqueuedAt: time.Now(),
		}

		serializedItem1, _ := marshalQueueItem(&queueItem1)
		serializedItem2, _ := marshalQueueItem(&queueItem2)

		if err := txn.Set([]byte(stateKey), []byte("state-data")); err != nil {
			return err
		}
		if err := txn.Set([]byte(readyKey1), serializedItem1); err != nil {
			return err
		}
		return txn.Set([]byte(readyKey2), serializedItem2)
	})
	require.NoError(t, err)

	orphanedItems, err := cleaner.GetOrphanedItems(context.Background())
	assert.NoError(t, err)
	assert.Len(t, orphanedItems, 1)
	assert.Equal(t, orphanedWorkflow, orphanedItems[0].WorkflowID)
}

func marshalQueueItem(item *ports.QueueItem) ([]byte, error) {
	return serializeItem(*item)
}
