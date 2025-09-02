package storage

import (
	"log/slog"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
)

func TestRunInTransactionGuardWithRaft(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()

	mockRaftNode := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaftNode, db, slog.Default())

	err = storage.RunInTransaction(func(tx ports.Transaction) error {
		return tx.Put("test-key", []byte("test-value"), 0)
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "RunInTransaction is disabled when Raft is active")

	var discoveryErr *domain.DiscoveryError
	assert.ErrorAs(t, err, &discoveryErr)
	assert.Equal(t, "storage", discoveryErr.Adapter)
	assert.Equal(t, "run_in_transaction", discoveryErr.Op)
}

func TestRunInTransactionWorksWithoutRaft(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()

	storage := NewAppStorage(nil, db, slog.Default())

	testKey := "test-key"
	testValue := []byte("test-value")

	err = storage.RunInTransaction(func(tx ports.Transaction) error {
		return tx.Put(testKey, testValue, 0)
	})
	assert.NoError(t, err)

	value, _, exists, err := storage.Get(testKey)
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, testValue, value)
}

func TestRaftConsistencyEnforcement(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()

	storage := NewAppStorage(nil, db, slog.Default())

	err = storage.RunInTransaction(func(tx ports.Transaction) error {
		return tx.Put("before-raft", []byte("works"), 0)
	})
	assert.NoError(t, err)

	mockRaftNode := mocks.NewMockRaftNode(t)
	storage.SetRaftNode(mockRaftNode)

	err = storage.RunInTransaction(func(tx ports.Transaction) error {
		return tx.Put("after-raft", []byte("should-fail"), 0)
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "RunInTransaction is disabled when Raft is active")

	storage.SetRaftNode(nil)

	err = storage.RunInTransaction(func(tx ports.Transaction) error {
		return tx.Put("raft-removed", []byte("works-again"), 0)
	})
	assert.NoError(t, err)
}
