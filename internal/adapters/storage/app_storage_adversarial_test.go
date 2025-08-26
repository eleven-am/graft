package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
)

func TestAppStorage_ConcurrentReadWriteRaceCondition(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	key := "race-test-key"
	initialValue := []byte("initial")
	finalValue := []byte("final")

	mockRaft.On("Apply", mock.AnythingOfType("domain.Command"), mock.AnythingOfType("time.Duration")).
		Return(&domain.CommandResult{
			Success: true,
			Events:  []domain.Event{},
		}, nil)

	storage.Put(key, initialValue, 1)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func(version int64) {
			defer wg.Done()
			if err := storage.Put(key, finalValue, version+10); err != nil {
				errors <- err
			}
		}(int64(i))
	}

	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			defer wg.Done()
			_, _, _, err := storage.Get(key)
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Race condition error: %v", err)
	}
}

func TestAppStorage_NilRaftNodeHandling(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	storage := NewAppStorage(nil, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	err := storage.Put("test-key", []byte("test-value"), 1)
	assert.Equal(t, domain.ErrNotStarted, err, "Should return ErrNotStarted when raftNode is nil")

	err = storage.Delete("test-key")
	assert.Equal(t, domain.ErrNotStarted, err, "Delete should return ErrNotStarted when raftNode is nil")

	ops := []ports.WriteOp{{Type: ports.OpPut, Key: "batch-key", Value: []byte("batch-value")}}
	err = storage.BatchWrite(ops)
	assert.Equal(t, domain.ErrNotStarted, err, "BatchWrite should return ErrNotStarted when raftNode is nil")
}

func TestAppStorage_PubSubEventDeliveryUnderStress(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	eventCount := 1000
	subscriberCount := 10
	prefix := "stress-test"

	mockRaft.On("Apply", mock.AnythingOfType("domain.Command"), mock.AnythingOfType("time.Duration")).
		Return(&domain.CommandResult{
			Success: true,
			Events: []domain.Event{{
				Type:      domain.EventPut,
				Key:       "stress-test-key",
				Version:   1,
				NodeID:    "test-node",
				Timestamp: time.Now(),
			}},
		}, nil)

	var subscribers []<-chan ports.StorageEvent
	var unsubscribers []func()
	eventCounts := make([]int, subscriberCount)
	var mu sync.Mutex

	for i := 0; i < subscriberCount; i++ {
		ch, unsub, err := storage.Subscribe(prefix)
		require.NoError(t, err)
		subscribers = append(subscribers, ch)
		unsubscribers = append(unsubscribers, unsub)

		go func(idx int) {
			for range ch {
				mu.Lock()
				eventCounts[idx]++
				mu.Unlock()
			}
		}(i)
	}

	var wg sync.WaitGroup
	wg.Add(eventCount)
	for i := 0; i < eventCount; i++ {
		go func(idx int) {
			defer wg.Done()
			key := "stress-test-key"
			storage.Put(key, []byte("value"), int64(idx))
		}(i)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	for _, unsub := range unsubscribers {
		unsub()
	}

	mu.Lock()
	for i, count := range eventCounts {
		t.Logf("Subscriber %d received %d events", i, count)
		if count == 0 {
			t.Errorf("Subscriber %d received no events - possible event delivery issue", i)
		}
	}
	mu.Unlock()
}

func TestAppStorage_TransactionIsolationViolation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	storage := NewAppStorage(nil, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	key := "tx-isolation-key"
	value1 := []byte("value1")
	value2 := []byte("value2")

	storage.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value1)
	})

	var wg sync.WaitGroup
	results := make([][]byte, 2)

	wg.Add(2)

	go func() {
		defer wg.Done()
		storage.RunInTransaction(func(tx ports.Transaction) error {
			tx.Put(key, value2, 1)
			time.Sleep(50 * time.Millisecond)
			val, _, _, _ := tx.Get(key)
			results[0] = val
			return nil
		})
	}()

	go func() {
		defer wg.Done()
		time.Sleep(25 * time.Millisecond)
		storage.RunInTransaction(func(tx ports.Transaction) error {
			val, _, _, _ := tx.Get(key)
			results[1] = val
			return nil
		})
	}()

	wg.Wait()

	if string(results[0]) != string(value2) {
		t.Errorf("Transaction isolation may be broken: expected %s, got %s", value2, results[0])
	}
}

func TestAppStorage_SubscriptionMemoryLeak(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	storage := NewAppStorage(nil, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	initialSubCount := len(storage.subs)

	for i := 0; i < 100; i++ {
		ch, unsub, err := storage.Subscribe("leak-test")
		require.NoError(t, err)

		go func() {
			for range ch {
			}
		}()

		unsub()
		_ = i
	}

	time.Sleep(10 * time.Millisecond)

	finalSubCount := len(storage.subs["leak-test"])

	if finalSubCount > initialSubCount {
		t.Errorf("Possible memory leak: subscription count increased from %d to %d after unsubscribe",
			initialSubCount, finalSubCount)
	}
}

func TestAppStorage_CloseRaceCondition(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	storage := NewAppStorage(nil, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	ch, _, err := storage.Subscribe("close-race")
	require.NoError(t, err)

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func(idx int) {
			defer wg.Done()
			time.Sleep(time.Duration(idx*2) * time.Millisecond)
			_, _, err := storage.Subscribe("close-race-new")
			if err != nil {
				errors <- err
			}
		}(i)
	}

	go func() {
		time.Sleep(1 * time.Millisecond)
		storage.Close()
	}()

	go func() {
		for event := range ch {
			_ = event
		}
	}()

	wg.Wait()
	close(errors)

	closedErrors := 0
	for err := range errors {
		if storageErr, ok := err.(*domain.StorageError); ok && storageErr.Type == domain.ErrClosed {
			closedErrors++
		}
	}

	if closedErrors == 0 {
		t.Error("Expected some operations to fail with ErrClosed during concurrent close")
	}
}

func TestAppStorage_TTLConsistencyIssue(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	key := "ttl-consistency-key"
	value := []byte("ttl-value")
	ttl := 100 * time.Millisecond

	mockRaft.On("Apply", mock.AnythingOfType("domain.Command"), mock.AnythingOfType("time.Duration")).
		Return(&domain.CommandResult{
			Success: true,
			Events:  []domain.Event{},
		}, nil)

	err := storage.PutWithTTL(key, value, 1, ttl)
	require.NoError(t, err)

	time.Sleep(ttl + 50*time.Millisecond)

	cleanupCount, cleanupErr := storage.CleanExpired()
	require.NoError(t, cleanupErr)

	_, _, exists, getErr := storage.Get(key)
	require.NoError(t, getErr)

	if exists && cleanupCount > 0 {
		t.Error("BUG: Key exists after TTL expiration AND cleanup reported cleaned items - inconsistent state")
	}

	if !exists && cleanupCount == 0 {
		t.Error("BUG: Key doesn't exist but cleanup reported no items cleaned - possible race condition")
	}
}

func TestAppStorage_VersionConsistencyUnderConcurrency(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	mockRaft.On("Apply", mock.AnythingOfType("domain.Command"), mock.AnythingOfType("time.Duration")).
		Return(&domain.CommandResult{
			Success: true,
			Events:  []domain.Event{},
		}, nil)

	key := "version-consistency-key"
	value := []byte("version-value")

	storage.Put(key, value, 1)

	var wg sync.WaitGroup
	versionResults := make([]int64, 50)

	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func(idx int) {
			defer wg.Done()
			newVersion, err := storage.IncrementVersion(key)
			if err == nil {
				versionResults[idx] = newVersion
			}
		}(i)
	}

	wg.Wait()

	versionMap := make(map[int64]int)
	for _, version := range versionResults {
		if version > 0 {
			versionMap[version]++
		}
	}

	duplicates := 0
	for version, count := range versionMap {
		if count > 1 {
			t.Errorf("BUG: Version %d was assigned %d times (should be unique)", version, count)
			duplicates++
		}
	}

	if duplicates > 0 {
		t.Errorf("Found %d version consistency violations", duplicates)
	}
}

func TestAppStorage_RaftApplyFailureHandling(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	key := "raft-failure-key"
	value := []byte("raft-failure-value")

	mockRaft.On("Apply", mock.AnythingOfType("domain.Command"), mock.AnythingOfType("time.Duration")).
		Return(nil, assert.AnError).Once()

	err := storage.Put(key, value, 1)
	assert.Error(t, err)

	_, _, exists, _ := storage.Get(key)
	assert.False(t, exists, "Key should not exist after failed Raft apply")

	mockRaft.On("Apply", mock.AnythingOfType("domain.Command"), mock.AnythingOfType("time.Duration")).
		Return(&domain.CommandResult{
			Success: false,
			Error:   "raft consensus failed",
		}, nil).Once()

	err = storage.Put(key, value, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "raft consensus failed")

	_, _, exists, _ = storage.Get(key)
	assert.False(t, exists, "Key should not exist after failed consensus")
}

func setupTestDB(t *testing.T) (*badger.DB, func()) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}
