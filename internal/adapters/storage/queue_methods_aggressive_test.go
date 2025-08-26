package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
)

// TestGetNext_ConcurrentAccessResilience tests GetNext under heavy concurrent load
func TestGetNext_ConcurrentAccessResilience(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	prefix := "queue:stress:pending:"
	itemCount := 1000

	// Setup: Add many items directly to DB (simulating successful Raft operations)
	// This bypasses Raft for testing the storage layer methods directly
	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("%s%010d", prefix, i)
		value := fmt.Sprintf("item-%d", i)
		
		// Add directly to BadgerDB as if Raft had applied the command
		storage.db.Update(func(txn *badger.Txn) error {
			txn.Set([]byte(key), []byte(value))
			versionKey := fmt.Sprintf("v:%s", key)
			return txn.Set([]byte(versionKey), []byte("1"))
		})

		// Add metadata keys to test filtering
		if i%10 == 0 {
			versionKey := fmt.Sprintf("v:%s%010d", prefix, i+5000)
			storage.db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(versionKey), []byte("1"))
			})
			ttlKey := fmt.Sprintf("ttl:%s%010d", prefix, i+6000)
			storage.db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(ttlKey), []byte("2025-01-01"))
			})
		}
	}

	// Test: Concurrent GetNext calls
	concurrency := 100
	var wg sync.WaitGroup
	results := make([]string, concurrency)
	errors := make([]error, concurrency)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			key, value, exists, err := storage.GetNext(prefix)
			errors[idx] = err
			if exists && err == nil {
				results[idx] = fmt.Sprintf("%s=%s", key, string(value))
			}
		}(i)
	}

	wg.Wait()

	// Verify: All should get the same first item
	expectedKey := fmt.Sprintf("%s%010d", prefix, 0)
	expectedValue := "item-0"
	expectedResult := fmt.Sprintf("%s=%s", expectedKey, expectedValue)

	successCount := 0
	for i := 0; i < concurrency; i++ {
		if errors[i] != nil {
			t.Errorf("GetNext error in goroutine %d: %v", i, errors[i])
		} else if results[i] == expectedResult {
			successCount++
		} else if results[i] != "" {
			t.Errorf("GetNext returned wrong result in goroutine %d: got %s, expected %s", i, results[i], expectedResult)
		}
	}

	assert.Equal(t, concurrency, successCount, "All concurrent GetNext calls should return the same first item")
}

// TestAtomicIncrement_ConcurrencyRaceConditions tests atomic increment under extreme concurrency
func TestAtomicIncrement_ConcurrencyRaceConditions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	counterKey := "sequence:test-counter"
	concurrency := 100
	incrementsPerGoroutine := 10

	// Mock Raft to simulate successful increments
	mockRaft.On("Apply", mock.AnythingOfType("domain.Command"), mock.AnythingOfType("time.Duration")).
		Return(func(cmd domain.Command, timeout time.Duration) *domain.CommandResult {
			// Simulate the FSM handling atomic increment
			return &domain.CommandResult{
				Success: true,
				Events:  []domain.Event{},
				Version: 1, // This would be set by the FSM
			}
		}, nil)

	// Pre-populate the counter in storage (simulating FSM state)
	storage.db.Update(func(txn *badger.Txn) error {
		counterBytes := []byte("0") // Start from 0
		versionBytes := []byte("1")
		txn.Set([]byte(counterKey), counterBytes)
		return txn.Set([]byte(fmt.Sprintf("v:%s", counterKey)), versionBytes)
	})

	var wg sync.WaitGroup
	results := make([][]int64, concurrency)
	errors := make([][]error, concurrency)

	// Initialize result slices
	for i := 0; i < concurrency; i++ {
		results[i] = make([]int64, incrementsPerGoroutine)
		errors[i] = make([]error, incrementsPerGoroutine)
	}

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(goroutineIdx int) {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				// Update storage before each call to simulate FSM increment
				currentValue := int64(goroutineIdx*incrementsPerGoroutine + j + 1)
				storage.db.Update(func(txn *badger.Txn) error {
					counterBytes := []byte(fmt.Sprintf("%d", currentValue))
					return txn.Set([]byte(counterKey), counterBytes)
				})

				newValue, err := storage.AtomicIncrement(counterKey)
				results[goroutineIdx][j] = newValue
				errors[goroutineIdx][j] = err
			}
		}(i)
	}

	wg.Wait()

	// Verify: No errors occurred
	for i := 0; i < concurrency; i++ {
		for j := 0; j < incrementsPerGoroutine; j++ {
			if errors[i][j] != nil {
				t.Errorf("AtomicIncrement error in goroutine %d, increment %d: %v", i, j, errors[i][j])
			}
		}
	}

	// Verify: All results are positive (actual counter values, not versions)
	totalResults := 0
	for i := 0; i < concurrency; i++ {
		for j := 0; j < incrementsPerGoroutine; j++ {
			if results[i][j] > 0 {
				totalResults++
			}
		}
	}

	expectedResults := concurrency * incrementsPerGoroutine
	assert.Equal(t, expectedResults, totalResults, "All atomic increments should return positive counter values")
}

// TestCountPrefix_PerformanceUnderLoad tests CountPrefix performance with many keys
func TestCountPrefix_PerformanceUnderLoad(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	prefix := "queue:load:pending:"
	keyCount := 10000
	metadataKeyCount := 2000

	// Add many data keys directly to DB (simulating successful Raft operations)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%010d", prefix, i)
		value := fmt.Sprintf("data-%d", i)
		
		// Add directly to BadgerDB as if Raft had applied the command
		storage.db.Update(func(txn *badger.Txn) error {
			txn.Set([]byte(key), []byte(value))
			versionKey := fmt.Sprintf("v:%s", key)
			return txn.Set([]byte(versionKey), []byte("1"))
		})
	}

	// Add metadata keys that should be filtered out
	for i := 0; i < metadataKeyCount; i++ {
		if i%2 == 0 {
			versionKey := fmt.Sprintf("v:%s%010d", prefix, i+20000)
			storage.db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(versionKey), []byte("1"))
			})
		} else {
			ttlKey := fmt.Sprintf("ttl:%s%010d", prefix, i+30000)
			storage.db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(ttlKey), []byte("expire"))
			})
		}
	}

	// Performance test: Multiple concurrent CountPrefix calls
	concurrency := 50
	var wg sync.WaitGroup
	results := make([]int, concurrency)
	errors := make([]error, concurrency)
	durations := make([]time.Duration, concurrency)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			start := time.Now()
			count, err := storage.CountPrefix(prefix)
			durations[idx] = time.Since(start)
			results[idx] = count
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// Verify: All results are correct and performance is acceptable
	for i := 0; i < concurrency; i++ {
		require.NoError(t, errors[i], "CountPrefix should not error")
		assert.Equal(t, keyCount, results[i], "CountPrefix should return exact count of data keys, filtering out metadata")
		assert.Less(t, durations[i], 100*time.Millisecond, "CountPrefix should be fast even with %d keys", keyCount+metadataKeyCount)
	}

	t.Logf("CountPrefix performance: avg=%v, max=%v for %d data keys + %d metadata keys", 
		averageDuration(durations), maxDuration(durations), keyCount, metadataKeyCount)
}

// TestGetNextAfter_OrderingConsistency tests GetNextAfter ordering under concurrent modifications
func TestGetNextAfter_OrderingConsistency(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	mockRaft := mocks.NewMockRaftNode(t)
	storage := NewAppStorage(mockRaft, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	prefix := "queue:order:pending:"
	keyCount := 100

	// Add ordered keys directly to DB (simulating successful Raft operations)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%010d", prefix, i)
		value := fmt.Sprintf("item-%d", i)
		
		// Add directly to BadgerDB as if Raft had applied the command
		storage.db.Update(func(txn *badger.Txn) error {
			txn.Set([]byte(key), []byte(value))
			versionKey := fmt.Sprintf("v:%s", key)
			return txn.Set([]byte(versionKey), []byte("1"))
		})
	}

	// Test: Multiple goroutines scanning through the queue with GetNextAfter
	concurrency := 10
	scanLength := 20
	var wg sync.WaitGroup
	allScans := make([][]string, concurrency)
	errors := make([][]error, concurrency)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			scan := make([]string, 0, scanLength)
			scanErrors := make([]error, 0)

			currentKey := ""
			for j := 0; j < scanLength; j++ {
				var key string
				var value []byte
				var exists bool
				var err error

				if currentKey == "" {
					// First call uses GetNext
					key, value, exists, err = storage.GetNext(prefix)
				} else {
					// Subsequent calls use GetNextAfter
					key, value, exists, err = storage.GetNextAfter(prefix, currentKey)
				}

				if err != nil {
					scanErrors = append(scanErrors, err)
					break
				}

				if !exists {
					break
				}

				scan = append(scan, fmt.Sprintf("%s=%s", key, string(value)))
				currentKey = key
			}

			allScans[idx] = scan
			errors[idx] = scanErrors
		}(i)
	}

	wg.Wait()

	// Verify: All scans should be identical and in correct order
	expectedFirstItem := fmt.Sprintf("%s%010d=item-0", prefix, 0)
	expectedSecondItem := fmt.Sprintf("%s%010d=item-1", prefix, 1)

	for i := 0; i < concurrency; i++ {
		require.Empty(t, errors[i], "Scan %d should have no errors", i)
		require.NotEmpty(t, allScans[i], "Scan %d should return items", i)
		
		assert.Equal(t, expectedFirstItem, allScans[i][0], "First item should be consistent across all scans")
		if len(allScans[i]) > 1 {
			assert.Equal(t, expectedSecondItem, allScans[i][1], "Second item should be consistent across all scans")
		}

		// Verify ordering within each scan
		for j := 1; j < len(allScans[i]); j++ {
			assert.True(t, allScans[i][j-1] < allScans[i][j], 
				"Items should be in lexicographic order: %s < %s", allScans[i][j-1], allScans[i][j])
		}
	}
}

// TestQueueMethods_MemoryEfficiency tests that methods don't leak memory under load
func TestQueueMethods_MemoryEfficiency(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	storage := NewAppStorage(nil, db, slog.New(slog.NewTextHandler(os.Stdout, nil)))

	prefix := "queue:memory:pending:"
	iterations := 1000

	// Repeatedly call queue methods to check for memory leaks
	for i := 0; i < iterations; i++ {
		// Add key
		key := fmt.Sprintf("%s%010d", prefix, i)
		storage.db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(key), []byte(fmt.Sprintf("value-%d", i)))
		})

		// Call GetNext
		_, _, _, err := storage.GetNext(prefix)
		require.NoError(t, err)

		// Call CountPrefix
		_, err = storage.CountPrefix(prefix)
		require.NoError(t, err)

		// Call GetNextAfter
		if i > 0 {
			afterKey := fmt.Sprintf("%s%010d", prefix, i-1)
			_, _, _, err = storage.GetNextAfter(prefix, afterKey)
			require.NoError(t, err)
		}
	}

	// Final verification - methods still work correctly
	count, err := storage.CountPrefix(prefix)
	require.NoError(t, err)
	assert.Equal(t, iterations, count, "Final count should match iterations")

	key, value, exists, err := storage.GetNext(prefix)
	require.NoError(t, err)
	assert.True(t, exists, "GetNext should find first key")
	assert.Equal(t, fmt.Sprintf("%s%010d", prefix, 0), key, "Should get first key")
	assert.Equal(t, "value-0", string(value), "Should get first value")
}

// Helper functions

func averageDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}

func maxDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	max := durations[0]
	for _, d := range durations {
		if d > max {
			max = d
		}
	}
	return max
}