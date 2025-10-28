package queue

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueue_HasClaimedItemsWithPrefix_BasicFunctionality tests the basic functionality
func TestQueue_HasClaimedItemsWithPrefix_BasicFunctionality(t *testing.T) {
	tests := []struct {
		name         string
		claimedItems []struct {
			data    string
			claimID string
		}
		searchPrefix   string
		expectedResult bool
	}{
		{
			name: "exact prefix match",
			claimedItems: []struct {
				data    string
				claimID string
			}{
				{data: `{"workflow_id":"test-123","action":"transform"}`, claimID: "claim-1"},
			},
			searchPrefix:   `"workflow_id":"test-123"`,
			expectedResult: true,
		},
		{
			name: "partial prefix match",
			claimedItems: []struct {
				data    string
				claimID string
			}{
				{data: `{"workflow_id":"test-456","action":"publish"}`, claimID: "claim-2"},
			},
			searchPrefix:   `"workflow_id":"test`,
			expectedResult: true,
		},
		{
			name: "no match",
			claimedItems: []struct {
				data    string
				claimID string
			}{
				{data: `{"workflow_id":"other-789","action":"validate"}`, claimID: "claim-3"},
			},
			searchPrefix:   `"workflow_id":"test-123"`,
			expectedResult: false,
		},
		{
			name: "empty prefix",
			claimedItems: []struct {
				data    string
				claimID string
			}{
				{data: `{"workflow_id":"test-123","action":"transform"}`, claimID: "claim-1"},
			},
			searchPrefix:   "",
			expectedResult: true,
		},
		{
			name: "empty claimed items",
			claimedItems: []struct {
				data    string
				claimID string
			}{},
			searchPrefix:   `"workflow_id":"test-123"`,
			expectedResult: false,
		},
		{
			name: "multiple items with match",
			claimedItems: []struct {
				data    string
				claimID string
			}{
				{data: `{"workflow_id":"other-111","action":"validate"}`, claimID: "claim-1"},
				{data: `{"workflow_id":"test-123","action":"transform"}`, claimID: "claim-2"},
				{data: `{"workflow_id":"another-222","action":"publish"}`, claimID: "claim-3"},
			},
			searchPrefix:   `"workflow_id":"test-123"`,
			expectedResult: true,
		},
		{
			name: "malformed json data",
			claimedItems: []struct {
				data    string
				claimID string
			}{
				{data: `malformed json`, claimID: "claim-1"},
				{data: `{"workflow_id":"test-123","action":"transform"}`, claimID: "claim-2"},
			},
			searchPrefix:   `"workflow_id":"test-123"`,
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := &mocks.MockStoragePort{}
			queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)

			var mockItems []ports.KeyValueVersion
			for _, item := range tt.claimedItems {
				claimedItem := domain.NewClaimedItem([]byte(item.data), item.claimID, 1)
				itemBytes, _ := claimedItem.ToBytes()
				mockItems = append(mockItems, ports.KeyValueVersion{
					Key:   fmt.Sprintf("queue:test:claimed:%s", item.claimID),
					Value: itemBytes,
				})
			}

			mockStorage.On("ListByPrefix", "queue:test:claimed:").Return(mockItems, nil).Maybe()

			result, err := queue.HasClaimedItemsWithPrefix(tt.searchPrefix)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult, result)
			mockStorage.AssertExpectations(t)
		})
	}
}

// TestQueue_HasClaimedItemsWithPrefix_EdgeCases tests edge cases and error scenarios
func TestQueue_HasClaimedItemsWithPrefix_EdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		setupQueue     func() *Queue
		searchPrefix   string
		expectError    bool
		expectedResult bool
	}{
		{
			name: "queue closed",
			setupQueue: func() *Queue {
				mockStorage := &mocks.MockStoragePort{}
				queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)
				queue.Close()
				return queue
			},
			searchPrefix:   `"workflow_id":"test"`,
			expectError:    true,
			expectedResult: false,
		},
		{
			name: "storage error",
			setupQueue: func() *Queue {
				mockStorage := &mocks.MockStoragePort{}
				mockStorage.On("ListByPrefix", "queue:test:claimed:").Return(nil, fmt.Errorf("storage error")).Maybe()
				return NewQueue("test", mockStorage, nil, nil, "", 0, nil)
			},
			searchPrefix:   `"workflow_id":"test"`,
			expectError:    true,
			expectedResult: false,
		},
		{
			name: "corrupted claimed item data",
			setupQueue: func() *Queue {
				mockStorage := &mocks.MockStoragePort{}
				mockItems := []ports.KeyValueVersion{
					{
						Key:   "queue:test:claimed:corrupt",
						Value: []byte("invalid json"),
					},
				}
				mockStorage.On("ListByPrefix", "queue:test:claimed:").Return(mockItems, nil).Maybe()
				return NewQueue("test", mockStorage, nil, nil, "", 0, nil)
			},
			searchPrefix:   `"workflow_id":"test"`,
			expectError:    false,
			expectedResult: false,
		},
		{
			name: "very long prefix",
			setupQueue: func() *Queue {
				mockStorage := &mocks.MockStoragePort{}
				longData := fmt.Sprintf(`{"workflow_id":"test-123","data":"%s"}`, strings.Repeat("x", 10000))
				claimedItem := domain.NewClaimedItem([]byte(longData), "claim-1", 1)
				itemBytes, _ := claimedItem.ToBytes()
				mockItems := []ports.KeyValueVersion{
					{
						Key:   "queue:test:claimed:claim-1",
						Value: itemBytes,
					},
				}
				mockStorage.On("ListByPrefix", "queue:test:claimed:").Return(mockItems, nil).Maybe()
				return NewQueue("test", mockStorage, nil, nil, "", 0, nil)
			},
			searchPrefix:   strings.Repeat("x", 5000),
			expectError:    false,
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue := tt.setupQueue()

			result, err := queue.HasClaimedItemsWithPrefix(tt.searchPrefix)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

// TestQueue_HasClaimedItemsWithPrefix_ConcurrencyStress tests concurrent access
func TestQueue_HasClaimedItemsWithPrefix_ConcurrencyStress(t *testing.T) {
	const numGoroutines = 100
	const numIterations = 10

	mockStorage := &mocks.MockStoragePort{}
	queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)

	claimedItem := domain.NewClaimedItem([]byte(`{"workflow_id":"test-123","action":"transform"}`), "claim-1", 1)
	itemBytes, _ := claimedItem.ToBytes()
	mockItems := []ports.KeyValueVersion{
		{
			Key:   "queue:test:claimed:claim-1",
			Value: itemBytes,
		},
	}

	mockStorage.On("ListByPrefix", "queue:test:claimed:").Return(mockItems, nil).Times(numGoroutines * numIterations)

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numIterations)
	results := make(chan bool, numGoroutines*numIterations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				result, err := queue.HasClaimedItemsWithPrefix(`"workflow_id":"test-123"`)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d iteration %d: %v", goroutineID, j, err)
					return
				}
				results <- result
			}
		}(i)
	}

	wg.Wait()
	close(errors)
	close(results)

	for err := range errors {
		t.Errorf("Concurrent access error: %v", err)
	}

	expectedResultCount := numGoroutines * numIterations
	actualResultCount := 0
	trueCount := 0

	for result := range results {
		actualResultCount++
		if result {
			trueCount++
		}
	}

	assert.Equal(t, expectedResultCount, actualResultCount, "Missing results from concurrent operations")
	assert.Equal(t, expectedResultCount, trueCount, "All concurrent operations should return true")

	mockStorage.AssertExpectations(t)
}

// TestQueue_HasClaimedItemsWithPrefix_LargeDatasetStress tests performance with large datasets
func TestQueue_HasClaimedItemsWithPrefix_LargeDatasetStress(t *testing.T) {
	const numItems = 10000

	mockStorage := &mocks.MockStoragePort{}
	queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)

	var mockItems []ports.KeyValueVersion
	for i := 0; i < numItems; i++ {
		var data string
		if i == numItems/2 {
			data = `{"workflow_id":"target-match","action":"transform"}`
		} else {
			data = fmt.Sprintf(`{"workflow_id":"other-%d","action":"validate"}`, i)
		}

		claimedItem := domain.NewClaimedItem([]byte(data), fmt.Sprintf("claim-%d", i), int64(i))
		itemBytes, _ := claimedItem.ToBytes()
		mockItems = append(mockItems, ports.KeyValueVersion{
			Key:   fmt.Sprintf("queue:test:claimed:claim-%d", i),
			Value: itemBytes,
		})
	}

	mockStorage.On("ListByPrefix", "queue:test:claimed:").Return(mockItems, nil).Maybe()

	tests := []struct {
		name           string
		searchPrefix   string
		expectedResult bool
		maxDuration    time.Duration
	}{
		{
			name:           "find match in large dataset",
			searchPrefix:   `"workflow_id":"target-match"`,
			expectedResult: true,
			maxDuration:    100 * time.Millisecond,
		},
		{
			name:           "no match in large dataset",
			searchPrefix:   `"workflow_id":"nonexistent"`,
			expectedResult: false,
			maxDuration:    100 * time.Millisecond,
		},
		{
			name:           "partial match in large dataset",
			searchPrefix:   `"workflow_id":"other-1"`,
			expectedResult: true,
			maxDuration:    100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()
			result, err := queue.HasClaimedItemsWithPrefix(tt.searchPrefix)
			duration := time.Since(start)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult, result)
			assert.Less(t, duration, tt.maxDuration,
				"Operation took too long with %d items: %v", numItems, duration)
		})
	}

	mockStorage.AssertExpectations(t)
}

// TestQueue_HasClaimedItemsWithPrefix_MemoryStress tests memory usage patterns
func TestQueue_HasClaimedItemsWithPrefix_MemoryStress(t *testing.T) {
	const iterations = 1000

	mockStorage := &mocks.MockStoragePort{}
	queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)

	var mockItems []ports.KeyValueVersion
	for i := 0; i < 100; i++ {

		dataSize := (i%10 + 1) * 100
		largeData := fmt.Sprintf(`{"workflow_id":"test-%d","data":"%s"}`,
			i, strings.Repeat("x", dataSize))

		claimedItem := domain.NewClaimedItem([]byte(largeData), fmt.Sprintf("claim-%d", i), int64(i))
		itemBytes, _ := claimedItem.ToBytes()
		mockItems = append(mockItems, ports.KeyValueVersion{
			Key:   fmt.Sprintf("queue:test:claimed:claim-%d", i),
			Value: itemBytes,
		})
	}

	mockStorage.On("ListByPrefix", "queue:test:claimed:").Return(mockItems, nil).Times(iterations)

	for i := 0; i < iterations; i++ {
		result, err := queue.HasClaimedItemsWithPrefix(`"workflow_id":"test-50"`)
		require.NoError(t, err)
		assert.True(t, result)

		if i%100 == 0 {

			t.Logf("Completed %d iterations", i)
		}
	}

	mockStorage.AssertExpectations(t)
}

// TestQueue_HasClaimedItemsWithPrefix_RaceConditionStress tests for race conditions
func TestQueue_HasClaimedItemsWithPrefix_RaceConditionStress(t *testing.T) {
	const numReaders = 50
	const numWriters = 10
	const duration = 2 * time.Second

	mockStorage := &mocks.MockStoragePort{}
	queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)

	claimedItem := domain.NewClaimedItem([]byte(`{"workflow_id":"race-test","action":"transform"}`), "race-claim", 1)
	itemBytes, _ := claimedItem.ToBytes()
	mockItems := []ports.KeyValueVersion{
		{
			Key:   "queue:test:claimed:race-claim",
			Value: itemBytes,
		},
	}

	mockStorage.On("ListByPrefix", "queue:test:claimed:").Return(mockItems, nil).Maybe()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errors := make(chan error, numReaders+numWriters)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, err := queue.HasClaimedItemsWithPrefix(`"workflow_id":"race-test"`)
					if err != nil && !strings.Contains(err.Error(), "queue is closed") {
						errors <- fmt.Errorf("reader %d: %v", readerID, err)
						return
					}
				}
			}
		}(i)
	}

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:

					_, err := queue.HasClaimedItemsWithPrefix(`"workflow_id":"race-test"`)
					if err != nil && !strings.Contains(err.Error(), "queue is closed") {
						errors <- fmt.Errorf("writer %d: %v", writerID, err)
						return
					}
				}
			}
		}(i)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	close(errors)

	for err := range errors {
		t.Errorf("Race condition detected: %v", err)
	}
}

// BenchmarkQueue_HasClaimedItemsWithPrefix_Performance benchmarks the method performance
func BenchmarkQueue_HasClaimedItemsWithPrefix_Performance(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Items_%d", size), func(b *testing.B) {
			mockStorage := &mocks.MockStoragePort{}
			queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)

			var mockItems []ports.KeyValueVersion
			for i := 0; i < size; i++ {
				data := fmt.Sprintf(`{"workflow_id":"test-%d","action":"transform"}`, i)
				claimedItem := domain.NewClaimedItem([]byte(data), fmt.Sprintf("claim-%d", i), int64(i))
				itemBytes, _ := claimedItem.ToBytes()
				mockItems = append(mockItems, ports.KeyValueVersion{
					Key:   fmt.Sprintf("queue:test:claimed:claim-%d", i),
					Value: itemBytes,
				})
			}

			mockStorage.On("ListByPrefix", "queue:test:claimed:").Return(mockItems, nil).Times(b.N)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := queue.HasClaimedItemsWithPrefix(`"workflow_id":"test-500"`)
				if err != nil {
					b.Fatalf("Benchmark failed: %v", err)
				}
			}
		})
	}
}
