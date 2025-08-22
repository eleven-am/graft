package manager

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewResourceManager(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: map[string]int{"test": 5},
		DefaultPerTypeLimit:  3,
	}

	rm := NewResourceManager(config, slog.Default())

	assert.NotNil(t, rm)
	assert.Equal(t, config.MaxConcurrentTotal, rm.config.MaxConcurrentTotal)
	assert.Equal(t, config.DefaultPerTypeLimit, rm.config.DefaultPerTypeLimit)
	assert.Equal(t, 0, rm.totalCount)
	assert.Empty(t, rm.executing)
}

func TestResourceManager_CanExecuteNode(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   5,
		MaxConcurrentPerType: map[string]int{"limited": 2},
		DefaultPerTypeLimit:  3,
	}

	rm := NewResourceManager(config, slog.Default())

	assert.True(t, rm.CanExecuteNode("limited"))
	assert.True(t, rm.CanExecuteNode("unlimited"))

	require.NoError(t, rm.AcquireNode("limited"))
	require.NoError(t, rm.AcquireNode("limited"))

	assert.False(t, rm.CanExecuteNode("limited"))

	assert.True(t, rm.CanExecuteNode("unlimited"))

	require.NoError(t, rm.AcquireNode("unlimited"))
	require.NoError(t, rm.AcquireNode("unlimited"))
	require.NoError(t, rm.AcquireNode("unlimited"))

	assert.False(t, rm.CanExecuteNode("unlimited"))
	assert.False(t, rm.CanExecuteNode("other"))
}

func TestResourceManager_AcquireNode_Success(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: map[string]int{"test": 5},
		DefaultPerTypeLimit:  3,
	}

	rm := NewResourceManager(config, slog.Default())

	err := rm.AcquireNode("test")
	assert.NoError(t, err)
	assert.Equal(t, 1, rm.totalCount)
	assert.Equal(t, 1, rm.executing["test"])

	err = rm.AcquireNode("test")
	assert.NoError(t, err)
	assert.Equal(t, 2, rm.totalCount)
	assert.Equal(t, 2, rm.executing["test"])

	err = rm.AcquireNode("other")
	assert.NoError(t, err)
	assert.Equal(t, 3, rm.totalCount)
	assert.Equal(t, 1, rm.executing["other"])
}

func TestResourceManager_AcquireNode_Failures(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   3,
		MaxConcurrentPerType: map[string]int{"limited": 1},
		DefaultPerTypeLimit:  2,
	}

	rm := NewResourceManager(config, slog.Default())

	require.NoError(t, rm.AcquireNode("limited"))

	err := rm.AcquireNode("limited")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node_type_limited limit reached")

	require.NoError(t, rm.AcquireNode("other"))
	require.NoError(t, rm.AcquireNode("other"))

	err = rm.AcquireNode("another")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "concurrent_executions limit reached")
}

func TestResourceManager_ReleaseNode_Success(t *testing.T) {
	config := DefaultResourceConfig()
	rm := NewResourceManager(config, slog.Default())

	require.NoError(t, rm.AcquireNode("test"))
	assert.Equal(t, 1, rm.totalCount)

	err := rm.ReleaseNode("test")
	assert.NoError(t, err)
	assert.Equal(t, 0, rm.totalCount)
}

func TestResourceManager_ReleaseNode_Failures(t *testing.T) {
	config := DefaultResourceConfig()
	rm := NewResourceManager(config, slog.Default())

	err := rm.ReleaseNode("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no executions to release")
}

func TestResourceManager_GetExecutionStats(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: map[string]int{"http": 5, "email": 3},
		DefaultPerTypeLimit:  4,
	}

	rm := NewResourceManager(config, slog.Default())

	require.NoError(t, rm.AcquireNode("http"))
	require.NoError(t, rm.AcquireNode("http"))
	require.NoError(t, rm.AcquireNode("email"))
	require.NoError(t, rm.AcquireNode("unconfigured"))

	stats := rm.GetExecutionStats()

	assert.Equal(t, 4, stats.TotalExecuting)
	assert.Equal(t, 10, stats.TotalCapacity)
	assert.Equal(t, 6, stats.AvailableSlots)

	assert.Equal(t, 2, stats.PerTypeExecuting["http"])
	assert.Equal(t, 1, stats.PerTypeExecuting["email"])
	assert.Equal(t, 1, stats.PerTypeExecuting["unconfigured"])

	assert.Equal(t, 5, stats.PerTypeCapacity["http"])
	assert.Equal(t, 3, stats.PerTypeCapacity["email"])
}

func TestResourceManager_UpdateConfig(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: map[string]int{"test": 5},
		DefaultPerTypeLimit:  3,
	}

	rm := NewResourceManager(config, slog.Default())

	require.NoError(t, rm.AcquireNode("test"))
	require.NoError(t, rm.AcquireNode("test"))

	newConfig := ports.ResourceConfig{
		MaxConcurrentTotal:   20,
		MaxConcurrentPerType: map[string]int{"test": 10, "new": 5},
		DefaultPerTypeLimit:  8,
	}

	err := rm.UpdateConfig(newConfig)
	assert.NoError(t, err)
	assert.Equal(t, newConfig.MaxConcurrentTotal, rm.config.MaxConcurrentTotal)

	badConfig := ports.ResourceConfig{
		MaxConcurrentTotal:   1,                          // Much less than current executing count (2)
		MaxConcurrentPerType: map[string]int{"test": 10}, // Keep test at 10, don't reduce it
		DefaultPerTypeLimit:  8,
	}

	err = rm.UpdateConfig(badConfig)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot reduce MaxConcurrentTotal")
}

func TestResourceManager_ConcurrentAccess(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   100,
		MaxConcurrentPerType: map[string]int{},
		DefaultPerTypeLimit:  20,
	}

	rm := NewResourceManager(config, slog.Default())

	const numWorkers = 10
	const operationsPerWorker = 50

	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*operationsPerWorker)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			nodeType := "concurrent_test"

			for j := 0; j < operationsPerWorker; j++ {
				if rm.CanExecuteNode(nodeType) {
					if err := rm.AcquireNode(nodeType); err != nil {
						errors <- err
						continue
					}

					time.Sleep(time.Microsecond)

					if err := rm.ReleaseNode(nodeType); err != nil {
						errors <- err
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent operation error: %v", err)
	}

	assert.Equal(t, 0, rm.totalCount)
	assert.Empty(t, rm.executing)
}

func TestResourceManager_StressTest(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   50,
		MaxConcurrentPerType: map[string]int{"stress": 30},
		DefaultPerTypeLimit:  20,
	}

	rm := NewResourceManager(config, slog.Default())

	acquired := 0
	for i := 0; i < 100; i++ {
		if rm.CanExecuteNode("stress") {
			err := rm.AcquireNode("stress")
			if err == nil {
				acquired++
			}
		}
	}

	assert.Equal(t, 30, acquired)
	assert.Equal(t, 30, rm.totalCount)

	for i := 0; i < acquired; i++ {
		require.NoError(t, rm.ReleaseNode("stress"))
	}

	assert.Equal(t, 0, rm.totalCount)
	assert.Empty(t, rm.executing)
}

func TestResourceManager_MixedNodeTypes(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: map[string]int{"http": 3, "email": 2},
		DefaultPerTypeLimit:  4,
	}

	rm := NewResourceManager(config, slog.Default())

	require.NoError(t, rm.AcquireNode("http"))
	require.NoError(t, rm.AcquireNode("http"))
	require.NoError(t, rm.AcquireNode("email"))

	// After acquiring 3 nodes (2 http, 1 email), totalCount should be 3
	assert.Equal(t, 3, rm.totalCount)

	// We can still execute more nodes (database uses default limit of 4)
	assert.True(t, rm.CanExecuteNode("database"))

	// Fill up to the max total using different node types
	require.NoError(t, rm.AcquireNode("http"))     // http: 3/3
	require.NoError(t, rm.AcquireNode("email"))    // email: 2/2
	require.NoError(t, rm.AcquireNode("database")) // database: 1/4
	require.NoError(t, rm.AcquireNode("database")) // database: 2/4
	require.NoError(t, rm.AcquireNode("database")) // database: 3/4
	require.NoError(t, rm.AcquireNode("other"))    // other: 1/4 (uses default)
	require.NoError(t, rm.AcquireNode("another"))  // another: 1/4 (uses default)

	// Now we should be at max total
	assert.Equal(t, 10, rm.totalCount)

	// No more nodes can be executed
	assert.False(t, rm.CanExecuteNode("any"))
}
