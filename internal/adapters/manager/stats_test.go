package manager

import (
	"log/slog"
	"testing"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceManager_GetDetailedStats(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   20,
		MaxConcurrentPerType: map[string]int{"http": 10, "email": 5},
		DefaultPerTypeLimit:  8,
	}

	rm := NewResourceManager(config, slog.Default())

	require.NoError(t, rm.AcquireNode("http"))
	require.NoError(t, rm.AcquireNode("http"))
	require.NoError(t, rm.AcquireNode("email"))
	require.NoError(t, rm.AcquireNode("database"))

	stats := rm.GetDetailedStats()

	assert.Equal(t, 4, stats.TotalExecuting)
	assert.Equal(t, 20, stats.TotalCapacity)
	assert.Equal(t, 16, stats.AvailableSlots)

}

func TestResourceManager_FormatStats(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: map[string]int{"test": 5},
		DefaultPerTypeLimit:  3,
	}

	rm := NewResourceManager(config, slog.Default())

	require.NoError(t, rm.AcquireNode("test"))
	require.NoError(t, rm.AcquireNode("test"))

	statsStr := rm.FormatStats()

	assert.Contains(t, statsStr, "Resource Manager Statistics:")
	assert.Contains(t, statsStr, "Total: 2/10 (20.0% utilization)")
	assert.Contains(t, statsStr, "Available Slots: 8")
	assert.Contains(t, statsStr, "Active Node Types: 1")
	assert.Contains(t, statsStr, "test: 2/5 (40.0%)")
}

func TestResourceManager_FormatStats_Empty(t *testing.T) {
	config := DefaultResourceConfig()
	rm := NewResourceManager(config, slog.Default())

	statsStr := rm.FormatStats()

	assert.Contains(t, statsStr, "Resource Manager Statistics:")
	assert.Contains(t, statsStr, "Total: 0/50 (0.0% utilization)")
	assert.Contains(t, statsStr, "Available Slots: 50")
	assert.Contains(t, statsStr, "Active Node Types: 0")
	assert.NotContains(t, statsStr, "Per-Type Execution:")
}

func TestResourceManager_IsHealthy(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: map[string]int{"limited": 2},
		DefaultPerTypeLimit:  5,
	}

	rm := NewResourceManager(config, slog.Default())

	assert.True(t, rm.IsHealthy())

	for i := 0; i < 4; i++ {
		require.NoError(t, rm.AcquireNode("normal"))
	}
	assert.True(t, rm.IsHealthy())

	for i := 0; i < 5; i++ {
		require.NoError(t, rm.AcquireNode("other"))
	}
	assert.False(t, rm.IsHealthy())
}

func TestResourceManager_IsHealthy_TypeAtCapacity(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   20,
		MaxConcurrentPerType: map[string]int{"limited": 2},
		DefaultPerTypeLimit:  10,
	}

	rm := NewResourceManager(config, slog.Default())

	require.NoError(t, rm.AcquireNode("limited"))
	require.NoError(t, rm.AcquireNode("limited"))

	assert.False(t, rm.IsHealthy())
}

func TestResourceManager_GetResourcePressure(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: make(map[string]int),
		DefaultPerTypeLimit:  10,
	}

	rm := NewResourceManager(config, slog.Default())

	assert.Equal(t, 0.0, rm.GetResourcePressure())

	for i := 0; i < 5; i++ {
		require.NoError(t, rm.AcquireNode("test"))
	}

	assert.Equal(t, 0.5, rm.GetResourcePressure())

	for i := 0; i < 5; i++ {
		require.NoError(t, rm.AcquireNode("test"))
	}

	assert.Equal(t, 1.0, rm.GetResourcePressure())
}

func TestDetailedStats_UtilizationWithZeroCapacity(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: make(map[string]int),
	}

	rm := &ResourceManager{
		config:     config,
		executing:  map[string]int{"test": 1},
		totalCount: 1,
		logger:     slog.Default(),
	}

	stats := rm.GetDetailedStats()

	assert.Equal(t, 0.0, stats.PerTypeUtilization["test"])
}

func TestResourceManager_StatsConsistency(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   15,
		MaxConcurrentPerType: map[string]int{"a": 5, "b": 4},
		DefaultPerTypeLimit:  6,
	}

	rm := NewResourceManager(config, slog.Default())

	require.NoError(t, rm.AcquireNode("a"))
	require.NoError(t, rm.AcquireNode("a"))
	require.NoError(t, rm.AcquireNode("b"))

	regularStats := rm.GetExecutionStats()
	detailedStats := rm.GetDetailedStats()

	assert.Equal(t, regularStats.TotalExecuting, detailedStats.TotalExecuting)
	assert.Equal(t, regularStats.TotalCapacity, detailedStats.TotalCapacity)
	assert.Equal(t, regularStats.AvailableSlots, detailedStats.AvailableSlots)

	for nodeType, executing := range regularStats.PerTypeExecuting {
		assert.Equal(t, executing, detailedStats.PerTypeExecuting[nodeType])
	}

	for nodeType, capacity := range regularStats.PerTypeCapacity {
		assert.Equal(t, capacity, detailedStats.PerTypeCapacity[nodeType])
	}
}
