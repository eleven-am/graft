package resource_manager

import (
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

func TestAdapter_CanExecuteNode(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
		MaxConcurrentPerType: map[string]int{
			"worker": 2,
		},
	}

	rm := NewAdapter(config, nil)

	if !rm.CanExecuteNode("worker") {
		t.Error("Expected to be able to execute worker initially")
	}

	if !rm.CanExecuteNode("other") {
		t.Error("Expected to be able to execute other initially")
	}
}

func TestResourceManager_AcquireNode_Success(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
		MaxConcurrentPerType: map[string]int{
			"worker": 2,
		},
	}

	rm := NewAdapter(config, nil)

	err := rm.AcquireNode("worker")
	if err != nil {
		t.Errorf("Failed to acquire first worker: %v", err)
	}

	err = rm.AcquireNode("worker")
	if err != nil {
		t.Errorf("Failed to acquire second worker: %v", err)
	}

	stats := rm.GetExecutionStats()
	if stats.TotalExecuting != 2 {
		t.Errorf("Expected 2 total executing, got %d", stats.TotalExecuting)
	}

	if stats.PerTypeExecuting["worker"] != 2 {
		t.Errorf("Expected 2 workers executing, got %d", stats.PerTypeExecuting["worker"])
	}
}

func TestResourceManager_AcquireNode_TypeLimit(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
		MaxConcurrentPerType: map[string]int{
			"worker": 2,
		},
	}

	rm := NewAdapter(config, nil)

	rm.AcquireNode("worker")
	rm.AcquireNode("worker")

	err := rm.AcquireNode("worker")
	if err == nil {
		t.Error("Expected error when exceeding worker type limit")
	}
}

func TestResourceManager_AcquireNode_TotalLimit(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  2,
		DefaultPerTypeLimit: 5,
	}

	rm := NewAdapter(config, nil)

	rm.AcquireNode("type1")
	rm.AcquireNode("type2")

	err := rm.AcquireNode("type3")
	if err == nil {
		t.Error("Expected error when exceeding total limit")
	}
}

func TestResourceManager_ReleaseNode_Success(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
	}

	rm := NewAdapter(config, nil)

	rm.AcquireNode("worker")
	rm.AcquireNode("worker")

	err := rm.ReleaseNode("worker")
	if err != nil {
		t.Errorf("Failed to release worker: %v", err)
	}

	stats := rm.GetExecutionStats()
	if stats.TotalExecuting != 1 {
		t.Errorf("Expected 1 total executing after release, got %d", stats.TotalExecuting)
	}

	if stats.PerTypeExecuting["worker"] != 1 {
		t.Errorf("Expected 1 worker executing after release, got %d", stats.PerTypeExecuting["worker"])
	}
}

func TestResourceManager_ReleaseNode_Error(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
	}

	rm := NewAdapter(config, nil)

	err := rm.ReleaseNode("worker")
	if err == nil {
		t.Error("Expected error when releasing non-executing node")
	}
}

func TestResourceManager_ReleaseNode_CleansUpMap(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
	}

	rm := NewAdapter(config, nil)

	rm.AcquireNode("worker")
	rm.ReleaseNode("worker")

	stats := rm.GetExecutionStats()
	if _, exists := stats.PerTypeExecuting["worker"]; exists {
		t.Error("Expected worker type to be cleaned up from executing map")
	}
}

func TestResourceManager_GetExecutionStats(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  20,
		DefaultPerTypeLimit: 5,
		MaxConcurrentPerType: map[string]int{
			"worker": 3,
			"batch":  7,
		},
	}

	rm := NewAdapter(config, nil)

	rm.AcquireNode("worker")
	rm.AcquireNode("worker")
	rm.AcquireNode("batch")
	rm.AcquireNode("other")

	stats := rm.GetExecutionStats()

	if stats.TotalExecuting != 4 {
		t.Errorf("Expected 4 total executing, got %d", stats.TotalExecuting)
	}

	if stats.TotalCapacity != 20 {
		t.Errorf("Expected 20 total capacity, got %d", stats.TotalCapacity)
	}

	if stats.AvailableSlots != 16 {
		t.Errorf("Expected 16 available slots, got %d", stats.AvailableSlots)
	}

	if stats.PerTypeExecuting["worker"] != 2 {
		t.Errorf("Expected 2 workers executing, got %d", stats.PerTypeExecuting["worker"])
	}

	if stats.PerTypeCapacity["worker"] != 3 {
		t.Errorf("Expected 3 worker capacity, got %d", stats.PerTypeCapacity["worker"])
	}

	if stats.PerTypeCapacity["other"] != 5 {
		t.Errorf("Expected 5 other capacity (default), got %d", stats.PerTypeCapacity["other"])
	}
}

func TestResourceManager_UpdateConfig_Success(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
	}

	rm := NewAdapter(config, nil)

	rm.AcquireNode("worker")
	rm.AcquireNode("batch")

	newConfig := ports.ResourceConfig{
		MaxConcurrentTotal:  20,
		DefaultPerTypeLimit: 5,
		MaxConcurrentPerType: map[string]int{
			"worker": 4,
		},
	}

	err := rm.UpdateConfig(newConfig)
	if err != nil {
		t.Errorf("Failed to update config: %v", err)
	}

	stats := rm.GetExecutionStats()
	if stats.TotalCapacity != 20 {
		t.Errorf("Expected 20 total capacity, got %d", stats.TotalCapacity)
	}

	if stats.PerTypeCapacity["worker"] != 4 {
		t.Errorf("Expected 4 worker capacity, got %d", stats.PerTypeCapacity["worker"])
	}
}

func TestResourceManager_UpdateConfig_ValidationErrors(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
	}

	rm := NewAdapter(config, nil)

	err := rm.UpdateConfig(ports.ResourceConfig{
		MaxConcurrentTotal:  0,
		DefaultPerTypeLimit: 3,
	})
	if err == nil {
		t.Error("Expected error for zero MaxConcurrentTotal")
	}

	err = rm.UpdateConfig(ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 0,
	})
	if err == nil {
		t.Error("Expected error for zero DefaultPerTypeLimit")
	}

	err = rm.UpdateConfig(ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
		MaxConcurrentPerType: map[string]int{
			"worker": 0,
		},
	})
	if err == nil {
		t.Error("Expected error for zero MaxConcurrentPerType")
	}
}

func TestResourceManager_UpdateConfig_BelowCurrentUsage(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 5,
	}

	rm := NewAdapter(config, nil)

	rm.AcquireNode("worker")
	rm.AcquireNode("worker")
	rm.AcquireNode("worker")

	err := rm.UpdateConfig(ports.ResourceConfig{
		MaxConcurrentTotal:  2,
		DefaultPerTypeLimit: 5,
	})
	if err == nil {
		t.Error("Expected error when reducing total below current usage")
	}

	err = rm.UpdateConfig(ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 5,
		MaxConcurrentPerType: map[string]int{
			"worker": 2,
		},
	})
	if err == nil {
		t.Error("Expected error when reducing type limit below current usage")
	}
}

func TestResourceManager_IsHealthy(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
	}

	rm := NewAdapter(config, nil)

	if !rm.IsHealthy() {
		t.Error("Expected resource manager to be healthy initially")
	}

	rm.AcquireNode("worker")
	rm.AcquireNode("batch")

	if !rm.IsHealthy() {
		t.Error("Expected resource manager to be healthy after normal operations")
	}

	rm.ReleaseNode("worker")

	if !rm.IsHealthy() {
		t.Error("Expected resource manager to be healthy after release")
	}
}

func TestResourceManager_AcquireNode_WithPriority(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   5,
		MaxConcurrentPerType: map[string]int{"worker": 3},
		DefaultPerTypeLimit:  2,
		NodePriorities:       map[string]int{"worker": 2},
		HealthThresholds: ports.HealthConfig{
			MaxResponseTime:    100 * time.Millisecond,
			MinSuccessRate:     0.95,
			MaxUtilizationRate: 0.8,
		},
	}

	rm := NewAdapter(config, slog.Default())

	err := rm.AcquireNode("worker", 1)
	if err != nil {
		t.Errorf("Failed to acquire worker with priority 1: %v", err)
	}

	err = rm.AcquireNode("worker")
	if err != nil {
		t.Errorf("Failed to acquire worker with default priority: %v", err)
	}
}

func TestResourceManager_QueuePullPattern(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal: 3,
		MaxConcurrentPerType: map[string]int{
			"data-processor": 2,
			"email-sender":   1,
			"file-processor": 0,
		},
		DefaultPerTypeLimit: 1,
	}

	rm := NewAdapter(config, slog.Default())

	if !rm.CanExecuteNode("data-processor") {
		t.Error("Expected machine to handle data-processor work")
	}

	err := rm.AcquireNode("data-processor")
	if err != nil {
		t.Errorf("Failed to acquire data-processor: %v", err)
	}

	if !rm.CanExecuteNode("data-processor") {
		t.Error("Expected machine to handle one more data-processor")
	}

	err = rm.AcquireNode("data-processor")
	if err != nil {
		t.Errorf("Failed to acquire second data-processor: %v", err)
	}

	if rm.CanExecuteNode("data-processor") {
		t.Error("Expected data-processor to be at capacity")
	}

	if !rm.CanExecuteNode("email-sender") {
		t.Error("Expected machine to handle email-sender work")
	}

	if rm.CanExecuteNode("file-processor") {
		t.Error("Expected machine to reject file-processor work")
	}

	err = rm.ReleaseNode("data-processor")
	if err != nil {
		t.Errorf("Failed to release data-processor: %v", err)
	}

	if !rm.CanExecuteNode("data-processor") {
		t.Error("Expected machine to handle data-processor after release")
	}
}

func TestResourceManager_DefaultPerTypeLimit(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 2,
		MaxConcurrentPerType: map[string]int{
			"worker": 5,
		},
	}

	rm := NewAdapter(config, nil)

	for i := 0; i < 5; i++ {
		if !rm.CanExecuteNode("worker") {
			t.Errorf("Expected to be able to execute worker %d", i+1)
		}
		rm.AcquireNode("worker")
	}

	if rm.CanExecuteNode("worker") {
		t.Error("Expected worker to be at limit")
	}

	for i := 0; i < 2; i++ {
		if !rm.CanExecuteNode("other") {
			t.Errorf("Expected to be able to execute other %d", i+1)
		}
		rm.AcquireNode("other")
	}

	if rm.CanExecuteNode("other") {
		t.Error("Expected other to be at default limit")
	}
}

func TestResourceManager_ConcurrentAccess_RaceConditions(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  3,
		DefaultPerTypeLimit: 2,
	}

	rm := NewAdapter(config, nil)

	done := make(chan bool)
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()

			err := rm.AcquireNode("worker")
			if err != nil {
				errors <- err
				return
			}

			time.Sleep(1 * time.Millisecond)

			err = rm.ReleaseNode("worker")
			if err != nil {
				errors <- err
			}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
	close(errors)

	for err := range errors {
		if err.Error() != "resource[worker] acquire: capacity limit reached" {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	stats := rm.GetExecutionStats()
	if stats.TotalExecuting != 0 {
		t.Errorf("Expected 0 executing after all releases, got %d", stats.TotalExecuting)
	}
}

func TestResourceManager_MemoryLeak_MapCleanup(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 5,
	}

	rm := NewAdapter(config, nil)

	nodeTypes := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	for _, nodeType := range nodeTypes {
		rm.AcquireNode(nodeType)
	}

	stats := rm.GetExecutionStats()
	if len(stats.PerTypeExecuting) != 10 {
		t.Errorf("Expected 10 node types executing, got %d", len(stats.PerTypeExecuting))
	}

	for _, nodeType := range nodeTypes {
		rm.ReleaseNode(nodeType)
	}

	stats = rm.GetExecutionStats()
	if len(stats.PerTypeExecuting) != 0 {
		t.Errorf("Expected 0 node types in map after cleanup, got %d", len(stats.PerTypeExecuting))
	}
}

func TestResourceManager_InvalidStateDetection(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  5,
		DefaultPerTypeLimit: 3,
	}

	rm := NewAdapter(config, nil)
	adapter := rm

	rm.AcquireNode("worker")
	rm.AcquireNode("worker")

	adapter.mu.Lock()
	adapter.totalCount = 10
	adapter.mu.Unlock()

	if rm.IsHealthy() {
		t.Error("Expected IsHealthy to detect corrupted state")
	}

	adapter.mu.Lock()
	adapter.totalCount = 2
	adapter.executing["worker"] = -1
	adapter.mu.Unlock()

	if rm.IsHealthy() {
		t.Error("Expected IsHealthy to detect negative count")
	}
}

func TestResourceManager_EdgeCase_ZeroLimits(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  0,
		DefaultPerTypeLimit: 1,
	}

	rm := NewAdapter(config, nil)

	if rm.CanExecuteNode("worker") {
		t.Error("Expected CanExecuteNode to return false with zero total limit")
	}

	err := rm.AcquireNode("worker")
	if err == nil {
		t.Error("Expected error when acquiring with zero total limit")
	}
}

func TestResourceManager_EdgeCase_NegativePriority(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  5,
		DefaultPerTypeLimit: 3,
	}

	rm := NewAdapter(config, nil)

	err := rm.AcquireNode("worker", -100)
	if err != nil {
		t.Errorf("Unexpected error with negative priority: %v", err)
	}

	err = rm.AcquireNode("worker", 999999)
	if err != nil {
		t.Errorf("Unexpected error with large priority: %v", err)
	}
}

func TestResourceManager_ConfigUpdate_CornerCases(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  5,
		DefaultPerTypeLimit: 2,
		MaxConcurrentPerType: map[string]int{
			"worker": 3,
		},
	}

	rm := NewAdapter(config, nil)

	rm.AcquireNode("worker")
	rm.AcquireNode("worker")
	rm.AcquireNode("batch")

	newConfig := ports.ResourceConfig{
		MaxConcurrentTotal:  2,
		DefaultPerTypeLimit: 1,
	}

	err := rm.UpdateConfig(newConfig)
	if err == nil {
		t.Error("Expected error when reducing total below current usage")
	}

	newConfig = ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 1,
		MaxConcurrentPerType: map[string]int{
			"worker": 1,
		},
	}

	err = rm.UpdateConfig(newConfig)
	if err == nil {
		t.Error("Expected error when reducing worker limit below current usage")
	}

	emptyConfig := ports.ResourceConfig{}
	err = rm.UpdateConfig(emptyConfig)
	if err == nil {
		t.Error("Expected error with empty config")
	}
}

func TestResourceManager_StatsConsistency(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:  10,
		DefaultPerTypeLimit: 3,
		MaxConcurrentPerType: map[string]int{
			"worker": 2,
			"batch":  4,
		},
	}

	rm := NewAdapter(config, nil)

	rm.AcquireNode("worker")
	rm.AcquireNode("worker")
	rm.AcquireNode("batch")
	rm.AcquireNode("other")

	stats := rm.GetExecutionStats()

	calculatedTotal := 0
	for _, count := range stats.PerTypeExecuting {
		calculatedTotal += count
	}

	if calculatedTotal != stats.TotalExecuting {
		t.Errorf("Stats inconsistent: calculated %d, reported %d", calculatedTotal, stats.TotalExecuting)
	}

	expectedAvailable := config.MaxConcurrentTotal - stats.TotalExecuting
	if stats.AvailableSlots != expectedAvailable {
		t.Errorf("Available slots incorrect: expected %d, got %d", expectedAvailable, stats.AvailableSlots)
	}

	if stats.PerTypeCapacity["other"] != config.DefaultPerTypeLimit {
		t.Errorf("Default capacity not applied to 'other': expected %d, got %d",
			config.DefaultPerTypeLimit, stats.PerTypeCapacity["other"])
	}
}
