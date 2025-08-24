package resource_manager

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Adapter struct {
	config          ports.ResourceConfig
	mu              sync.RWMutex
	logger          *slog.Logger
	executing       map[string]int
	totalCount      int
	poolManager     *ResourcePoolManager
	priorityManager *PriorityManager
	healthMonitor   *HealthMonitor
}

func NewAdapter(config ports.ResourceConfig, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}

	adapter := &Adapter{
		config:          config,
		executing:       make(map[string]int),
		logger:          logger.With("component", "resource-manager"),
		poolManager:     NewResourcePoolManager(),
		priorityManager: NewPriorityManager(),
		healthMonitor:   NewHealthMonitor(config.HealthThresholds),
	}

	if len(config.MaxConcurrentPerType) > 0 {
		adapter.initializePools()
	}
	return adapter
}

func (rm *Adapter) CanExecuteNode(nodeType string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	rm.logger.Debug("checking if node can execute", "node_type", nodeType)

	if rm.totalCount >= rm.config.MaxConcurrentTotal {
		rm.logger.Debug("cannot execute node - total capacity reached", "node_type", nodeType, "total_executing", rm.totalCount, "max_total", rm.config.MaxConcurrentTotal)
		return false
	}

	currentCount := rm.executing[nodeType]
	maxForType := rm.getMaxForType(nodeType)

	canExecute := currentCount < maxForType
	rm.logger.Debug("node execution check completed", "node_type", nodeType, "can_execute", canExecute, "current_count", currentCount, "max_for_type", maxForType)

	return canExecute
}

func (rm *Adapter) AcquireNode(nodeType string, priority ...int) error {
	requestPriority := 0
	if len(priority) > 0 {
		requestPriority = priority[0]
	}

	startTime := time.Now()
	success := false

	defer func() {
		rm.healthMonitor.RecordRequest(nodeType, time.Since(startTime), success)
	}()

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.logger.Debug("attempting to acquire node", "node_type", nodeType, "priority", requestPriority)

	if rm.totalCount >= rm.config.MaxConcurrentTotal {
		rm.logger.Info("node acquisition failed - total capacity reached", "node_type", nodeType, "total_executing", rm.totalCount, "max_total", rm.config.MaxConcurrentTotal)
		return domain.NewResourceError("total", "acquire", domain.ErrCapacityLimit)
	}

	currentCount := rm.executing[nodeType]
	maxForType := rm.getMaxForType(nodeType)

	if currentCount >= maxForType {
		rm.logger.Info("node acquisition failed - type capacity reached", "node_type", nodeType, "current_count", currentCount, "max_for_type", maxForType)
		return domain.NewResourceError(nodeType, "acquire", domain.ErrCapacityLimit)
	}

	rm.executing[nodeType] = currentCount + 1
	rm.totalCount++
	success = true

	rm.logger.Debug("node acquired", "node_type", nodeType, "type_executing", rm.executing[nodeType], "total_executing", rm.totalCount, "priority", requestPriority)
	return nil
}

func (rm *Adapter) ReleaseNode(nodeType string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.logger.Debug("attempting to release node", "node_type", nodeType)

	currentCount := rm.executing[nodeType]
	if currentCount <= 0 {
		rm.logger.Error("attempted to release node with no executions", "node_type", nodeType, "current_count", currentCount)
		return domain.NewResourceError(nodeType, "release", domain.ErrInvalidState)
	}

	rm.executing[nodeType] = currentCount - 1
	rm.totalCount--

	if rm.executing[nodeType] == 0 {
		delete(rm.executing, nodeType)
	}

	rm.logger.Debug("node released", "node_type", nodeType, "type_executing", rm.executing[nodeType], "total_executing", rm.totalCount)
	return nil
}

func (rm *Adapter) GetExecutionStats() ports.ExecutionStats {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	rm.logger.Debug("generating execution stats")

	perTypeExecuting := make(map[string]int)
	perTypeCapacity := make(map[string]int)

	for nodeType, count := range rm.executing {
		perTypeExecuting[nodeType] = count
	}

	for nodeType, maxCount := range rm.config.MaxConcurrentPerType {
		perTypeCapacity[nodeType] = maxCount
	}

	for nodeType := range rm.executing {
		if _, exists := perTypeCapacity[nodeType]; !exists {
			perTypeCapacity[nodeType] = rm.config.DefaultPerTypeLimit
		}
	}

	resourcePools := rm.poolManager.GetPools()
	healthMetrics := rm.healthMonitor.GetHealthMetrics()

	for nodeType, count := range rm.executing {
		rm.healthMonitor.GetNodeUtilization(nodeType, count, rm.getMaxForType(nodeType))
	}

	stats := ports.ExecutionStats{
		TotalExecuting:   rm.totalCount,
		PerTypeExecuting: perTypeExecuting,
		TotalCapacity:    rm.config.MaxConcurrentTotal,
		PerTypeCapacity:  perTypeCapacity,
		AvailableSlots:   rm.config.MaxConcurrentTotal - rm.totalCount,
		ResourcePools:    resourcePools,
		HealthMetrics:    healthMetrics,
	}

	rm.logger.Debug("execution stats generated", "total_executing", stats.TotalExecuting, "total_capacity", stats.TotalCapacity, "available_slots", stats.AvailableSlots)
	return stats
}

func (rm *Adapter) UpdateConfig(config ports.ResourceConfig) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if config.MaxConcurrentTotal <= 0 {
		return domain.NewConfigError("MaxConcurrentTotal", domain.ErrInvalidConfig)
	}

	if config.DefaultPerTypeLimit <= 0 {
		return domain.NewConfigError("DefaultPerTypeLimit", domain.ErrInvalidConfig)
	}

	for nodeType, limit := range config.MaxConcurrentPerType {
		if limit <= 0 {
			return domain.NewConfigError("MaxConcurrentPerType["+nodeType+"]", domain.ErrInvalidConfig)
		}
	}

	if rm.totalCount > config.MaxConcurrentTotal {
		return domain.NewResourceError("total", "config_update", domain.ErrInvalidState)
	}

	for nodeType, currentCount := range rm.executing {
		newLimit := rm.getMaxForTypeWithConfig(nodeType, config)
		if currentCount > newLimit {
			return domain.NewResourceError(nodeType, "config_update", domain.ErrInvalidState)
		}
	}

	oldConfig := rm.config
	rm.config = config

	for nodeType, priority := range config.NodePriorities {
		rm.priorityManager.SetNodePriority(nodeType, priority)
	}

	for nodeType, limit := range config.MaxConcurrentPerType {
		priority := config.NodePriorities[nodeType]
		if !rm.poolManager.UpdatePool(nodeType, limit, priority) {
			rm.poolManager.CreatePool(nodeType, limit, priority)
		}
	}

	rm.healthMonitor.UpdateThresholds(config.HealthThresholds)

	rm.logger.Debug("resource manager configuration updated",
		"old_max_total", oldConfig.MaxConcurrentTotal,
		"new_max_total", config.MaxConcurrentTotal)

	return nil
}

func (rm *Adapter) IsHealthy() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.totalCount < 0 || rm.totalCount > rm.config.MaxConcurrentTotal {
		return false
	}

	calculatedTotal := 0
	for nodeType, count := range rm.executing {
		if count < 0 || count > rm.getMaxForType(nodeType) {
			return false
		}
		calculatedTotal += count
	}

	return calculatedTotal == rm.totalCount
}

func (rm *Adapter) GetResourcePools() []ports.ResourcePool {
	return rm.poolManager.GetPools()
}

func (rm *Adapter) GetHealthMetrics() ports.HealthMetrics {
	return rm.healthMonitor.GetHealthMetrics()
}

func (rm *Adapter) initializePools() {
	for nodeType, limit := range rm.config.MaxConcurrentPerType {
		priority := rm.config.NodePriorities[nodeType]
		rm.poolManager.CreatePool(nodeType, limit, priority)
		rm.priorityManager.SetNodePriority(nodeType, priority)
	}
}

func (rm *Adapter) getMaxForType(nodeType string) int {
	return rm.getMaxForTypeWithConfig(nodeType, rm.config)
}

func (rm *Adapter) getMaxForTypeWithConfig(nodeType string, config ports.ResourceConfig) int {
	if limit, exists := config.MaxConcurrentPerType[nodeType]; exists {
		return limit
	}
	return config.DefaultPerTypeLimit
}

func (rm *Adapter) GetResourceStates(ctx context.Context, workflowID string) ([]ports.ResourceState, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	states := make([]ports.ResourceState, 0)

	for nodeType, executingCount := range rm.executing {
		maxForType := rm.getMaxForType(nodeType)
		availableSlots := maxForType - executingCount

		state := ports.ResourceState{
			ID:   fmt.Sprintf("resource-pool-%s", nodeType),
			Type: nodeType,
			State: map[string]interface{}{
				"executing_count":  executingCount,
				"max_capacity":     maxForType,
				"available_slots":  availableSlots,
				"utilization_rate": float64(executingCount) / float64(maxForType),
			},
			ClaimedBy: workflowID,
			ClaimedAt: nil,
			ExpiresAt: nil,
			Metadata: map[string]interface{}{
				"pool_type": "execution_slots",
				"priority":  rm.config.NodePriorities[nodeType],
			},
		}

		if executingCount > 0 {
			now := time.Now()
			state.ClaimedAt = &now
		}

		states = append(states, state)
	}

	return states, nil
}
