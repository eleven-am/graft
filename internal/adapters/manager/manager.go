package manager

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type ResourceManager struct {
	config       ports.ResourceConfig
	mu           sync.RWMutex
	logger       *slog.Logger
	executing    map[string]int
	totalCount   int
}

func NewResourceManager(config ports.ResourceConfig, logger *slog.Logger) *ResourceManager {
	if logger == nil {
		logger = slog.Default()
	}

	rm := &ResourceManager{
		config:    config,
		executing: make(map[string]int),
		logger: logger.With(
			"component", "resource-manager",
			"adapter", "manager",
		),
	}

	rm.logger.Info("resource manager initialized",
		"max_concurrent_total", config.MaxConcurrentTotal,
		"default_per_type_limit", config.DefaultPerTypeLimit,
		"per_type_limits", config.MaxConcurrentPerType,
	)

	return rm
}

func (rm *ResourceManager) CanExecuteNode(nodeType string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.totalCount >= rm.config.MaxConcurrentTotal {
		rm.logger.Debug("cannot execute node - total capacity reached",
			"node_type", nodeType,
			"total_executing", rm.totalCount,
			"max_total", rm.config.MaxConcurrentTotal,
		)
		return false
	}

	currentCount := rm.executing[nodeType]
	maxForType := rm.getMaxForType(nodeType)

	if currentCount >= maxForType {
		rm.logger.Debug("cannot execute node - type capacity reached",
			"node_type", nodeType,
			"type_executing", currentCount,
			"max_for_type", maxForType,
		)
		return false
	}

	rm.logger.Debug("node can be executed",
		"node_type", nodeType,
		"type_executing", currentCount,
		"max_for_type", maxForType,
		"total_executing", rm.totalCount,
		"max_total", rm.config.MaxConcurrentTotal,
	)

	return true
}

func (rm *ResourceManager) AcquireNode(nodeType string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.totalCount >= rm.config.MaxConcurrentTotal {
		return domain.NewResourceExhaustionError("concurrent_executions", fmt.Sprintf("%d/%d", rm.totalCount, rm.config.MaxConcurrentTotal))
	}

	currentCount := rm.executing[nodeType]
	maxForType := rm.getMaxForType(nodeType)

	if currentCount >= maxForType {
		return domain.NewResourceExhaustionError(fmt.Sprintf("node_type_%s", nodeType), fmt.Sprintf("%d/%d", currentCount, maxForType))
	}

	rm.executing[nodeType] = currentCount + 1
	rm.totalCount++

	rm.logger.Info("node resource acquired",
		"node_type", nodeType,
		"type_executing", rm.executing[nodeType],
		"max_for_type", maxForType,
		"total_executing", rm.totalCount,
		"max_total", rm.config.MaxConcurrentTotal,
	)

	return nil
}

func (rm *ResourceManager) ReleaseNode(nodeType string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	currentCount := rm.executing[nodeType]
	if currentCount <= 0 {
		rm.logger.Error("attempted to release node with no executions",
			"node_type", nodeType,
			"current_count", currentCount,
		)
		return domain.NewValidationError("node_type", fmt.Sprintf("no executions to release for '%s'", nodeType))
	}

	rm.executing[nodeType] = currentCount - 1
	rm.totalCount--

	if rm.executing[nodeType] == 0 {
		delete(rm.executing, nodeType)
	}

	rm.logger.Info("node resource released",
		"node_type", nodeType,
		"type_executing", rm.executing[nodeType],
		"total_executing", rm.totalCount,
	)

	return nil
}

func (rm *ResourceManager) GetExecutionStats() ports.ExecutionStats {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

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

	availableSlots := rm.config.MaxConcurrentTotal - rm.totalCount

	stats := ports.ExecutionStats{
		TotalExecuting:  rm.totalCount,
		PerTypeExecuting: perTypeExecuting,
		TotalCapacity:   rm.config.MaxConcurrentTotal,
		PerTypeCapacity: perTypeCapacity,
		AvailableSlots:  availableSlots,
	}

	rm.logger.Debug("execution stats requested",
		"total_executing", stats.TotalExecuting,
		"total_capacity", stats.TotalCapacity,
		"available_slots", stats.AvailableSlots,
	)

	return stats
}

func (rm *ResourceManager) UpdateConfig(config ports.ResourceConfig) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if config.MaxConcurrentTotal <= 0 {
		return domain.NewValidationError("MaxConcurrentTotal", "must be positive")
	}

	if config.DefaultPerTypeLimit <= 0 {
		return domain.NewValidationError("DefaultPerTypeLimit", "must be positive")
	}

	for nodeType, limit := range config.MaxConcurrentPerType {
		if limit <= 0 {
			return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "MaxConcurrentPerType must be positive",
			Details: map[string]interface{}{
				"node_type": nodeType,
				"limit":     limit,
			},
		}
		}
	}

	if rm.totalCount > config.MaxConcurrentTotal {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "cannot reduce MaxConcurrentTotal below current executions",
			Details: map[string]interface{}{
				"new_limit":         config.MaxConcurrentTotal,
				"current_executing": rm.totalCount,
			},
		}
	}

	for nodeType, currentCount := range rm.executing {
		newLimit := rm.getMaxForTypeWithConfig(nodeType, config)
		if currentCount > newLimit {
			return domain.Error{
				Type:    domain.ErrorTypeConflict,
				Message: "cannot reduce node type limit below current executions",
				Details: map[string]interface{}{
					"node_type":         nodeType,
					"new_limit":         newLimit,
					"current_executing": currentCount,
				},
			}
		}
	}

	oldConfig := rm.config
	rm.config = config

	rm.logger.Info("resource manager configuration updated",
		"old_max_total", oldConfig.MaxConcurrentTotal,
		"new_max_total", config.MaxConcurrentTotal,
		"old_default_per_type", oldConfig.DefaultPerTypeLimit,
		"new_default_per_type", config.DefaultPerTypeLimit,
	)

	return nil
}

func (rm *ResourceManager) IsHealthy() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.totalCount < 0 {
		rm.logger.Warn("resource manager unhealthy: negative total count", "total_count", rm.totalCount)
		return false
	}

	if rm.totalCount > rm.config.MaxConcurrentTotal {
		rm.logger.Warn("resource manager unhealthy: total count exceeds maximum",
			"total_count", rm.totalCount,
			"max_total", rm.config.MaxConcurrentTotal)
		return false
	}

	calculatedTotal := 0
	for nodeType, count := range rm.executing {
		if count < 0 {
			rm.logger.Warn("resource manager unhealthy: negative count for node type",
				"node_type", nodeType,
				"count", count)
			return false
		}
		
		maxForType := rm.getMaxForType(nodeType)
		if count > maxForType {
			rm.logger.Warn("resource manager unhealthy: type count exceeds maximum",
				"node_type", nodeType,
				"count", count,
				"max_for_type", maxForType)
			return false
		}
		
		if count == maxForType && maxForType > 0 {
			rm.logger.Warn("resource manager unhealthy: type at full capacity",
				"node_type", nodeType,
				"count", count,
				"max_for_type", maxForType)
			return false
		}
		
		calculatedTotal += count
	}

	if calculatedTotal != rm.totalCount {
		rm.logger.Warn("resource manager unhealthy: total count mismatch",
			"total_count", rm.totalCount,
			"calculated_total", calculatedTotal)
		return false
	}

	if rm.config.MaxConcurrentTotal > 0 {
		totalUtilization := float64(rm.totalCount) / float64(rm.config.MaxConcurrentTotal) * 100
		if totalUtilization > 90.0 {
			rm.logger.Warn("resource manager unhealthy: total utilization too high",
				"total_utilization", totalUtilization,
				"total_executing", rm.totalCount,
				"max_total", rm.config.MaxConcurrentTotal)
			return false
		}
	}

	rm.logger.Debug("resource manager health check passed",
		"total_executing", rm.totalCount,
		"max_total", rm.config.MaxConcurrentTotal)

	return true
}

func (rm *ResourceManager) getMaxForType(nodeType string) int {
	return rm.getMaxForTypeWithConfig(nodeType, rm.config)
}

func (rm *ResourceManager) getMaxForTypeWithConfig(nodeType string, config ports.ResourceConfig) int {
	if limit, exists := config.MaxConcurrentPerType[nodeType]; exists {
		return limit
	}
	return config.DefaultPerTypeLimit
}