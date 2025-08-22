package manager

import (
	"fmt"
	"strings"

	"github.com/eleven-am/graft/internal/ports"
)

func (rm *ResourceManager) GetDetailedStats() DetailedStats {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	stats := rm.GetExecutionStats()

	totalUtilization := float64(stats.TotalExecuting) / float64(stats.TotalCapacity) * 100

	perTypeUtilization := make(map[string]float64)
	for nodeType, capacity := range stats.PerTypeCapacity {
		executing := stats.PerTypeExecuting[nodeType]
		if capacity > 0 {
			perTypeUtilization[nodeType] = float64(executing) / float64(capacity) * 100
		}
	}

	return DetailedStats{
		ExecutionStats:      stats,
		TotalUtilization:    totalUtilization,
		PerTypeUtilization:  perTypeUtilization,
		ActiveNodeTypes:     len(stats.PerTypeExecuting),
		ConfiguredNodeTypes: len(stats.PerTypeCapacity),
	}
}

func (rm *ResourceManager) FormatStats() string {
	stats := rm.GetDetailedStats()
	
	return fmt.Sprintf(`Resource Manager Statistics:
  Total: %d/%d (%.1f%% utilization)
  Available Slots: %d
  Active Node Types: %d
  Configured Node Types: %d
%s`, 
		stats.TotalExecuting, stats.TotalCapacity, stats.TotalUtilization,
		stats.AvailableSlots, stats.ActiveNodeTypes, stats.ConfiguredNodeTypes,
		formatPerTypeStats(stats))
}

func formatPerTypeStats(stats DetailedStats) string {
	if len(stats.PerTypeExecuting) == 0 {
		return ""
	}
	
	var sb strings.Builder
	sb.WriteString("  Per-Type Execution:\n")
	for nodeType, executing := range stats.PerTypeExecuting {
		capacity := stats.PerTypeCapacity[nodeType]
		utilization := stats.PerTypeUtilization[nodeType]
		sb.WriteString(fmt.Sprintf("    %s: %d/%d (%.1f%%)\n", 
			nodeType, executing, capacity, utilization))
	}
	return sb.String()
}


func (rm *ResourceManager) GetResourcePressure() float64 {
	stats := rm.GetDetailedStats()
	return stats.TotalUtilization / 100.0
}

type DetailedStats struct {
	ports.ExecutionStats
	TotalUtilization     float64            `json:"total_utilization"`
	PerTypeUtilization   map[string]float64 `json:"per_type_utilization"`
	ActiveNodeTypes      int                `json:"active_node_types"`
	ConfiguredNodeTypes  int                `json:"configured_node_types"`
}