package ports

import (
	"context"
	"time"
)

type ResourceManagerPort interface {
	CanExecuteNode(nodeType string) bool
	AcquireNode(nodeType string, priority ...int) error
	ReleaseNode(nodeType string) error
	GetExecutionStats() ExecutionStats
	UpdateConfig(config ResourceConfig) error
	IsHealthy() bool
	GetResourcePools() []ResourcePool
	GetHealthMetrics() HealthMetrics
	GetResourceStates(ctx context.Context, workflowID string) ([]ResourceState, error)
}

type ResourceConfig struct {
	MaxConcurrentTotal   int            `json:"max_concurrent_total"`
	MaxConcurrentPerType map[string]int `json:"max_concurrent_per_type"`
	DefaultPerTypeLimit  int            `json:"default_per_type_limit"`
	NodePriorities       map[string]int `json:"node_priorities"`
	HealthThresholds     HealthConfig   `json:"health_thresholds"`
}

type HealthConfig struct {
	MaxResponseTime    time.Duration `json:"max_response_time"`
	MinSuccessRate     float64       `json:"min_success_rate"`
	MaxUtilizationRate float64       `json:"max_utilization_rate"`
}

type ExecutionStats struct {
	TotalExecuting   int            `json:"total_executing"`
	PerTypeExecuting map[string]int `json:"per_type_executing"`
	TotalCapacity    int            `json:"total_capacity"`
	PerTypeCapacity  map[string]int `json:"per_type_capacity"`
	AvailableSlots   int            `json:"available_slots"`
	ResourcePools    []ResourcePool `json:"resource_pools"`
	HealthMetrics    HealthMetrics  `json:"health_metrics"`
}

type ResourcePool struct {
	NodeType     string    `json:"node_type"`
	TotalSlots   int       `json:"total_slots"`
	ActiveSlots  int       `json:"active_slots"`
	IdleSlots    int       `json:"idle_slots"`
	Priority     int       `json:"priority"`
	LastActivity time.Time `json:"last_activity"`
}

type HealthMetrics struct {
	PerTypeMetrics map[string]NodeHealthMetric `json:"per_type_metrics"`
	OverallHealth  HealthStatus                `json:"overall_health"`
	LastUpdated    time.Time                   `json:"last_updated"`
}

type NodeHealthMetric struct {
	AvgResponseTime time.Duration `json:"avg_response_time"`
	SuccessRate     float64       `json:"success_rate"`
	UtilizationRate float64       `json:"utilization_rate"`
	TotalRequests   int64         `json:"total_requests"`
	FailedRequests  int64         `json:"failed_requests"`
	LastFailure     *time.Time    `json:"last_failure,omitempty"`
}

type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

type ResourceState struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	State     interface{}            `json:"state"`
	ClaimedBy string                 `json:"claimed_by,omitempty"`
	ClaimedAt *time.Time             `json:"claimed_at,omitempty"`
	ExpiresAt *time.Time             `json:"expires_at,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}
