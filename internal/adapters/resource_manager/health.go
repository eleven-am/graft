package resource_manager

import (
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type HealthMonitor struct {
	metrics    map[string]*NodeHealthData
	thresholds ports.HealthConfig
	mu         sync.RWMutex
}

type NodeHealthData struct {
	totalRequests   int64
	failedRequests  int64
	responseTimes   []time.Duration
	lastFailure     *time.Time
	lastUpdated     time.Time
	utilizationRate float64
}

func NewHealthMonitor(thresholds ports.HealthConfig) *HealthMonitor {
	return &HealthMonitor{
		metrics:    make(map[string]*NodeHealthData),
		thresholds: thresholds,
	}
}

func (hm *HealthMonitor) RecordRequest(nodeType string, duration time.Duration, success bool) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	data, exists := hm.metrics[nodeType]
	if !exists {
		data = &NodeHealthData{
			responseTimes: make([]time.Duration, 0, 100),
			lastUpdated:   time.Now(),
		}
		hm.metrics[nodeType] = data
	}

	data.totalRequests++
	if !success {
		data.failedRequests++
		now := time.Now()
		data.lastFailure = &now
	}

	data.responseTimes = append(data.responseTimes, duration)
	if len(data.responseTimes) > 100 {
		data.responseTimes = data.responseTimes[1:]
	}

	data.lastUpdated = time.Now()
}

func (hm *HealthMonitor) GetHealthMetrics() ports.HealthMetrics {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	perTypeMetrics := make(map[string]ports.NodeHealthMetric)
	overallHealthy := true
	overallDegraded := false

	for nodeType, data := range hm.metrics {
		avgResponseTime := hm.calculateAvgResponseTime(data.responseTimes)
		successRate := hm.calculateSuccessRate(data.totalRequests, data.failedRequests)

		metric := ports.NodeHealthMetric{
			AvgResponseTime: avgResponseTime,
			SuccessRate:     successRate,
			UtilizationRate: data.utilizationRate,
			TotalRequests:   data.totalRequests,
			FailedRequests:  data.failedRequests,
			LastFailure:     data.lastFailure,
		}

		perTypeMetrics[nodeType] = metric

		if avgResponseTime > hm.thresholds.MaxResponseTime ||
			successRate < hm.thresholds.MinSuccessRate {
			overallHealthy = false
			overallDegraded = true
		}
	}

	var overallHealth ports.HealthStatus
	if overallHealthy {
		overallHealth = ports.HealthStatusHealthy
	} else if overallDegraded {
		overallHealth = ports.HealthStatusDegraded
	} else {
		overallHealth = ports.HealthStatusUnhealthy
	}

	return ports.HealthMetrics{
		PerTypeMetrics: perTypeMetrics,
		OverallHealth:  overallHealth,
		LastUpdated:    time.Now(),
	}
}

func (hm *HealthMonitor) IsNodeHealthy(nodeType string) bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	data, exists := hm.metrics[nodeType]
	if !exists {
		return true
	}

	avgResponseTime := hm.calculateAvgResponseTime(data.responseTimes)
	successRate := hm.calculateSuccessRate(data.totalRequests, data.failedRequests)

	return avgResponseTime <= hm.thresholds.MaxResponseTime &&
		successRate >= hm.thresholds.MinSuccessRate
}

func (hm *HealthMonitor) UpdateThresholds(thresholds ports.HealthConfig) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.thresholds = thresholds
}

func (hm *HealthMonitor) calculateAvgResponseTime(responseTimes []time.Duration) time.Duration {
	if len(responseTimes) == 0 {
		return 0
	}

	var total time.Duration
	for _, duration := range responseTimes {
		total += duration
	}

	return total / time.Duration(len(responseTimes))
}

func (hm *HealthMonitor) calculateSuccessRate(total, failed int64) float64 {
	if total == 0 {
		return 1.0
	}

	return float64(total-failed) / float64(total)
}

func (hm *HealthMonitor) GetNodeUtilization(nodeType string, currentActive, maxCapacity int) float64 {
	if maxCapacity == 0 {
		return 0.0
	}

	utilization := float64(currentActive) / float64(maxCapacity)

	hm.mu.Lock()
	data, exists := hm.metrics[nodeType]
	if !exists {
		data = &NodeHealthData{}
		hm.metrics[nodeType] = data
	}

	data.utilizationRate = utilization
	hm.mu.Unlock()

	return utilization
}
