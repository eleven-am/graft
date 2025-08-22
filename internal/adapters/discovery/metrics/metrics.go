package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	DiscoveryOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "graft_discovery_operations_total",
			Help: "Total number of discovery operations performed",
		},
		[]string{"adapter", "operation", "status"},
	)

	DiscoveryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "graft_discovery_duration_seconds",
			Help:    "Duration of discovery operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"adapter", "operation"},
	)

	DiscoveryPeersFound = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "graft_discovery_peers_found",
			Help: "Number of peers found by discovery",
		},
		[]string{"adapter"},
	)

	DiscoveryFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "graft_discovery_failures_total",
			Help: "Total number of discovery failures",
		},
		[]string{"adapter", "error_type"},
	)

	DiscoveryBackoffDuration = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "graft_discovery_backoff_duration_seconds",
			Help: "Current backoff duration for discovery failures",
		},
		[]string{"adapter"},
	)

	HealthCheckOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "graft_health_check_operations_total",
			Help: "Total number of health check operations performed",
		},
		[]string{"adapter", "status"},
	)

	HealthCheckDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "graft_health_check_duration_seconds",
			Help:    "Duration of health check operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"adapter"},
	)

	HealthyPeersCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "graft_healthy_peers_count",
			Help: "Number of healthy peers",
		},
		[]string{"adapter"},
	)

	UnhealthyPeersCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "graft_unhealthy_peers_count",
			Help: "Number of unhealthy peers",
		},
		[]string{"adapter"},
	)
)

func RecordDiscoveryOperation(adapter, operation, status string) {
	DiscoveryOperationsTotal.WithLabelValues(adapter, operation, status).Inc()
}

func RecordDiscoveryDuration(adapter, operation string, duration float64) {
	DiscoveryDuration.WithLabelValues(adapter, operation).Observe(duration)
}

func SetPeersFound(adapter string, count int) {
	DiscoveryPeersFound.WithLabelValues(adapter).Set(float64(count))
}

func RecordDiscoveryFailure(adapter, errorType string) {
	DiscoveryFailuresTotal.WithLabelValues(adapter, errorType).Inc()
}

func SetBackoffDuration(adapter string, duration float64) {
	DiscoveryBackoffDuration.WithLabelValues(adapter).Set(duration)
}

func RecordHealthCheck(adapter, status string) {
	HealthCheckOperationsTotal.WithLabelValues(adapter, status).Inc()
}

func RecordHealthCheckDuration(adapter string, duration float64) {
	HealthCheckDuration.WithLabelValues(adapter).Observe(duration)
}

func SetHealthyPeersCount(adapter string, count int) {
	HealthyPeersCount.WithLabelValues(adapter).Set(float64(count))
}

func SetUnhealthyPeersCount(adapter string, count int) {
	UnhealthyPeersCount.WithLabelValues(adapter).Set(float64(count))
}