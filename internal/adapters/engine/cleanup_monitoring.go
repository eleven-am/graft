package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type CleanupMonitor struct {
	orchestrator     *CleanupOrchestrator
	scheduler        *CleanupScheduler
	metricsTracker   *CleanupMetricsTracker
	logger           *slog.Logger
	healthThresholds HealthThresholds
}

type HealthThresholds struct {
	MaxFailureRate     float64       `json:"max_failure_rate"`
	MaxPendingCleanups int           `json:"max_pending_cleanups"`
	MaxRetryQueueSize  int           `json:"max_retry_queue_size"`
	MaxDeadLetterSize  int           `json:"max_dead_letter_size"`
	MaxCleanupDuration time.Duration `json:"max_cleanup_duration"`
	AlertOnThresholds  bool          `json:"alert_on_thresholds"`
}

type CleanupHealthStatus struct {
	Overall         string         `json:"overall"`
	Timestamp       time.Time      `json:"timestamp"`
	Metrics         CleanupMetrics `json:"metrics"`
	PendingCleanups int            `json:"pending_cleanups"`
	RetryQueueSize  int            `json:"retry_queue_size"`
	DeadLetterSize  int            `json:"dead_letter_size"`
	FailureRate     float64        `json:"failure_rate"`
	Issues          []HealthIssue  `json:"issues,omitempty"`
	Recommendations []string       `json:"recommendations,omitempty"`
}

type HealthIssue struct {
	Type         string    `json:"type"`
	Severity     string    `json:"severity"`
	Description  string    `json:"description"`
	DetectedAt   time.Time `json:"detected_at"`
	Threshold    string    `json:"threshold,omitempty"`
	CurrentValue string    `json:"current_value,omitempty"`
}

type CleanupDashboard struct {
	GeneratedAt     time.Time           `json:"generated_at"`
	HealthStatus    CleanupHealthStatus `json:"health_status"`
	RecentCleanups  []CleanupSummary    `json:"recent_cleanups"`
	TrendingMetrics TrendingMetrics     `json:"trending_metrics"`
	SystemLoad      SystemLoadInfo      `json:"system_load"`
	Alerts          []CleanupAlert      `json:"alerts"`
}

type CleanupSummary struct {
	WorkflowID   string        `json:"workflow_id"`
	Status       string        `json:"status"`
	Duration     time.Duration `json:"duration"`
	Operations   int           `json:"operations"`
	ItemsCleaned int           `json:"items_cleaned"`
	CompletedAt  time.Time     `json:"completed_at"`
}

type TrendingMetrics struct {
	HourlyCleanupRate []int           `json:"hourly_cleanup_rate"`
	DailySuccessRate  []float64       `json:"daily_success_rate"`
	WeeklyVolumeStats []int64         `json:"weekly_volume_stats"`
	PerformanceTrends []time.Duration `json:"performance_trends"`
}

type SystemLoadInfo struct {
	ActiveCleanups     int     `json:"active_cleanups"`
	QueueUtilization   float64 `json:"queue_utilization"`
	StorageUtilization float64 `json:"storage_utilization"`
	ResourcePressure   string  `json:"resource_pressure"`
}

type CleanupAlert struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Severity    string    `json:"severity"`
	Message     string    `json:"message"`
	TriggeredAt time.Time `json:"triggered_at"`
	WorkflowID  string    `json:"workflow_id,omitempty"`
	Resolved    bool      `json:"resolved"`
}

func NewCleanupMonitor(orchestrator *CleanupOrchestrator, scheduler *CleanupScheduler, logger *slog.Logger) *CleanupMonitor {
	return &CleanupMonitor{
		orchestrator:   orchestrator,
		scheduler:      scheduler,
		metricsTracker: NewCleanupMetricsTracker(),
		logger:         logger.With("component", "cleanup_monitor"),
		healthThresholds: HealthThresholds{
			MaxFailureRate:     10.0,
			MaxPendingCleanups: 100,
			MaxRetryQueueSize:  50,
			MaxDeadLetterSize:  10,
			MaxCleanupDuration: time.Minute * 5,
			AlertOnThresholds:  true,
		},
	}
}

func (cm *CleanupMonitor) GetHealthStatus() CleanupHealthStatus {
	metrics := cm.orchestrator.GetMetrics()
	pendingCleanups := len(cm.scheduler.GetPendingCleanups())
	retryQueueSize := len(cm.scheduler.GetRetryQueue())
	deadLetterSize := len(cm.scheduler.GetDeadLetterQueue())

	failureRate := cm.calculateFailureRate(metrics)

	status := CleanupHealthStatus{
		Overall:         "healthy",
		Timestamp:       time.Now(),
		Metrics:         metrics,
		PendingCleanups: pendingCleanups,
		RetryQueueSize:  retryQueueSize,
		DeadLetterSize:  deadLetterSize,
		FailureRate:     failureRate,
		Issues:          []HealthIssue{},
		Recommendations: []string{},
	}

	cm.evaluateHealth(&status)
	return status
}

func (cm *CleanupMonitor) GenerateDashboard() CleanupDashboard {
	healthStatus := cm.GetHealthStatus()

	dashboard := CleanupDashboard{
		GeneratedAt:     time.Now(),
		HealthStatus:    healthStatus,
		RecentCleanups:  cm.getRecentCleanups(),
		TrendingMetrics: cm.getTrendingMetrics(),
		SystemLoad:      cm.getSystemLoadInfo(),
		Alerts:          cm.getActiveAlerts(),
	}

	return dashboard
}

func (cm *CleanupMonitor) PerformHealthCheck(ctx context.Context) error {
	cm.logger.Info("performing cleanup system health check")

	status := cm.GetHealthStatus()

	if status.Overall != "healthy" {
		cm.logger.Warn("cleanup system health issues detected",
			"overall_status", status.Overall,
			"issues_count", len(status.Issues),
			"failure_rate", status.FailureRate)

		for _, issue := range status.Issues {
			cm.logger.Warn("health issue detected",
				"type", issue.Type,
				"severity", issue.Severity,
				"description", issue.Description)
		}

		if cm.healthThresholds.AlertOnThresholds {
			return domain.Error{
				Type:    domain.ErrorTypeUnavailable,
				Message: "cleanup system health check failed",
				Details: map[string]interface{}{
					"issues_detected": len(status.Issues),
					"overall_status":  status.Overall,
					"failure_rate":    status.FailureRate,
				},
			}
		}
	}

	cm.logger.Info("cleanup system health check completed",
		"status", status.Overall,
		"pending_cleanups", status.PendingCleanups,
		"failure_rate", status.FailureRate)

	return nil
}

func (cm *CleanupMonitor) GetPerformanceReport(duration time.Duration) PerformanceReport {
	metrics := cm.orchestrator.GetMetrics()

	report := PerformanceReport{
		Period:              duration,
		GeneratedAt:         time.Now(),
		TotalCleanups:       metrics.TotalCleanups,
		SuccessfulCleanups:  metrics.SuccessfulCleanups,
		FailedCleanups:      metrics.FailedCleanups,
		AverageCleanupTime:  metrics.AverageCleanupTime,
		TotalItemsProcessed: metrics.StateItemsDeleted + metrics.QueueItemsDeleted + metrics.ClaimsDeleted + metrics.AuditLogsDeleted,
		TotalItemsArchived:  metrics.ItemsArchived,
		ThroughputPerHour:   cm.calculateThroughput(metrics, duration),
		PeakCleanupTime:     cm.getPeakCleanupTime(),
		ResourceUtilization: cm.getResourceUtilization(),
	}

	return report
}

func (cm *CleanupMonitor) CreateCleanupTroubleshootingGuide() TroubleshootingGuide {
	return TroubleshootingGuide{
		GeneratedAt: time.Now(),
		CommonIssues: []TroubleshootingItem{
			{
				Issue:     "High failure rate",
				Symptoms:  []string{"Multiple cleanup operations failing", "Errors in logs", "Growing retry queue"},
				Causes:    []string{"Storage connectivity issues", "Permission problems", "Resource exhaustion"},
				Solutions: []string{"Check storage health", "Verify permissions", "Scale resources", "Review error logs"},
			},
			{
				Issue:     "Cleanup performance degradation",
				Symptoms:  []string{"Increasing cleanup duration", "Queue backlog", "High resource usage"},
				Causes:    []string{"Large workflow state", "Database performance", "Concurrent cleanup load"},
				Solutions: []string{"Optimize batch sizes", "Tune database", "Implement throttling", "Archive old data"},
			},
			{
				Issue:     "Dead letter queue growth",
				Symptoms:  []string{"Items accumulating in dead letter queue", "Repeated failures"},
				Causes:    []string{"Invalid workflow state", "Permission issues", "Corrupted data"},
				Solutions: []string{"Manual investigation", "Fix data issues", "Update permissions", "Clear invalid entries"},
			},
		},
		BestPractices: []string{
			"Monitor cleanup metrics regularly",
			"Set appropriate retention policies",
			"Use archival for important data",
			"Implement gradual cleanup for large workflows",
			"Test cleanup operations in staging environment",
			"Keep audit trails for compliance",
		},
		MonitoringChecks: []string{
			"Failure rate < 5%",
			"Average cleanup time < 30 seconds",
			"Dead letter queue size < 10",
			"Pending cleanup queue manageable",
			"Storage utilization within limits",
		},
	}
}

func (cm *CleanupMonitor) calculateFailureRate(metrics CleanupMetrics) float64 {
	if metrics.TotalCleanups == 0 {
		return 0.0
	}
	return (float64(metrics.FailedCleanups) / float64(metrics.TotalCleanups)) * 100.0
}

func (cm *CleanupMonitor) evaluateHealth(status *CleanupHealthStatus) {
	if status.FailureRate > cm.healthThresholds.MaxFailureRate {
		status.Overall = "degraded"
		status.Issues = append(status.Issues, HealthIssue{
			Type:         "high_failure_rate",
			Severity:     "warning",
			Description:  fmt.Sprintf("Cleanup failure rate (%.1f%%) exceeds threshold", status.FailureRate),
			DetectedAt:   time.Now(),
			Threshold:    fmt.Sprintf("%.1f%%", cm.healthThresholds.MaxFailureRate),
			CurrentValue: fmt.Sprintf("%.1f%%", status.FailureRate),
		})
		status.Recommendations = append(status.Recommendations, "Investigate recent cleanup failures")
	}

	if status.PendingCleanups > cm.healthThresholds.MaxPendingCleanups {
		status.Overall = "degraded"
		status.Issues = append(status.Issues, HealthIssue{
			Type:         "high_pending_count",
			Severity:     "warning",
			Description:  "High number of pending cleanups may indicate processing bottleneck",
			DetectedAt:   time.Now(),
			Threshold:    fmt.Sprintf("%d", cm.healthThresholds.MaxPendingCleanups),
			CurrentValue: fmt.Sprintf("%d", status.PendingCleanups),
		})
		status.Recommendations = append(status.Recommendations, "Consider increasing cleanup worker count")
	}

	if status.DeadLetterSize > cm.healthThresholds.MaxDeadLetterSize {
		status.Overall = "unhealthy"
		status.Issues = append(status.Issues, HealthIssue{
			Type:         "dead_letter_overflow",
			Severity:     "critical",
			Description:  "Dead letter queue size indicates persistent cleanup failures",
			DetectedAt:   time.Now(),
			Threshold:    fmt.Sprintf("%d", cm.healthThresholds.MaxDeadLetterSize),
			CurrentValue: fmt.Sprintf("%d", status.DeadLetterSize),
		})
		status.Recommendations = append(status.Recommendations, "Investigate and resolve dead letter queue items")
	}
}

func (cm *CleanupMonitor) getRecentCleanups() []CleanupSummary {
	return []CleanupSummary{}
}

func (cm *CleanupMonitor) getTrendingMetrics() TrendingMetrics {
	return TrendingMetrics{}
}

func (cm *CleanupMonitor) getSystemLoadInfo() SystemLoadInfo {
	return SystemLoadInfo{
		ActiveCleanups:     0,
		QueueUtilization:   0.0,
		StorageUtilization: 0.0,
		ResourcePressure:   "low",
	}
}

func (cm *CleanupMonitor) getActiveAlerts() []CleanupAlert {
	return []CleanupAlert{}
}

func (cm *CleanupMonitor) calculateThroughput(metrics CleanupMetrics, duration time.Duration) float64 {
	if duration == 0 {
		return 0.0
	}
	hours := duration.Hours()
	return float64(metrics.TotalCleanups) / hours
}

func (cm *CleanupMonitor) getPeakCleanupTime() time.Duration {
	return time.Second * 30
}

func (cm *CleanupMonitor) getResourceUtilization() float64 {
	return 0.0
}

type PerformanceReport struct {
	Period              time.Duration `json:"period"`
	GeneratedAt         time.Time     `json:"generated_at"`
	TotalCleanups       int64         `json:"total_cleanups"`
	SuccessfulCleanups  int64         `json:"successful_cleanups"`
	FailedCleanups      int64         `json:"failed_cleanups"`
	AverageCleanupTime  time.Duration `json:"average_cleanup_time"`
	TotalItemsProcessed int64         `json:"total_items_processed"`
	TotalItemsArchived  int64         `json:"total_items_archived"`
	ThroughputPerHour   float64       `json:"throughput_per_hour"`
	PeakCleanupTime     time.Duration `json:"peak_cleanup_time"`
	ResourceUtilization float64       `json:"resource_utilization"`
}

type TroubleshootingGuide struct {
	GeneratedAt      time.Time             `json:"generated_at"`
	CommonIssues     []TroubleshootingItem `json:"common_issues"`
	BestPractices    []string              `json:"best_practices"`
	MonitoringChecks []string              `json:"monitoring_checks"`
}

type TroubleshootingItem struct {
	Issue     string   `json:"issue"`
	Symptoms  []string `json:"symptoms"`
	Causes    []string `json:"causes"`
	Solutions []string `json:"solutions"`
}
