package engine

import (
	"sync"
	"time"
)

type CleanupMetrics struct {
	TotalCleanups        int64         `json:"total_cleanups"`
	SuccessfulCleanups   int64         `json:"successful_cleanups"`
	FailedCleanups       int64         `json:"failed_cleanups"`
	AverageCleanupTime   time.Duration `json:"average_cleanup_time"`
	TotalCleanupTime     time.Duration `json:"total_cleanup_time"`
	StateItemsDeleted    int64         `json:"state_items_deleted"`
	QueueItemsDeleted    int64         `json:"queue_items_deleted"`
	ClaimsDeleted        int64         `json:"claims_deleted"`
	AuditLogsDeleted     int64         `json:"audit_logs_deleted"`
	ItemsArchived        int64         `json:"items_archived"`
	LastCleanupTime      *time.Time    `json:"last_cleanup_time,omitempty"`
	mu                   sync.RWMutex  `json:"-"`
}

type CleanupMetricsTracker struct {
	metrics *CleanupMetrics
}

func NewCleanupMetricsTracker() *CleanupMetricsTracker {
	return &CleanupMetricsTracker{
		metrics: &CleanupMetrics{},
	}
}

func (cmt *CleanupMetricsTracker) RecordCleanupStart() {
	cmt.metrics.mu.Lock()
	defer cmt.metrics.mu.Unlock()
	
	cmt.metrics.TotalCleanups++
}

func (cmt *CleanupMetricsTracker) RecordCleanupSuccess(duration time.Duration, results map[string]string) {
	cmt.metrics.mu.Lock()
	defer cmt.metrics.mu.Unlock()
	
	cmt.metrics.SuccessfulCleanups++
	cmt.metrics.TotalCleanupTime += duration
	
	if cmt.metrics.SuccessfulCleanups > 0 {
		cmt.metrics.AverageCleanupTime = cmt.metrics.TotalCleanupTime / time.Duration(cmt.metrics.SuccessfulCleanups)
	}
	
	now := time.Now()
	cmt.metrics.LastCleanupTime = &now
	
	cmt.updateCountersFromResults(results)
}

func (cmt *CleanupMetricsTracker) RecordCleanupFailure(duration time.Duration) {
	cmt.metrics.mu.Lock()
	defer cmt.metrics.mu.Unlock()
	
	cmt.metrics.FailedCleanups++
	cmt.metrics.TotalCleanupTime += duration
}

func (cmt *CleanupMetricsTracker) updateCountersFromResults(results map[string]string) {
	for target, result := range results {
		if result == "" {
			continue
		}
		
		var count int64
		if n, err := parseItemCount(result); err == nil {
			count = n
		}
		
		switch target {
		case "state":
			if isDeleteOperation(result) {
				cmt.metrics.StateItemsDeleted += count
			} else if isArchiveOperation(result) {
				cmt.metrics.ItemsArchived += count
			}
		case "queue":
			if isDeleteOperation(result) {
				cmt.metrics.QueueItemsDeleted += count
			} else if isArchiveOperation(result) {
				cmt.metrics.ItemsArchived += count
			}
		case "claims":
			if isDeleteOperation(result) {
				cmt.metrics.ClaimsDeleted += count
			} else if isArchiveOperation(result) {
				cmt.metrics.ItemsArchived += count
			}
		case "audit":
			if isDeleteOperation(result) {
				cmt.metrics.AuditLogsDeleted += count
			} else if isArchiveOperation(result) {
				cmt.metrics.ItemsArchived += count
			}
		}
	}
}

func (cmt *CleanupMetricsTracker) GetMetrics() CleanupMetrics {
	cmt.metrics.mu.RLock()
	defer cmt.metrics.mu.RUnlock()
	
	return *cmt.metrics
}

func (cmt *CleanupMetricsTracker) Reset() {
	cmt.metrics.mu.Lock()
	defer cmt.metrics.mu.Unlock()
	
	cmt.metrics = &CleanupMetrics{}
}

func parseItemCount(result string) (int64, error) {
	var count int64
	if n, err := extractNumber(result); err == nil {
		count = n
	}
	return count, nil
}

func isDeleteOperation(result string) bool {
	return contains(result, "deleted")
}

func isArchiveOperation(result string) bool {
	return contains(result, "archived")
}

func extractNumber(s string) (int64, error) {
	var num int64
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			digit := int64(s[i] - '0')
			num = num*10 + digit
		} else if num > 0 {
			break
		}
	}
	return num, nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && indexOfSubstring(s, substr) >= 0
}

func indexOfSubstring(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(substr) > len(s) {
		return -1
	}
	
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}