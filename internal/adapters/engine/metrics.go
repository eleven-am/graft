package engine

import (
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type MetricsTracker struct {
	panicMetrics   PanicMetricsData
	handlerMetrics HandlerMetricsData
	mu             sync.RWMutex
}

type PanicMetricsData struct {
	TotalPanics      int64
	PanicsLastHour   []time.Time
	RecoveryTimes    []time.Duration
	LastPanicAt      *time.Time
}

type HandlerMetricsData struct {
	CompletionHandlersExecuted int64
	ErrorHandlersExecuted      int64
	HandlerFailures            int64
	HandlerTimes               []time.Duration
	HandlerTimeouts            int64
}

func NewMetricsTracker() *MetricsTracker {
	return &MetricsTracker{
		panicMetrics: PanicMetricsData{
			PanicsLastHour: make([]time.Time, 0),
			RecoveryTimes:  make([]time.Duration, 0, 100),
		},
		handlerMetrics: HandlerMetricsData{
			HandlerTimes: make([]time.Duration, 0, 1000),
		},
	}
}

func (mt *MetricsTracker) RecordPanic(recoveryTime time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	now := time.Now()
	mt.panicMetrics.TotalPanics++
	mt.panicMetrics.LastPanicAt = &now
	mt.panicMetrics.PanicsLastHour = append(mt.panicMetrics.PanicsLastHour, now)
	
	mt.panicMetrics.RecoveryTimes = append(mt.panicMetrics.RecoveryTimes, recoveryTime)
	if len(mt.panicMetrics.RecoveryTimes) > 100 {
		mt.panicMetrics.RecoveryTimes = mt.panicMetrics.RecoveryTimes[1:]
	}
	
	mt.cleanupOldPanics()
}

func (mt *MetricsTracker) RecordCompletionHandler(duration time.Duration, success bool) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.handlerMetrics.CompletionHandlersExecuted++
	if !success {
		mt.handlerMetrics.HandlerFailures++
	}
	
	mt.handlerMetrics.HandlerTimes = append(mt.handlerMetrics.HandlerTimes, duration)
	if len(mt.handlerMetrics.HandlerTimes) > 1000 {
		mt.handlerMetrics.HandlerTimes = mt.handlerMetrics.HandlerTimes[1:]
	}
}

func (mt *MetricsTracker) RecordErrorHandler(duration time.Duration, success bool) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.handlerMetrics.ErrorHandlersExecuted++
	if !success {
		mt.handlerMetrics.HandlerFailures++
	}
	
	mt.handlerMetrics.HandlerTimes = append(mt.handlerMetrics.HandlerTimes, duration)
	if len(mt.handlerMetrics.HandlerTimes) > 1000 {
		mt.handlerMetrics.HandlerTimes = mt.handlerMetrics.HandlerTimes[1:]
	}
}

func (mt *MetricsTracker) RecordHandlerTimeout() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	
	mt.handlerMetrics.HandlerTimeouts++
}

func (mt *MetricsTracker) GetPanicMetrics() ports.PanicMetrics {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	mt.cleanupOldPanics()
	
	var avgRecoveryTime time.Duration
	if len(mt.panicMetrics.RecoveryTimes) > 0 {
		var total time.Duration
		for _, t := range mt.panicMetrics.RecoveryTimes {
			total += t
		}
		avgRecoveryTime = total / time.Duration(len(mt.panicMetrics.RecoveryTimes))
	}

	return ports.PanicMetrics{
		TotalPanics:         mt.panicMetrics.TotalPanics,
		PanicsLastHour:      int64(len(mt.panicMetrics.PanicsLastHour)),
		AverageRecoveryTime: avgRecoveryTime,
		LastPanicAt:         mt.panicMetrics.LastPanicAt,
	}
}

func (mt *MetricsTracker) GetHandlerMetrics() ports.HandlerMetrics {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	var avgHandlerTime time.Duration
	if len(mt.handlerMetrics.HandlerTimes) > 0 {
		var total time.Duration
		for _, t := range mt.handlerMetrics.HandlerTimes {
			total += t
		}
		avgHandlerTime = total / time.Duration(len(mt.handlerMetrics.HandlerTimes))
	}

	return ports.HandlerMetrics{
		CompletionHandlersExecuted: mt.handlerMetrics.CompletionHandlersExecuted,
		ErrorHandlersExecuted:      mt.handlerMetrics.ErrorHandlersExecuted,
		HandlerFailures:            mt.handlerMetrics.HandlerFailures,
		AverageHandlerTime:         avgHandlerTime,
		HandlerTimeouts:            mt.handlerMetrics.HandlerTimeouts,
	}
}

func (mt *MetricsTracker) cleanupOldPanics() {
	cutoff := time.Now().Add(-1 * time.Hour)
	filtered := make([]time.Time, 0, len(mt.panicMetrics.PanicsLastHour))
	
	for _, panicTime := range mt.panicMetrics.PanicsLastHour {
		if panicTime.After(cutoff) {
			filtered = append(filtered, panicTime)
		}
	}
	
	mt.panicMetrics.PanicsLastHour = filtered
}