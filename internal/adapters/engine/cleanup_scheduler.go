package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type CleanupScheduler struct {
	orchestrator    *CleanupOrchestrator
	logger          *slog.Logger
	config          SchedulerConfig
	pendingCleanups map[string]*ScheduledCleanup
	retryQueue      []*ScheduledCleanup
	deadLetterQueue []*ScheduledCleanup
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

type SchedulerConfig struct {
	RetentionPolicy  RetentionPolicy `json:"retention_policy"`
	DelayBasedConfig DelayConfig     `json:"delay_config"`
	WorkerCount      int             `json:"worker_count"`
	BatchSize        int             `json:"batch_size"`
	RetryConfig      RetryConfig     `json:"retry_config"`
}

type RetentionPolicy struct {
	CompletedWorkflows time.Duration `json:"completed_workflows"`
	FailedWorkflows    time.Duration `json:"failed_workflows"`
	AuditLogs          time.Duration `json:"audit_logs"`
}

type DelayConfig struct {
	DefaultDelay    time.Duration `json:"default_delay"`
	MinDelay        time.Duration `json:"min_delay"`
	MaxDelay        time.Duration `json:"max_delay"`
	BackoffMultiple float64       `json:"backoff_multiple"`
}

type RetryConfig struct {
	MaxRetries    int           `json:"max_retries"`
	RetryDelay    time.Duration `json:"retry_delay"`
	BackoffFactor float64       `json:"backoff_factor"`
	MaxRetryDelay time.Duration `json:"max_retry_delay"`
}

type ScheduledCleanup struct {
	WorkflowID  string         `json:"workflow_id"`
	ScheduledAt time.Time      `json:"scheduled_at"`
	ExecuteAt   time.Time      `json:"execute_at"`
	Options     CleanupOptions `json:"options"`
	Attempts    int            `json:"attempts"`
	LastError   *string        `json:"last_error,omitempty"`
	Status      string         `json:"status"`
}

func NewCleanupScheduler(orchestrator *CleanupOrchestrator, config SchedulerConfig, logger *slog.Logger) *CleanupScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	return &CleanupScheduler{
		orchestrator:    orchestrator,
		logger:          logger.With("component", "cleanup_scheduler"),
		config:          config,
		pendingCleanups: make(map[string]*ScheduledCleanup),
		retryQueue:      make([]*ScheduledCleanup, 0),
		deadLetterQueue: make([]*ScheduledCleanup, 0),
		ctx:             ctx,
		cancel:          cancel,
	}
}

func (cs *CleanupScheduler) Start() error {
	cs.logger.Info("starting cleanup scheduler",
		"worker_count", cs.config.WorkerCount,
		"batch_size", cs.config.BatchSize)

	for i := 0; i < cs.config.WorkerCount; i++ {
		cs.wg.Add(1)
		go cs.worker(i)
	}

	cs.wg.Add(1)
	go cs.retryWorker()

	return nil
}

func (cs *CleanupScheduler) Stop() error {
	cs.logger.Info("stopping cleanup scheduler")

	cs.cancel()
	cs.wg.Wait()

	cs.logger.Info("cleanup scheduler stopped")
	return nil
}

func (cs *CleanupScheduler) ScheduleWorkflowCleanup(workflowID string, workflowStatus ports.WorkflowState, options CleanupOptions) error {
	delay := cs.calculateDelay(workflowStatus)
	executeAt := time.Now().Add(delay)

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if existing, exists := cs.pendingCleanups[workflowID]; exists {
		cs.logger.Debug("updating existing scheduled cleanup",
			"workflow_id", workflowID,
			"old_execute_at", existing.ExecuteAt,
			"new_execute_at", executeAt)
	}

	scheduled := &ScheduledCleanup{
		WorkflowID:  workflowID,
		ScheduledAt: time.Now(),
		ExecuteAt:   executeAt,
		Options:     options,
		Status:      "pending",
	}

	cs.pendingCleanups[workflowID] = scheduled

	cs.logger.Info("workflow cleanup scheduled",
		"workflow_id", workflowID,
		"status", workflowStatus,
		"delay", delay,
		"execute_at", executeAt)

	return nil
}

func (cs *CleanupScheduler) CancelScheduledCleanup(workflowID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cleanup, exists := cs.pendingCleanups[workflowID]; exists {
		cleanup.Status = "cancelled"
		delete(cs.pendingCleanups, workflowID)

		cs.logger.Info("cancelled scheduled cleanup",
			"workflow_id", workflowID)
		return nil
	}

	return domain.Error{
		Type:    domain.ErrorTypeNotFound,
		Message: "no scheduled cleanup found for workflow",
		Details: map[string]interface{}{
			"workflow_id": workflowID,
		},
	}
}

func (cs *CleanupScheduler) GetPendingCleanups() []*ScheduledCleanup {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	cleanups := make([]*ScheduledCleanup, 0, len(cs.pendingCleanups))
	for _, cleanup := range cs.pendingCleanups {
		cleanups = append(cleanups, cleanup)
	}

	return cleanups
}

func (cs *CleanupScheduler) GetRetryQueue() []*ScheduledCleanup {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	queue := make([]*ScheduledCleanup, len(cs.retryQueue))
	copy(queue, cs.retryQueue)
	return queue
}

func (cs *CleanupScheduler) GetDeadLetterQueue() []*ScheduledCleanup {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	queue := make([]*ScheduledCleanup, len(cs.deadLetterQueue))
	copy(queue, cs.deadLetterQueue)
	return queue
}

func (cs *CleanupScheduler) worker(workerID int) {
	defer cs.wg.Done()

	cs.logger.Debug("cleanup worker started", "worker_id", workerID)

	ticker := time.NewTicker(time.Second * 30)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			cs.logger.Debug("cleanup worker stopping", "worker_id", workerID)
			return
		case <-ticker.C:
			cs.processReadyCleanups(workerID)
		}
	}
}

func (cs *CleanupScheduler) retryWorker() {
	defer cs.wg.Done()

	cs.logger.Debug("retry worker started")

	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			cs.logger.Debug("retry worker stopping")
			return
		case <-ticker.C:
			cs.processRetryQueue()
		}
	}
}

func (cs *CleanupScheduler) processReadyCleanups(workerID int) {
	now := time.Now()
	var readyCleanups []*ScheduledCleanup

	cs.mu.Lock()
	for workflowID, cleanup := range cs.pendingCleanups {
		if cleanup.ExecuteAt.Before(now) || cleanup.ExecuteAt.Equal(now) {
			readyCleanups = append(readyCleanups, cleanup)
			delete(cs.pendingCleanups, workflowID)
		}
	}
	cs.mu.Unlock()

	if len(readyCleanups) == 0 {
		return
	}

	cs.logger.Debug("processing ready cleanups",
		"worker_id", workerID,
		"count", len(readyCleanups))

	for _, cleanup := range readyCleanups {
		if err := cs.executeCleanup(cleanup); err != nil {
			cs.logger.Error("cleanup execution failed",
				"worker_id", workerID,
				"workflow_id", cleanup.WorkflowID,
				"error", err)
			cs.handleCleanupFailure(cleanup, err)
		} else {
			cs.logger.Info("cleanup executed successfully",
				"worker_id", workerID,
				"workflow_id", cleanup.WorkflowID)
			cleanup.Status = "completed"
		}
	}
}

func (cs *CleanupScheduler) processRetryQueue() {
	cs.mu.Lock()
	if len(cs.retryQueue) == 0 {
		cs.mu.Unlock()
		return
	}

	retryCleanups := make([]*ScheduledCleanup, 0)
	newRetryQueue := make([]*ScheduledCleanup, 0)

	now := time.Now()
	for _, cleanup := range cs.retryQueue {
		if cleanup.ExecuteAt.Before(now) || cleanup.ExecuteAt.Equal(now) {
			retryCleanups = append(retryCleanups, cleanup)
		} else {
			newRetryQueue = append(newRetryQueue, cleanup)
		}
	}

	cs.retryQueue = newRetryQueue
	cs.mu.Unlock()

	cs.logger.Debug("processing retry queue",
		"retry_count", len(retryCleanups))

	for _, cleanup := range retryCleanups {
		if err := cs.executeCleanup(cleanup); err != nil {
			cs.logger.Error("retry cleanup execution failed",
				"workflow_id", cleanup.WorkflowID,
				"attempt", cleanup.Attempts,
				"error", err)
			cs.handleCleanupFailure(cleanup, err)
		} else {
			cs.logger.Info("retry cleanup executed successfully",
				"workflow_id", cleanup.WorkflowID,
				"attempt", cleanup.Attempts)
			cleanup.Status = "completed"
		}
	}
}

func (cs *CleanupScheduler) executeCleanup(cleanup *ScheduledCleanup) error {
	cleanup.Attempts++
	cleanup.Status = "executing"

	ctx, cancel := context.WithTimeout(cs.ctx, time.Minute*5)
	defer cancel()

	return cs.orchestrator.CleanupWorkflow(ctx, cleanup.WorkflowID, cleanup.Options)
}

func (cs *CleanupScheduler) handleCleanupFailure(cleanup *ScheduledCleanup, err error) {
	errorStr := err.Error()
	cleanup.LastError = &errorStr
	cleanup.Status = "failed"

	if cleanup.Attempts >= cs.config.RetryConfig.MaxRetries {
		cs.logger.Error("cleanup moved to dead letter queue after max retries",
			"workflow_id", cleanup.WorkflowID,
			"attempts", cleanup.Attempts)

		cs.mu.Lock()
		cs.deadLetterQueue = append(cs.deadLetterQueue, cleanup)
		cs.mu.Unlock()
		return
	}

	retryDelay := cs.calculateRetryDelay(cleanup.Attempts)
	cleanup.ExecuteAt = time.Now().Add(retryDelay)
	cleanup.Status = "retry_pending"

	cs.mu.Lock()
	cs.retryQueue = append(cs.retryQueue, cleanup)
	cs.mu.Unlock()

	cs.logger.Info("cleanup scheduled for retry",
		"workflow_id", cleanup.WorkflowID,
		"attempt", cleanup.Attempts,
		"retry_delay", retryDelay,
		"next_attempt_at", cleanup.ExecuteAt)
}

func (cs *CleanupScheduler) calculateDelay(status ports.WorkflowState) time.Duration {
	switch status {
	case ports.WorkflowStateCompleted:
		return cs.config.RetentionPolicy.CompletedWorkflows
	case ports.WorkflowStateFailed:
		return cs.config.RetentionPolicy.FailedWorkflows
	default:
		return cs.config.DelayBasedConfig.DefaultDelay
	}
}

func (cs *CleanupScheduler) calculateRetryDelay(attempt int) time.Duration {
	baseDelay := cs.config.RetryConfig.RetryDelay
	backoffFactor := cs.config.RetryConfig.BackoffFactor

	delay := time.Duration(float64(baseDelay) * powFloat(backoffFactor, float64(attempt-1)))

	if delay > cs.config.RetryConfig.MaxRetryDelay {
		delay = cs.config.RetryConfig.MaxRetryDelay
	}

	return delay
}

func powFloat(base, exp float64) float64 {
	if exp == 0 {
		return 1
	}
	result := base
	for i := 1; i < int(exp); i++ {
		result *= base
	}
	return result
}

func (cs *CleanupScheduler) GetOrchestrator() *CleanupOrchestrator {
	return cs.orchestrator
}
