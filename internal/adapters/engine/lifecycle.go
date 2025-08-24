package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type LifecycleManager struct {
	completionHandlers []ports.CompletionHandler
	errorHandlers      []ports.ErrorHandler
	progressHandlers   []ProgressHandler
	cleanupCallback    CleanupCallback
	logger             *slog.Logger
	handlerTimeout     time.Duration
	maxRetries         int
	metricsTracker     *MetricsTracker
	mu                 sync.RWMutex
}

type CleanupCallback func(workflowID string) error

type ProgressHandler func(workflowID string, nodeCompleted string, currentState interface{})

type HandlerConfig struct {
	Timeout    time.Duration
	MaxRetries int
}

func NewLifecycleManager(logger *slog.Logger, config HandlerConfig, metricsTracker *MetricsTracker) *LifecycleManager {
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}

	return &LifecycleManager{
		completionHandlers: make([]ports.CompletionHandler, 0),
		errorHandlers:      make([]ports.ErrorHandler, 0),
		progressHandlers:   make([]ProgressHandler, 0),
		logger:             logger.With("component", "lifecycle-manager"),
		handlerTimeout:     config.Timeout,
		maxRetries:         config.MaxRetries,
		metricsTracker:     metricsTracker,
	}
}

func (lm *LifecycleManager) SetCleanupCallback(callback CleanupCallback) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.cleanupCallback = callback
}

func (lm *LifecycleManager) RegisterHandlers(completion []ports.CompletionHandler, error []ports.ErrorHandler) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.completionHandlers = append(lm.completionHandlers, completion...)
	lm.errorHandlers = append(lm.errorHandlers, error...)

	lm.logger.Debug("lifecycle handlers registered",
		"completion_handlers", len(completion),
		"error_handlers", len(error),
		"total_completion", len(lm.completionHandlers),
		"total_error", len(lm.errorHandlers),
	)
}

func (lm *LifecycleManager) TriggerCompletion(ctx context.Context, data domain.WorkflowCompletionData) error {
	lm.mu.RLock()
	handlers := make([]ports.CompletionHandler, len(lm.completionHandlers))
	copy(handlers, lm.completionHandlers)
	cleanupCallback := lm.cleanupCallback
	lm.mu.RUnlock()

	lm.logger.Info("triggering workflow completion handlers",
		"workflow_id", data.WorkflowID,
		"handler_count", len(handlers),
	)

	handlerErrors := make(chan error, len(handlers))

	for i, handler := range handlers {
		go func(i int, h ports.CompletionHandler) {
			err := lm.executeCompletionHandler(ctx, i, h, data)
			handlerErrors <- err
		}(i, handler)
	}

	var firstError error
	successCount := 0
	for i := 0; i < len(handlers); i++ {
		if err := <-handlerErrors; err != nil {
			if firstError == nil {
				firstError = err
			}
		} else {
			successCount++
		}
	}

	if successCount > 0 && cleanupCallback != nil {
		lm.logger.Debug("triggering nuclear cleanup after successful completion handlers",
			"workflow_id", data.WorkflowID,
			"successful_handlers", successCount,
			"total_handlers", len(handlers),
		)

		if cleanupErr := cleanupCallback(data.WorkflowID); cleanupErr != nil {
			lm.logger.Error("nuclear cleanup failed after completion",
				"workflow_id", data.WorkflowID,
				"error", cleanupErr)
		} else {
			lm.logger.Debug("nuclear cleanup completed successfully", "workflow_id", data.WorkflowID)
		}
	}

	return firstError
}

func (lm *LifecycleManager) TriggerError(ctx context.Context, data domain.WorkflowErrorData) error {
	lm.mu.RLock()
	handlers := make([]ports.ErrorHandler, len(lm.errorHandlers))
	copy(handlers, lm.errorHandlers)
	cleanupCallback := lm.cleanupCallback
	lm.mu.RUnlock()

	lm.logger.Info("triggering workflow error handlers",
		"workflow_id", data.WorkflowID,
		"handler_count", len(handlers),
		"error_type", data.ErrorType,
		"failed_node", data.FailedNode,
	)

	handlerErrors := make(chan error, len(handlers))

	for i, handler := range handlers {
		go func(i int, h ports.ErrorHandler) {
			err := lm.executeErrorHandler(ctx, i, h, data)
			handlerErrors <- err
		}(i, handler)
	}

	var firstError error
	successCount := 0
	for i := 0; i < len(handlers); i++ {
		if err := <-handlerErrors; err != nil {
			if firstError == nil {
				firstError = err
			}
		} else {
			successCount++
		}
	}

	if successCount > 0 && cleanupCallback != nil {
		lm.logger.Debug("triggering nuclear cleanup after successful error handlers",
			"workflow_id", data.WorkflowID,
			"successful_handlers", successCount,
			"total_handlers", len(handlers),
		)

		if cleanupErr := cleanupCallback(data.WorkflowID); cleanupErr != nil {
			lm.logger.Error("nuclear cleanup failed after error handling",
				"workflow_id", data.WorkflowID,
				"error", cleanupErr)
		} else {
			lm.logger.Debug("nuclear cleanup completed successfully after error", "workflow_id", data.WorkflowID)
		}
	}

	return firstError
}

func (lm *LifecycleManager) TriggerProgress(workflowID, nodeCompleted string, currentState interface{}) error {
	lm.mu.RLock()
	handlers := make([]ProgressHandler, len(lm.progressHandlers))
	copy(handlers, lm.progressHandlers)
	lm.mu.RUnlock()

	lm.logger.Debug("triggering workflow progress handlers",
		"workflow_id", workflowID,
		"node_completed", nodeCompleted,
		"handler_count", len(handlers),
	)

	for i, handler := range handlers {
		go lm.executeProgressHandler(i, handler, workflowID, nodeCompleted, currentState)
	}

	return nil
}

func (lm *LifecycleManager) executeCompletionHandler(ctx context.Context, handlerIndex int, handler ports.CompletionHandler, data domain.WorkflowCompletionData) error {
	result := lm.executeWithRecovery(
		"completion",
		handlerIndex,
		data.WorkflowID,
		func() error {
			return handler(ctx, data)
		},
	)

	if lm.metricsTracker != nil {
		lm.metricsTracker.RecordCompletionHandler(result.Duration, result.Success)
	}

	if result.Success {
		lm.logger.Debug("completion handler executed successfully",
			"workflow_id", data.WorkflowID,
			"handler_index", handlerIndex,
			"duration", result.Duration,
		)
		return nil
	} else {
		lm.logger.Error("completion handler failed",
			"workflow_id", data.WorkflowID,
			"handler_index", handlerIndex,
			"error", result.Error,
			"retries", result.Retries,
		)
		return fmt.Errorf("completion handler %d failed: %s", handlerIndex, result.Error)
	}
}

func (lm *LifecycleManager) executeErrorHandler(ctx context.Context, handlerIndex int, handler ports.ErrorHandler, data domain.WorkflowErrorData) error {
	result := lm.executeWithRecovery(
		"error",
		handlerIndex,
		data.WorkflowID,
		func() error {
			return handler(ctx, data)
		},
	)

	if lm.metricsTracker != nil {
		lm.metricsTracker.RecordErrorHandler(result.Duration, result.Success)
	}

	if result.Success {
		lm.logger.Debug("error handler executed successfully",
			"workflow_id", data.WorkflowID,
			"handler_index", handlerIndex,
			"duration", result.Duration,
		)
		return nil
	} else {
		lm.logger.Error("error handler failed",
			"workflow_id", data.WorkflowID,
			"handler_index", handlerIndex,
			"error", result.Error,
			"retries", result.Retries,
		)
		return fmt.Errorf("error handler %d failed: %s", handlerIndex, result.Error)
	}
}

func (lm *LifecycleManager) executeProgressHandler(handlerIndex int, handler ProgressHandler, workflowID, nodeCompleted string, currentState interface{}) {
	result := lm.executeWithRecovery(
		"progress",
		handlerIndex,
		workflowID,
		func() error {
			handler(workflowID, nodeCompleted, currentState)
			return nil
		},
	)

	if result.Success {
		lm.logger.Debug("progress handler executed successfully",
			"workflow_id", workflowID,
			"handler_index", handlerIndex,
			"duration", result.Duration,
		)
	} else {
		lm.logger.Error("progress handler failed",
			"workflow_id", workflowID,
			"handler_index", handlerIndex,
			"error", result.Error,
			"retries", result.Retries,
		)
	}
}

func (lm *LifecycleManager) executeWithRecovery(handlerType string, handlerIndex int, workflowID string, handlerFunc func() error) domain.HandlerExecutionResult {
	result := domain.HandlerExecutionResult{
		HandlerType: handlerType,
		WorkflowID:  workflowID,
		ExecutedAt:  time.Now(),
		Success:     false,
		Retries:     0,
	}

	for attempt := 0; attempt <= lm.maxRetries; attempt++ {
		if attempt > 0 {
			result.Retries = attempt
			backoff := time.Duration(attempt) * 100 * time.Millisecond
			time.Sleep(backoff)
		}

		startTime := time.Now()

		func() {
			defer func() {
				result.Duration = time.Since(startTime)
			}()

			ctx, cancel := context.WithTimeout(context.Background(), lm.handlerTimeout)
			defer cancel()

			done := make(chan error, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						result.Error = "handler panicked: " + lm.formatPanic(r)
						lm.logger.Error("lifecycle handler panicked",
							"handler_type", handlerType,
							"handler_index", handlerIndex,
							"workflow_id", workflowID,
							"panic_value", r,
							"attempt", attempt+1,
						)
						done <- domain.Error{
							Type:    domain.ErrorTypeInternal,
							Message: "handler panicked during execution",
							Details: map[string]interface{}{
								"panic_value":  r,
								"handler_type": "lifecycle",
							},
						}
						return
					}
				}()
				done <- handlerFunc()
			}()

			select {
			case err := <-done:
				if err != nil {
					result.Error = err.Error()
				} else {
					result.Success = true
				}
			case <-ctx.Done():
				result.Error = "handler timeout exceeded"
				if lm.metricsTracker != nil {
					lm.metricsTracker.RecordHandlerTimeout()
				}
				lm.logger.Warn("lifecycle handler timeout",
					"handler_type", handlerType,
					"handler_index", handlerIndex,
					"workflow_id", workflowID,
					"timeout", lm.handlerTimeout,
					"attempt", attempt+1,
				)
			}
		}()

		if result.Success {
			break
		}

		if attempt < lm.maxRetries {
			lm.logger.Warn("retrying handler execution",
				"handler_type", handlerType,
				"handler_index", handlerIndex,
				"workflow_id", workflowID,
				"attempt", attempt+1,
				"error", result.Error,
			)
		}
	}

	return result
}

func (lm *LifecycleManager) formatPanic(r interface{}) string {
	if err, ok := r.(error); ok {
		return err.Error()
	}
	return "unknown panic value"
}
