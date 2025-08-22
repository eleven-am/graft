package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type StateChangeEvent struct {
	WorkflowID string      `json:"workflow_id"`
	ChangedBy  string      `json:"changed_by"`
	NewState   interface{} `json:"new_state"`
	Timestamp  time.Time   `json:"timestamp"`
	NodeName   string      `json:"node_name,omitempty"`
	EventType  EventType   `json:"event_type"`
}

func (e StateChangeEvent) String() string {
	return fmt.Sprintf("StateChangeEvent{WorkflowID: %s, ChangedBy: %s, EventType: %s, Timestamp: %s}",
		e.WorkflowID, e.ChangedBy, e.EventType, e.Timestamp.Format(time.RFC3339))
}

type EventType string

const (
	EventTypeNodeCompleted EventType = "node_completed"
	EventTypeNodeFailed    EventType = "node_failed"
	EventTypeStateUpdated  EventType = "state_updated"
	EventTypeNodeStarted   EventType = "node_started"
)

type EvaluationTrigger interface {
	TriggerEvaluation(ctx context.Context, event StateChangeEvent) error
	RegisterEvaluator(evaluator PendingEvaluator)
	Start(ctx context.Context) error
	Stop() error
}

type eventDispatcher struct {
	evaluator   PendingEvaluator
	eventChan   chan StateChangeEvent
	workerCount int
	logger      *slog.Logger
	mu          sync.RWMutex
	closed      bool
}

func NewEvaluationTrigger(workerCount int, logger *slog.Logger) EvaluationTrigger {
	return &eventDispatcher{
		eventChan:   make(chan StateChangeEvent, 1000),
		workerCount: workerCount,
		logger:      logger,
	}
}

func (ed *eventDispatcher) RegisterEvaluator(evaluator PendingEvaluator) {
	ed.evaluator = evaluator
}

func (ed *eventDispatcher) TriggerEvaluation(ctx context.Context, event StateChangeEvent) error {
	ed.mu.RLock()
	defer ed.mu.RUnlock()

	if ed.closed {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "event dispatcher is closed",
			Details: map[string]interface{}{
				"workflow_id": event.WorkflowID,
				"event_type":  string(event.EventType),
			},
		}
	}

	select {
	case ed.eventChan <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return domain.Error{
			Type:    domain.ErrorTypeRateLimit,
			Message: "event channel full, evaluation trigger dropped",
			Details: map[string]interface{}{
				"workflow_id": event.WorkflowID,
				"event_type":  string(event.EventType),
			},
		}
	}
}

func (ed *eventDispatcher) Start(ctx context.Context) error {
	for i := 0; i < ed.workerCount; i++ {
		go ed.worker(ctx, i)
	}
	return nil
}

func (ed *eventDispatcher) Stop() error {
	ed.mu.Lock()
	defer ed.mu.Unlock()

	if !ed.closed {
		ed.closed = true
		close(ed.eventChan)
	}
	return nil
}

func (ed *eventDispatcher) worker(ctx context.Context, workerID int) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ed.eventChan:
			if !ok {
				return
			}

			if ed.evaluator != nil {
				if err := ed.evaluator.EvaluatePendingNodes(ctx, event.WorkflowID, event.NewState); err != nil {
					ed.logger.Error("evaluation failed", "error", err, "workflow_id", event.WorkflowID)
					continue
				}
			}
		}
	}
}
