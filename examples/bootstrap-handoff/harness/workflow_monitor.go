package harness

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/eleven-am/graft"
)

type WorkflowEvent struct {
	WorkflowID string
	EventType  string
	Timestamp  time.Time
	NodeID     string
	Data       interface{}
}

type WorkflowMonitor struct {
	events []WorkflowEvent
	mu     sync.RWMutex
}

func NewWorkflowMonitor() *WorkflowMonitor {
	return &WorkflowMonitor{
		events: make([]WorkflowEvent, 0),
	}
}

func (wm *WorkflowMonitor) AddEvent(event WorkflowEvent) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	wm.events = append(wm.events, event)
}

func (wm *WorkflowMonitor) GetEvents() []WorkflowEvent {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	events := make([]WorkflowEvent, len(wm.events))
	copy(events, wm.events)
	return events
}

func (wm *WorkflowMonitor) GetEventsForWorkflow(workflowID string) []WorkflowEvent {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	var filtered []WorkflowEvent
	for _, event := range wm.events {
		if event.WorkflowID == workflowID {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func (wm *WorkflowMonitor) AttachToNode(instance *NodeInstance) error {
	nodeID := instance.Config.NodeID

	if err := instance.Manager.OnWorkflowStarted(func(event *graft.WorkflowStartedEvent) {
		wm.AddEvent(WorkflowEvent{
			WorkflowID: event.WorkflowID,
			EventType:  "started",
			Timestamp:  time.Now(),
			NodeID:     nodeID,
			Data:       event,
		})
	}); err != nil {
		return fmt.Errorf("failed to attach workflow started handler: %w", err)
	}

	if err := instance.Manager.OnWorkflowCompleted(func(event *graft.WorkflowCompletedEvent) {
		wm.AddEvent(WorkflowEvent{
			WorkflowID: event.WorkflowID,
			EventType:  "completed",
			Timestamp:  time.Now(),
			NodeID:     nodeID,
			Data:       event,
		})
	}); err != nil {
		return fmt.Errorf("failed to attach workflow completed handler: %w", err)
	}

	if err := instance.Manager.OnWorkflowFailed(func(event *graft.WorkflowErrorEvent) {
		wm.AddEvent(WorkflowEvent{
			WorkflowID: event.WorkflowID,
			EventType:  "failed",
			Timestamp:  time.Now(),
			NodeID:     nodeID,
			Data:       event,
		})
	}); err != nil {
		return fmt.Errorf("failed to attach workflow failed handler: %w", err)
	}

	if err := instance.Manager.OnNodeStarted(func(event *graft.NodeStartedEvent) {
		wm.AddEvent(WorkflowEvent{
			WorkflowID: event.WorkflowID,
			EventType:  "node_started",
			Timestamp:  time.Now(),
			NodeID:     nodeID,
			Data:       event,
		})
	}); err != nil {
		return fmt.Errorf("failed to attach node started handler: %w", err)
	}

	if err := instance.Manager.OnNodeCompleted(func(event *graft.NodeCompletedEvent) {
		wm.AddEvent(WorkflowEvent{
			WorkflowID: event.WorkflowID,
			EventType:  "node_completed",
			Timestamp:  time.Now(),
			NodeID:     nodeID,
			Data:       event,
		})
	}); err != nil {
		return fmt.Errorf("failed to attach node completed handler: %w", err)
	}

	return nil
}

type ContinuityResult struct {
	WorkflowID            string
	StartedBeforeHandoff  bool
	CompletedAfterHandoff bool
	ProcessingContinued   bool
	HandoffDetected       bool
	TotalDuration         time.Duration
	HandoffDuration       time.Duration
	EventCount            int
}

func (wm *WorkflowMonitor) AnalyzeContinuity(workflowID string, handoffStartTime, handoffEndTime time.Time) ContinuityResult {
	events := wm.GetEventsForWorkflow(workflowID)

	result := ContinuityResult{
		WorkflowID: workflowID,
		EventCount: len(events),
	}

	if len(events) == 0 {
		return result
	}

	firstEvent := events[0]
	lastEvent := events[len(events)-1]

	result.StartedBeforeHandoff = firstEvent.Timestamp.Before(handoffStartTime)
	result.CompletedAfterHandoff = lastEvent.Timestamp.After(handoffEndTime)
	result.TotalDuration = lastEvent.Timestamp.Sub(firstEvent.Timestamp)

	if !handoffStartTime.IsZero() && !handoffEndTime.IsZero() {
		result.HandoffDuration = handoffEndTime.Sub(handoffStartTime)
		result.HandoffDetected = true

		eventsBeforeHandoff := 0
		eventsAfterHandoff := 0
		eventsDuringHandoff := 0

		for _, event := range events {
			if event.Timestamp.Before(handoffStartTime) {
				eventsBeforeHandoff++
			} else if event.Timestamp.After(handoffEndTime) {
				eventsAfterHandoff++
			} else {
				eventsDuringHandoff++
			}
		}

		result.ProcessingContinued = eventsBeforeHandoff > 0 && eventsAfterHandoff > 0
	}

	return result
}

type TestWorkflow struct {
	ID   string
	Data interface{}
}

func StartTestWorkflow(ctx context.Context, instance *NodeInstance, workflowID string) error {
	if !instance.Manager.IsReady() {
		return fmt.Errorf("node %s not ready for workflow", instance.Config.NodeID)
	}

	trigger := graft.WorkflowTrigger{
		WorkflowID: workflowID,
		InitialNodes: []graft.NodeConfig{
			{
				Name: "test_node",
				Config: map[string]interface{}{
					"processor": instance.Config.NodeID,
					"duration":  1000,
				},
			},
		},
		InitialState: map[string]interface{}{
			"test_id":   workflowID,
			"timestamp": time.Now().Unix(),
			"node":      instance.Config.NodeID,
		},
	}

	return instance.Manager.StartWorkflow(trigger)
}

func StartContinuousWorkflows(ctx context.Context, instance *NodeInstance, workflowPrefix string, interval time.Duration, monitor *WorkflowMonitor) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	counter := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !instance.Manager.IsReady() {
				continue
			}

			counter++
			workflowID := fmt.Sprintf("%s-%d", workflowPrefix, counter)

			if err := StartTestWorkflow(ctx, instance, workflowID); err != nil {
				continue
			}
		}
	}
}
