package engine

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventDrivenEvaluationBasics(t *testing.T) {
	t.Run("NewPendingEvaluator", func(t *testing.T) {
		engine := NewEngine(Config{}, slog.Default())
		logger := slog.Default()
		
		evaluator := NewPendingEvaluator(engine, logger)
		
		assert.NotNil(t, evaluator)
	})
	
	t.Run("StateChangeEvent_String", func(t *testing.T) {
		event := StateChangeEvent{
			WorkflowID: "workflow-123",
			ChangedBy:  "test-node",
			NewState:   map[string]interface{}{"ready": true},
			Timestamp:  time.Now(),
			NodeName:   "test-node",
			EventType:  EventTypeNodeCompleted,
		}
		
		str := event.String()
		assert.Contains(t, str, "workflow-123")
		assert.Contains(t, str, "test-node")
		assert.Contains(t, str, "node_completed")
	})
	
	t.Run("NewEvaluationTrigger", func(t *testing.T) {
		trigger := NewEvaluationTrigger(3, slog.Default())
		
		assert.NotNil(t, trigger)
		
		concrete := trigger.(*eventDispatcher)
		assert.Equal(t, 3, concrete.workerCount)
		assert.NotNil(t, concrete.eventChan)
		assert.NotNil(t, concrete.logger)
	})
}

func TestEvaluationTriggerLifecycle(t *testing.T) {
	trigger := NewEvaluationTrigger(1, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	err := trigger.Start(ctx)
	require.NoError(t, err)
	
	// Note: eventDispatcher doesn't have a running field, but Start() doesn't error
	assert.NotNil(t, trigger)
	
	err = trigger.Stop()
	require.NoError(t, err)
	
	// Stop() closes the channel, trigger is stopped
}

func TestEvaluationTriggerChannelFull(t *testing.T) {
	trigger := NewEvaluationTrigger(1, slog.Default())
	
	// Fill the channel by sending events without starting workers
	
	// Fill the channel buffer (1000 items)
	for i := 0; i < 1001; i++ {
		event := StateChangeEvent{
			WorkflowID: "workflow-123",
			EventType:  EventTypeNodeCompleted,
		}
		
		err := trigger.TriggerEvaluation(context.Background(), event)
		if i < 1000 {
			assert.NoError(t, err)
		} else {
			// The 1001st event should fail
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "event channel full")
		}
	}
}

func TestEvaluationTriggerWithMockEvaluator(t *testing.T) {
	trigger := NewEvaluationTrigger(1, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	mockEvaluator := &simpleMockEvaluator{
		evaluations: make(chan evaluationCall, 10),
	}
	trigger.RegisterEvaluator(mockEvaluator)
	
	err := trigger.Start(ctx)
	require.NoError(t, err)
	defer trigger.Stop()
	
	event := StateChangeEvent{
		WorkflowID: "workflow-123",
		EventType:  EventTypeNodeCompleted,
		NewState:   map[string]interface{}{"ready": true},
		Timestamp:  time.Now(),
	}
	
	err = trigger.TriggerEvaluation(context.Background(), event)
	require.NoError(t, err)
	
	select {
	case call := <-mockEvaluator.evaluations:
		assert.Equal(t, "workflow-123", call.workflowID)
		assert.Equal(t, map[string]interface{}{"ready": true}, call.state)
	case <-time.After(time.Second):
		t.Fatal("Expected evaluation call within timeout")
	}
}

func TestPendingEvaluatorEmptyWorkflowID(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	evaluator := NewPendingEvaluator(engine, slog.Default())
	
	err := evaluator.EvaluatePendingNodes(context.Background(), "", nil)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workflow ID cannot be empty")
}

func TestPendingEvaluatorQueueNotAvailable(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	evaluator := NewPendingEvaluator(engine, slog.Default())
	
	err := evaluator.EvaluatePendingNodes(context.Background(), "workflow-123", nil)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue not available")
}

func TestEngineHasEventDrivenComponents(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	
	assert.NotNil(t, engine.pendingEvaluator, "Engine should have a pending evaluator")
	assert.NotNil(t, engine.evaluationTrigger, "Engine should have an evaluation trigger")
	
	dispatcher := engine.evaluationTrigger.(*eventDispatcher)
	assert.NotNil(t, dispatcher.evaluator, "Evaluation trigger should have the pending evaluator registered")
	assert.Equal(t, engine.pendingEvaluator, dispatcher.evaluator, "Registered evaluator should be the engine's pending evaluator")
}

func TestUtilityFunctions(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	evaluator := NewPendingEvaluator(engine, slog.Default()).(*pendingEvaluator)
	
	t.Run("convertToMap", func(t *testing.T) {
		tests := []struct {
			name     string
			input    interface{}
			expected map[string]interface{}
			hasError bool
		}{
			{
				name:     "nil input",
				input:    nil,
				expected: map[string]interface{}{},
				hasError: false,
			},
			{
				name:     "map input",
				input:    map[string]interface{}{"key": "value"},
				expected: map[string]interface{}{"key": "value"},
				hasError: false,
			},
			{
				name:     "struct input",
				input:    struct{ Name string }{"test"},
				expected: map[string]interface{}{"Name": "test"},
				hasError: false,
			},
		}
		
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := evaluator.convertToMap(tt.input)
				
				if tt.hasError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, result)
				}
			})
		}
	})
	
	t.Run("valuesEqual", func(t *testing.T) {
		tests := []struct {
			name     string
			a        interface{}
			b        interface{}
			expected bool
		}{
			{"both nil", nil, nil, true},
			{"one nil", nil, "value", false},
			{"equal strings", "test", "test", true},
			{"different strings", "test1", "test2", false},
			{"equal numbers", 42, 42, true},
			{"different numbers", 42, 43, false},
		}
		
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := evaluator.valuesEqual(tt.a, tt.b)
				assert.Equal(t, tt.expected, result)
			})
		}
	})
}

type evaluationCall struct {
	workflowID string
	state      interface{}
}

type simpleMockEvaluator struct {
	evaluations chan evaluationCall
}

func (s *simpleMockEvaluator) EvaluatePendingNodes(ctx context.Context, workflowID string, currentState interface{}) error {
	select {
	case s.evaluations <- evaluationCall{workflowID: workflowID, state: currentState}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (s *simpleMockEvaluator) CheckNodeReadiness(node *ports.PendingNode, state interface{}, config interface{}) bool {
	return true
}

func (s *simpleMockEvaluator) MovePendingToReady(ctx context.Context, items []ports.QueueItem) error {
	return nil
}