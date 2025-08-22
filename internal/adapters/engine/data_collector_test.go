package engine

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/testutil/workflow"
	"github.com/stretchr/testify/assert"
)

func TestWorkflowDataCollector_CollectWorkflowData(t *testing.T) {
	mockComponents := workflow.SetupMockComponents(t)
	collector := NewWorkflowDataCollector(mockComponents.Storage, mockComponents.Queue, mockComponents.NodeRegistry)

	workflowID := "test-workflow"
	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()

	workflowInstance := &WorkflowInstance{
		ID:     workflowID,
		Status: ports.WorkflowStateCompleted,
		CurrentState: map[string]interface{}{
			"result": "success",
			"value":  42,
		},
		StartedAt:   startTime,
		CompletedAt: &endTime,
		Metadata: map[string]string{
			"env":     "test",
			"version": "1.0",
		},
	}

	mockComponents.Storage.EXPECT().List(context.Background(), "workflow:execution:test-workflow:").Return([]ports.KeyValue{}, nil)
	mockComponents.Storage.EXPECT().List(context.Background(), "workflow:checkpoint:test-workflow:").Return([]ports.KeyValue{}, nil)
	mockComponents.Storage.EXPECT().List(context.Background(), "workflow:idempotency:test-workflow:").Return([]ports.KeyValue{}, nil)
	mockComponents.Storage.EXPECT().List(context.Background(), "claim:test-workflow:").Return([]ports.KeyValue{}, nil)

	mockComponents.Queue.EXPECT().GetPendingItems(context.Background()).Return([]ports.QueueItem{}, nil)

	data, err := collector.CollectWorkflowData(context.Background(), workflowInstance)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, workflowID, data.WorkflowID)
	assert.Equal(t, "completed", data.Status)
	assert.Equal(t, startTime, data.StartedAt)
	assert.Equal(t, endTime, data.CompletedAt)
	assert.Equal(t, endTime.Sub(startTime), data.Duration)
	assert.Equal(t, map[string]string{"env": "test", "version": "1.0"}, data.Metadata)

	assert.Empty(t, data.ExecutedNodes)

	assert.Empty(t, data.Checkpoints)
	assert.Empty(t, data.QueueItems)
	assert.Empty(t, data.IdempotencyKeys)
	assert.Empty(t, data.ClaimsData)
}
