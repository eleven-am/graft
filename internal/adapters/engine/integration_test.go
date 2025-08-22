package engine

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type TestNode struct{}

func (n *TestNode) GetName() string {
	return "test-node"
}

func (n *TestNode) Execute(ctx context.Context, state map[string]interface{}, config map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{
		"output": "test-result",
	}, nil
}

func (n *TestNode) CanStart(ctx context.Context, state map[string]interface{}) (bool, error) {
	return true, nil
}

func TestWorkflowCompletionDataCollection(t *testing.T) {
	mockStorage := mocks.NewMockStoragePort(t)
	mockQueue := mocks.NewMockQueuePort(t)

	workflowID := "test-workflow-123"

	mockStorage.EXPECT().List(mock.Anything, "workflow:idempotency:"+workflowID+":").Return([]ports.KeyValue{}, nil)
	mockStorage.EXPECT().List(mock.Anything, "workflow:checkpoint:"+workflowID+":").Return([]ports.KeyValue{}, nil)
	mockStorage.EXPECT().List(mock.Anything, "claim:"+workflowID+":").Return([]ports.KeyValue{}, nil)
	mockStorage.EXPECT().List(mock.Anything, "workflow:execution:"+workflowID+":").Return([]ports.KeyValue{
		{Key: "workflow:execution:" + workflowID + ":test-node_1234567890", Value: []byte(`{"node_name":"test-node","status":"completed","executed_at":"2024-01-01T00:00:00.000000000Z","duration":1000000000,"data":{"output":"test-result"}}`)},
	}, nil)

	mockQueue.EXPECT().GetPendingItems(mock.Anything).Return([]ports.QueueItem{}, nil)

	mockRegistry := mocks.NewMockNodeRegistryPort(t)
	dataCollector := NewWorkflowDataCollector(mockStorage, mockQueue, mockRegistry)

	workflow := &WorkflowInstance{
		ID:          workflowID,
		Status:      ports.WorkflowStateCompleted,
		StartedAt:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		CompletedAt: &time.Time{},
		CurrentState: map[string]interface{}{
			"output": "test-result",
		},
		Metadata: map[string]string{
			"test": "metadata",
		},
	}
	*workflow.CompletedAt = time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)

	data, err := dataCollector.CollectWorkflowData(context.Background(), workflow)
	require.NoError(t, err)
	require.NotNil(t, data)

	assert.Equal(t, workflowID, data.WorkflowID)
	assert.Equal(t, "completed", data.Status)
	assert.Equal(t, workflow.CurrentState, data.FinalState)
	assert.Contains(t, data.Metadata, "test")
	assert.Equal(t, "metadata", data.Metadata["test"])
	assert.True(t, data.Duration > 0)
	assert.Len(t, data.ExecutedNodes, 1)
	assert.Equal(t, "test-node", data.ExecutedNodes[0].NodeName)
	assert.Equal(t, "completed", data.ExecutedNodes[0].Status)
}
