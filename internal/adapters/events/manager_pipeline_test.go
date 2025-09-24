package events

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBroadcastCommand_DispatchesOnceViaStorage(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	mgr := NewManager(storage, "node-1", nil)

	var called int32
	_ = mgr.RegisterCommandHandler("deploy", func(ctx context.Context, from string, params interface{}) error {
		atomic.AddInt32(&called, 1)
		return nil
	})

	storage.On("PutWithTTL", mock.AnythingOfType("string"), mock.Anything, int64(0), mock.Anything).Return(nil).Once()

	dev := domain.DevCommand{Command: "deploy", Params: map[string]interface{}{"x": 1}}
	devBytes, _ := json.Marshal(dev)
	storage.On("Get", "dev-cmd:deploy").Return(devBytes, int64(0), true, nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := mgr.Start(ctx)
	assert.NoError(t, err)

	_ = mgr.BroadcastCommand(ctx, &dev)
	// Simulate storage-published event
	_ = mgr.PublishStorageEvents([]domain.Event{{Type: domain.EventPut, Key: "dev-cmd:deploy", NodeID: "node-1", Timestamp: time.Now()}})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&called) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&called))
	_ = mgr.Stop()
	storage.AssertExpectations(t)
}

func TestEventPipelineUnification_NoDuplicateProcessing(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	mgr := NewManager(storage, "node-1", nil)

	var workflowStartedCount int32
	mgr.OnWorkflowStarted(func(event *domain.WorkflowStartedEvent) {
		atomic.AddInt32(&workflowStartedCount, 1)
	})

	workflowEvent := domain.WorkflowStartedEvent{
		WorkflowID: "test-wf",
		StartedAt:  time.Now(),
		NodeID:     "node-1",
	}
	eventData, _ := json.Marshal(workflowEvent)
	storage.On("Get", "workflow:test-wf:started").Return(eventData, int64(1), true, nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := mgr.Start(ctx)
	assert.NoError(t, err)

	domainEvent := domain.Event{
		Type:      domain.EventPut,
		Key:       "workflow:test-wf:started",
		Version:   1,
		NodeID:    "node-1",
		Timestamp: time.Now(),
	}

	err = mgr.Broadcast(domainEvent)
	assert.NoError(t, err)
	_ = mgr.PublishStorageEvents([]domain.Event{domainEvent})

	time.Sleep(200 * time.Millisecond)

	count := atomic.LoadInt32(&workflowStartedCount)
	assert.Equal(t, int32(1), count)

	_ = mgr.Stop()
	storage.AssertExpectations(t)
}
