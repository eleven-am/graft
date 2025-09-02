package events

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBroadcastCommand_DispatchesOnceViaStorage(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	mgr := NewManager(nil)
	mgr.SetStorage(storage, "node-1")

	var called int32
	_ = mgr.RegisterCommandHandler("deploy", func(ctx context.Context, from string, params interface{}) error {
		atomic.AddInt32(&called, 1)
		return nil
	})

	workflowCh := make(chan ports.StorageEvent, 1)
	nodeCh := make(chan ports.StorageEvent, 1)
	devcmdCh := make(chan ports.StorageEvent, 1)

	storage.On("Subscribe", "workflow:").Return((<-chan ports.StorageEvent)(workflowCh), func() {}, nil).Maybe()
	storage.On("Subscribe", "node:").Return((<-chan ports.StorageEvent)(nodeCh), func() {}, nil).Maybe()
	storage.On("Subscribe", "dev-cmd:").Return((<-chan ports.StorageEvent)(devcmdCh), func() {}, nil).Once()

	storage.On("PutWithTTL", mock.AnythingOfType("string"), mock.Anything, int64(0), mock.Anything).Return(nil).Once()

	dev := domain.DevCommand{Command: "deploy", Params: map[string]interface{}{"x": 1}}
	devBytes, _ := json.Marshal(dev)
	storage.On("Get", "dev-cmd:deploy").Return(devBytes, int64(0), true, nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := mgr.Start(ctx)
	assert.NoError(t, err)

	_ = mgr.BroadcastCommand(ctx, &dev)

	devcmdCh <- ports.StorageEvent{Type: domain.EventPut, Key: "dev-cmd:deploy", NodeID: "node-1"}

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
	mgr := NewManagerWithStorage(storage, "node-1", nil)

	var workflowStartedCount int32
	mgr.OnWorkflowStarted(func(event *domain.WorkflowStartedEvent) {
		atomic.AddInt32(&workflowStartedCount, 1)
	})

	workflowCh := make(chan ports.StorageEvent, 10)
	nodeCh := make(chan ports.StorageEvent, 10)
	devcmdCh := make(chan ports.StorageEvent, 10)

	storage.On("Subscribe", "workflow:").Return((<-chan ports.StorageEvent)(workflowCh), func() {}, nil).Maybe()
	storage.On("Subscribe", "node:").Return((<-chan ports.StorageEvent)(nodeCh), func() {}, nil).Maybe()
	storage.On("Subscribe", "dev-cmd:").Return((<-chan ports.StorageEvent)(devcmdCh), func() {}, nil).Once()

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

	storageEvent := ports.StorageEvent{
		Type:      domain.EventPut,
		Key:       "workflow:test-wf:started",
		Version:   1,
		NodeID:    "node-1",
		Timestamp: time.Now(),
	}
	workflowCh <- storageEvent

	time.Sleep(200 * time.Millisecond)

	count := atomic.LoadInt32(&workflowStartedCount)
	assert.Equal(t, int32(1), count)

	_ = mgr.Stop()
	storage.AssertExpectations(t)
}
