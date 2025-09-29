package engine

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	json "github.com/eleven-am/graft/internal/xjson"
	"github.com/stretchr/testify/assert"
)

func TestEngine_LBReject_UsesReleaseNotComplete(t *testing.T) {

	wi := WorkItem{
		WorkflowID:   "wf-lb",
		NodeName:     "node",
		Config:       json.RawMessage(`{}`),
		EnqueuedAt:   time.Now(),
		ProcessAfter: time.Now(),
		RetryCount:   0,
	}
	data, _ := json.Marshal(wi)

	mockQueue := mocks.NewMockQueuePort(t)
	mockQueue.On("Claim").Return(data, "c1", true, nil).Once()
	mockQueue.On("Release", "c1").Return(nil).Once()

	lb := mocks.NewMockLoadBalancer(t)
	lb.EXPECT().ShouldExecuteNode("node-id", "wf-lb", "node").Return(false, nil)

	nr := mocks.NewMockNodeRegistryPort(t)
	ev := mocks.NewMockEventManager(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	eng := NewEngine(domain.EngineConfig{}, "node-id", nr, mockQueue, nil, ev, lb, logger)

	eng.ctx = context.Background()

	processed, err := eng.processNextItem()
	assert.NoError(t, err)
	assert.True(t, processed)

	mockQueue.AssertExpectations(t)
}
