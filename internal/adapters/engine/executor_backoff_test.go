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
	"github.com/stretchr/testify/mock"
)

func TestExecutor_RequeueBackoff_ClampAndJitter(t *testing.T) {
	var lastEnqueued WorkItem
	mockQueue := mocks.NewMockQueuePort(t)
	mockQueue.On("Enqueue", mock.MatchedBy(func(item []byte) bool {
		_ = json.Unmarshal(item, &lastEnqueued)
		return true
	})).Return(nil).Twice()

	nr := mocks.NewMockNodeRegistryPort(t)
	ev := mocks.NewMockEventManager(t)
	lb := mocks.NewMockLoadBalancer(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	config := domain.EngineConfig{RetryAttempts: 25}
	ex := NewExecutor(config, "node-1", nr, nil, mockQueue, nil, ev, lb, logger, domain.NewExecutionMetrics())

	_ = ex.requeueNodeWithBackoff(context.Background(), "wf-a", "n1", json.RawMessage(`{}`), 0)
	d := lastEnqueued.ProcessAfter.Sub(lastEnqueued.EnqueuedAt)
	assert.GreaterOrEqual(t, d, 500*time.Millisecond)
	assert.LessOrEqual(t, d, 1500*time.Millisecond)

	_ = ex.requeueNodeWithBackoff(context.Background(), "wf-b", "n1", json.RawMessage(`{}`), 21)
	d2 := lastEnqueued.ProcessAfter.Sub(lastEnqueued.EnqueuedAt)

	assert.GreaterOrEqual(t, d2, 150*time.Second)
	assert.LessOrEqual(t, d2, 450*time.Second)

	mockQueue.AssertExpectations(t)
}
