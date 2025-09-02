package engine

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	json "github.com/eleven-am/graft/internal/xjson"
	"github.com/stretchr/testify/assert"
)

type enqueueCatcher struct {
	last WorkItem
}

func (e *enqueueCatcher) Enqueue(item []byte) error            { _ = json.Unmarshal(item, &e.last); return nil }
func (e *enqueueCatcher) Peek() ([]byte, bool, error)          { return nil, false, nil }
func (e *enqueueCatcher) Claim() ([]byte, string, bool, error) { return nil, "", false, nil }
func (e *enqueueCatcher) Complete(string) error                { return nil }
func (e *enqueueCatcher) Release(string) error                 { return nil }
func (e *enqueueCatcher) WaitForItem(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (e *enqueueCatcher) Size() (int, error)                             { return 0, nil }
func (e *enqueueCatcher) HasItemsWithPrefix(string) (bool, error)        { return false, nil }
func (e *enqueueCatcher) GetItemsWithPrefix(string) ([][]byte, error)    { return nil, nil }
func (e *enqueueCatcher) HasClaimedItemsWithPrefix(string) (bool, error) { return false, nil }
func (e *enqueueCatcher) GetClaimedItemsWithPrefix(string) ([]ports.ClaimedItem, error) {
	return nil, nil
}
func (e *enqueueCatcher) Close() error                                           { return nil }
func (e *enqueueCatcher) SendToDeadLetter([]byte, string) error                  { return nil }
func (e *enqueueCatcher) GetDeadLetterItems(int) ([]ports.DeadLetterItem, error) { return nil, nil }
func (e *enqueueCatcher) GetDeadLetterSize() (int, error)                        { return 0, nil }
func (e *enqueueCatcher) RetryFromDeadLetter(string) error                       { return nil }

func TestExecutor_RequeueBackoff_ClampAndJitter(t *testing.T) {
	q := &enqueueCatcher{}
	nr := mocks.NewMockNodeRegistryPort(t)
	ev := mocks.NewMockEventManager(t)
	lb := mocks.NewMockLoadBalancer(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	config := domain.EngineConfig{RetryAttempts: 25}
	ex := NewExecutor(config, "node-1", nr, nil, q, nil, ev, lb, logger, domain.NewExecutionMetrics())

	_ = ex.requeueNodeWithBackoff(context.Background(), "wf-a", "n1", json.RawMessage(`{}`), 0)
	d := q.last.ProcessAfter.Sub(q.last.EnqueuedAt)
	assert.GreaterOrEqual(t, d, 500*time.Millisecond)
	assert.LessOrEqual(t, d, 1500*time.Millisecond)

	_ = ex.requeueNodeWithBackoff(context.Background(), "wf-b", "n1", json.RawMessage(`{}`), 21)
	d2 := q.last.ProcessAfter.Sub(q.last.EnqueuedAt)

	assert.GreaterOrEqual(t, d2, 150*time.Second)
	assert.LessOrEqual(t, d2, 450*time.Second)
}
