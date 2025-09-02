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

// fakeQueue implements ports.QueuePort for testing processNextItem release path.
type fakeQueue struct {
	claimedItem []byte
	claimID     string
	released    []string
	completed   []string
}

func (f *fakeQueue) Enqueue(item []byte) error   { return nil }
func (f *fakeQueue) Peek() ([]byte, bool, error) { return nil, false, nil }
func (f *fakeQueue) Claim() ([]byte, string, bool, error) {
	if f.claimedItem == nil {
		return nil, "", false, nil
	}
	return f.claimedItem, f.claimID, true, nil
}
func (f *fakeQueue) Complete(claimID string) error {
	f.completed = append(f.completed, claimID)
	return nil
}
func (f *fakeQueue) Release(claimID string) error {
	f.released = append(f.released, claimID)
	return nil
}
func (f *fakeQueue) WaitForItem(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (f *fakeQueue) Size() (int, error)                                            { return 0, nil }
func (f *fakeQueue) HasItemsWithPrefix(string) (bool, error)                       { return false, nil }
func (f *fakeQueue) GetItemsWithPrefix(string) ([][]byte, error)                   { return nil, nil }
func (f *fakeQueue) HasClaimedItemsWithPrefix(string) (bool, error)                { return false, nil }
func (f *fakeQueue) GetClaimedItemsWithPrefix(string) ([]ports.ClaimedItem, error) { return nil, nil }
func (f *fakeQueue) Close() error                                                  { return nil }
func (f *fakeQueue) SendToDeadLetter([]byte, string) error                         { return nil }
func (f *fakeQueue) GetDeadLetterItems(int) ([]ports.DeadLetterItem, error)        { return nil, nil }
func (f *fakeQueue) GetDeadLetterSize() (int, error)                               { return 0, nil }
func (f *fakeQueue) RetryFromDeadLetter(string) error                              { return nil }

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

	fq := &fakeQueue{claimedItem: data, claimID: "c1"}

	lb := mocks.NewMockLoadBalancer(t)
	lb.EXPECT().ShouldExecuteNode("node-id", "wf-lb", "node").Return(false, nil)

	nr := mocks.NewMockNodeRegistryPort(t)
	ev := mocks.NewMockEventManager(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	eng := NewEngine(domain.EngineConfig{}, "node-id", nr, fq, nil, ev, lb, logger)

	eng.ctx = context.Background()

	processed, err := eng.processNextItem()
	assert.NoError(t, err)
	assert.True(t, processed)

	assert.Equal(t, []string{"c1"}, fq.released)
	assert.Empty(t, fq.completed)
}
