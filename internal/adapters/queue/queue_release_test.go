package queue

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestQueue_Release_AddsDeferralAndDelay(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	q := NewQueue("test", storage, nil, nil, "", 0, nil)

	work := map[string]interface{}{
		"workflow_id": "wf-1",
		"node_name":   "N",
		"deferrals":   0,
	}
	workBytes, _ := json.Marshal(work)
	claimed := domain.NewClaimedItem(workBytes, "claim-1", 1)
	claimedBytes, _ := claimed.ToBytes()

	storage.On("Get", "queue:test:claimed:claim-1").Return(claimedBytes, int64(0), true, nil).Once()

	storage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
		if len(ops) != 2 {
			return false
		}
		if !(ops[0].Type == ports.OpPut && strings.HasPrefix(ops[0].Key, "queue:test:ready:")) {
			return false
		}
		if !(ops[1].Type == ports.OpDelete && ops[1].Key == "queue:test:claimed:claim-1") {
			return false
		}

		var qi domain.QueueItem
		if err := json.Unmarshal(ops[0].Value, &qi); err != nil {
			return false
		}
		// Value is QueueItem containing Data with updated meta
		var meta struct {
			ProcessAfter time.Time `json:"process_after"`
			Deferrals    int       `json:"deferrals"`
		}
		if err := json.Unmarshal(qi.Data, &meta); err != nil {
			return false
		}
		if meta.Deferrals != 1 {
			return false
		}
		if !meta.ProcessAfter.After(time.Now()) {
			return false
		}
		return true
	})).Return(nil).Once()

	err := q.Release("claim-1")
	assert.NoError(t, err)
	storage.AssertExpectations(t)
}

func TestQueue_Release_ToDLQAfterDeferrals(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	q := NewQueue("test", storage, nil, nil, "", 0, nil)

	work := map[string]interface{}{
		"workflow_id": "wf-2",
		"node_name":   "N",
		"deferrals":   9,
	}
	workBytes, _ := json.Marshal(work)
	claimed := domain.NewClaimedItem(workBytes, "claim-2", 2)
	claimedBytes, _ := claimed.ToBytes()

	storage.On("Get", "queue:test:claimed:claim-2").Return(claimedBytes, int64(0), true, nil).Once()

	storage.On("AtomicIncrement", "queue:test:deadletter:sequence").Return(int64(42), nil).Once()

	storage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
		if len(ops) != 2 {
			return false
		}
		if !(ops[0].Type == ports.OpPut && strings.HasPrefix(ops[0].Key, "queue:test:deadletter:")) {
			return false
		}
		if !(ops[1].Type == ports.OpDelete && ops[1].Key == "queue:test:claimed:claim-2") {
			return false
		}

		var dlqItem domain.DeadLetterQueueItem
		if err := json.Unmarshal(ops[0].Value, &dlqItem); err != nil {
			return false
		}
		if dlqItem.RetryCount < 10 {
			return false
		}
		if !strings.Contains(dlqItem.Reason, "exceeded deferrals") {
			return false
		}
		return true
	})).Return(nil).Once()

	err := q.Release("claim-2")
	assert.NoError(t, err)
	storage.AssertExpectations(t)
}

func TestQueue_ClaimedIndex_AddAndRemove(t *testing.T) {
	storage := &mocks.MockStoragePort{}
	q := NewQueue("test", storage, nil, nil, "", 0, nil)

	work := map[string]interface{}{
		"workflow_id": "wf-idx",
		"node_name":   "X",
	}
	workBytes, _ := json.Marshal(work)
	qi := domain.NewQueueItem(workBytes, 7)
	qiBytes, _ := qi.ToBytes()

	storage.On("GetNext", "queue:test:ready:").Return(
		"queue:test:ready:00000000000000000007", qiBytes, true, nil,
	).Once()

	storage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
		return len(ops) == 2 && ops[0].Type == ports.OpDeleteIfExists && strings.HasPrefix(ops[1].Key, "queue:test:claimed:")
	})).Return(nil).Once()

	_, claimID, exists, err := q.Claim()
	assert.NoError(t, err)
	assert.True(t, exists)

	has1, err := q.HasClaimedItemsWithPrefix("\"workflow_id\":\"wf-idx\"")
	assert.NoError(t, err)
	assert.True(t, has1)

	storage.On("Get", mock.Anything).Return(nil, int64(0), false, nil).Maybe()
	storage.On("Delete", mock.Anything).Return(nil).Once()
	err = q.Complete(claimID)
	assert.NoError(t, err)

	storage.On("ListByPrefix", "queue:test:claimed:").Return([]ports.KeyValueVersion{}, nil).Maybe()

	has2, err := q.HasClaimedItemsWithPrefix("\"workflow_id\":\"wf-idx\"")
	assert.NoError(t, err)
	assert.False(t, has2)

	storage.AssertExpectations(t)
}
