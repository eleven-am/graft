package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type leaseManagerStub struct{}

func (leaseManagerStub) Key(namespace, id string) string {
	return "lease:" + namespace + ":" + id
}

func (leaseManagerStub) TryAcquire(string, string, time.Duration, map[string]string) (*ports.LeaseRecord, bool, error) {
	panic("not implemented")
}

func (leaseManagerStub) Renew(string, string, time.Duration) (*ports.LeaseRecord, error) {
	panic("not implemented")
}

func (leaseManagerStub) Release(string, string) error {
	panic("not implemented")
}

func (leaseManagerStub) ForceRelease(string) error {
	panic("not implemented")
}

func (leaseManagerStub) Get(string) (*ports.LeaseRecord, bool, error) {
	panic("not implemented")
}

func TestQueue_Enqueue(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(1), nil).Once()
	mockStorage.On("Put", "queue:test:ready:00000000000000000001", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()
	mockEventManager.On("Broadcast", mock.Anything).Return(nil).Once()

	err := queue.Enqueue([]byte("test data"))
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
}

func TestQueue_EnqueueMultiple(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(1), nil).Once()
	mockStorage.On("Put", "queue:test:ready:00000000000000000001", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()
	mockEventManager.On("Broadcast", mock.Anything).Return(nil).Once()

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(2), nil).Once()
	mockStorage.On("Put", "queue:test:ready:00000000000000000002", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()
	mockEventManager.On("Broadcast", mock.Anything).Return(nil).Once()

	err := queue.Enqueue([]byte("data 1"))
	assert.NoError(t, err)

	err = queue.Enqueue([]byte("data 2"))
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
}

func TestQueue_Peek(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	queueItem := domain.NewQueueItem([]byte("test data"), 1)
	itemBytes, _ := queueItem.ToBytes()

	mockStorage.On("GetNext", "queue:test:ready:").Return(
		"queue:test:ready:00000000000000000001", itemBytes, true, nil).Once()

	data, exists, err := queue.Peek()
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("test data"), data)

	mockStorage.AssertExpectations(t)
}

func TestQueue_PeekEmpty(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	mockStorage.On("GetNext", "queue:test:ready:").Return("", nil, false, nil).Once()

	data, exists, err := queue.Peek()
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Nil(t, data)

	mockStorage.AssertExpectations(t)
}

func TestQueue_PeekOrdering(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	item1 := domain.NewQueueItem([]byte("first"), 1)
	item1Bytes, _ := item1.ToBytes()

	mockStorage.On("GetNext", "queue:test:ready:").Return(
		"queue:test:ready:00000000000000000001", item1Bytes, true, nil).Once()

	data, exists, err := queue.Peek()
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("first"), data)

	mockStorage.AssertExpectations(t)
}

func TestQueue_Claim(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	queueItem := domain.NewQueueItem([]byte("test data"), 1)
	itemBytes, _ := queueItem.ToBytes()

	mockStorage.On("GetNext", "queue:test:ready:").Return(
		"queue:test:ready:00000000000000000001", itemBytes, true, nil).Once()
	mockStorage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
		return len(ops) == 2 &&
			ops[0].Type == ports.OpDelete &&
			ops[0].Key == "queue:test:ready:00000000000000000001" &&
			ops[1].Type == ports.OpPut &&
			len(ops[1].Key) > 0 &&
			len(ops[1].Value) > 0
	})).Return(nil).Once()

	data, claimID, exists, err := queue.Claim()
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("test data"), data)
	assert.NotEmpty(t, claimID)

	mockStorage.AssertExpectations(t)
}

func TestQueue_ClaimEmpty(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	mockStorage.On("GetNext", "queue:test:ready:").Return("", nil, false, nil).Once()

	data, claimID, exists, err := queue.Claim()
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Nil(t, data)
	assert.Empty(t, claimID)

	mockStorage.AssertExpectations(t)
}

func TestQueue_ClaimOrdering(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	item1 := domain.NewQueueItem([]byte("first"), 1)
	item1Bytes, _ := item1.ToBytes()

	mockStorage.On("GetNext", "queue:test:ready:").Return(
		"queue:test:ready:00000000000000000001", item1Bytes, true, nil).Once()
	mockStorage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
		return ops[0].Key == "queue:test:ready:00000000000000000001"
	})).Return(nil).Once()

	data, claimID, exists, err := queue.Claim()
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("first"), data)
	assert.NotEmpty(t, claimID)

	mockStorage.AssertExpectations(t)
}

func TestQueue_ClaimProcessAfterDecodeError(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	payload := []byte("{\"process_after\":\"not-a-time\"}")
	queueItem := domain.NewQueueItem(payload, 1)
	itemBytes, _ := queueItem.ToBytes()

	mockStorage.On("GetNext", "queue:test:ready:").Return(
		"queue:test:ready:00000000000000000001", itemBytes, true, nil).Once()

	data, claimID, exists, err := queue.Claim()
	assert.Error(t, err)
	assert.False(t, exists)
	assert.Nil(t, data)
	assert.Empty(t, claimID)

	mockStorage.AssertExpectations(t)
}

func TestQueue_Complete(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	claimID := "test-claim-id"
	expectedKey := "queue:test:claimed:test-claim-id"

	mockStorage.On("Delete", expectedKey).Return(nil).Once()

	err := queue.Complete(claimID)
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
}

func TestQueue_Size(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	mockStorage.On("CountPrefix", "queue:test:ready:").Return(2, nil).Once()
	mockStorage.On("CountPrefix", "queue:test:blocked:").Return(1, nil).Once()

	size, err := queue.Size()
	assert.NoError(t, err)
	assert.Equal(t, 3, size)

	mockStorage.AssertExpectations(t)
}

func TestQueue_SizeEmpty(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	mockStorage.On("CountPrefix", "queue:test:ready:").Return(0, nil).Once()
	mockStorage.On("CountPrefix", "queue:test:blocked:").Return(0, nil).Once()

	size, err := queue.Size()
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	mockStorage.AssertExpectations(t)
}

func TestQueue_WaitForItem(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	waitCh := queue.WaitForItem(ctx)

	select {
	case <-waitCh:
		t.Fatal("Should timeout since no events sent")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestQueue_WaitForItemIgnoreDelete(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	waitCh := queue.WaitForItem(ctx)

	select {
	case <-waitCh:
		t.Fatal("Should timeout since no events sent")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestQueue_WaitForItemLeaseEvent(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEvents := &mocks.MockEventManager{}
	leaseMgr := leaseManagerStub{}
	queue := NewQueue("test", mockStorage, mockEvents, leaseMgr, "", 0, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readyCh := make(chan domain.Event)
	readyUnsubCalled := false
	readyUnsub := func() {
		readyUnsubCalled = true
		close(readyCh)
	}

	leasePrefix := leaseMgr.Key("queue-claim:test", "")
	leaseCh := make(chan domain.Event)
	leaseUnsubCalled := false
	leaseUnsub := func() {
		leaseUnsubCalled = true
		close(leaseCh)
	}

	mockEvents.On("SubscribeToChannel", "queue:test:ready:").Return((<-chan domain.Event)(readyCh), readyUnsub, nil).Once()
	mockEvents.On("SubscribeToChannel", leasePrefix).Return((<-chan domain.Event)(leaseCh), leaseUnsub, nil).Once()

	waitCh := queue.WaitForItem(ctx)

	select {
	case <-waitCh:
		t.Fatal("unexpected notification before lease event")
	case <-time.After(20 * time.Millisecond):
	}

	leaseCh <- domain.Event{Type: domain.EventDelete, Key: leaseMgr.Key("queue-claim:test", "claim-1"), Timestamp: time.Now()}

	select {
	case <-waitCh:
		// expected
	case <-time.After(150 * time.Millisecond):
		t.Fatal("expected lease delete event to signal wait channel")
	}

	cancel()
	<-waitCh

	assert.True(t, readyUnsubCalled)
	assert.True(t, leaseUnsubCalled)
}

func TestQueue_ClaimSkipsDeferredBeyondLimit(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	queue := NewQueue("test", mockStorage, nil, nil, "", 0, nil)

	const deferredCount = 150
	prefix := "queue:test:ready:"

	keys := make([]string, deferredCount+1)
	values := make([][]byte, deferredCount+1)

	future := time.Now().Add(2 * time.Hour)
	for i := 0; i < deferredCount; i++ {
		seq := int64(i + 1)
		metadata := struct {
			ProcessAfter time.Time `json:"process_after"`
		}{ProcessAfter: future}
		payload, err := json.Marshal(metadata)
		if err != nil {
			t.Fatalf("failed to marshal payload: %v", err)
		}
		item := domain.NewQueueItem(payload, seq)
		itemBytes, err := item.ToBytes()
		if err != nil {
			t.Fatalf("failed to encode queue item: %v", err)
		}
		keys[i] = fmt.Sprintf("%s%020d", prefix, seq)
		values[i] = itemBytes
	}

	readySeq := int64(deferredCount + 1)
	readyItem := domain.NewQueueItem([]byte("{\"data\":\"ready\"}"), readySeq)
	readyBytes, err := readyItem.ToBytes()
	if err != nil {
		t.Fatalf("failed to encode ready item: %v", err)
	}
	keys[deferredCount] = fmt.Sprintf("%s%020d", prefix, readySeq)
	values[deferredCount] = readyBytes

	mockStorage.On("GetNext", prefix).Return(keys[0], values[0], true, nil).Once()

	index := 0
	mockStorage.EXPECT().GetNextAfter(prefix, mock.Anything).RunAndReturn(func(string, string) (string, []byte, bool, error) {
		index++
		if index >= len(keys) {
			return "", nil, false, nil
		}
		return keys[index], values[index], true, nil
	})

	mockStorage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
		if len(ops) != 2 {
			return false
		}
		return ops[0].Type == ports.OpDelete && ops[0].Key == keys[deferredCount] &&
			ops[1].Type == ports.OpPut
	})).Return(nil).Once()

	data, claimID, exists, err := queue.Claim()
	require.NoError(t, err)
	require.True(t, exists)
	require.NotEmpty(t, claimID)
	require.Equal(t, []byte("{\"data\":\"ready\"}"), data)

	mockStorage.AssertExpectations(t)
}

func TestQueue_Close(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	err := queue.Close()
	assert.NoError(t, err)

	err = queue.Enqueue([]byte("test"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue is closed")
}

func TestQueue_ClosedOperations(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	queue.Close()

	_, exists, err := queue.Peek()
	assert.Error(t, err)
	assert.False(t, exists)

	_, _, exists, err = queue.Claim()
	assert.Error(t, err)
	assert.False(t, exists)

	err = queue.Complete("test")
	assert.Error(t, err)

	_, err = queue.Size()
	assert.Error(t, err)
}

func TestQueue_ConcurrentEnqueue(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	for i := 1; i <= 10; i++ {
		mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(i), nil).Once()
	}
	mockStorage.On("Put", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Times(10)
	mockEventManager.On("Broadcast", mock.Anything).Return(nil).Times(10)

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()
			data := []byte("data " + string(rune('0'+id)))
			err := queue.Enqueue(data)
			require.NoError(t, err)
		}(i)
	}

	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for concurrent enqueues")
		}
	}
}

func TestQueue_EnqueueClaimComplete(t *testing.T) {
	mockStorage := &mocks.MockStoragePort{}
	mockEventManager := &mocks.MockEventManager{}
	queue := NewQueue("test", mockStorage, mockEventManager, nil, "", 0, nil)

	queueItem := domain.NewQueueItem([]byte("test data"), 1)
	itemBytes, _ := queueItem.ToBytes()

	mockStorage.On("AtomicIncrement", "queue:test:sequence").Return(int64(1), nil).Once()
	mockStorage.On("Put", "queue:test:ready:00000000000000000001", mock.AnythingOfType("[]uint8"), int64(0)).Return(nil).Once()
	mockEventManager.On("Broadcast", mock.Anything).Return(nil).Once()

	mockStorage.On("GetNext", "queue:test:ready:").Return(
		"queue:test:ready:00000000000000000001", itemBytes, true, nil).Once()

	mockStorage.On("BatchWrite", mock.MatchedBy(func(ops []ports.WriteOp) bool {
		return len(ops) == 2 && ops[0].Type == ports.OpDelete
	})).Return(nil).Once()

	mockStorage.On("Delete", mock.AnythingOfType("string")).Return(nil).Once()

	err := queue.Enqueue([]byte("test data"))
	require.NoError(t, err)

	data, claimID, exists, err := queue.Claim()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, []byte("test data"), data)
	require.NotEmpty(t, claimID)

	err = queue.Complete(claimID)
	require.NoError(t, err)

	mockStorage.AssertExpectations(t)
}
