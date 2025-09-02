package storage

import (
	"testing"

	"github.com/eleven-am/graft/internal/mocks"
)

func TestStoragePort_NewQueueMethods(t *testing.T) {

	mockStorage := mocks.NewMockStoragePort(t)

	mockStorage.On("GetNext", "queue:test:").Return("", []byte(nil), false, nil).Once()
	key, value, exists, err := mockStorage.GetNext("queue:test:")
	if err != nil {
		t.Fatalf("GetNext failed: %v", err)
	}
	t.Logf("GetNext returned: key=%s, exists=%v, valueLen=%d", key, exists, len(value))

	mockStorage.On("CountPrefix", "queue:test:").Return(0, nil).Once()
	count, err := mockStorage.CountPrefix("queue:test:")
	if err != nil {
		t.Fatalf("CountPrefix failed: %v", err)
	}
	t.Logf("CountPrefix returned: count=%d", count)

	mockStorage.On("AtomicIncrement", "counter:test").Return(int64(1), nil).Once()
	newValue, err := mockStorage.AtomicIncrement("counter:test")
	if err != nil {
		t.Fatalf("AtomicIncrement failed: %v", err)
	}
	t.Logf("AtomicIncrement returned: newValue=%d", newValue)

	mockStorage.AssertExpectations(t)
}
