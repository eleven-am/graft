package semaphore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSemaphoreManager_Acquire(t *testing.T) {
	t.Run("acquire new semaphore", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := mocks.NewMockStoragePort(t)
		manager := NewSemaphoreManager(mockStorage)

		mockStorage.On("Get", ctx, "semaphore:workflow-1").Return(nil, domain.NewNotFoundError("key", "semaphore:workflow-1"))
		mockStorage.On("Put", ctx, "semaphore:workflow-1", mock.Anything).Return(nil)

		err := manager.Acquire(ctx, "workflow-1", "node-1", time.Minute)
		assert.NoError(t, err)

		mockStorage.AssertExpectations(t)
	})
}

func TestSemaphoreManager_Release(t *testing.T) {
	t.Run("release existing semaphore", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := mocks.NewMockStoragePort(t)
		manager := NewSemaphoreManager(mockStorage)

		semaphore := domain.NewSemaphore("workflow-1", "node-1", time.Minute)
		data, _ := json.Marshal(semaphore)

		mockStorage.On("Get", ctx, "semaphore:workflow-1").Return(data, nil)
		mockStorage.On("Delete", ctx, "semaphore:workflow-1").Return(nil)

		err := manager.Release(ctx, "workflow-1", "node-1")
		assert.NoError(t, err)

		mockStorage.AssertExpectations(t)
	})

	t.Run("release non-existent semaphore", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := mocks.NewMockStoragePort(t)
		manager := NewSemaphoreManager(mockStorage)

		mockStorage.On("Get", ctx, "semaphore:non-existent").Return(nil, domain.NewNotFoundError("key", "semaphore:non-existent"))

		err := manager.Release(ctx, "non-existent", "node-1")
		assert.Error(t, err)

		var domainErr domain.Error
		assert.ErrorAs(t, err, &domainErr)
		assert.Equal(t, domain.ErrorTypeNotFound, domainErr.Type)

		mockStorage.AssertExpectations(t)
	})
}

func TestSemaphoreManager_IsAcquired(t *testing.T) {
	t.Run("check non-existent semaphore", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := mocks.NewMockStoragePort(t)
		manager := NewSemaphoreManager(mockStorage)

		mockStorage.On("Get", ctx, "semaphore:non-existent").Return(nil, domain.NewNotFoundError("key", "semaphore:non-existent"))

		acquired, err := manager.IsAcquired(ctx, "non-existent")
		assert.NoError(t, err)
		assert.False(t, acquired)

		mockStorage.AssertExpectations(t)
	})

	t.Run("check active semaphore", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := mocks.NewMockStoragePort(t)
		manager := NewSemaphoreManager(mockStorage)

		semaphore := domain.NewSemaphore("workflow-1", "node-1", time.Minute)
		data, _ := json.Marshal(semaphore)

		mockStorage.On("Get", ctx, "semaphore:workflow-1").Return(data, nil)

		acquired, err := manager.IsAcquired(ctx, "workflow-1")
		assert.NoError(t, err)
		assert.True(t, acquired)

		mockStorage.AssertExpectations(t)
	})
}
