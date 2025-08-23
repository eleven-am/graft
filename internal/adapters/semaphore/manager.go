package semaphore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type SemaphoreManager struct {
	storage ports.StoragePort
}

func NewSemaphoreManager(storage ports.StoragePort) *SemaphoreManager {
	return &SemaphoreManager{
		storage: storage,
	}
}

func (sm *SemaphoreManager) Acquire(ctx context.Context, semaphoreID string, nodeID string, duration time.Duration) error {
	semaphoreKey := domain.GenerateSemaphoreKey(semaphoreID)

	existingData, err := sm.storage.Get(ctx, semaphoreKey)
	if err == nil {
		var existing domain.SemaphoreEntity
		if err := json.Unmarshal(existingData, &existing); err == nil {
			if existing.IsActive() {
				if existing.NodeID == nodeID {
					existing.Extend(duration)
					data, err := json.Marshal(existing)
					if err != nil {
						return fmt.Errorf("failed to marshal extended semaphore: %w", err)
					}
					return sm.storage.Put(ctx, semaphoreKey, data)
				}
				return domain.Error{
					Type:    domain.ErrorTypeConflict,
					Message: "semaphore already acquired by another node",
					Details: map[string]interface{}{
						"semaphore_id":    semaphoreID,
						"owner_node":      existing.NodeID,
						"requesting_node": nodeID,
					},
				}
			}
			if err := sm.storage.Delete(ctx, semaphoreKey); err != nil {
				return fmt.Errorf("failed to clean up inactive semaphore: %w", err)
			}
		}
	} else if !domain.IsKeyNotFound(err) {
		return fmt.Errorf("failed to check existing semaphore: %w", err)
	}

	semaphore := domain.NewSemaphore(semaphoreID, nodeID, duration)
	if err := semaphore.Validate(); err != nil {
		return err
	}

	data, err := json.Marshal(semaphore)
	if err != nil {
		return fmt.Errorf("failed to marshal semaphore: %w", err)
	}

	return sm.storage.Put(ctx, semaphoreKey, data)
}

func (sm *SemaphoreManager) Release(ctx context.Context, semaphoreID string, nodeID string) error {
	semaphoreKey := domain.GenerateSemaphoreKey(semaphoreID)

	existingData, err := sm.storage.Get(ctx, semaphoreKey)
	if err != nil {
		if domain.IsKeyNotFound(err) {
			return domain.NewNotFoundError("semaphore", semaphoreID)
		}
		return fmt.Errorf("failed to get semaphore: %w", err)
	}

	var semaphore domain.SemaphoreEntity
	if err := json.Unmarshal(existingData, &semaphore); err != nil {
		return fmt.Errorf("failed to unmarshal semaphore: %w", err)
	}

	if semaphore.NodeID != nodeID {
		return domain.Error{
			Type:    domain.ErrorTypeUnauthorized,
			Message: "cannot release semaphore owned by another node",
			Details: map[string]interface{}{
				"semaphore_id":    semaphoreID,
				"owner_node":      semaphore.NodeID,
				"requesting_node": nodeID,
			},
		}
	}

	return sm.storage.Delete(ctx, semaphoreKey)
}

func (sm *SemaphoreManager) IsAcquired(ctx context.Context, semaphoreID string) (bool, error) {
	semaphoreKey := domain.GenerateSemaphoreKey(semaphoreID)

	existingData, err := sm.storage.Get(ctx, semaphoreKey)
	if err != nil {
		if domain.IsKeyNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get semaphore: %w", err)
	}

	var semaphore domain.SemaphoreEntity
	if err := json.Unmarshal(existingData, &semaphore); err != nil {
		return false, fmt.Errorf("failed to unmarshal semaphore: %w", err)
	}

	if !semaphore.IsActive() {
		if err := sm.storage.Delete(ctx, semaphoreKey); err != nil {
			return false, fmt.Errorf("failed to clean up inactive semaphore: %w", err)
		}
		return false, nil
	}

	return true, nil
}

func (sm *SemaphoreManager) IsAcquiredByNode(ctx context.Context, semaphoreID string, nodeID string) (bool, error) {
	semaphoreKey := domain.GenerateSemaphoreKey(semaphoreID)

	existingData, err := sm.storage.Get(ctx, semaphoreKey)
	if err != nil {
		if domain.IsKeyNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get semaphore: %w", err)
	}

	var semaphore domain.SemaphoreEntity
	if err := json.Unmarshal(existingData, &semaphore); err != nil {
		return false, fmt.Errorf("failed to unmarshal semaphore: %w", err)
	}

	if !semaphore.IsActive() {
		if err := sm.storage.Delete(ctx, semaphoreKey); err != nil {
			return false, fmt.Errorf("failed to clean up inactive semaphore: %w", err)
		}
		return false, nil
	}

	return semaphore.NodeID == nodeID, nil
}

func (sm *SemaphoreManager) Extend(ctx context.Context, semaphoreID string, nodeID string, newDuration time.Duration) error {
	semaphoreKey := domain.GenerateSemaphoreKey(semaphoreID)

	existingData, err := sm.storage.Get(ctx, semaphoreKey)
	if err != nil {
		if domain.IsKeyNotFound(err) {
			return domain.NewNotFoundError("semaphore", semaphoreID)
		}
		return fmt.Errorf("failed to get semaphore: %w", err)
	}

	var semaphore domain.SemaphoreEntity
	if err := json.Unmarshal(existingData, &semaphore); err != nil {
		return fmt.Errorf("failed to unmarshal semaphore: %w", err)
	}

	if semaphore.NodeID != nodeID {
		return domain.Error{
			Type:    domain.ErrorTypeUnauthorized,
			Message: "cannot extend semaphore owned by another node",
			Details: map[string]interface{}{
				"semaphore_id":    semaphoreID,
				"owner_node":      semaphore.NodeID,
				"requesting_node": nodeID,
			},
		}
	}

	if !semaphore.IsActive() {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "cannot extend inactive semaphore",
			Details: map[string]interface{}{
				"semaphore_id": semaphoreID,
				"status":       semaphore.Status,
			},
		}
	}

	semaphore.Extend(newDuration)

	data, err := json.Marshal(semaphore)
	if err != nil {
		return fmt.Errorf("failed to marshal extended semaphore: %w", err)
	}

	return sm.storage.Put(ctx, semaphoreKey, data)
}
