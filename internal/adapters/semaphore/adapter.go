package semaphore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Status string

const (
	StatusAcquired Status = "acquired"
	StatusExpired  Status = "expired"
)

type Record struct {
	ID         string    `json:"id"`
	NodeID     string    `json:"node_id"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Status     Status    `json:"status"`
}

func (s *Record) IsActive() bool {
	return s.Status == StatusAcquired && time.Now().Before(s.ExpiresAt)
}

func (s *Record) Extend(duration time.Duration) {
	s.ExpiresAt = time.Now().Add(duration)
}

type Adapter struct {
	storage ports.StoragePort
	logger  *slog.Logger
}

func NewAdapter(storage ports.StoragePort, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}

	return &Adapter{
		storage: storage,
		logger:  logger.With("component", "semaphore"),
	}
}

func (sm *Adapter) Acquire(ctx context.Context, semaphoreID string, nodeID string, duration time.Duration) error {
	key := sm.getSemaphoreKey(semaphoreID)
	sm.logger.Debug("attempting to acquire semaphore", "semaphore_id", semaphoreID, "node_id", nodeID, "duration", duration)

	existingData, err := sm.storage.Get(ctx, key)
	if err == nil {
		var existing Record
		if err := json.Unmarshal(existingData, &existing); err == nil {
			if existing.IsActive() {
				if existing.NodeID == nodeID {
					sm.logger.Debug("extending existing semaphore", "semaphore_id", semaphoreID, "node_id", nodeID, "new_duration", duration)
					existing.Extend(duration)
					data, _ := json.Marshal(existing)
					return sm.storage.Put(ctx, key, data)
				}
				sm.logger.Info("semaphore acquisition failed - already owned", "semaphore_id", semaphoreID, "owner", existing.NodeID, "requester", nodeID)
				return domain.NewSemaphoreError(semaphoreID, "acquire", domain.ErrConflict)
			}
			sm.logger.Debug("removing inactive semaphore", "semaphore_id", semaphoreID)
		}
	}

	now := time.Now()
	semaphore := Record{
		ID:         semaphoreID,
		NodeID:     nodeID,
		AcquiredAt: now,
		ExpiresAt:  now.Add(duration),
		Status:     StatusAcquired,
	}

	data, err := json.Marshal(semaphore)
	if err != nil {
		sm.logger.Error("failed to marshal semaphore", "semaphore_id", semaphoreID, "error", err)
		return domain.NewSemaphoreError(semaphoreID, "marshal", err)
	}

	if err := sm.storage.Put(ctx, key, data); err != nil {
		sm.logger.Error("failed to store semaphore", "semaphore_id", semaphoreID, "error", err)
		return err
	}

	sm.logger.Info("semaphore acquired", "semaphore_id", semaphoreID, "node_id", nodeID, "expires_at", semaphore.ExpiresAt)
	return nil
}

func (sm *Adapter) Release(ctx context.Context, semaphoreID string, nodeID string) error {
	key := sm.getSemaphoreKey(semaphoreID)
	sm.logger.Debug("attempting to release semaphore", "semaphore_id", semaphoreID, "node_id", nodeID)

	existingData, err := sm.storage.Get(ctx, key)
	if err != nil {
		sm.logger.Info("semaphore release failed - not found", "semaphore_id", semaphoreID, "node_id", nodeID)
		return domain.NewSemaphoreError(semaphoreID, "release", domain.ErrNotFound)
	}

	var semaphore Record
	if err := json.Unmarshal(existingData, &semaphore); err != nil {
		sm.logger.Error("failed to unmarshal semaphore", "semaphore_id", semaphoreID, "error", err)
		return domain.NewSemaphoreError(semaphoreID, "unmarshal", err)
	}

	if semaphore.NodeID != nodeID {
		sm.logger.Info("semaphore release failed - wrong owner", "semaphore_id", semaphoreID, "owner", semaphore.NodeID, "requester", nodeID)
		return domain.NewSemaphoreError(semaphoreID, "release", domain.ErrUnauthorized)
	}

	if err := sm.storage.Delete(ctx, key); err != nil {
		sm.logger.Error("failed to delete semaphore", "semaphore_id", semaphoreID, "error", err)
		return err
	}

	sm.logger.Info("semaphore released", "semaphore_id", semaphoreID, "node_id", nodeID)
	return nil
}

func (sm *Adapter) IsAcquired(ctx context.Context, semaphoreID string) (bool, error) {
	key := sm.getSemaphoreKey(semaphoreID)
	sm.logger.Debug("checking if semaphore is acquired", "semaphore_id", semaphoreID)

	existingData, err := sm.storage.Get(ctx, key)
	if err != nil {
		sm.logger.Debug("semaphore not found", "semaphore_id", semaphoreID)
		return false, nil
	}

	var semaphore Record
	if err := json.Unmarshal(existingData, &semaphore); err != nil {
		sm.logger.Debug("failed to unmarshal semaphore", "semaphore_id", semaphoreID, "error", err)
		return false, nil
	}

	if !semaphore.IsActive() {
		sm.logger.Debug("cleaning up inactive semaphore", "semaphore_id", semaphoreID)
		sm.storage.Delete(ctx, key)
		return false, nil
	}

	sm.logger.Debug("semaphore is active", "semaphore_id", semaphoreID, "owner", semaphore.NodeID)
	return true, nil
}

func (sm *Adapter) IsAcquiredByNode(ctx context.Context, semaphoreID string, nodeID string) (bool, error) {
	key := sm.getSemaphoreKey(semaphoreID)
	sm.logger.Debug("checking if semaphore is acquired by node", "semaphore_id", semaphoreID, "node_id", nodeID)

	existingData, err := sm.storage.Get(ctx, key)
	if err != nil {
		sm.logger.Debug("semaphore not found for node check", "semaphore_id", semaphoreID, "node_id", nodeID)
		return false, nil
	}

	var semaphore Record
	if err := json.Unmarshal(existingData, &semaphore); err != nil {
		sm.logger.Debug("failed to unmarshal semaphore for node check", "semaphore_id", semaphoreID, "error", err)
		return false, nil
	}

	if !semaphore.IsActive() {
		sm.logger.Debug("cleaning up inactive semaphore during node check", "semaphore_id", semaphoreID)
		sm.storage.Delete(ctx, key)
		return false, nil
	}

	isOwner := semaphore.NodeID == nodeID
	sm.logger.Debug("semaphore ownership check completed", "semaphore_id", semaphoreID, "node_id", nodeID, "is_owner", isOwner)
	return isOwner, nil
}

func (sm *Adapter) Extend(ctx context.Context, semaphoreID string, nodeID string, newDuration time.Duration) error {
	key := sm.getSemaphoreKey(semaphoreID)
	sm.logger.Debug("attempting to extend semaphore", "semaphore_id", semaphoreID, "node_id", nodeID, "new_duration", newDuration)

	existingData, err := sm.storage.Get(ctx, key)
	if err != nil {
		sm.logger.Info("semaphore extend failed - not found", "semaphore_id", semaphoreID, "node_id", nodeID)
		return domain.NewSemaphoreError(semaphoreID, "extend", domain.ErrNotFound)
	}

	var semaphore Record
	if err := json.Unmarshal(existingData, &semaphore); err != nil {
		sm.logger.Error("failed to unmarshal semaphore for extend", "semaphore_id", semaphoreID, "error", err)
		return domain.NewSemaphoreError(semaphoreID, "unmarshal", err)
	}

	if semaphore.NodeID != nodeID {
		sm.logger.Info("semaphore extend failed - wrong owner", "semaphore_id", semaphoreID, "owner", semaphore.NodeID, "requester", nodeID)
		return domain.NewSemaphoreError(semaphoreID, "extend", domain.ErrUnauthorized)
	}

	if !semaphore.IsActive() {
		sm.logger.Info("semaphore extend failed - inactive", "semaphore_id", semaphoreID, "node_id", nodeID)
		return domain.NewSemaphoreError(semaphoreID, "extend", domain.ErrExpired)
	}

	oldExpiresAt := semaphore.ExpiresAt
	semaphore.Extend(newDuration)

	data, err := json.Marshal(semaphore)
	if err != nil {
		sm.logger.Error("failed to marshal extended semaphore", "semaphore_id", semaphoreID, "error", err)
		return domain.NewSemaphoreError(semaphoreID, "marshal", err)
	}

	if err := sm.storage.Put(ctx, key, data); err != nil {
		sm.logger.Error("failed to store extended semaphore", "semaphore_id", semaphoreID, "error", err)
		return err
	}

	sm.logger.Info("semaphore extended", "semaphore_id", semaphoreID, "node_id", nodeID, "old_expires_at", oldExpiresAt, "new_expires_at", semaphore.ExpiresAt)
	return nil
}

func (sm *Adapter) getSemaphoreKey(semaphoreID string) string {
	return fmt.Sprintf("semaphore:%s", semaphoreID)
}
