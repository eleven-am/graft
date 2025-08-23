package domain

import (
	"fmt"
	"time"
)

type SemaphoreStatus string

const (
	SemaphoreStatusAcquired SemaphoreStatus = "acquired"
	SemaphoreStatusReleased SemaphoreStatus = "released"
	SemaphoreStatusExpired  SemaphoreStatus = "expired"
)

type SemaphoreEntity struct {
	ID         string          `json:"id"`
	NodeID     string          `json:"node_id"`
	AcquiredAt time.Time       `json:"acquired_at"`
	ExpiresAt  time.Time       `json:"expires_at"`
	Status     SemaphoreStatus `json:"status"`
}

func NewSemaphore(id, nodeID string, duration time.Duration) *SemaphoreEntity {
	now := time.Now()
	return &SemaphoreEntity{
		ID:         id,
		NodeID:     nodeID,
		AcquiredAt: now,
		ExpiresAt:  now.Add(duration),
		Status:     SemaphoreStatusAcquired,
	}
}

func (s *SemaphoreEntity) IsExpired() bool {
	return time.Now().After(s.ExpiresAt)
}

func (s *SemaphoreEntity) IsActive() bool {
	return s.Status == SemaphoreStatusAcquired && !s.IsExpired()
}

func (s *SemaphoreEntity) Release() {
	s.Status = SemaphoreStatusReleased
}

func (s *SemaphoreEntity) Expire() {
	s.Status = SemaphoreStatusExpired
}

func (s *SemaphoreEntity) Extend(newDuration time.Duration) {
	if s.IsActive() {
		s.ExpiresAt = time.Now().Add(newDuration)
	}
}

func (s *SemaphoreEntity) Validate() error {
	if s.ID == "" {
		return NewValidationError("semaphore", "id cannot be empty")
	}
	if s.NodeID == "" {
		return NewValidationError("semaphore", "node_id cannot be empty")
	}
	if s.AcquiredAt.IsZero() {
		return NewValidationError("semaphore", "acquired_at cannot be zero")
	}
	if s.ExpiresAt.IsZero() {
		return NewValidationError("semaphore", "expires_at cannot be zero")
	}
	if s.ExpiresAt.Before(s.AcquiredAt) {
		return NewValidationError("semaphore", "expires_at must be after acquired_at")
	}
	if s.Status != SemaphoreStatusAcquired && s.Status != SemaphoreStatusReleased && s.Status != SemaphoreStatusExpired {
		return NewValidationError("semaphore", fmt.Sprintf("invalid status: %s", s.Status))
	}
	return nil
}

func (s *SemaphoreEntity) GenerateKey() string {
	return fmt.Sprintf("semaphore:%s", s.ID)
}

func GenerateSemaphoreKey(id string) string {
	return fmt.Sprintf("semaphore:%s", id)
}