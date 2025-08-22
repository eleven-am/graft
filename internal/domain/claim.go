package domain

import (
	"fmt"
	"time"
)

type ClaimStatus string

const (
	ClaimStatusActive   ClaimStatus = "active"
	ClaimStatusReleased ClaimStatus = "released"
	ClaimStatusExpired  ClaimStatus = "expired"
)

type ClaimEntity struct {
	WorkItemID string      `json:"work_item_id"`
	NodeID     string      `json:"node_id"`
	ClaimedAt  time.Time   `json:"claimed_at"`
	ExpiresAt  time.Time   `json:"expires_at"`
	Status     ClaimStatus `json:"status"`
}

func NewClaim(workItemID, nodeID string, duration time.Duration) *ClaimEntity {
	now := time.Now()
	return &ClaimEntity{
		WorkItemID: workItemID,
		NodeID:     nodeID,
		ClaimedAt:  now,
		ExpiresAt:  now.Add(duration),
		Status:     ClaimStatusActive,
	}
}

func (c *ClaimEntity) IsExpired() bool {
	return time.Now().After(c.ExpiresAt)
}

func (c *ClaimEntity) IsActive() bool {
	return c.Status == ClaimStatusActive && !c.IsExpired()
}

func (c *ClaimEntity) Release() {
	c.Status = ClaimStatusReleased
}

func (c *ClaimEntity) Expire() {
	c.Status = ClaimStatusExpired
}

func (c *ClaimEntity) Extend(newDuration time.Duration) {
	if c.IsActive() {
		c.ExpiresAt = time.Now().Add(newDuration)
	}
}

func (c *ClaimEntity) Validate() error {
	if c.WorkItemID == "" {
		return NewValidationError("claim", "work_item_id cannot be empty")
	}
	if c.NodeID == "" {
		return NewValidationError("claim", "node_id cannot be empty")
	}
	if c.ClaimedAt.IsZero() {
		return NewValidationError("claim", "claimed_at cannot be zero")
	}
	if c.ExpiresAt.IsZero() {
		return NewValidationError("claim", "expires_at cannot be zero")
	}
	if c.ExpiresAt.Before(c.ClaimedAt) {
		return NewValidationError("claim", "expires_at must be after claimed_at")
	}
	if c.Status != ClaimStatusActive && c.Status != ClaimStatusReleased && c.Status != ClaimStatusExpired {
		return NewValidationError("claim", fmt.Sprintf("invalid status: %s", c.Status))
	}
	return nil
}

func (c *ClaimEntity) GenerateKey() string {
	return fmt.Sprintf("claims:%s", c.WorkItemID)
}

func GenerateClaimKey(workItemID string) string {
	return fmt.Sprintf("claims:%s", workItemID)
}