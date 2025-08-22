package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClaim(t *testing.T) {
	workItemID := "work-item-1"
	nodeID := "node-1" 
	duration := 5 * time.Minute

	claim := NewClaim(workItemID, nodeID, duration)

	require.NotNil(t, claim)
	assert.Equal(t, workItemID, claim.WorkItemID)
	assert.Equal(t, nodeID, claim.NodeID)
	assert.Equal(t, ClaimStatusActive, claim.Status)
	assert.False(t, claim.ClaimedAt.IsZero())
	assert.False(t, claim.ExpiresAt.IsZero())
	assert.True(t, claim.ExpiresAt.After(claim.ClaimedAt))
	assert.WithinDuration(t, time.Now().Add(duration), claim.ExpiresAt, time.Second)
}

func TestClaimEntity_IsExpired(t *testing.T) {
	tests := []struct {
		name        string
		expiresAt   time.Time
		shouldExpire bool
	}{
		{
			name:        "not expired - future expiry",
			expiresAt:   time.Now().Add(5 * time.Minute),
			shouldExpire: false,
		},
		{
			name:        "expired - past expiry",
			expiresAt:   time.Now().Add(-5 * time.Minute),
			shouldExpire: true,
		},
		{
			name:        "just expired - 1 second ago",
			expiresAt:   time.Now().Add(-1 * time.Second),
			shouldExpire: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claim := &ClaimEntity{
				WorkItemID: "work-1",
				NodeID:     "node-1",
				ClaimedAt:  time.Now().Add(-10 * time.Minute),
				ExpiresAt:  tt.expiresAt,
				Status:     ClaimStatusActive,
			}

			assert.Equal(t, tt.shouldExpire, claim.IsExpired())
		})
	}
}

func TestClaimEntity_IsActive(t *testing.T) {
	tests := []struct {
		name     string
		status   ClaimStatus
		expiresAt time.Time
		expected bool
	}{
		{
			name:     "active and not expired",
			status:   ClaimStatusActive,
			expiresAt: time.Now().Add(5 * time.Minute),
			expected: true,
		},
		{
			name:     "active but expired",
			status:   ClaimStatusActive,
			expiresAt: time.Now().Add(-5 * time.Minute),
			expected: false,
		},
		{
			name:     "released and not expired",
			status:   ClaimStatusReleased,
			expiresAt: time.Now().Add(5 * time.Minute),
			expected: false,
		},
		{
			name:     "expired status and expired time",
			status:   ClaimStatusExpired,
			expiresAt: time.Now().Add(-5 * time.Minute),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claim := &ClaimEntity{
				WorkItemID: "work-1",
				NodeID:     "node-1",
				ClaimedAt:  time.Now().Add(-10 * time.Minute),
				ExpiresAt:  tt.expiresAt,
				Status:     tt.status,
			}

			assert.Equal(t, tt.expected, claim.IsActive())
		})
	}
}

func TestClaimEntity_Release(t *testing.T) {
	claim := &ClaimEntity{
		WorkItemID: "work-1",
		NodeID:     "node-1",
		ClaimedAt:  time.Now().Add(-5 * time.Minute),
		ExpiresAt:  time.Now().Add(5 * time.Minute),
		Status:     ClaimStatusActive,
	}

	assert.True(t, claim.IsActive())

	claim.Release()

	assert.Equal(t, ClaimStatusReleased, claim.Status)
	assert.False(t, claim.IsActive())
}

func TestClaimEntity_Expire(t *testing.T) {
	claim := &ClaimEntity{
		WorkItemID: "work-1",
		NodeID:     "node-1",
		ClaimedAt:  time.Now().Add(-5 * time.Minute),
		ExpiresAt:  time.Now().Add(5 * time.Minute),
		Status:     ClaimStatusActive,
	}

	assert.True(t, claim.IsActive())

	claim.Expire()

	assert.Equal(t, ClaimStatusExpired, claim.Status)
	assert.False(t, claim.IsActive())
}

func TestClaimEntity_Extend(t *testing.T) {
	claim := &ClaimEntity{
		WorkItemID: "work-1",
		NodeID:     "node-1",
		ClaimedAt:  time.Now().Add(-5 * time.Minute),
		ExpiresAt:  time.Now().Add(5 * time.Minute),
		Status:     ClaimStatusActive,
	}

	originalExpiry := claim.ExpiresAt
	newDuration := 10 * time.Minute

	claim.Extend(newDuration)

	assert.True(t, claim.ExpiresAt.After(originalExpiry))
	assert.WithinDuration(t, time.Now().Add(newDuration), claim.ExpiresAt, time.Second)
}

func TestClaimEntity_Extend_InactiveClaim(t *testing.T) {
	claim := &ClaimEntity{
		WorkItemID: "work-1",
		NodeID:     "node-1",
		ClaimedAt:  time.Now().Add(-5 * time.Minute),
		ExpiresAt:  time.Now().Add(5 * time.Minute),
		Status:     ClaimStatusReleased,
	}

	originalExpiry := claim.ExpiresAt
	newDuration := 10 * time.Minute

	claim.Extend(newDuration)

	assert.Equal(t, originalExpiry, claim.ExpiresAt)
}

func TestClaimEntity_Validate(t *testing.T) {
	tests := []struct {
		name          string
		claim         ClaimEntity
		expectedError string
	}{
		{
			name: "valid claim",
			claim: ClaimEntity{
				WorkItemID: "work-1",
				NodeID:     "node-1",
				ClaimedAt:  time.Now().Add(-5 * time.Minute),
				ExpiresAt:  time.Now().Add(5 * time.Minute),
				Status:     ClaimStatusActive,
			},
		},
		{
			name: "empty work item ID",
			claim: ClaimEntity{
				WorkItemID: "",
				NodeID:     "node-1",
				ClaimedAt:  time.Now().Add(-5 * time.Minute),
				ExpiresAt:  time.Now().Add(5 * time.Minute),
				Status:     ClaimStatusActive,
			},
			expectedError: "work_item_id cannot be empty",
		},
		{
			name: "empty node ID",
			claim: ClaimEntity{
				WorkItemID: "work-1",
				NodeID:     "",
				ClaimedAt:  time.Now().Add(-5 * time.Minute),
				ExpiresAt:  time.Now().Add(5 * time.Minute),
				Status:     ClaimStatusActive,
			},
			expectedError: "node_id cannot be empty",
		},
		{
			name: "zero claimed at",
			claim: ClaimEntity{
				WorkItemID: "work-1",
				NodeID:     "node-1",
				ClaimedAt:  time.Time{},
				ExpiresAt:  time.Now().Add(5 * time.Minute),
				Status:     ClaimStatusActive,
			},
			expectedError: "claimed_at cannot be zero",
		},
		{
			name: "zero expires at",
			claim: ClaimEntity{
				WorkItemID: "work-1",
				NodeID:     "node-1",
				ClaimedAt:  time.Now().Add(-5 * time.Minute),
				ExpiresAt:  time.Time{},
				Status:     ClaimStatusActive,
			},
			expectedError: "expires_at cannot be zero",
		},
		{
			name: "expires before claimed",
			claim: ClaimEntity{
				WorkItemID: "work-1",
				NodeID:     "node-1",
				ClaimedAt:  time.Now().Add(5 * time.Minute),
				ExpiresAt:  time.Now().Add(-5 * time.Minute),
				Status:     ClaimStatusActive,
			},
			expectedError: "expires_at must be after claimed_at",
		},
		{
			name: "invalid status",
			claim: ClaimEntity{
				WorkItemID: "work-1",
				NodeID:     "node-1",
				ClaimedAt:  time.Now().Add(-5 * time.Minute),
				ExpiresAt:  time.Now().Add(5 * time.Minute),
				Status:     ClaimStatus("invalid"),
			},
			expectedError: "invalid status: invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.claim.Validate()

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestClaimEntity_GenerateKey(t *testing.T) {
	claim := &ClaimEntity{
		WorkItemID: "work-item-123",
		NodeID:     "node-1",
		ClaimedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(5 * time.Minute),
		Status:     ClaimStatusActive,
	}

	key := claim.GenerateKey()
	expected := "claims:work-item-123"

	assert.Equal(t, expected, key)
}

func TestGenerateClaimKey(t *testing.T) {
	workItemID := "work-item-456"
	key := GenerateClaimKey(workItemID)
	expected := "claims:work-item-456"

	assert.Equal(t, expected, key)
}