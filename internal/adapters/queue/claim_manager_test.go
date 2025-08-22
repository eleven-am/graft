package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStorage struct {
	data map[string][]byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		data: make(map[string][]byte),
	}
}

func (m *mockStorage) Put(ctx context.Context, key string, value []byte) error {
	m.data[key] = value
	return nil
}

func (m *mockStorage) Get(ctx context.Context, key string) ([]byte, error) {
	value, exists := m.data[key]
	if !exists {
		return nil, fmt.Errorf("key not found")
	}
	return value, nil
}

func (m *mockStorage) Delete(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}

func (m *mockStorage) List(ctx context.Context, prefix string) ([]ports.KeyValue, error) {
	var result []ports.KeyValue
	for key, value := range m.data {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, ports.KeyValue{Key: key, Value: value})
		}
	}
	return result, nil
}

func (m *mockStorage) Batch(ctx context.Context, ops []ports.Operation) error {
	for _, op := range ops {
		switch op.Type {
		case ports.OpPut:
			m.data[op.Key] = op.Value
		case ports.OpDelete:
			delete(m.data, op.Key)
		}
	}
	return nil
}

func TestClaimManager_ClaimWork(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm := NewClaimManager(storage, "node-1", nil)

	tests := []struct {
		name          string
		workItemID    string
		duration      time.Duration
		expectedError string
	}{
		{
			name:       "successful claim",
			workItemID: "work-1",
			duration:   5 * time.Minute,
		},
		{
			name:          "empty work item ID",
			workItemID:    "",
			duration:      5 * time.Minute,
			expectedError: "validation failed for field work_item_id: cannot be empty",
		},
		{
			name:          "zero duration",
			workItemID:    "work-2",
			duration:      0,
			expectedError: "validation failed for field duration: must be positive",
		},
		{
			name:          "negative duration",
			workItemID:    "work-3",
			duration:      -1 * time.Minute,
			expectedError: "validation failed for field duration: must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cm.ClaimWork(ctx, tt.workItemID, tt.duration)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)

				claim, err := cm.GetClaim(ctx, tt.workItemID)
				require.NoError(t, err)
				assert.Equal(t, tt.workItemID, claim.WorkItemID)
				assert.Equal(t, "node-1", claim.NodeID)
				assert.Equal(t, domain.ClaimStatusActive, claim.Status)
				assert.False(t, claim.IsExpired())
			}
		})
	}
}

func TestClaimManager_ClaimConflict(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm1 := NewClaimManager(storage, "node-1", nil)
	cm2 := NewClaimManager(storage, "node-2", nil)

	workItemID := "work-conflict"
	duration := 5 * time.Minute

	err := cm1.ClaimWork(ctx, workItemID, duration)
	require.NoError(t, err)

	err = cm2.ClaimWork(ctx, workItemID, duration)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "work item work-conflict is already claimed by node node-1")
}

func TestClaimManager_ReleaseClaim(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm := NewClaimManager(storage, "node-1", nil)

	workItemID := "work-release"
	duration := 5 * time.Minute

	err := cm.ClaimWork(ctx, workItemID, duration)
	require.NoError(t, err)

	err = cm.ReleaseClaim(ctx, workItemID)
	require.NoError(t, err)

	_, err = cm.GetClaim(ctx, workItemID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active claim found")
}

func TestClaimManager_ReleaseNonExistentClaim(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm := NewClaimManager(storage, "node-1", nil)

	err := cm.ReleaseClaim(ctx, "non-existent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active claim found")
}

func TestClaimManager_ReleaseClaimNotOwned(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm1 := NewClaimManager(storage, "node-1", nil)
	cm2 := NewClaimManager(storage, "node-2", nil)

	workItemID := "work-not-owned"
	duration := 5 * time.Minute

	err := cm1.ClaimWork(ctx, workItemID, duration)
	require.NoError(t, err)

	err = cm2.ReleaseClaim(ctx, workItemID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active claim found")
}

func TestClaimManager_VerifyClaim(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm1 := NewClaimManager(storage, "node-1", nil)
	cm2 := NewClaimManager(storage, "node-2", nil)

	workItemID := "work-verify"
	duration := 5 * time.Minute

	err := cm1.ClaimWork(ctx, workItemID, duration)
	require.NoError(t, err)

	err = cm1.VerifyClaim(ctx, workItemID)
	require.NoError(t, err)

	err = cm2.VerifyClaim(ctx, workItemID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active claim found")
}

func TestClaimManager_VerifyExpiredClaim(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm := NewClaimManager(storage, "node-1", nil)

	workItemID := "work-expired"
	duration := 1 * time.Millisecond

	err := cm.ClaimWork(ctx, workItemID, duration)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	err = cm.VerifyClaim(ctx, workItemID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "claim for work item work-expired by node node-1 has expired")
}

func TestClaimManager_ListMyClaims(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm1 := NewClaimManager(storage, "node-1", nil)
	cm2 := NewClaimManager(storage, "node-2", nil)

	duration := 5 * time.Minute

	err := cm1.ClaimWork(ctx, "work-1", duration)
	require.NoError(t, err)

	err = cm1.ClaimWork(ctx, "work-2", duration)
	require.NoError(t, err)

	err = cm2.ClaimWork(ctx, "work-3", duration)
	require.NoError(t, err)

	claims, err := cm1.ListMyClaims(ctx)
	require.NoError(t, err)
	require.Len(t, claims, 2)

	workItemIDs := make(map[string]bool)
	for _, claim := range claims {
		workItemIDs[claim.WorkItemID] = true
		assert.Equal(t, "node-1", claim.NodeID)
	}
	assert.True(t, workItemIDs["work-1"])
	assert.True(t, workItemIDs["work-2"])
	assert.False(t, workItemIDs["work-3"])
}

func TestClaimManager_CleanupExpiredClaims(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm := NewClaimManager(storage, "node-1", nil)

	shortDuration := 1 * time.Millisecond
	longDuration := 1 * time.Hour

	err := cm.ClaimWork(ctx, "work-expired", shortDuration)
	require.NoError(t, err)

	err = cm.ClaimWork(ctx, "work-active", longDuration)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	err = cm.CleanupExpiredClaims(ctx)
	require.NoError(t, err)

	_, err = cm.GetClaim(ctx, "work-expired")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active claim found")

	_, err = cm.GetClaim(ctx, "work-active")
	require.NoError(t, err)
}

func TestClaimManager_ReclaimAfterExpiry(t *testing.T) {
	ctx := context.Background()
	storage := newMockStorage()
	cm1 := NewClaimManager(storage, "node-1", nil)
	cm2 := NewClaimManager(storage, "node-2", nil)

	workItemID := "work-reclaim"
	shortDuration := 1 * time.Millisecond
	longDuration := 5 * time.Minute

	err := cm1.ClaimWork(ctx, workItemID, shortDuration)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	err = cm2.ClaimWork(ctx, workItemID, longDuration)
	require.NoError(t, err)

	claim, err := cm2.GetClaim(ctx, workItemID)
	require.NoError(t, err)
	assert.Equal(t, "node-2", claim.NodeID)
	assert.True(t, claim.IsActive())
}
