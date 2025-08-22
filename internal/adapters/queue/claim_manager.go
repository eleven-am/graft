package queue

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type ClaimManager struct {
	storage ports.StoragePort
	nodeID  string
	logger  *slog.Logger
}

func NewClaimManager(storage ports.StoragePort, nodeID string, logger *slog.Logger) *ClaimManager {
	if logger == nil {
		logger = slog.Default()
	}

	return &ClaimManager{
		storage: storage,
		nodeID:  nodeID,
		logger:  logger.With("component", "claim_manager", "node_id", nodeID),
	}
}

func (cm *ClaimManager) ClaimWork(ctx context.Context, workItemID string, duration time.Duration) error {
	if workItemID == "" {
		return domain.NewValidationError("work_item_id", "cannot be empty")
	}
	if duration <= 0 {
		return domain.NewValidationError("duration", "must be positive")
	}

	claimKey := domain.GenerateClaimKey(workItemID)

	var existingClaim *domain.ClaimEntity
	claimData, err := cm.storage.Get(ctx, claimKey)
	if err == nil {
		existingClaim = &domain.ClaimEntity{}
		if err := json.Unmarshal(claimData, existingClaim); err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to unmarshal existing claim",
				Details: map[string]interface{}{"error": err.Error()},
			}
		}

		if existingClaim.IsActive() {
			return domain.NewClaimConflictError(workItemID, existingClaim.NodeID, cm.nodeID)
		}
	}

	newClaim := domain.NewClaim(workItemID, cm.nodeID, duration)
	claimData, err = json.Marshal(newClaim)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to marshal new claim",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	if err := cm.storage.Put(ctx, claimKey, claimData); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to store claim",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	cm.logger.Info("work item claimed successfully",
		"work_item_id", workItemID,
		"expires_at", newClaim.ExpiresAt,
		"duration", duration)

	return nil
}

func (cm *ClaimManager) ReleaseClaim(ctx context.Context, workItemID string) error {
	if workItemID == "" {
		return domain.NewValidationError("work_item_id", "cannot be empty")
	}

	claimKey := domain.GenerateClaimKey(workItemID)

	claimData, err := cm.storage.Get(ctx, claimKey)
	if err != nil {
		if domain.IsKeyNotFound(err) {
			return domain.NewClaimNotFoundError(workItemID, cm.nodeID)
		}
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get claim",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	var existingClaim domain.ClaimEntity
	if err := json.Unmarshal(claimData, &existingClaim); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal claim",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	if existingClaim.NodeID != cm.nodeID {
		return domain.NewClaimNotFoundError(workItemID, cm.nodeID)
	}

	if err := cm.storage.Delete(ctx, claimKey); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to delete claim",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	cm.logger.Info("claim released successfully",
		"work_item_id", workItemID)

	return nil
}

func (cm *ClaimManager) VerifyClaim(ctx context.Context, workItemID string) error {
	if workItemID == "" {
		return domain.NewValidationError("work_item_id", "cannot be empty")
	}

	claimKey := domain.GenerateClaimKey(workItemID)

	claimData, err := cm.storage.Get(ctx, claimKey)
	if err != nil {
		if domain.IsKeyNotFound(err) {
			return domain.NewClaimNotFoundError(workItemID, cm.nodeID)
		}
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get claim",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	var existingClaim domain.ClaimEntity
	if err := json.Unmarshal(claimData, &existingClaim); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal claim",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	if existingClaim.NodeID != cm.nodeID {
		return domain.NewClaimNotFoundError(workItemID, cm.nodeID)
	}

	if existingClaim.IsExpired() {
		return domain.NewClaimExpiredError(workItemID, cm.nodeID)
	}

	if !existingClaim.IsActive() {
		return domain.NewClaimNotFoundError(workItemID, cm.nodeID)
	}

	return nil
}

func (cm *ClaimManager) GetClaim(ctx context.Context, workItemID string) (*domain.ClaimEntity, error) {
	if workItemID == "" {
		return nil, domain.NewValidationError("work_item_id", "cannot be empty")
	}

	claimKey := domain.GenerateClaimKey(workItemID)

	claimData, err := cm.storage.Get(ctx, claimKey)
	if err != nil {
		if domain.IsKeyNotFound(err) {
			return nil, domain.NewClaimNotFoundError(workItemID, cm.nodeID)
		}
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get claim for validation",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	var claim domain.ClaimEntity
	if err := json.Unmarshal(claimData, &claim); err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal claim for validation",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	return &claim, nil
}

func (cm *ClaimManager) ListMyClaims(ctx context.Context) ([]*domain.ClaimEntity, error) {
	claimPrefix := "claims:"
	items, err := cm.storage.List(ctx, claimPrefix)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list claims",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	var myClaims []*domain.ClaimEntity
	for _, item := range items {
		var claim domain.ClaimEntity
		if err := json.Unmarshal(item.Value, &claim); err != nil {
			cm.logger.Error("failed to unmarshal claim",
				"key", item.Key,
				"error", err)
			continue
		}

		if claim.NodeID == cm.nodeID {
			myClaims = append(myClaims, &claim)
		}
	}

	return myClaims, nil
}

func (cm *ClaimManager) CleanupExpiredClaims(ctx context.Context) error {
	claimPrefix := "claims:"
	items, err := cm.storage.List(ctx, claimPrefix)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list claims for cleanup",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	var expiredKeys []string
	for _, item := range items {
		var claim domain.ClaimEntity
		if err := json.Unmarshal(item.Value, &claim); err != nil {
			cm.logger.Error("failed to unmarshal claim during cleanup",
				"key", item.Key,
				"error", err)
			continue
		}

		if claim.IsExpired() {
			expiredKeys = append(expiredKeys, item.Key)
		}
	}

	if len(expiredKeys) == 0 {
		return nil
	}

	ops := make([]ports.Operation, len(expiredKeys))
	for i, key := range expiredKeys {
		ops[i] = ports.Operation{
			Type: ports.OpDelete,
			Key:  key,
		}
	}

	if err := cm.storage.Batch(ctx, ops); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to cleanup expired claims",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	cm.logger.Info("cleaned up expired claims",
		"count", len(expiredKeys))

	return nil
}
