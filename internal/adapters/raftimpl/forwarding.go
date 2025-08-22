package raftimpl

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type ReadForwarder struct {
	transport         ports.TransportPort
	logger            *slog.Logger
	leaderCache       *LeaderCache
	maxRetries        int
	retryBackoff      time.Duration
	forwardingTimeout time.Duration
}

type LeaderCache struct {
	leaderInfo   *ports.LeaderInfo
	lastUpdate   time.Time
	cacheTTL     time.Duration
}

func NewReadForwarder(transport ports.TransportPort, logger *slog.Logger, config StorageConfig) *ReadForwarder {
	return &ReadForwarder{
		transport:         transport,
		logger:            logger.With("component", "read-forwarder"),
		leaderCache: &LeaderCache{
			cacheTTL: config.CacheTTL,
		},
		maxRetries:        config.MaxRetries,
		retryBackoff:      config.RetryBackoff,
		forwardingTimeout: config.ForwardingTimeout,
	}
}

func (rf *ReadForwarder) ForwardReadToLeader(ctx context.Context, key string) ([]byte, error) {
	rf.logger.Debug("forwarding read to leader", "key", key)

	leader, err := rf.getCurrentLeader(ctx)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "failed to get current leader for read forwarding",
			Details: map[string]interface{}{
				"key":   key,
				"error": err.Error(),
			},
		}
	}

	return rf.executeReadOnLeader(ctx, key, leader)
}

func (rf *ReadForwarder) ForwardReadToLeaderWithRetry(ctx context.Context, key string) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt < rf.maxRetries; attempt++ {
		if attempt > 0 {
			rf.logger.Debug("retrying read forwarding", "key", key, "attempt", attempt+1)
			
			rf.clearLeaderCache()

			backoffDuration := rf.calculateExponentialBackoff(attempt)
			select {
			case <-time.After(backoffDuration):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		result, err := rf.ForwardReadToLeader(ctx, key)
		if err == nil {
			if attempt > 0 {
				rf.logger.Info("read forwarding succeeded after retry", "key", key, "attempts", attempt+1)
			}
			return result, nil
		}

		lastErr = err

		if !rf.isRetryableError(err) {
			rf.logger.Debug("non-retryable error, not retrying", "key", key, "error", err)
			break
		}

		rf.logger.Warn("read forwarding attempt failed", "key", key, "attempt", attempt+1, "error", err)
	}

	rf.logger.Error("all read forwarding attempts failed", "key", key, "attempts", rf.maxRetries, "last_error", lastErr)
	return nil, lastErr
}

func (rf *ReadForwarder) getCurrentLeader(ctx context.Context) (*ports.LeaderInfo, error) {
	if rf.isLeaderCacheValid() {
		rf.logger.Debug("using cached leader info", "leader_id", rf.leaderCache.leaderInfo.NodeID)
		return rf.leaderCache.leaderInfo, nil
	}

	rf.logger.Debug("refreshing leader info from transport")

	leader, err := rf.transport.GetLeader(ctx)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "failed to get leader info from transport",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if leader == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "no leader available in cluster",
			Details: map[string]interface{}{
				"timestamp": time.Now(),
			},
		}
	}

	rf.leaderCache.leaderInfo = leader
	rf.leaderCache.lastUpdate = time.Now()

	rf.logger.Debug("leader info refreshed", "leader_id", leader.NodeID, "leader_address", leader.Address)
	return leader, nil
}

func (rf *ReadForwarder) executeReadOnLeader(ctx context.Context, key string, leader *ports.LeaderInfo) ([]byte, error) {
	rf.logger.Debug("executing read on leader", 
		"key", key, 
		"leader_id", leader.NodeID, 
		"leader_address", leader.Address)

	req := ports.ForwardRequest{
		Type:    "READ",
		Payload: []byte(key),
	}

	readCtx, cancel := context.WithTimeout(ctx, rf.forwardingTimeout)
	defer cancel()

	resp, err := rf.transport.ForwardToLeader(readCtx, req)
	if err != nil {
		rf.logger.Warn("leader read transport error", 
			"key", key, 
			"leader_id", leader.NodeID, 
			"error", err)

		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "failed to forward read to leader",
			Details: map[string]interface{}{
				"key":       key,
				"leader_id": leader.NodeID,
				"error":     err.Error(),
			},
		}
	}

	if !resp.Success {
		rf.logger.Warn("leader read operation failed", 
			"key", key, 
			"leader_id", leader.NodeID, 
			"error", resp.Error)

		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "leader read operation failed",
			Details: map[string]interface{}{
				"key":         key,
				"leader_id":   leader.NodeID,
				"leader_error": resp.Error,
			},
		}
	}

	rf.logger.Debug("leader read successful", 
		"key", key, 
		"leader_id", leader.NodeID, 
		"value_size", len(resp.Result))

	return resp.Result, nil
}

func (rf *ReadForwarder) isLeaderCacheValid() bool {
	if rf.leaderCache.leaderInfo == nil {
		return false
	}

	age := time.Since(rf.leaderCache.lastUpdate)
	return age < rf.leaderCache.cacheTTL
}

func (rf *ReadForwarder) clearLeaderCache() {
	rf.logger.Debug("clearing leader cache")
	rf.leaderCache.leaderInfo = nil
	rf.leaderCache.lastUpdate = time.Time{}
}

func (rf *ReadForwarder) isRetryableError(err error) bool {
	if domainErr, ok := err.(domain.Error); ok {
		switch domainErr.Type {
		case domain.ErrorTypeUnavailable, domain.ErrorTypeTimeout:
			return true
		case domain.ErrorTypeValidation, domain.ErrorTypeUnauthorized, domain.ErrorTypeNotFound:
			return false
		case domain.ErrorTypeInternal:
			return true
		case domain.ErrorTypeConflict, domain.ErrorTypeRateLimit:
			return true
		}
	}

	rf.logger.Debug("unknown error type, not retrying", "error", err)
	return false
}

func (rf *ReadForwarder) SetRetryConfig(maxRetries int, backoff time.Duration) {
	rf.maxRetries = maxRetries
	rf.retryBackoff = backoff
	rf.logger.Info("retry config updated", 
		"max_retries", maxRetries, 
		"backoff", backoff)
}

func (rf *ReadForwarder) SetLeaderCacheTTL(ttl time.Duration) {
	rf.leaderCache.cacheTTL = ttl
	rf.logger.Info("leader cache TTL updated", "ttl", ttl)
}

func (rf *ReadForwarder) GetCachedLeaderInfo() *ports.LeaderInfo {
	if rf.isLeaderCacheValid() {
		return rf.leaderCache.leaderInfo
	}
	return nil
}

func (rf *ReadForwarder) HandleLeaderChange(newLeaderID string) {
	rf.logger.Info("leader change detected", "new_leader", newLeaderID)
	
	rf.clearLeaderCache()
	
}

func (rf *ReadForwarder) calculateExponentialBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	
	baseDelay := rf.retryBackoff
	exponential := baseDelay * time.Duration(1<<uint(attempt-1))
	
	jitterRange := exponential / 2
	jitter := time.Duration(rand.Int63n(int64(jitterRange)))
	return exponential/2 + jitter
}