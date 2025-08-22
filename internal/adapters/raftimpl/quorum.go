package raftimpl

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type QuorumReadResult struct {
	Value     []byte
	NodeID    string
	Timestamp time.Time
	Error     error
}

type QuorumReader struct {
	transport ports.TransportPort
	logger    *slog.Logger
}

func NewQuorumReader(transport ports.TransportPort, logger *slog.Logger) *QuorumReader {
	return &QuorumReader{
		transport: transport,
		logger:    logger.With("component", "quorum-reader"),
	}
}

func (qr *QuorumReader) PerformQuorumRead(ctx context.Context, key string, config QuorumReadConfig, nodeList []string) ([]byte, error) {
	qr.logger.Debug("starting quorum read",
		"key", key,
		"min_replicas", config.MinReplicas,
		"nodes", len(nodeList))

	if len(nodeList) < config.MinReplicas {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "insufficient nodes available for quorum read",
			Details: map[string]interface{}{
				"available_nodes": len(nodeList),
				"required_nodes":  config.MinReplicas,
				"key":            key,
			},
		}
	}

	readCtx, cancel := context.WithTimeout(ctx, config.ReadTimeout)
	defer cancel()

	resultsChan := make(chan QuorumReadResult, len(nodeList))
	var wg sync.WaitGroup

	for _, nodeID := range nodeList {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			qr.readFromNode(readCtx, nodeID, key, resultsChan)
		}(nodeID)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	var results []QuorumReadResult
	for result := range resultsChan {
		results = append(results, result)
	}

	return qr.validateQuorumAndSelectValue(key, results, config)
}

func (qr *QuorumReader) readFromNode(ctx context.Context, nodeID string, key string, resultsChan chan<- QuorumReadResult) {
	qr.logger.Debug("reading from node", "node_id", nodeID, "key", key)

	req := ports.ForwardRequest{
		Type:    "READ",
		Payload: []byte(key),
	}

	result := QuorumReadResult{
		NodeID:    nodeID,
		Timestamp: time.Now(),
	}

	resp, err := qr.transport.ForwardToNode(ctx, nodeID, req)
	if err != nil {
		result.Error = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to read from node",
			Details: map[string]interface{}{
				"node_id": nodeID,
				"error":   err.Error(),
			},
		}
		qr.logger.Warn("node read failed", "node_id", nodeID, "key", key, "error", err)
	} else if !resp.Success {
		result.Error = domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "read failed on node",
			Details: map[string]interface{}{
				"node_id":     nodeID,
				"node_error": resp.Error,
			},
		}
		qr.logger.Warn("node read unsuccessful", "node_id", nodeID, "key", key, "error", resp.Error)
	} else {
		result.Value = resp.Result
		qr.logger.Debug("node read successful", "node_id", nodeID, "key", key, "value_size", len(resp.Result))
	}

	select {
	case resultsChan <- result:
	case <-ctx.Done():
		return
	}
}

func (qr *QuorumReader) validateQuorumAndSelectValue(key string, results []QuorumReadResult, config QuorumReadConfig) ([]byte, error) {
	qr.logger.Debug("validating quorum results",
		"key", key,
		"total_results", len(results),
		"min_replicas", config.MinReplicas)

	var successfulResults []QuorumReadResult
	for _, result := range results {
		if result.Error == nil {
			successfulResults = append(successfulResults, result)
		} else {
			qr.logger.Debug("excluding failed result", "node_id", result.NodeID, "error", result.Error)
		}
	}

	if len(successfulResults) < config.MinReplicas {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "insufficient successful reads for quorum",
			Details: map[string]interface{}{
				"successful_reads": len(successfulResults),
				"required_reads":   config.MinReplicas,
				"total_attempts":   len(results),
				"key":              key,
			},
		}
	}

	valueGroups := make(map[string][]QuorumReadResult)
	for _, result := range successfulResults {
		valueKey := string(result.Value)
		valueGroups[valueKey] = append(valueGroups[valueKey], result)
	}

	var consensusValue []byte
	var maxVotes int
	var consensusTimestamp time.Time

	for valueStr, group := range valueGroups {
		if len(group) > maxVotes {
			maxVotes = len(group)
			consensusValue = []byte(valueStr)
			consensusTimestamp = group[0].Timestamp
			for _, result := range group {
				if result.Timestamp.After(consensusTimestamp) {
					consensusTimestamp = result.Timestamp
				}
			}
		}
	}

	if maxVotes < config.MinReplicas {
		return nil, domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "no consensus reached among replicas",
			Details: map[string]interface{}{
				"max_votes":         maxVotes,
				"required_votes":    config.MinReplicas,
				"value_variations":  len(valueGroups),
				"key":               key,
			},
		}
	}

	qr.logger.Info("quorum read successful",
		"key", key,
		"consensus_votes", maxVotes,
		"total_successful", len(successfulResults),
		"value_size", len(consensusValue),
		"consensus_timestamp", consensusTimestamp)

	return consensusValue, nil
}

func (qr *QuorumReader) PerformQuorumReadWithRetry(ctx context.Context, key string, config QuorumReadConfig, nodeList []string) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= config.RetryAttempts; attempt++ {
		if attempt > 0 {
			qr.logger.Debug("retrying quorum read", "key", key, "attempt", attempt+1)
			
			backoffDuration := time.Duration(attempt) * 100 * time.Millisecond
			select {
			case <-time.After(backoffDuration):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		value, err := qr.PerformQuorumRead(ctx, key, config, nodeList)
		if err == nil {
			if attempt > 0 {
				qr.logger.Info("quorum read succeeded after retry", "key", key, "attempts", attempt+1)
			}
			return value, nil
		}

		lastErr = err

		if domainErr, ok := err.(domain.Error); ok {
			switch domainErr.Type {
			case domain.ErrorTypeValidation:
				qr.logger.Debug("not retrying due to validation error", "key", key, "error", err)
				return nil, err
			}
		}

		qr.logger.Warn("quorum read attempt failed", "key", key, "attempt", attempt+1, "error", err)
	}

	qr.logger.Error("all quorum read attempts failed", "key", key, "attempts", config.RetryAttempts+1, "last_error", lastErr)
	return nil, lastErr
}