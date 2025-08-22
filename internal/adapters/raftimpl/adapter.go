package raftimpl

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type ConsistencyLevel int

const (
	ConsistencyLeader ConsistencyLevel = iota
	ConsistencyEventual
	ConsistencyQuorum
)

type QuorumReadConfig struct {
	ConsistencyLevel ConsistencyLevel
	MinReplicas      int
	ReadTimeout      time.Duration
	RetryAttempts    int
}

type StorageAdapter struct {
	node          *RaftNode
	db            *badger.DB
	transport     ports.TransportPort
	logger        *slog.Logger
	config        StorageConfig
	quorumReader  *QuorumReader
	readForwarder *ReadForwarder
}

func NewStorageAdapter(node *RaftNode, db *badger.DB, transport ports.TransportPort, logger *slog.Logger) *StorageAdapter {
	config := DefaultStorageConfig()
	adapter := &StorageAdapter{
		node:          node,
		db:            db,
		transport:     transport,
		logger:        logger.With("component", "storage-adapter"),
		config:        config,
		quorumReader:  NewQuorumReader(transport, logger),
		readForwarder: NewReadForwarder(transport, logger, config),
	}

	adapter.updateTransportLeader()
	return adapter
}

func NewStorageAdapterWithConfig(node *RaftNode, db *badger.DB, transport ports.TransportPort, logger *slog.Logger, config StorageConfig) *StorageAdapter {
	if err := config.Validate(); err != nil {
		logger.Error("invalid storage config, using defaults", "error", err)
		config = DefaultStorageConfig()
	}

	return &StorageAdapter{
		node:          node,
		db:            db,
		transport:     transport,
		logger:        logger.With("component", "storage-adapter"),
		config:        config,
		quorumReader:  NewQuorumReader(transport, logger),
		readForwarder: NewReadForwarder(transport, logger, config),
	}
}

func (s *StorageAdapter) Put(ctx context.Context, key string, value []byte) error {
	s.updateTransportLeader()

	if !s.node.IsLeader() {
		return s.forwardToLeader(ctx, NewPutCommand(key, value))
	}

	cmd := NewPutCommand(key, value)
	data, err := cmd.Marshal()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to marshal command",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	timeout := s.config.ForwardingTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	if err := s.node.Apply(data, timeout); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to apply put command",
			Details: map[string]interface{}{
				"key":   key,
				"error": err.Error(),
			},
		}
	}

	s.logger.Debug("put operation completed", "key", key, "value_size", len(value))
	return nil
}

func (s *StorageAdapter) Get(ctx context.Context, key string) ([]byte, error) {
	switch s.config.DefaultConsistencyLevel {
	case ConsistencyEventual:
		return s.readLocal(key)
	case ConsistencyQuorum:
		return s.performQuorumRead(ctx, key)
	default:
		return s.readFromLeader(ctx, key)
	}
}

func (s *StorageAdapter) GetWithConsistency(ctx context.Context, key string, level ConsistencyLevel) ([]byte, error) {
	s.logger.Debug("get with explicit consistency", "key", key, "consistency_level", level)

	switch level {
	case ConsistencyEventual:
		return s.readLocal(key)
	case ConsistencyQuorum:
		return s.performQuorumRead(ctx, key)
	case ConsistencyLeader:
		return s.readFromLeader(ctx, key)
	default:
		return s.readFromLeader(ctx, key)
	}
}

func (s *StorageAdapter) Delete(ctx context.Context, key string) error {
	if !s.node.IsLeader() {
		return s.forwardToLeader(ctx, NewDeleteCommand(key))
	}

	cmd := NewDeleteCommand(key)
	data, err := cmd.Marshal()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to marshal command",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	timeout := s.config.ForwardingTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	if err := s.node.Apply(data, timeout); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to apply delete command",
			Details: map[string]interface{}{
				"key":   key,
				"error": err.Error(),
			},
		}
	}

	s.logger.Debug("delete operation completed", "key", key)
	return nil
}

func (s *StorageAdapter) List(ctx context.Context, prefix string) ([]ports.KeyValue, error) {
	var results []ports.KeyValue

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		opts.Prefix = []byte(prefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.KeyCopy(nil))

			if !strings.HasPrefix(key, prefix) {
				continue
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				s.logger.Error("failed to copy value", "key", key, "error", err)
				continue
			}

			results = append(results, ports.KeyValue{
				Key:   key,
				Value: value,
			})
		}

		return nil
	})

	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list keys",
			Details: map[string]interface{}{
				"prefix": prefix,
				"error":  err.Error(),
			},
		}
	}

	s.logger.Debug("list operation completed", "prefix", prefix, "count", len(results))
	return results, nil
}

func (s *StorageAdapter) Batch(ctx context.Context, ops []ports.Operation) error {
	if !s.node.IsLeader() {
		batchOps := make([]BatchOp, len(ops))
		for i, op := range ops {
			batchOps[i] = BatchOp{
				Type:  s.operationTypeToCommandType(op.Type),
				Key:   op.Key,
				Value: op.Value,
			}
		}
		return s.forwardToLeader(ctx, NewBatchCommand(batchOps))
	}

	batchOps := make([]BatchOp, len(ops))
	for i, op := range ops {
		batchOps[i] = BatchOp{
			Type:  s.operationTypeToCommandType(op.Type),
			Key:   op.Key,
			Value: op.Value,
		}
	}

	cmd := NewBatchCommand(batchOps)
	data, err := cmd.Marshal()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to marshal batch command",
			Details: map[string]interface{}{
				"operations": len(ops),
				"error":      err.Error(),
			},
		}
	}

	timeout := s.config.ForwardingTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	if err := s.node.Apply(data, timeout); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to apply batch command",
			Details: map[string]interface{}{
				"operations": len(ops),
				"error":      err.Error(),
			},
		}
	}

	s.logger.Debug("batch operation completed", "operations", len(ops))
	return nil
}

func (s *StorageAdapter) SetConsistencyLevel(level ConsistencyLevel) {
	s.config.DefaultConsistencyLevel = level
	s.logger.Info("consistency level updated", "level", level)
}

func (s *StorageAdapter) SetQuorumConfig(config QuorumReadConfig) {
	s.config.QuorumConfig = config
	s.logger.Info("quorum config updated",
		"min_replicas", config.MinReplicas,
		"read_timeout", config.ReadTimeout,
		"retry_attempts", config.RetryAttempts)
}

func (s *StorageAdapter) SetForwardingEnabled(enabled bool) {
	s.config.ForwardingEnabled = enabled
	s.logger.Info("leader forwarding updated", "enabled", enabled)
}

func (s *StorageAdapter) SetCacheSize(size int64) {
	s.config.CacheSize = size
	s.logger.Info("cache size updated", "size_bytes", size)
}

func (s *StorageAdapter) UpdateConfig(config StorageConfig) error {
	if err := config.Validate(); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "invalid storage configuration",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	s.config = config
	s.logger.Info("storage configuration updated",
		"consistency_level", config.DefaultConsistencyLevel,
		"forwarding_enabled", config.ForwardingEnabled,
		"cache_size", config.CacheSize)

	return nil
}

func (s *StorageAdapter) GetConfig() StorageConfig {
	return s.config
}

func (s *StorageAdapter) readLocal(key string) ([]byte, error) {
	var value []byte

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return domain.NewNotFoundError("key", key)
			}
			return err
		}

		value, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (s *StorageAdapter) readFromLeader(ctx context.Context, key string) ([]byte, error) {
	s.updateTransportLeader()

	if s.node.IsLeader() {
		return s.readLocal(key)
	}

	if !s.config.ForwardingEnabled || s.readForwarder == nil {
		s.logger.Debug("forwarding disabled, performing local read", "key", key)
		return s.readLocal(key)
	}

	s.logger.Debug("forwarding read to leader", "key", key)
	return s.readForwarder.ForwardReadToLeaderWithRetry(ctx, key)
}

func (s *StorageAdapter) forwardToLeader(ctx context.Context, cmd *Command) error {
	if s.transport == nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "transport not configured for leader forwarding",
			Details: map[string]interface{}{
				"component": "raft_storage",
			},
		}
	}

	cmdData, err := cmd.Marshal()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to marshal command for forwarding",
			Details: map[string]interface{}{
				"command_type": cmd.Type.String(),
				"key":          cmd.Key,
				"error":        err.Error(),
			},
		}
	}

	req := ports.ForwardRequest{
		Type:    cmd.Type.String(),
		Payload: cmdData,
	}

	resp, err := s.transport.ForwardToLeader(ctx, req)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "failed to forward to leader",
			Details: map[string]interface{}{
				"command_type": cmd.Type.String(),
				"key":          cmd.Key,
				"error":        err.Error(),
			},
		}
	}

	if !resp.Success {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "leader forwarding failed",
			Details: map[string]interface{}{
				"command_type": cmd.Type.String(),
				"key":          cmd.Key,
				"error":        resp.Error,
			},
		}
	}

	s.logger.Debug("successfully forwarded command to leader",
		"command_type", cmd.Type.String(),
		"key", cmd.Key)

	return nil
}

func (s *StorageAdapter) updateTransportLeader() {
	if s.transport == nil {
		return
	}

	if s.node.IsLeader() {
		leaderAddr := s.node.LeaderAddr()
		if leaderAddr == "" {
			leaderAddr = s.node.config.BindAddr
		}

		if transportUpdater, ok := s.transport.(interface {
			UpdateLeader(nodeID string, address string)
		}); ok {
			transportUpdater.UpdateLeader(s.node.config.NodeID, leaderAddr)
			s.logger.Debug("updated transport with leader info",
				"node_id", s.node.config.NodeID,
				"leader_addr", leaderAddr)
		}
	}
}

func (s *StorageAdapter) operationTypeToCommandType(op ports.OperationType) CommandType {
	switch op {
	case ports.OpPut:
		return CommandPut
	case ports.OpDelete:
		return CommandDelete
	default:
		return CommandPut
	}
}

func (s *StorageAdapter) performQuorumRead(ctx context.Context, key string) ([]byte, error) {
	s.logger.Debug("performing quorum read", "key", key, "config", s.config.QuorumConfig)

	nodeList := s.getAvailableNodes(ctx)
	if len(nodeList) == 0 {
		s.logger.Warn("no nodes available for quorum read, falling back to local read", "key", key)
		return s.readLocal(key)
	}

	return s.quorumReader.PerformQuorumReadWithRetry(ctx, key, s.config.QuorumConfig, nodeList)
}

func (s *StorageAdapter) getAvailableNodes(ctx context.Context) []string {
	if s.transport == nil {
		s.logger.Debug("no transport available, cannot perform distributed read")
		return []string{}
	}

	leader, err := s.transport.GetLeader(ctx)
	if err != nil {
		s.logger.Debug("cannot get leader info", "error", err)
		return []string{}
	}

	nodes := []string{}

	if peerTracker, ok := s.transport.(ports.PeerTracker); ok {
		peers := peerTracker.GetPeers()
		s.logger.Debug("retrieved peer information from transport", "peer_count", len(peers))

		for nodeID, peerInfo := range peers {
			if peerInfo.IsHealthy {
				nodes = append(nodes, nodeID)
			}
		}
	}

	if len(nodes) == 0 && leader != nil {
		s.logger.Debug("no peer info available, using leader for single-node read")
		nodes = []string{leader.NodeID}
	}

	if len(nodes) == 0 {
		s.logger.Warn("no cluster members available for distributed read")
		return []string{}
	}

	s.logger.Debug("available nodes for quorum read",
		"nodes", nodes,
		"count", len(nodes),
		"leader", func() string {
			if leader != nil {
				return leader.NodeID
			}
			return "unknown"
		}())

	return nodes
}
