package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Adapter struct {
	raft      ports.RaftPort
	transport ports.TransportPort
	logger    *slog.Logger
	nodeID    string
}

func NewAdapter(raft ports.RaftPort, transport ports.TransportPort, nodeID string, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}

	adapter := &Adapter{
		raft:      raft,
		transport: transport,
		nodeID:    nodeID,
		logger:    logger.With("component", "storage"),
	}

	transport.SetMessageHandler(adapter)

	return adapter
}

func (s *Adapter) Put(ctx context.Context, key string, value []byte) error {
	if s.raft.IsLeader() {
		s.logger.Debug("putting to badger as leader", "key", key, "value_length", len(value))
		cmd := domain.NewPutCommand(key, value)
		result, err := s.raft.Apply(ctx, cmd)
		if err != nil {
			s.logger.Error("raft apply failed for put", "key", key, "error", err.Error())
			return domain.NewStorageError("put", key, err)
		}

		if !result.Success {
			s.logger.Error("raft apply unsuccessful for put", "key", key, "error", result.Error)
			return domain.NewStorageError("put", key, fmt.Errorf(result.Error))
		}

		s.logger.Debug("successful put via raft", "key", key)
		return nil
	} else {
		payload, err := s.serializePutOperation(key, value)
		if err != nil {
			return domain.NewStorageError("put", key, err)
		}

		s.logger.Debug("not leader, forwarding put to leader", "key", key, "value_length", len(value))
		message := ports.Message{
			Type:    ports.StorageWrite,
			From:    s.nodeID,
			Payload: payload,
		}

		response, err := s.transport.SendToLeader(ctx, message)
		if err != nil {
			s.logger.Error("failed to forward put to leader", "key", key, "error", err.Error())
			return domain.NewStorageError("put", key, err)
		}

		if !response.Success {
			s.logger.Error("leader put failed", "key", key, "error", response.Error)
			return domain.NewStorageError("put", key, fmt.Errorf("%s", response.Error))
		}

		s.logger.Debug("successful put via leader", "key", key)
		return nil
	}
}

func (s *Adapter) Get(ctx context.Context, key string) ([]byte, error) {
	if s.raft.IsLeader() {
		s.logger.Debug("reading from badger as leader", "key", key)
		return s.readFromBadger(key)
	} else {
		s.logger.Debug("not leader, attempting to forward read to leader", "key", key)

		payload, err := s.serializeGetOperation(key)
		if err != nil {
			return nil, domain.NewStorageError("get", key, err)
		}

		message := ports.Message{
			Type:    ports.StorageRead,
			From:    s.nodeID,
			Payload: payload,
		}

		response, err := s.transport.SendToLeader(ctx, message)
		if err != nil {
			s.logger.Warn("failed to read from leader, falling back to direct badger read", "key", key, "error", err.Error())
			// Fallback: try reading directly from local BadgerDB
			if fallbackData, fallbackErr := s.readFromBadger(key); fallbackErr == nil {
				s.logger.Debug("fallback read successful", "key", key)
				return fallbackData, nil
			} else {
				s.logger.Debug("fallback read also failed", "key", key, "error", fallbackErr.Error())
			}
			return nil, domain.NewStorageError("get", key, err)
		}

		if !response.Success {
			s.logger.Warn("leader read failed, falling back to direct badger read", "key", key, "error", response.Error)
			// Fallback: try reading directly from local BadgerDB
			if fallbackData, fallbackErr := s.readFromBadger(key); fallbackErr == nil {
				s.logger.Debug("fallback read successful", "key", key)
				return fallbackData, nil
			} else {
				s.logger.Debug("fallback read also failed", "key", key, "error", fallbackErr.Error())
			}
			return nil, domain.NewStorageError("get", key, fmt.Errorf("%s", response.Error))
		}

		s.logger.Debug("successful read from leader", "key", key, "data_length", len(response.Data))
		return response.Data, nil
	}
}

func (s *Adapter) Delete(ctx context.Context, key string) error {
	if s.raft.IsLeader() {
		cmd := domain.NewDeleteCommand(key)
		result, err := s.raft.Apply(ctx, cmd)
		if err != nil {
			return domain.NewStorageError("delete", key, err)
		}

		if !result.Success {
			return domain.NewStorageError("delete", key, fmt.Errorf("%s", result.Error))
		}

		return nil
	} else {
		payload, err := s.serializeDeleteOperation(key)
		if err != nil {
			return domain.NewStorageError("delete", key, err)
		}

		message := ports.Message{
			Type:    ports.StorageDelete,
			From:    s.nodeID,
			Payload: payload,
		}

		response, err := s.transport.SendToLeader(ctx, message)
		if err != nil {
			return domain.NewStorageError("delete", key, err)
		}

		if !response.Success {
			return domain.NewStorageError("delete", key, fmt.Errorf("%s", response.Error))
		}

		return nil
	}
}

func (s *Adapter) List(ctx context.Context, prefix string) ([]ports.KeyValue, error) {
	if s.raft.IsLeader() {
		s.logger.Debug("listing from badger as leader", "prefix", prefix)
		return s.listFromBadger(prefix)
	} else {
		s.logger.Debug("not leader, attempting to forward list to leader", "prefix", prefix)

		payload, err := s.serializeListOperation(prefix)
		if err != nil {
			return nil, domain.NewStorageError("list", prefix, err)
		}

		message := ports.Message{
			Type:    ports.StorageRead,
			From:    s.nodeID,
			Payload: payload,
		}

		response, err := s.transport.SendToLeader(ctx, message)
		if err != nil {
			s.logger.Warn("failed to list from leader, falling back to direct badger list", "prefix", prefix, "error", err.Error())
			// Fallback: try listing directly from local BadgerDB
			if fallbackData, fallbackErr := s.listFromBadger(prefix); fallbackErr == nil {
				s.logger.Debug("fallback list successful", "prefix", prefix, "count", len(fallbackData))
				return fallbackData, nil
			} else {
				s.logger.Debug("fallback list also failed", "prefix", prefix, "error", fallbackErr.Error())
			}
			return nil, domain.NewStorageError("list", prefix, err)
		}

		if !response.Success {
			s.logger.Warn("leader list failed, falling back to direct badger list", "prefix", prefix, "error", response.Error)
			// Fallback: try listing directly from local BadgerDB
			if fallbackData, fallbackErr := s.listFromBadger(prefix); fallbackErr == nil {
				s.logger.Debug("fallback list successful", "prefix", prefix, "count", len(fallbackData))
				return fallbackData, nil
			} else {
				s.logger.Debug("fallback list also failed", "prefix", prefix, "error", fallbackErr.Error())
			}
			return nil, domain.NewStorageError("list", prefix, fmt.Errorf(response.Error))
		}

		var result []ports.KeyValue
		if err := json.Unmarshal(response.Data, &result); err != nil {
			return nil, domain.NewStorageError("list", prefix, err)
		}

		s.logger.Debug("successful list from leader", "prefix", prefix, "count", len(result))
		return result, nil
	}
}

func (s *Adapter) Batch(ctx context.Context, ops []ports.Operation) error {
	if s.raft.IsLeader() {
		batchOps := make([]domain.BatchOp, len(ops))
		for i, op := range ops {
			batchOps[i] = domain.BatchOp{
				Type:  s.convertOperationType(op.Type),
				Key:   op.Key,
				Value: op.Value,
			}
		}

		cmd := domain.NewBatchCommand(batchOps)
		result, err := s.raft.Apply(ctx, cmd)
		if err != nil {
			return domain.NewStorageError("batch", "", err)
		}

		if !result.Success {
			return domain.NewStorageError("batch", "", fmt.Errorf(result.Error))
		}

		return nil
	} else {
		payload, err := s.serializeBatchOperation(ops)
		if err != nil {
			return domain.NewStorageError("batch", "", err)
		}

		message := ports.Message{
			Type:    ports.StorageWrite,
			From:    s.nodeID,
			Payload: payload,
		}

		response, err := s.transport.SendToLeader(ctx, message)
		if err != nil {
			return domain.NewStorageError("batch", "", err)
		}

		if !response.Success {
			return domain.NewStorageError("batch", "", fmt.Errorf(response.Error))
		}

		return nil
	}
}

func (s *Adapter) Close() error {
	s.logger.Info("closing storage adapter")
	return nil
}

func (s *Adapter) HandleMessage(ctx context.Context, message ports.Message) (*ports.Response, error) {
	s.logger.Debug("handling message", "type", message.Type, "from", message.From)
	if !s.raft.IsLeader() {
		return &ports.Response{
			Success: false,
			Error:   "not the leader",
		}, nil
	}

	switch message.Type {
	case ports.StorageRead:
		return s.handleReadMessage(ctx, message.Payload)
	case ports.StorageWrite:
		return s.handleWriteMessage(ctx, message.Payload)
	case ports.StorageDelete:
		return s.handleDeleteMessage(ctx, message.Payload)
	case ports.HealthCheck:
		return &ports.Response{
			Success: true,
		}, nil
	default:
		return &ports.Response{
			Success: false,
			Error:   fmt.Sprintf("unknown message type: %v", message.Type),
		}, nil
	}
}

func (s *Adapter) convertOperationType(opType ports.OperationType) domain.CommandType {
	switch opType {
	case ports.OpPut:
		return domain.CommandPut
	case ports.OpDelete:
		return domain.CommandDelete
	default:
		return domain.CommandPut
	}
}

func (s *Adapter) readFromBadger(key string) ([]byte, error) {
	db := s.raft.GetStateDB()
	if db == nil {
		s.logger.Error("badgerDB is nil", "key", key)
		return nil, domain.NewStorageError("read", key, domain.ErrNotFound)
	}

	var value []byte
	var found bool
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				found = false
				return nil
			}
			return err
		}
		found = true
		value, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		s.logger.Error("badger read transaction failed", "key", key, "error", err.Error())
		return nil, domain.NewStorageError("read", key, err)
	}

	if !found {
		s.logger.Debug("key not found in badger", "key", key)
		return nil, nil // Return nil, nil for not found to match behavior expected by state manager
	}

	s.logger.Debug("successful badger read", "key", key, "value_length", len(value))
	return value, nil
}

func (s *Adapter) listFromBadger(prefix string) ([]ports.KeyValue, error) {
	db := s.raft.GetStateDB()
	if db == nil {
		return nil, domain.NewStorageError("list", prefix, domain.ErrNotFound)
	}

	var results []ports.KeyValue

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			value, err := item.ValueCopy(nil)
			if err != nil {
				return domain.NewStorageError("list", string(key), err)
			}

			results = append(results, ports.KeyValue{
				Key:   string(key),
				Value: value,
			})
		}

		return nil
	})

	if err != nil {
		return nil, domain.NewStorageError("list", prefix, err)
	}

	s.logger.Debug("storage list completed", "prefix", prefix, "count", len(results))
	return results, nil
}
