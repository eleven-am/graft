package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type putPayload struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type getPayload struct {
	Key string `json:"key"`
}

type deletePayload struct {
	Key string `json:"key"`
}

type listPayload struct {
	Prefix string `json:"prefix"`
}

type batchPayload struct {
	Operations []ports.Operation `json:"operations"`
}

func (s *Adapter) serializePutOperation(key string, value []byte) ([]byte, error) {
	payload := putPayload{
		Key:   key,
		Value: value,
	}
	return json.Marshal(payload)
}

func (s *Adapter) serializeGetOperation(key string) ([]byte, error) {
	payload := getPayload{
		Key: key,
	}
	return json.Marshal(payload)
}

func (s *Adapter) serializeDeleteOperation(key string) ([]byte, error) {
	payload := deletePayload{
		Key: key,
	}
	return json.Marshal(payload)
}

func (s *Adapter) serializeListOperation(prefix string) ([]byte, error) {
	payload := listPayload{
		Prefix: prefix,
	}
	return json.Marshal(payload)
}

func (s *Adapter) serializeBatchOperation(ops []ports.Operation) ([]byte, error) {
	payload := batchPayload{
		Operations: ops,
	}
	return json.Marshal(payload)
}

func (s *Adapter) handleReadMessage(ctx context.Context, payload []byte) (*ports.Response, error) {
	var getReq getPayload
	if err := json.Unmarshal(payload, &getReq); err == nil && getReq.Key != "" {
		value, err := s.readFromBadger(getReq.Key)
		if err != nil {
			return &ports.Response{
				Success: false,
				Error:   err.Error(),
			}, nil
		}

		return &ports.Response{
			Success: true,
			Data:    value,
		}, nil
	}

	var listReq listPayload
	if err := json.Unmarshal(payload, &listReq); err == nil {
		result, err := s.listFromBadger(listReq.Prefix)
		if err != nil {
			return &ports.Response{
				Success: false,
				Error:   err.Error(),
			}, nil
		}

		data, err := json.Marshal(result)
		if err != nil {
			return &ports.Response{
				Success: false,
				Error:   fmt.Sprintf("failed to serialize list result: %v", err),
			}, nil
		}

		return &ports.Response{
			Success: true,
			Data:    data,
		}, nil
	}

	return &ports.Response{
		Success: false,
		Error:   "invalid read message payload",
	}, nil
}

func (s *Adapter) handleWriteMessage(ctx context.Context, payload []byte) (*ports.Response, error) {
	var putReq putPayload
	if err := json.Unmarshal(payload, &putReq); err == nil && putReq.Key != "" {
		cmd := domain.NewPutCommand(putReq.Key, putReq.Value)
		result, err := s.raft.Apply(ctx, cmd)
		if err != nil {
			return &ports.Response{
				Success: false,
				Error:   fmt.Sprintf("raft apply failed: %v", err),
			}, nil
		}

		return &ports.Response{
			Success: result.Success,
			Error:   result.Error,
		}, nil
	}

	var batchReq batchPayload
	if err := json.Unmarshal(payload, &batchReq); err == nil {
		batchOps := make([]domain.BatchOp, len(batchReq.Operations))
		for i, op := range batchReq.Operations {
			batchOps[i] = domain.BatchOp{
				Type:  s.convertOperationType(op.Type),
				Key:   op.Key,
				Value: op.Value,
			}
		}

		cmd := domain.NewBatchCommand(batchOps)
		result, err := s.raft.Apply(ctx, cmd)
		if err != nil {
			return &ports.Response{
				Success: false,
				Error:   fmt.Sprintf("raft apply failed: %v", err),
			}, nil
		}

		return &ports.Response{
			Success: result.Success,
			Error:   result.Error,
		}, nil
	}

	return &ports.Response{
		Success: false,
		Error:   "invalid write message payload",
	}, nil
}

func (s *Adapter) handleDeleteMessage(ctx context.Context, payload []byte) (*ports.Response, error) {
	var deleteReq deletePayload
	if err := json.Unmarshal(payload, &deleteReq); err != nil {
		return &ports.Response{
			Success: false,
			Error:   fmt.Sprintf("invalid delete payload: %v", err),
		}, nil
	}

	cmd := domain.NewDeleteCommand(deleteReq.Key)
	result, err := s.raft.Apply(ctx, cmd)
	if err != nil {
		return &ports.Response{
			Success: false,
			Error:   fmt.Sprintf("raft apply failed: %v", err),
		}, nil
	}

	return &ports.Response{
		Success: result.Success,
		Error:   result.Error,
	}, nil
}
