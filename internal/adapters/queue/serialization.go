package queue

import (
	"encoding/json"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

const currentVersion = 1

type QueueItemData struct {
	Version    int         `json:"v"`
	ID         string      `json:"id"`
	WorkflowID string      `json:"wf"`
	NodeName   string      `json:"node"`
	Config     interface{} `json:"cfg"`
	Priority   int         `json:"pri"`
	EnqueuedAt int64       `json:"ts"`
}

func serializeItem(item ports.QueueItem) ([]byte, error) {
	if item.ID == "" {
		return nil, domain.NewValidationError("item.ID", "ID is required")
	}
	if item.WorkflowID == "" {
		return nil, domain.NewValidationError("item.WorkflowID", "WorkflowID is required")
	}
	if item.NodeName == "" {
		return nil, domain.NewValidationError("item.NodeName", "NodeName is required")
	}

	data := QueueItemData{
		Version:    currentVersion,
		ID:         item.ID,
		WorkflowID: item.WorkflowID,
		NodeName:   item.NodeName,
		Config:     item.Config,
		Priority:   item.Priority,
		EnqueuedAt: item.EnqueuedAt.UnixNano(),
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to serialize queue item",
			Details: map[string]interface{}{
				"error": err.Error(),
				"id":    item.ID,
			},
		}
	}

	return bytes, nil
}

func deserializeItem(data []byte) (*ports.QueueItem, error) {
	if len(data) == 0 {
		return nil, domain.NewValidationError("data", "empty data")
	}

	var itemData QueueItemData
	if err := json.Unmarshal(data, &itemData); err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to deserialize queue item",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if itemData.Version != currentVersion {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "unsupported queue item version",
			Details: map[string]interface{}{
				"version":  itemData.Version,
				"expected": currentVersion,
			},
		}
	}

	item := &ports.QueueItem{
		ID:         itemData.ID,
		WorkflowID: itemData.WorkflowID,
		NodeName:   itemData.NodeName,
		Config:     itemData.Config,
		Priority:   itemData.Priority,
		EnqueuedAt: time.Unix(0, itemData.EnqueuedAt),
	}

	return item, nil
}

type QueueMetadata struct {
	Version     int   `json:"v"`
	Size        int   `json:"size"`
	LastUpdated int64 `json:"updated"`
}

func serializeMetadata(meta QueueMetadata) ([]byte, error) {
	meta.Version = currentVersion
	meta.LastUpdated = time.Now().UnixNano()

	bytes, err := json.Marshal(meta)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to serialize queue metadata",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return bytes, nil
}

func deserializeMetadata(data []byte) (*QueueMetadata, error) {
	if len(data) == 0 {
		return &QueueMetadata{
			Version:     currentVersion,
			Size:        0,
			LastUpdated: time.Now().UnixNano(),
		}, nil
	}

	var meta QueueMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to deserialize queue metadata",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return &meta, nil
}
