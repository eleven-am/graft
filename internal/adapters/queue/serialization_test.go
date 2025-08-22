package queue

import (
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func TestSerializeDeserializeItem(t *testing.T) {
	tests := []struct {
		name      string
		item      ports.QueueItem
		wantError bool
		errorType domain.ErrorType
	}{
		{
			name: "valid item",
			item: ports.QueueItem{
				ID:         "test-id",
				WorkflowID: "workflow-1",
				NodeName:   "node-1",
				Config:     map[string]interface{}{"key": "value", "number": 42},
				Priority:   5,
				EnqueuedAt: time.Now(),
			},
			wantError: false,
		},
		{
			name: "empty ID",
			item: ports.QueueItem{
				WorkflowID: "workflow-1",
				NodeName:   "node-1",
			},
			wantError: true,
			errorType: domain.ErrorTypeValidation,
		},
		{
			name: "empty WorkflowID",
			item: ports.QueueItem{
				ID:       "test-id",
				NodeName: "node-1",
			},
			wantError: true,
			errorType: domain.ErrorTypeValidation,
		},
		{
			name: "empty NodeName",
			item: ports.QueueItem{
				ID:         "test-id",
				WorkflowID: "workflow-1",
			},
			wantError: true,
			errorType: domain.ErrorTypeValidation,
		},
		{
			name: "nil config",
			item: ports.QueueItem{
				ID:         "test-id",
				WorkflowID: "workflow-1",
				NodeName:   "node-1",
				Config:     nil,
				Priority:   0,
				EnqueuedAt: time.Now(),
			},
			wantError: false,
		},
		{
			name: "complex config",
			item: ports.QueueItem{
				ID:         "test-id",
				WorkflowID: "workflow-1",
				NodeName:   "node-1",
				Config: map[string]interface{}{
					"nested": map[string]interface{}{
						"key":   "value",
						"array": []interface{}{1, 2, 3},
					},
					"bool":  true,
					"float": 3.14,
				},
				Priority:   10,
				EnqueuedAt: time.Now(),
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := serializeItem(tt.item)

			if tt.wantError {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if domainErr, ok := err.(domain.Error); ok {
					if domainErr.Type != tt.errorType {
						t.Errorf("expected error type %v, got %v", tt.errorType, domainErr.Type)
					}
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(data) == 0 {
				t.Errorf("expected non-empty data")
				return
			}

			deserialized, err := deserializeItem(data)
			if err != nil {
				t.Errorf("failed to deserialize: %v", err)
				return
			}

			if deserialized.ID != tt.item.ID {
				t.Errorf("ID mismatch: expected %s, got %s", tt.item.ID, deserialized.ID)
			}
			if deserialized.WorkflowID != tt.item.WorkflowID {
				t.Errorf("WorkflowID mismatch: expected %s, got %s", tt.item.WorkflowID, deserialized.WorkflowID)
			}
			if deserialized.NodeName != tt.item.NodeName {
				t.Errorf("NodeName mismatch: expected %s, got %s", tt.item.NodeName, deserialized.NodeName)
			}
			if deserialized.Priority != tt.item.Priority {
				t.Errorf("Priority mismatch: expected %d, got %d", tt.item.Priority, deserialized.Priority)
			}
		})
	}
}

func TestDeserializeItemErrors(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		wantError bool
		errorType domain.ErrorType
	}{
		{
			name:      "empty data",
			data:      []byte{},
			wantError: true,
			errorType: domain.ErrorTypeValidation,
		},
		{
			name:      "invalid JSON",
			data:      []byte("not valid json"),
			wantError: true,
			errorType: domain.ErrorTypeInternal,
		},
		{
			name:      "wrong version",
			data:      []byte(`{"v":999,"id":"test","wf":"workflow","node":"node","cfg":{},"pri":0,"ts":0}`),
			wantError: true,
			errorType: domain.ErrorTypeInternal,
		},
		{
			name:      "missing fields",
			data:      []byte(`{"v":1}`),
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := deserializeItem(tt.data)

			if tt.wantError {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if domainErr, ok := err.(domain.Error); ok {
					if domainErr.Type != tt.errorType {
						t.Errorf("expected error type %v, got %v", tt.errorType, domainErr.Type)
					}
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestSerializeDeserializeMetadata(t *testing.T) {
	tests := []struct {
		name string
		meta QueueMetadata
	}{
		{
			name: "empty metadata",
			meta: QueueMetadata{
				Size:        0,
				LastUpdated: 0,
			},
		},
		{
			name: "metadata with values",
			meta: QueueMetadata{
				Size:        100,
				LastUpdated: time.Now().UnixNano(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := serializeMetadata(tt.meta)
			if err != nil {
				t.Errorf("failed to serialize: %v", err)
				return
			}

			deserialized, err := deserializeMetadata(data)
			if err != nil {
				t.Errorf("failed to deserialize: %v", err)
				return
			}

			if deserialized.Size != tt.meta.Size {
				t.Errorf("Size mismatch: expected %d, got %d", tt.meta.Size, deserialized.Size)
			}

			if deserialized.Version != currentVersion {
				t.Errorf("Version mismatch: expected %d, got %d", currentVersion, deserialized.Version)
			}
		})
	}
}

func TestDeserializeMetadataEmptyData(t *testing.T) {
	meta, err := deserializeMetadata([]byte{})
	if err != nil {
		t.Errorf("unexpected error for empty data: %v", err)
	}

	if meta == nil {
		t.Errorf("expected non-nil metadata")
		return
	}

	if meta.Size != 0 {
		t.Errorf("expected size 0, got %d", meta.Size)
	}

	if meta.Version != currentVersion {
		t.Errorf("expected version %d, got %d", currentVersion, meta.Version)
	}
}

func TestDeserializeMetadataInvalidJSON(t *testing.T) {
	_, err := deserializeMetadata([]byte("invalid json"))
	if err == nil {
		t.Errorf("expected error for invalid JSON")
	}

	if domainErr, ok := err.(domain.Error); ok {
		if domainErr.Type != domain.ErrorTypeInternal {
			t.Errorf("expected internal error, got %v", domainErr.Type)
		}
	}
}

func BenchmarkSerializeItem(b *testing.B) {
	item := ports.QueueItem{
		ID:         "bench-id",
		WorkflowID: "workflow-bench",
		NodeName:   "node-bench",
		Config: map[string]interface{}{
			"key1": "value1",
			"key2": 42,
			"key3": true,
		},
		Priority:   5,
		EnqueuedAt: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = serializeItem(item)
	}
}

func BenchmarkDeserializeItem(b *testing.B) {
	item := ports.QueueItem{
		ID:         "bench-id",
		WorkflowID: "workflow-bench",
		NodeName:   "node-bench",
		Config: map[string]interface{}{
			"key1": "value1",
			"key2": 42,
			"key3": true,
		},
		Priority:   5,
		EnqueuedAt: time.Now(),
	}

	data, _ := serializeItem(item)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = deserializeItem(data)
	}
}
