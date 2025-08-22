package queue

import (
	"strings"
	"testing"
	"time"
)

func TestGenerateQueueKey(t *testing.T) {
	tests := []struct {
		name      string
		queueType QueueType
		timestamp time.Time
		id        string
		want      string
	}{
		{
			name:      "ready queue key",
			queueType: QueueTypeReady,
			timestamp: time.Unix(0, 1234567890123456789),
			id:        "test-id",
			want:      "queue:ready:1234567890123456789:test-id",
		},
		{
			name:      "pending queue key",
			queueType: QueueTypePending,
			timestamp: time.Unix(0, 8876543210987654321),
			id:        "another-id",
			want:      "queue:pending:8876543210987654321:another-id",
		},
		{
			name:      "zero timestamp",
			queueType: QueueTypeReady,
			timestamp: time.Unix(0, 0),
			id:        "zero-time-id",
			want:      "queue:ready:0000000000000000000:zero-time-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateQueueKey(tt.queueType, tt.timestamp, tt.id)
			if got != tt.want {
				t.Errorf("generateQueueKey() = %v, want %v", got, tt.want)
			}

			parts := strings.Split(got, ":")
			if len(parts) != 4 {
				t.Errorf("expected 4 parts, got %d", len(parts))
			}

			if len(parts[2]) != timestampWidth {
				t.Errorf("timestamp width should be %d, got %d", timestampWidth, len(parts[2]))
			}
		})
	}
}

func TestGenerateItemDataKey(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want string
	}{
		{
			name: "simple id",
			id:   "test-id",
			want: "queue:item:test-id",
		},
		{
			name: "uuid format",
			id:   "550e8400-e29b-41d4-a716-446655440000",
			want: "queue:item:550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name: "empty id",
			id:   "",
			want: "queue:item:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateItemDataKey(tt.id)
			if got != tt.want {
				t.Errorf("generateItemDataKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenerateMetadataKey(t *testing.T) {
	tests := []struct {
		name      string
		queueType QueueType
		want      string
	}{
		{
			name:      "ready queue metadata",
			queueType: QueueTypeReady,
			want:      "queue:meta:ready",
		},
		{
			name:      "pending queue metadata",
			queueType: QueueTypePending,
			want:      "queue:meta:pending",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateMetadataKey(tt.queueType)
			if got != tt.want {
				t.Errorf("generateMetadataKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetQueuePrefix(t *testing.T) {
	tests := []struct {
		name      string
		queueType QueueType
		want      string
	}{
		{
			name:      "ready queue prefix",
			queueType: QueueTypeReady,
			want:      "queue:ready:",
		},
		{
			name:      "pending queue prefix",
			queueType: QueueTypePending,
			want:      "queue:pending:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getQueuePrefix(tt.queueType)
			if got != tt.want {
				t.Errorf("getQueuePrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseQueueKey(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		wantQueueType QueueType
		wantTimestamp time.Time
		wantID        string
		wantErr       bool
	}{
		{
			name:          "valid ready queue key",
			key:           "queue:ready:1234567890123456789:test-id",
			wantQueueType: QueueTypeReady,
			wantTimestamp: time.Unix(0, 1234567890123456789),
			wantID:        "test-id",
			wantErr:       false,
		},
		{
			name:          "valid pending queue key",
			key:           "queue:pending:8876543210987654321:another-id",
			wantQueueType: QueueTypePending,
			wantTimestamp: time.Unix(0, 8876543210987654321),
			wantID:        "another-id",
			wantErr:       false,
		},
		{
			name:          "zero timestamp",
			key:           "queue:ready:0000000000000000000:zero-id",
			wantQueueType: QueueTypeReady,
			wantTimestamp: time.Unix(0, 0),
			wantID:        "zero-id",
			wantErr:       false,
		},
		{
			name:    "invalid format - too few parts",
			key:     "queue:ready:123",
			wantErr: true,
		},
		{
			name:    "invalid format - too many parts",
			key:     "queue:ready:123:id:extra",
			wantErr: true,
		},
		{
			name:    "invalid prefix",
			key:     "notqueue:ready:1234567890123456789:id",
			wantErr: true,
		},
		{
			name:    "invalid timestamp",
			key:     "queue:ready:notanumber:id",
			wantErr: true,
		},
		{
			name:    "empty key",
			key:     "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queueType, timestamp, id, err := parseQueueKey(tt.key)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if queueType != tt.wantQueueType {
				t.Errorf("queueType = %v, want %v", queueType, tt.wantQueueType)
			}
			if !timestamp.Equal(tt.wantTimestamp) {
				t.Errorf("timestamp = %v, want %v", timestamp, tt.wantTimestamp)
			}
			if id != tt.wantID {
				t.Errorf("id = %v, want %v", id, tt.wantID)
			}
		})
	}
}

func TestExtractIDFromItemKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantID  string
		wantErr bool
	}{
		{
			name:    "valid item key",
			key:     "queue:item:test-id",
			wantID:  "test-id",
			wantErr: false,
		},
		{
			name:    "valid item key with uuid",
			key:     "queue:item:550e8400-e29b-41d4-a716-446655440000",
			wantID:  "550e8400-e29b-41d4-a716-446655440000",
			wantErr: false,
		},
		{
			name:    "invalid format - too few parts",
			key:     "queue:item",
			wantErr: true,
		},
		{
			name:    "invalid format - too many parts",
			key:     "queue:item:id:extra",
			wantErr: true,
		},
		{
			name:    "invalid prefix",
			key:     "notqueue:item:id",
			wantErr: true,
		},
		{
			name:    "invalid type",
			key:     "queue:notitem:id",
			wantErr: true,
		},
		{
			name:    "empty key",
			key:     "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := extractIDFromItemKey(tt.key)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if id != tt.wantID {
				t.Errorf("extractIDFromItemKey() = %v, want %v", id, tt.wantID)
			}
		})
	}
}

func TestKeyOrdering(t *testing.T) {
	earlier := time.Unix(0, 1000000000)
	later := time.Unix(0, 2000000000)

	key1 := generateQueueKey(QueueTypeReady, earlier, "id1")
	key2 := generateQueueKey(QueueTypeReady, later, "id2")

	if key1 >= key2 {
		t.Errorf("earlier key should be less than later key: %s >= %s", key1, key2)
	}

	sameTimes := []string{
		generateQueueKey(QueueTypeReady, earlier, "a"),
		generateQueueKey(QueueTypeReady, earlier, "b"),
		generateQueueKey(QueueTypeReady, earlier, "c"),
	}

	for i := 0; i < len(sameTimes)-1; i++ {
		if sameTimes[i] >= sameTimes[i+1] {
			t.Errorf("keys with same timestamp should be ordered by ID: %s >= %s",
				sameTimes[i], sameTimes[i+1])
		}
	}
}

func BenchmarkGenerateQueueKey(b *testing.B) {
	timestamp := time.Now()
	for i := 0; i < b.N; i++ {
		_ = generateQueueKey(QueueTypeReady, timestamp, "bench-id")
	}
}

func BenchmarkParseQueueKey(b *testing.B) {
	key := "queue:ready:1234567890123456789:bench-id"
	for i := 0; i < b.N; i++ {
		_, _, _, _ = parseQueueKey(key)
	}
}
