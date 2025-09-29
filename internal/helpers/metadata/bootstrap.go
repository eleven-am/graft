package metadata

import (
	"time"

	"github.com/google/uuid"
)

const (
	BootIDKey          = "boot_id"
	LaunchTimestampKey = "launch_timestamp"
)

// Deprecated: Use GetProvider().GetMetadata() instead
func GenerateBootID() string {
	return uuid.New().String()
}

// Deprecated: Use GetProvider().GetLaunchTimestamp() instead
func GetLaunchTimestamp() int64 {
	return time.Now().UnixNano()
}

// NewBootstrapMetadata creates new bootstrap metadata using the shared provider.
// This ensures consistency across all components that need bootstrap metadata.
func NewBootstrapMetadata() map[string]string {
	return GetProvider().GetMetadata()
}

// GetGlobalBootstrapMetadata returns bootstrap metadata from the shared provider.
// This ensures all subsystems use the same boot ID and launch timestamp.
func GetGlobalBootstrapMetadata() map[string]string {
	return GetProvider().GetMetadata()
}

func ExtendMetadata(existing map[string]string, bootMetadata map[string]string) map[string]string {
	if existing == nil {
		existing = make(map[string]string)
	}

	result := make(map[string]string)
	for k, v := range existing {
		result[k] = v
	}

	for k, v := range bootMetadata {
		result[k] = v
	}

	return result
}

func GetBootID(metadata map[string]string) string {
	if metadata == nil {
		return ""
	}
	return metadata[BootIDKey]
}

func ExtractLaunchTimestamp(metadata map[string]string) int64 {
	if metadata == nil {
		return 0
	}
	if ts, exists := metadata[LaunchTimestampKey]; exists {
		return parseTimestamp(ts)
	}
	return 0
}

func formatTimestamp(nanos int64) string {
	return time.Unix(0, nanos).Format(time.RFC3339Nano)
}

func parseTimestamp(ts string) int64 {
	if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return parsed.UnixNano()
	}
	return 0
}

func HasBootstrapMetadata(metadata map[string]string) bool {
	return GetBootID(metadata) != "" && ExtractLaunchTimestamp(metadata) != 0
}

// ResetGlobalBootstrapMetadata resets the shared provider state.
// This is primarily for testing purposes to ensure clean state between tests.
func ResetGlobalBootstrapMetadata() {
	Reset()
}
