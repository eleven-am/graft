package discovery

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/helpers/metadata"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/mdns"
)

func TestManagerBootstrapMetadataPropagation(t *testing.T) {
	logger := slog.Default()
	manager := NewManager("test-node", logger)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := manager.Start(ctx, "127.0.0.1", 8080, 9080, "test-cluster")
	if err != nil {
		t.Fatalf("Failed to start discovery manager: %v", err)
	}
	defer manager.Stop()

	time.Sleep(100 * time.Millisecond)

	peers := manager.GetPeers()
	if len(peers) != 0 {
		t.Errorf("Expected no peers in self-discovery, got %d", len(peers))
	}
}

func TestMetadataHelperFunctions(t *testing.T) {
	t.Run("GenerateBootID", func(t *testing.T) {
		id1 := metadata.GenerateBootID()
		id2 := metadata.GenerateBootID()

		if id1 == "" {
			t.Error("Expected non-empty boot ID")
		}
		if id1 == id2 {
			t.Error("Expected unique boot IDs")
		}
		if len(id1) != 36 {
			t.Errorf("Expected UUID format (36 chars), got %d", len(id1))
		}
	})

	t.Run("GetLaunchTimestamp", func(t *testing.T) {
		ts1 := metadata.GetLaunchTimestamp()
		time.Sleep(1 * time.Millisecond)
		ts2 := metadata.GetLaunchTimestamp()

		if ts1 <= 0 {
			t.Error("Expected positive timestamp")
		}
		if ts2 <= ts1 {
			t.Error("Expected timestamp to increase")
		}
	})

	t.Run("NewBootstrapMetadata", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		meta := metadata.NewBootstrapMetadata()

		bootID := metadata.GetBootID(meta)
		if bootID == "" {
			t.Error("Expected boot_id in metadata")
		}

		timestamp := metadata.ExtractLaunchTimestamp(meta)
		if timestamp <= 0 {
			t.Error("Expected launch_timestamp in metadata")
		}

		if !metadata.HasBootstrapMetadata(meta) {
			t.Error("Expected HasBootstrapMetadata to return true")
		}

		meta2 := metadata.GetGlobalBootstrapMetadata()
		if metadata.GetBootID(meta) != metadata.GetBootID(meta2) {
			t.Error("Expected consistent boot_id across calls")
		}
		if metadata.ExtractLaunchTimestamp(meta) != metadata.ExtractLaunchTimestamp(meta2) {
			t.Error("Expected consistent launch_timestamp across calls")
		}
	})

	t.Run("ExtendMetadata", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		existing := map[string]string{
			"version":    "1.0.0",
			"cluster_id": "test-cluster",
		}

		bootMeta := metadata.GetGlobalBootstrapMetadata()
		extended := metadata.ExtendMetadata(existing, bootMeta)

		if extended["version"] != "1.0.0" {
			t.Error("Expected existing metadata to be preserved")
		}
		if extended["cluster_id"] != "test-cluster" {
			t.Error("Expected existing metadata to be preserved")
		}

		if !metadata.HasBootstrapMetadata(extended) {
			t.Error("Expected bootstrap metadata to be added")
		}
	})

	t.Run("BackwardCompatibility", func(t *testing.T) {
		emptyMeta := map[string]string{}
		nilMeta := map[string]string(nil)

		if metadata.HasBootstrapMetadata(emptyMeta) {
			t.Error("Expected HasBootstrapMetadata to return false for empty metadata")
		}
		if metadata.HasBootstrapMetadata(nilMeta) {
			t.Error("Expected HasBootstrapMetadata to return false for nil metadata")
		}

		bootID := metadata.GetBootID(emptyMeta)
		if bootID != "" {
			t.Error("Expected empty boot ID for metadata without boot_id")
		}

		timestamp := metadata.ExtractLaunchTimestamp(emptyMeta)
		if timestamp != 0 {
			t.Error("Expected zero timestamp for metadata without launch_timestamp")
		}
	})
}

func TestStaticProviderMetadataPreservation(t *testing.T) {
	existingMeta := map[string]string{
		"boot_id":          "test-boot-id",
		"launch_timestamp": "2023-01-01T00:00:00Z",
		"custom_field":     "custom_value",
	}

	peers := []ports.Peer{
		{
			ID:       "peer1",
			Address:  "127.0.0.1",
			Port:     8081,
			Metadata: existingMeta,
		},
	}

	provider := NewStaticProvider(peers)
	snapshot := provider.Snapshot()

	if len(snapshot) != 1 {
		t.Fatalf("Expected 1 peer, got %d", len(snapshot))
	}

	peer := snapshot[0]
	if !metadata.HasBootstrapMetadata(peer.Metadata) {
		t.Error("Expected bootstrap metadata to be preserved")
	}

	if peer.Metadata["custom_field"] != "custom_value" {
		t.Error("Expected custom metadata to be preserved")
	}
}

func TestMDNSMetadataExtraction(t *testing.T) {
	logger := slog.Default()
	provider := NewMDNSProvider("_test._tcp", "local.", "test-host", true, logger)

	tests := []struct {
		name      string
		txtFields []string
		expected  map[string]string
	}{
		{
			name:      "WithBootstrapMetadata",
			txtFields: []string{"id=test-peer", "boot_id=test-boot-123", "launch_timestamp=2023-01-01T00:00:00Z", "version=1.0.0"},
			expected: map[string]string{
				"boot_id":          "test-boot-123",
				"launch_timestamp": "2023-01-01T00:00:00Z",
				"version":          "1.0.0",
			},
		},
		{
			name:      "WithoutBootstrapMetadata",
			txtFields: []string{"id=test-peer", "version=1.0.0"},
			expected: map[string]string{
				"version": "1.0.0",
			},
		},
		{
			name:      "EmptyTxtFields",
			txtFields: []string{"id=test-peer"},
			expected:  map[string]string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockEntry := &mdns.ServiceEntry{
				Name:       "test-service",
				Host:       "test-host.local.",
				AddrV4:     net.ParseIP("127.0.0.1"),
				Port:       8080,
				InfoFields: test.txtFields,
			}

			extractedMeta := provider.extractAllMetadata(mockEntry)

			for key, expectedValue := range test.expected {
				if actualValue, exists := extractedMeta[key]; !exists {
					t.Errorf("Expected key %s to exist in extracted metadata", key)
				} else if actualValue != expectedValue {
					t.Errorf("Expected %s=%s, got %s=%s", key, expectedValue, key, actualValue)
				}
			}

			for key := range extractedMeta {
				if _, expected := test.expected[key]; !expected {
					t.Errorf("Unexpected key %s in extracted metadata", key)
				}
			}
		})
	}
}
