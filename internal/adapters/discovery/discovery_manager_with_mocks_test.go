package discovery

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/helpers/metadata"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDiscoveryManagerWithMockedProviders(t *testing.T) {
	t.Run("ManagerStartsProviderWithBootstrapMetadata", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		logger := slog.Default()
		manager := NewManager("test-node-123", logger)

		mockProvider := mocks.NewMockProvider(t)
		mockProvider.EXPECT().Name().Return("mock-provider")

		capturedNodeInfo := ports.NodeInfo{}

		mockProvider.EXPECT().Start(mock.Anything, mock.MatchedBy(func(nodeInfo ports.NodeInfo) bool {
			capturedNodeInfo = nodeInfo
			return nodeInfo.ID == "test-node-123"
		})).Return(nil)

		eventChan := make(chan ports.Event)
		mockProvider.EXPECT().Events().Return((<-chan ports.Event)(eventChan)).Maybe()
		mockProvider.EXPECT().Snapshot().Return([]ports.Peer{}).Maybe()

		err := manager.Add(mockProvider)
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err = manager.Start(ctx, "127.0.0.1", 8080)
		assert.NoError(t, err)

		assert.Equal(t, "test-node-123", capturedNodeInfo.ID)
		assert.Equal(t, "127.0.0.1", capturedNodeInfo.Address)
		assert.Equal(t, 8080, capturedNodeInfo.Port)

		assert.Contains(t, capturedNodeInfo.Metadata, "version")
		assert.Equal(t, "1.0.0", capturedNodeInfo.Metadata["version"])

		assert.Contains(t, capturedNodeInfo.Metadata, metadata.BootIDKey)
		assert.Contains(t, capturedNodeInfo.Metadata, metadata.LaunchTimestampKey)

		bootID := metadata.GetBootID(capturedNodeInfo.Metadata)
		assert.NotEmpty(t, bootID)
		assert.Len(t, bootID, 36)

		timestamp := metadata.ExtractLaunchTimestamp(capturedNodeInfo.Metadata)
		assert.Greater(t, timestamp, int64(0))

		assert.True(t, metadata.HasBootstrapMetadata(capturedNodeInfo.Metadata))

		mockProvider.EXPECT().Stop().Return(nil)
		close(eventChan)
		err = manager.Stop()
		assert.NoError(t, err)
	})

	t.Run("ManagerHandlesProviderEvents", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		logger := slog.Default()
		manager := NewManager("test-node-456", logger)

		eventChan := make(chan ports.Event, 10)
		mockProvider := mocks.NewMockProvider(t)

		mockProvider.EXPECT().Name().Return("mock-provider")
		mockProvider.EXPECT().Start(mock.Anything, mock.Anything).Return(nil)
		mockProvider.EXPECT().Events().Return((<-chan ports.Event)(eventChan))
		mockProvider.EXPECT().Snapshot().Return([]ports.Peer{}).Maybe()

		err := manager.Add(mockProvider)
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err = manager.Start(ctx, "127.0.0.1", 8081)
		assert.NoError(t, err)

		peerWithMetadata := ports.Peer{
			ID:      "peer-1",
			Address: "127.0.0.1",
			Port:    8082,
			Metadata: map[string]string{
				metadata.BootIDKey:          "test-boot-id-456",
				metadata.LaunchTimestampKey: "2023-01-01T10:00:00Z",
				"version":                   "1.0.0",
			},
		}

		eventChan <- ports.Event{
			Type: ports.PeerAdded,
			Peer: peerWithMetadata,
		}

		time.Sleep(100 * time.Millisecond)

		peers := manager.GetPeers()
		assert.Len(t, peers, 1)
		assert.Equal(t, "peer-1", peers[0].ID)
		assert.True(t, metadata.HasBootstrapMetadata(peers[0].Metadata))

		mockProvider.EXPECT().Stop().Return(nil)
		close(eventChan)
		err = manager.Stop()
		assert.NoError(t, err)
	})

	t.Run("ManagerHandlesMultipleProviders", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		logger := slog.Default()
		manager := NewManager("test-node-789", logger)

		mockProvider1 := mocks.NewMockProvider(t)
		mockProvider2 := mocks.NewMockProvider(t)

		mockProvider1.EXPECT().Name().Return("mock-provider-1")
		mockProvider1.EXPECT().Start(mock.Anything, mock.Anything).Return(nil)
		mockProvider1.EXPECT().Events().Return(make(<-chan ports.Event))
		mockProvider1.EXPECT().Snapshot().Return([]ports.Peer{
			{
				ID:      "peer-from-provider1",
				Address: "127.0.0.1",
				Port:    8083,
				Metadata: map[string]string{
					metadata.BootIDKey:          "boot-1",
					metadata.LaunchTimestampKey: "2023-01-01T09:00:00Z",
				},
			},
		})

		mockProvider2.EXPECT().Name().Return("mock-provider-2")
		mockProvider2.EXPECT().Start(mock.Anything, mock.Anything).Return(nil)
		mockProvider2.EXPECT().Events().Return(make(<-chan ports.Event))
		mockProvider2.EXPECT().Snapshot().Return([]ports.Peer{
			{
				ID:      "peer-from-provider2",
				Address: "127.0.0.1",
				Port:    8084,
				Metadata: map[string]string{
					metadata.BootIDKey:          "boot-2",
					metadata.LaunchTimestampKey: "2023-01-01T11:00:00Z",
				},
			},
		})

		err := manager.Add(mockProvider1)
		assert.NoError(t, err)
		err = manager.Add(mockProvider2)
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err = manager.Start(ctx, "127.0.0.1", 8080)
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		peers := manager.GetPeers()
		assert.Len(t, peers, 2)

		peerIDs := make([]string, len(peers))
		for i, peer := range peers {
			peerIDs[i] = peer.ID
			assert.True(t, metadata.HasBootstrapMetadata(peer.Metadata))
		}

		assert.Contains(t, peerIDs, "peer-from-provider1")
		assert.Contains(t, peerIDs, "peer-from-provider2")

		mockProvider1.EXPECT().Stop().Return(nil)
		mockProvider2.EXPECT().Stop().Return(nil)
		err = manager.Stop()
		assert.NoError(t, err)
	})

	t.Run("ManagerHandlesBackwardCompatibility", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		logger := slog.Default()
		manager := NewManager("test-node-compat", logger)

		eventChan := make(chan ports.Event, 10)
		mockProvider := mocks.NewMockProvider(t)

		mockProvider.EXPECT().Name().Return("legacy-provider")
		mockProvider.EXPECT().Start(mock.Anything, mock.Anything).Return(nil)
		mockProvider.EXPECT().Events().Return((<-chan ports.Event)(eventChan))
		mockProvider.EXPECT().Snapshot().Return([]ports.Peer{}).Maybe()

		err := manager.Add(mockProvider)
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err = manager.Start(ctx, "127.0.0.1", 8081)
		assert.NoError(t, err)

		legacyPeer := ports.Peer{
			ID:      "legacy-peer",
			Address: "127.0.0.1",
			Port:    8085,
			Metadata: map[string]string{
				"version": "0.9.0",
			},
		}

		eventChan <- ports.Event{
			Type: ports.PeerAdded,
			Peer: legacyPeer,
		}

		time.Sleep(100 * time.Millisecond)

		peers := manager.GetPeers()
		assert.Len(t, peers, 1)
		assert.Equal(t, "legacy-peer", peers[0].ID)

		assert.False(t, metadata.HasBootstrapMetadata(peers[0].Metadata))
		assert.Equal(t, "", metadata.GetBootID(peers[0].Metadata))
		assert.Equal(t, int64(0), metadata.ExtractLaunchTimestamp(peers[0].Metadata))

		assert.Equal(t, "0.9.0", peers[0].Metadata["version"])

		mockProvider.EXPECT().Stop().Return(nil)
		close(eventChan)
		err = manager.Stop()
		assert.NoError(t, err)
	})
}
