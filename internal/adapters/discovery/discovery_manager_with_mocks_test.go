package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/helpers/metadata"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

		err = manager.Start(ctx, "127.0.0.1", 8080, 9080)
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

		err = manager.Start(ctx, "127.0.0.1", 8081, 9081)
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

		err = manager.Start(ctx, "127.0.0.1", 8080, 9080)
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

		err = manager.Start(ctx, "127.0.0.1", 8081, 9081)
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

	t.Run("ManagerBroadcastsEventsToMultipleSubscribers", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		logger := slog.Default()
		manager := NewManager("test-node-broadcast", logger)

		eventChan := make(chan ports.Event, 10)
		mockProvider := mocks.NewMockProvider(t)

		mockProvider.EXPECT().Name().Return("mock-provider")
		mockProvider.EXPECT().Start(mock.Anything, mock.Anything).Return(nil)
		mockProvider.EXPECT().Events().Return((<-chan ports.Event)(eventChan))
		mockProvider.EXPECT().Snapshot().Return([]ports.Peer{}).Maybe()

		err := manager.Add(mockProvider)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		err = manager.Start(ctx, "127.0.0.1", 8080, 9080)
		require.NoError(t, err)

		sub1, unsub1 := manager.Subscribe()
		sub2, unsub2 := manager.Subscribe()

		peer := ports.Peer{ID: "peer-1", Address: "127.0.0.1", Port: 8086}
		eventChan <- ports.Event{Type: ports.PeerAdded, Peer: peer}

		select {
		case evt := <-sub1:
			assert.Equal(t, peer.ID, evt.Peer.ID)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for subscriber 1")
		}

		select {
		case evt := <-sub2:
			assert.Equal(t, peer.ID, evt.Peer.ID)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for subscriber 2")
		}

		unsub1()
		unsub2()

		select {
		case _, ok := <-sub1:
			assert.False(t, ok)
		case <-time.After(time.Second):
			t.Fatal("subscriber 1 channel not closed")
		}

		select {
		case _, ok := <-sub2:
			assert.False(t, ok)
		case <-time.After(time.Second):
			t.Fatal("subscriber 2 channel not closed")
		}

		mockProvider.EXPECT().Stop().Return(nil)
		err = manager.Stop()
		assert.NoError(t, err)
	})

	t.Run("ManagerKeepsBroadcastingWhenSubscriberBacksUp", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		logger := slog.Default()
		manager := NewManager("test-node-stall", logger)

		eventChan := make(chan ports.Event, 10)
		mockProvider := mocks.NewMockProvider(t)

		mockProvider.EXPECT().Name().Return("mock-provider")
		mockProvider.EXPECT().Start(mock.Anything, mock.Anything).Return(nil)
		mockProvider.EXPECT().Events().Return((<-chan ports.Event)(eventChan))
		mockProvider.EXPECT().Snapshot().Return([]ports.Peer{}).Maybe()

		err := manager.Add(mockProvider)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = manager.Start(ctx, "127.0.0.1", 8080, 9080)
		require.NoError(t, err)

		_, unsubStalled := manager.Subscribe()
		activeSub, unsubActive := manager.Subscribe()

		var receivedMu sync.Mutex
		received := make([]string, 0, 11)

		done := make(chan struct{})

		go func() {
			for i := 0; i < 11; i++ {
				select {
				case evt := <-activeSub:
					receivedMu.Lock()
					received = append(received, evt.Peer.ID)
					receivedMu.Unlock()
					if len(received) == 11 {
						close(done)
					}
				case <-time.After(2 * time.Second):
					return
				}
			}
		}()

		for i := 0; i < 11; i++ {
			peer := ports.Peer{ID: fmt.Sprintf("peer-%d", i)}
			eventChan <- ports.Event{Type: ports.PeerAdded, Peer: peer}
		}

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for active subscriber to receive events")
		}

		receivedMu.Lock()
		assert.Len(t, received, 11)
		receivedMu.Unlock()

		require.Eventually(t, func() bool {
			manager.subscribersMu.RLock()
			defer manager.subscribersMu.RUnlock()
			return len(manager.subscribers) == 1
		}, time.Second, 10*time.Millisecond)

		unsubStalled()
		unsubActive()

		mockProvider.EXPECT().Stop().Return(nil)
		err = manager.Stop()
		assert.NoError(t, err)
	})

	t.Run("ManagerContinuesBroadcastAfterSubscriberChannelClosed", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		logger := slog.Default()
		manager := NewManager("test-node-closed", logger)

		manager.ctx, _ = context.WithCancel(context.Background())
		require.NotNil(t, manager.ctx)

		sub1, unsub1 := manager.Subscribe()
		sub2, unsub2 := manager.Subscribe()

		manager.subscribersMu.RLock()
		var rawSub1 chan ports.Event
		var rawSub2 chan ports.Event
		for _, ch := range manager.subscribers {
			switch reflect.ValueOf(ch).Pointer() {
			case reflect.ValueOf(sub1).Pointer():
				rawSub1 = ch
			case reflect.ValueOf(sub2).Pointer():
				rawSub2 = ch
			}
		}
		manager.subscribersMu.RUnlock()

		require.NotNil(t, rawSub1)
		require.Equal(t, reflect.ValueOf(sub1).Pointer(), reflect.ValueOf(rawSub1).Pointer())
		require.NotNil(t, rawSub2)
		require.Equal(t, reflect.ValueOf(sub2).Pointer(), reflect.ValueOf(rawSub2).Pointer())

		closeSubscriberChannel(rawSub1)

		manager.subscribersMu.RLock()
		require.Len(t, manager.subscribers, 2)
		manager.subscribersMu.RUnlock()

		peer := ports.Peer{ID: "peer-after-close"}
		manager.broadcastEvent(ports.Event{Type: ports.PeerAdded, Peer: peer})

		select {
		case evt, ok := <-rawSub2:
			require.True(t, ok, "second subscriber channel closed prematurely")
			assert.Equal(t, peer.ID, evt.Peer.ID)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timed out waiting for second subscriber")
		}

		manager.subscribersMu.RLock()
		assert.Len(t, manager.subscribers, 1)
		manager.subscribersMu.RUnlock()

		unsub1()
		unsub2()
	})

	t.Run("ManagerStopCanBeCalledMultipleTimes", func(t *testing.T) {
		metadata.ResetGlobalBootstrapMetadata()
		logger := slog.Default()
		manager := NewManager("test-node-stop", logger)

		eventChan := make(chan ports.Event, 1)
		mockProvider := mocks.NewMockProvider(t)

		mockProvider.EXPECT().Name().Return("mock-provider")
		mockProvider.EXPECT().Start(mock.Anything, mock.Anything).Return(nil)
		mockProvider.EXPECT().Events().Return((<-chan ports.Event)(eventChan))
		mockProvider.EXPECT().Snapshot().Return([]ports.Peer{}).Maybe()

		err := manager.Add(mockProvider)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err = manager.Start(ctx, "127.0.0.1", 8080, 9080)
		require.NoError(t, err)

		// trigger at least one event send to exercise shutdown path
		eventChan <- ports.Event{Type: ports.PeerRemoved, Peer: ports.Peer{ID: "peer-stop"}}

		mockProvider.EXPECT().Stop().Return(nil)
		err = manager.Stop()
		assert.NoError(t, err)

		assert.NotPanics(t, func() {
			err = manager.Stop()
			assert.NoError(t, err)
		})
	})
}

func TestSendToSubscriberRecover(t *testing.T) {
	logger := slog.Default()
	manager := NewManager("test-node-send", logger)
	manager.ctx, _ = context.WithCancel(context.Background())

	ch := make(chan ports.Event)
	close(ch)

	cont, prune := manager.sendToSubscriber(0, ch, ports.Event{})
	assert.True(t, cont)
	assert.True(t, prune)
}
