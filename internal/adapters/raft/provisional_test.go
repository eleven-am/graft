package raft

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/adapters/transport"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/helpers/metadata"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestProvisionalLeadershipBootstrap(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	storage, err := NewStorage(tempDir, logger)
	require.NoError(t, err)
	defer storage.Close()

	config := DefaultRaftConfig("test-node", "test-cluster", "127.0.0.1:0", tempDir, domain.ClusterPolicyStrict)

	eventManager := mocks.NewMockEventManager(t)
	appTransport := mocks.NewMockTransportPort(t)

	node, err := NewNode(config, storage, eventManager, appTransport, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = node.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer node.Stop()

	assert.True(t, node.IsProvisional(), "Node should start as provisional when no peers provided")

	bootID, timestamp := node.GetBootMetadata()
	assert.NotEmpty(t, bootID, "Boot ID should not be empty")
	assert.Greater(t, timestamp, int64(0), "Launch timestamp should be positive")

	err = node.WaitForLeader(ctx)
	require.NoError(t, err)

	assert.True(t, node.IsLeader(), "Provisional node should become leader")
}

func TestDemoteAndJoinFlow(t *testing.T) {
	provisionalNode, existingNode := setupProvisionalAndExistingNodes(t)
	defer provisionalNode.Stop()
	defer existingNode.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	readinessCallbackCalled := make(chan bool, 2)
	provisionalNode.SetReadinessCallback(func(ready bool) {
		readinessCallbackCalled <- ready
	})

	existingPeer := ports.Peer{
		ID:      "existing-node",
		Address: "127.0.0.1",
		Port:    8081,
	}

	assert.True(t, provisionalNode.IsProvisional(), "Node should be provisional before demotion")
	assert.True(t, provisionalNode.IsLeader(), "Provisional node should be leader")

	err := provisionalNode.DemoteAndJoin(ctx, existingPeer)
	require.NoError(t, err)

	assert.False(t, provisionalNode.IsProvisional(), "Node should not be provisional after demotion")

	select {
	case ready := <-readinessCallbackCalled:
		assert.False(t, ready, "First callback should indicate not ready")
	case <-time.After(5 * time.Second):
		t.Fatal("Expected readiness callback for not ready state")
	}

	select {
	case ready := <-readinessCallbackCalled:
		assert.True(t, ready, "Second callback should indicate ready")
	case <-time.After(5 * time.Second):
		t.Fatal("Expected readiness callback for ready state")
	}
}

func TestDemoteAndJoinWithContext(t *testing.T) {
	provisionalNode, existingNode := setupProvisionalAndExistingNodes(t)
	defer provisionalNode.Stop()
	defer existingNode.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	existingPeer := ports.Peer{
		ID:      "existing-node",
		Address: "127.0.0.1",
		Port:    8081,
	}

	err := provisionalNode.DemoteAndJoin(ctx, existingPeer)
	assert.Error(t, err)

	assert.NotNil(t, err)
	if err != nil {
		t.Logf("Expected error occurred: %s", err.Error())
	} else {
		t.Logf("Unexpected: No error occurred")
	}
}

func TestDemoteAndJoinNonProvisional(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	storage, err := NewStorage(tempDir, logger)
	require.NoError(t, err)
	defer storage.Close()

	config := DefaultRaftConfig("test-node", "test-cluster", "127.0.0.1:0", tempDir, domain.ClusterPolicyStrict)

	eventManager := mocks.NewMockEventManager(t)
	appTransport := mocks.NewMockTransportPort(t)

	node, err := NewNode(config, storage, eventManager, appTransport, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existingPeer := ports.Peer{ID: "peer1", Address: "127.0.0.1", Port: 8081}
	err = node.Start(ctx, []ports.Peer{existingPeer})
	require.NoError(t, err)
	defer node.Stop()

	assert.False(t, node.IsProvisional(), "Node should not be provisional when peers provided")

	targetPeer := ports.Peer{
		ID:      "target-node",
		Address: "127.0.0.1",
		Port:    8082,
	}

	err = node.DemoteAndJoin(ctx, targetPeer)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in provisional state")
}

func TestBootMetadataConsistency(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	storage, err := NewStorage(tempDir, logger)
	require.NoError(t, err)
	defer storage.Close()

	config := DefaultRaftConfig("test-node", "test-cluster", "127.0.0.1:0", tempDir, domain.ClusterPolicyStrict)

	eventManager := mocks.NewMockEventManager(t)
	appTransport := mocks.NewMockTransportPort(t)

	node, err := NewNode(config, storage, eventManager, appTransport, logger)
	require.NoError(t, err)

	bootID1, timestamp1 := node.GetBootMetadata()
	bootID2, timestamp2 := node.GetBootMetadata()

	assert.Equal(t, bootID1, bootID2, "Boot ID should be consistent across calls")
	assert.Equal(t, timestamp1, timestamp2, "Launch timestamp should be consistent across calls")
	assert.NotEmpty(t, bootID1, "Boot ID should not be empty")
	assert.Greater(t, timestamp1, int64(0), "Launch timestamp should be positive")
}

func TestStoragePreservationDuringDemotion(t *testing.T) {
	provisionalNode, existingNode := setupProvisionalAndExistingNodes(t)
	defer provisionalNode.Stop()
	defer existingNode.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := provisionalNode.WaitForLeader(ctx)
	require.NoError(t, err)

	testKey := "test-key"
	testValue := []byte("test-value")

	db := provisionalNode.StateDB()
	require.NotNil(t, db)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(testKey), testValue)
	})
	require.NoError(t, err)

	existingPeer := ports.Peer{
		ID:      "existing-node",
		Address: "127.0.0.1",
		Port:    8081,
	}

	err = provisionalNode.DemoteAndJoin(ctx, existingPeer)
	require.NoError(t, err)

	retrievedValue, err := provisionalNode.ReadStale(testKey)
	require.NoError(t, err)
	assert.Equal(t, testValue, retrievedValue, "Data should be preserved across demotion")
}

func TestRealNodesProvisionalHandoff(t *testing.T) {

	seniorNode, juniorNode, cleanup := setupRealRaftNodes(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	assert.True(t, seniorNode.IsProvisional(), "Senior node should start provisional")
	assert.True(t, juniorNode.IsProvisional(), "Junior node should start provisional")
	assert.True(t, seniorNode.IsLeader(), "Senior node should be leader")
	assert.True(t, juniorNode.IsLeader(), "Junior node should be leader")

	readinessEvents := make(chan bool, 4)
	juniorNode.SetReadinessCallback(func(ready bool) {
		readinessEvents <- ready
	})

	seniorBootID, seniorTimestamp := seniorNode.GetBootMetadata()

	seniorPeer := ports.Peer{
		ID:      "senior-node",
		Address: "127.0.0.1",
		Port:    9080,
		Metadata: map[string]string{
			"boot_id":          seniorBootID,
			"launch_timestamp": time.Unix(0, seniorTimestamp).Format(time.RFC3339Nano),
		},
	}

	err := juniorNode.DemoteAndJoin(ctx, seniorPeer)
	require.NoError(t, err)

	assert.False(t, juniorNode.IsProvisional(), "Junior node should no longer be provisional")
	assert.False(t, juniorNode.IsLeader(), "Junior node should no longer be leader")

	assert.True(t, seniorNode.IsLeader(), "Senior node should remain leader")

	select {
	case ready := <-readinessEvents:
		assert.False(t, ready, "First callback should indicate not ready (during demotion)")
	case <-time.After(5 * time.Second):
		t.Fatal("Expected readiness callback for not ready state")
	}

	select {
	case ready := <-readinessEvents:
		assert.True(t, ready, "Second callback should indicate ready (after successful join)")
	case <-time.After(5 * time.Second):
		t.Fatal("Expected readiness callback for ready state")
	}

	seniorClusterInfo := seniorNode.GetClusterInfo()
	juniorClusterInfo := juniorNode.GetClusterInfo()

	assert.Equal(t, 2, len(seniorClusterInfo.Members), "Senior should see 2 members in cluster")
	assert.Equal(t, 2, len(juniorClusterInfo.Members), "Junior should see 2 members in cluster")

	assert.Equal(t, seniorClusterInfo.Leader.ID, "senior-node", "Cluster should recognize senior as leader")
	assert.Equal(t, juniorClusterInfo.Leader.ID, "senior-node", "Junior should recognize senior as leader")
}

func setupRealRaftNodes(t *testing.T) (*Node, *Node, func()) {
	t.Helper()

	seniorDir := t.TempDir()
	juniorDir := t.TempDir()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	seniorStorage, err := NewStorage(seniorDir, logger)
	require.NoError(t, err)

	juniorStorage, err := NewStorage(juniorDir, logger)
	require.NoError(t, err)

	seniorTransport := transport.NewGRPCTransport(logger, domain.TransportConfig{})
	juniorTransport := transport.NewGRPCTransport(logger, domain.TransportConfig{})

	ctx := context.Background()
	err = seniorTransport.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)

	err = juniorTransport.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)

	seniorPort := 9080
	juniorPort := 9081

	seniorConfig := DefaultRaftConfig("senior-node", "test-cluster", fmt.Sprintf("127.0.0.1:%d", seniorPort), seniorDir, domain.ClusterPolicyStrict)
	juniorConfig := DefaultRaftConfig("junior-node", "test-cluster", fmt.Sprintf("127.0.0.1:%d", juniorPort), juniorDir, domain.ClusterPolicyStrict)

	seniorEventManager := mocks.NewMockEventManager(t)
	juniorEventManager := mocks.NewMockEventManager(t)

	metadata.ResetGlobalBootstrapMetadata()
	seniorNode, err := NewNode(seniorConfig, seniorStorage, seniorEventManager, seniorTransport, logger)
	require.NoError(t, err)

	_, _ = seniorNode.GetBootMetadata()

	metadata.ResetGlobalBootstrapMetadata()

	time.Sleep(10 * time.Millisecond)
	juniorNode, err := NewNode(juniorConfig, juniorStorage, juniorEventManager, juniorTransport, logger)
	require.NoError(t, err)

	seniorTime := time.Now().Add(-5 * time.Minute).UnixNano()
	juniorTime := time.Now().UnixNano()
	seniorNode.provisionalState.launchTimestamp = seniorTime
	juniorNode.provisionalState.launchTimestamp = juniorTime

	seniorTransport.RegisterRaft(seniorNode)
	juniorTransport.RegisterRaft(juniorNode)

	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = seniorNode.Start(testCtx, []ports.Peer{})
	require.NoError(t, err)

	err = seniorNode.WaitForLeader(testCtx)
	require.NoError(t, err)

	err = juniorNode.Start(testCtx, []ports.Peer{})
	require.NoError(t, err)

	err = juniorNode.WaitForLeader(testCtx)
	require.NoError(t, err)

	cleanup := func() {
		seniorNode.Stop()
		juniorNode.Stop()
		seniorTransport.Stop()
		juniorTransport.Stop()
		seniorStorage.Close()
		juniorStorage.Close()
	}

	return seniorNode, juniorNode, cleanup
}

func setupProvisionalAndExistingNodes(t *testing.T) (*Node, *Node) {
	provisionalDir := t.TempDir()
	existingDir := t.TempDir()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	provisionalStorage, err := NewStorage(provisionalDir, logger)
	require.NoError(t, err)

	existingStorage, err := NewStorage(existingDir, logger)
	require.NoError(t, err)

	provisionalConfig := DefaultRaftConfig("provisional-node", "test-cluster", "127.0.0.1:0", provisionalDir, domain.ClusterPolicyStrict)
	existingConfig := DefaultRaftConfig("existing-node", "test-cluster", "127.0.0.1:8081", existingDir, domain.ClusterPolicyStrict)

	eventManager := mocks.NewMockEventManager(t)

	mockTransport := mocks.NewMockTransportPort(t)
	mockTransport.On("SendJoinRequest", mock.Anything, "127.0.0.1:8081", mock.AnythingOfType("*ports.JoinRequest")).Return(&ports.JoinResponse{Accepted: true, Message: "Join accepted"}, nil)

	provisionalNode, err := NewNode(provisionalConfig, provisionalStorage, eventManager, mockTransport, logger)
	require.NoError(t, err)

	existingTransport := mocks.NewMockTransportPort(t)
	existingNode, err := NewNode(existingConfig, existingStorage, eventManager, existingTransport, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = provisionalNode.Start(ctx, []ports.Peer{})
	require.NoError(t, err)

	err = existingNode.Start(ctx, []ports.Peer{})
	require.NoError(t, err)

	err = provisionalNode.WaitForLeader(ctx)
	require.NoError(t, err)

	err = existingNode.WaitForLeader(ctx)
	require.NoError(t, err)

	return provisionalNode, existingNode
}
