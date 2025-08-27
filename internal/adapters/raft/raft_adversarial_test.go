package raft

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNode_ConcurrentOperations(t *testing.T) {
	tempDir := t.TempDir()

	config := &Config{
		NodeID:             "node1",
		BindAddr:           "127.0.0.1:0",
		DataDir:            tempDir,
		ClusterID:          "test-cluster",
		SnapshotInterval:   24 * time.Hour,
		SnapshotThreshold:  8192,
		MaxSnapshots:       3,
		MaxJoinAttempts:    3,
		HeartbeatTimeout:   1 * time.Second,
		ElectionTimeout:    1 * time.Second,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		TrailingLogs:       10240,
	}

	storage, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)
	defer storage.Close()

	node, err := NewNode(config, storage, nil, slog.Default())
	require.NoError(t, err)

	err = node.Start(context.Background(), nil)
	require.NoError(t, err)
	defer node.Shutdown()

	time.Sleep(2 * time.Second)

	const numGoroutines = 10
	const numOperations = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", routineID, j)
				value := []byte(fmt.Sprintf("value-%d-%d", routineID, j))

				cmd := domain.Command{
					Type:      domain.CommandPut,
					Key:       key,
					Value:     value,
					RequestID: fmt.Sprintf("req-%d-%d", routineID, j),
					Timestamp: time.Now(),
				}

				_, err := node.Apply(cmd, 5*time.Second)
				if err != nil && err != raft.ErrNotLeader {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	assert.True(t, node.IsLeader())
}

func TestNode_LeaderElection(t *testing.T) {
	tempDir1 := t.TempDir()
	tempDir2 := t.TempDir()
	tempDir3 := t.TempDir()

	configs := []*Config{
		{
			NodeID:             "node1",
			BindAddr:           "127.0.0.1:7000",
			DataDir:            tempDir1,
			ClusterID:          "test-cluster",
			SnapshotInterval:   24 * time.Hour,
			SnapshotThreshold:  8192,
			MaxSnapshots:       3,
			MaxJoinAttempts:    3,
			HeartbeatTimeout:   500 * time.Millisecond,
			ElectionTimeout:    500 * time.Millisecond,
			CommitTimeout:      50 * time.Millisecond,
			MaxAppendEntries:   64,
			LeaderLeaseTimeout: 500 * time.Millisecond,
			TrailingLogs:       10240,
		},
		{
			NodeID:             "node2",
			BindAddr:           "127.0.0.1:7001",
			DataDir:            tempDir2,
			ClusterID:          "test-cluster",
			SnapshotInterval:   24 * time.Hour,
			SnapshotThreshold:  8192,
			MaxSnapshots:       3,
			MaxJoinAttempts:    3,
			HeartbeatTimeout:   500 * time.Millisecond,
			ElectionTimeout:    500 * time.Millisecond,
			CommitTimeout:      50 * time.Millisecond,
			MaxAppendEntries:   64,
			LeaderLeaseTimeout: 500 * time.Millisecond,
			TrailingLogs:       10240,
		},
		{
			NodeID:             "node3",
			BindAddr:           "127.0.0.1:7002",
			DataDir:            tempDir3,
			ClusterID:          "test-cluster",
			SnapshotInterval:   24 * time.Hour,
			SnapshotThreshold:  8192,
			MaxSnapshots:       3,
			MaxJoinAttempts:    3,
			HeartbeatTimeout:   500 * time.Millisecond,
			ElectionTimeout:    500 * time.Millisecond,
			CommitTimeout:      50 * time.Millisecond,
			MaxAppendEntries:   64,
			LeaderLeaseTimeout: 500 * time.Millisecond,
			TrailingLogs:       10240,
		},
	}

	var nodes []*Node
	var storages []*Storage

	for i, config := range configs {
		storage, err := NewStorage(config.DataDir, slog.Default())
		require.NoError(t, err)
		storages = append(storages, storage)

		node, err := NewNode(config, storage, nil, slog.Default())
		require.NoError(t, err)
		nodes = append(nodes, node)

		var peers []ports.Peer
		if i == 0 {
			peers = nil
		} else {
			peers = []ports.Peer{{ID: "node1", Address: "127.0.0.1:7000"}}
		}
		err = node.Start(context.Background(), peers)
		require.NoError(t, err)
	}

	defer func() {
		for _, node := range nodes {
			node.Shutdown()
		}
		for _, storage := range storages {
			storage.Close()
		}
	}()

	time.Sleep(3 * time.Second)

	leaderCount := 0
	for _, node := range nodes {
		if node.IsLeader() {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "Exactly one node should be leader")

	var leaderNode *Node
	for _, node := range nodes {
		if node.IsLeader() {
			leaderNode = node
			break
		}
	}
	require.NotNil(t, leaderNode)

	cmd := domain.Command{
		Type:      domain.CommandPut,
		Key:       "test-key",
		Value:     []byte("test-value"),
		RequestID: "test-req",
		Timestamp: time.Now(),
	}

	_, err := leaderNode.Apply(cmd, 5*time.Second)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)
}

func TestNode_SnapshotRestore(t *testing.T) {
	tempDir := t.TempDir()

	config := &Config{
		NodeID:             "node1",
		BindAddr:           "127.0.0.1:0",
		DataDir:            tempDir,
		ClusterID:          "test-cluster",
		SnapshotInterval:   100 * time.Millisecond,
		SnapshotThreshold:  10,
		MaxSnapshots:       3,
		MaxJoinAttempts:    3,
		HeartbeatTimeout:   1 * time.Second,
		ElectionTimeout:    1 * time.Second,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		TrailingLogs:       10240,
	}

	storage, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)

	node, err := NewNode(config, storage, nil, slog.Default())
	require.NoError(t, err)

	err = node.Start(context.Background(), nil)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		cmd := domain.Command{
			Type:      domain.CommandPut,
			Key:       key,
			Value:     value,
			RequestID: fmt.Sprintf("req-%d", i),
			Timestamp: time.Now(),
		}
		_, err := node.Apply(cmd, 5*time.Second)
		require.NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	node.Shutdown()
	storage.Close()

	storage2, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)
	defer storage2.Close()

	node2, err := NewNode(config, storage2, nil, slog.Default())
	require.NoError(t, err)

	err = node2.Start(context.Background(), nil)
	require.NoError(t, err)
	defer node2.Shutdown()

	time.Sleep(2 * time.Second)

	db := node2.StateDB()
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		expectedValue := []byte(fmt.Sprintf("value-%d", i))

		err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				assert.Equal(t, expectedValue, val)
				return nil
			})
		})
		assert.NoError(t, err)
	}
}

func TestNode_DataPersistence(t *testing.T) {
	tempDir := t.TempDir()

	config := &Config{
		NodeID:             "node1",
		BindAddr:           "127.0.0.1:0",
		DataDir:            tempDir,
		ClusterID:          "test-cluster",
		SnapshotInterval:   24 * time.Hour,
		SnapshotThreshold:  8192,
		MaxSnapshots:       3,
		MaxJoinAttempts:    3,
		HeartbeatTimeout:   1 * time.Second,
		ElectionTimeout:    1 * time.Second,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		TrailingLogs:       10240,
	}

	storage1, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)

	node1, err := NewNode(config, storage1, nil, slog.Default())
	require.NoError(t, err)

	err = node1.Start(context.Background(), nil)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for key, value := range testData {
		cmd := domain.Command{
			Type:      domain.CommandPut,
			Key:       key,
			Value:     value,
			RequestID: "req-" + key,
			Timestamp: time.Now(),
		}
		_, err := node1.Apply(cmd, 5*time.Second)
		require.NoError(t, err)
	}

	node1.Shutdown()
	storage1.Close()

	storage2, err := NewStorage(tempDir, slog.Default())
	require.NoError(t, err)
	defer storage2.Close()

	node2, err := NewNode(config, storage2, nil, slog.Default())
	require.NoError(t, err)

	err = node2.Start(context.Background(), nil)
	require.NoError(t, err)
	defer node2.Shutdown()

	time.Sleep(2 * time.Second)

	db := node2.StateDB()
	for key, expectedValue := range testData {
		err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				assert.Equal(t, expectedValue, val)
				return nil
			})
		})
		assert.NoError(t, err)
	}
}
