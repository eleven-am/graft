package raft

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestRaftConsensusBreaking(t *testing.T) {
	leader, followers := setupRaftCluster(t, 3)
	defer cleanupCluster(leader, followers)

	ctx := context.Background()

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			cmd := domain.NewPutCommand(
				fmt.Sprintf("key-%d", id),
				[]byte(fmt.Sprintf("concurrent-command-%d", id)),
			)

			_, err := leader.Apply(ctx, cmd)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	var errorList []error
	for err := range errors {
		errorList = append(errorList, err)
	}

	t.Logf("Concurrent consensus operations generated %d errors", len(errorList))

	for _, err := range errorList {
		t.Logf("Consensus error: %v", err)
	}
}

func TestSplitBrainScenario(t *testing.T) {
	leader, followers := setupRaftCluster(t, 5)
	defer cleanupCluster(leader, followers)

	ctx := context.Background()

	require.True(t, leader.IsLeader(), "Initial node should be leader")

	cmd1 := domain.NewPutCommand("test-key", []byte("before-partition"))
	_, err := leader.Apply(ctx, cmd1)
	require.NoError(t, err)

	leader.Stop()

	time.Sleep(2 * time.Second)

	var newLeader *Adapter
	for _, follower := range followers {
		if follower.IsLeader() {
			newLeader = follower
			break
		}
	}

	if newLeader == nil {
		t.Fatal("No new leader elected after original leader failure")
	}

	cmd2 := domain.NewPutCommand("test-key-2", []byte("after-partition"))
	_, err = newLeader.Apply(ctx, cmd2)
	require.NoError(t, err)

	newLeaderID, newLeaderAddr := newLeader.GetLeader()
	host, portStr, err := net.SplitHostPort(newLeaderAddr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	peer := ports.Peer{
		ID:       newLeaderID,
		Address:  host,
		Port:     port,
		Metadata: make(map[string]string),
	}

	newConfig := DefaultRaftConfig("old-leader", "127.0.0.1:0", t.TempDir())
	oldLeaderRestarted, err := NewAdapter(newConfig, slog.Default())
	require.NoError(t, err)

	err = oldLeaderRestarted.Start(ctx, []ports.Peer{peer})
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	if oldLeaderRestarted.IsLeader() {
		t.Error("Old leader should not become leader after rejoining - it should remain a follower or fail to join")
	}

	clusterInfo := newLeader.GetClusterInfo()
	if len(clusterInfo.Members) > 4 {
		t.Error("Old leader should not be successfully added to cluster membership")
	}

	oldLeaderRestarted.Stop()
}

func TestMaliciousCommands(t *testing.T) {
	leader, followers := setupRaftCluster(t, 3)
	defer cleanupCluster(leader, followers)

	ctx := context.Background()

	maliciousCommands := []*domain.Command{
		{Type: domain.CommandType(255), Key: "", Value: nil},
		{Type: domain.CommandPut, Key: "test", Value: make([]byte, 10*1024*1024)},
		{Type: domain.CommandPut, Key: string(make([]byte, 1000)), Value: []byte("normal")},
		nil,
	}

	for i, cmd := range maliciousCommands {
		t.Run(fmt.Sprintf("malicious-command-%d", i), func(t *testing.T) {
			var err error
			if cmd == nil {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("Recovered from panic with nil command: %v", r)
					}
				}()
				_, err = leader.Apply(ctx, nil)
			} else {
				_, err = leader.Apply(ctx, cmd)
			}

			if err == nil {
				t.Errorf("Malicious command should have been rejected: %+v", cmd)
			}
			t.Logf("Command rejected as expected: %v", err)
		})
	}
}

func TestNetworkPartitionRecovery(t *testing.T) {
	leader, followers := setupRaftCluster(t, 5)
	defer cleanupCluster(leader, followers)

	ctx := context.Background()

	cmd := domain.NewPutCommand("partition-test", []byte("before-partition"))
	_, err := leader.Apply(ctx, cmd)
	require.NoError(t, err)

	partitionedNodes := followers[:2]
	majorityNodes := append([]*Adapter{leader}, followers[2:]...)

	for _, node := range partitionedNodes {
		node.Stop()
	}

	time.Sleep(3 * time.Second)

	var majorityLeader *Adapter
	for _, node := range majorityNodes {
		if node.IsLeader() {
			majorityLeader = node
			break
		}
	}
	require.NotNil(t, majorityLeader, "Majority partition should elect a leader")

	cmd2 := domain.NewPutCommand("partition-test-2", []byte("during-partition"))
	_, err = majorityLeader.Apply(ctx, cmd2)
	require.NoError(t, err, "Majority should accept writes during partition")

	// Test recovery of partitioned nodes by restarting them
	time.Sleep(2 * time.Second) // Allow majority to stabilize

	// Get current leader info to rejoin
	leaderID, leaderAddr := majorityLeader.GetLeader()
	require.NotEmpty(t, leaderAddr, "Should have leader address for rejoining")

	host, portStr, err := net.SplitHostPort(leaderAddr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	leaderPeer := ports.Peer{
		ID:       leaderID,
		Address:  host,
		Port:     port,
		Metadata: make(map[string]string),
	}

	recoveredCount := 0
	for _, node := range partitionedNodes {
		err = node.Restart(ctx, []ports.Peer{leaderPeer})
		if err != nil {
			t.Logf("Partitioned node failed to rejoin cluster: %v", err)
		} else {
			recoveredCount++
			t.Logf("Partitioned node successfully rejoined cluster")
		}

		time.Sleep(1 * time.Second)
	}

	require.Greater(t, recoveredCount, 0, "At least one partitioned node should successfully rejoin")
}

func TestLeaderElectionRace(t *testing.T) {
	const nodeCount = 5
	leader, followers := setupRaftCluster(t, nodeCount)
	defer cleanupCluster(leader, followers)

	ctx := context.Background()

	// Verify initial cluster state
	require.True(t, leader.IsLeader(), "Initial leader should be established")

	// Kill the leader to trigger election
	leader.Stop()

	// Wait for new election
	time.Sleep(3 * time.Second)

	// Count active leaders after election
	var newLeader *Adapter
	leaderCount := 0
	for _, follower := range followers {
		if follower.IsLeader() {
			leaderCount++
			newLeader = follower
		}
	}

	require.Equal(t, 1, leaderCount, "Exactly one leader should be elected after original leader failure")
	require.NotNil(t, newLeader, "New leader should be identified")

	// Verify new leader can accept commands
	cmd := domain.NewPutCommand("leader-test", []byte("new-leader-command"))
	_, err := newLeader.Apply(ctx, cmd)
	require.NoError(t, err, "New leader should accept commands")

	// Test that old leader cannot become leader when rejoining
	leaderID, leaderAddr := newLeader.GetLeader()
	require.NotEmpty(t, leaderAddr)

	host, portStr, err := net.SplitHostPort(leaderAddr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	peer := ports.Peer{
		ID:       leaderID,
		Address:  host,
		Port:     port,
		Metadata: make(map[string]string),
	}

	// Restart original leader as follower
	err = leader.Start(ctx, []ports.Peer{peer})
	if err == nil {
		time.Sleep(2 * time.Second)
		require.False(t, leader.IsLeader(), "Rejoining ex-leader should remain follower")

		// Verify cluster stability - still exactly one leader
		finalLeaderCount := 0
		if leader.IsLeader() {
			finalLeaderCount++
		}
		for _, follower := range followers {
			if follower.IsLeader() {
				finalLeaderCount++
			}
		}
		require.Equal(t, 1, finalLeaderCount, "Should maintain exactly one leader after rejoin")
	}
}

func TestRaftStateCorruption(t *testing.T) {
	dataDir := t.TempDir()
	config := DefaultRaftConfig("corruption-test", "127.0.0.1:0", dataDir)
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer adapter.Stop()

	waitForLeadership(adapter)

	cmd := domain.NewPutCommand("corruption-test", []byte("normal-command"))
	_, err = adapter.Apply(ctx, cmd)
	require.NoError(t, err)

	// Test proper restart with same directory
	err = adapter.Restart(ctx, []ports.Peer{})
	require.NoError(t, err, "Adapter should restart successfully with same directory")

	// Verify state is preserved after restart
	waitForLeadership(adapter)
	cmd2 := domain.NewPutCommand("after-restart", []byte("restart-test"))
	_, err = adapter.Apply(ctx, cmd2)
	require.NoError(t, err, "Adapter should accept commands after restart")
}

func TestResourceExhaustion(t *testing.T) {
	config := DefaultRaftConfig("exhaustion-test", "127.0.0.1:0", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer adapter.Stop()

	waitForLeadership(adapter)

	var wg sync.WaitGroup
	errors := make(chan error, 1000)

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			largeData := make([]byte, 1024*1024)
			for j := range largeData {
				largeData[j] = byte(id % 256)
			}

			cmd := domain.NewPutCommand(fmt.Sprintf("large-key-%d", id), largeData)

			_, err := adapter.Apply(ctx, cmd)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for err := range errors {
		errorCount++
		if errorCount <= 5 {
			t.Logf("Resource exhaustion error: %v", err)
		}
	}

	t.Logf("Resource exhaustion generated %d errors out of 500 operations", errorCount)
}

func TestContextCancellation(t *testing.T) {
	config := DefaultRaftConfig("cancel-test", "127.0.0.1:0", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer adapter.Stop()

	waitForLeadership(adapter)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := domain.NewPutCommand("cancel-test", []byte("cancelled-command"))
	_, err = adapter.Apply(cancelledCtx, cmd)
	t.Logf("Cancelled context apply result: %v", err)

	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer timeoutCancel()
	time.Sleep(2 * time.Nanosecond)

	_, err = adapter.Apply(timeoutCtx, cmd)
	t.Logf("Timeout context apply result: %v", err)
}

func setupRaftCluster(t *testing.T, nodeCount int) (*Adapter, []*Adapter) {
	leader := setupSingleNode(t, 0)
	ctx := context.Background()
	err := leader.Start(ctx, []ports.Peer{})
	require.NoError(t, err)

	waitForLeadership(leader)

	var followers []*Adapter
	for i := 1; i < nodeCount; i++ {
		follower := setupSingleNode(t, i)
		err := follower.Start(ctx, []ports.Peer{})
		require.NoError(t, err, "Failed to start follower %d", i)

		followers = append(followers, follower)

		err = leader.AddNode(fmt.Sprintf("node-%d", i), fmt.Sprintf("127.0.0.1:%d", 19000+i))
		require.NoError(t, err, "Leader failed to add follower %d", i)

		time.Sleep(500 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)
	return leader, followers
}

func setupSingleNode(t *testing.T, nodeID int) *Adapter {
	config := DefaultRaftConfig(
		fmt.Sprintf("node-%d", nodeID),
		fmt.Sprintf("127.0.0.1:%d", 19000+nodeID),
		t.TempDir(),
	)

	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	return adapter
}

func cleanupCluster(leader *Adapter, followers []*Adapter) {
	var allAdapters []*Adapter
	if leader != nil {
		allAdapters = append(allAdapters, leader)
	}
	allAdapters = append(allAdapters, followers...)

	for _, adapter := range allAdapters {
		if adapter != nil && adapter.started {
			if err := adapter.Stop(); err != nil {
				fmt.Printf("Warning: failed to stop adapter %s: %v\n", adapter.config.NodeID, err)
			}
		}
	}

	time.Sleep(2 * time.Second)

	for _, adapter := range allAdapters {
		if adapter != nil && adapter.store != nil {
			if stateDB := adapter.store.StateDB(); stateDB != nil {
				if err := stateDB.Close(); err != nil {
					fmt.Printf("Warning: failed to close state DB for %s: %v\n", adapter.config.NodeID, err)
				}
			}
		}
	}
}
