package raft

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestRaftBootstrapFailureVulnerability(t *testing.T) {
	config := DefaultRaftConfig("bootstrap-fail", "127.0.0.1:8001", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})

	t.Logf("Bootstrap result: %v", err)

	if err == nil {
		t.Log("EXPECTED: Single node bootstrap succeeded - this is legitimate functionality")
		adapter.Stop()

		err = adapter.Start(ctx, []ports.Peer{})
		if err == nil {
			t.Log("EXPECTED: Restart from existing state succeeded - this is secure behavior (ErrCantBootstrap was handled)")
			adapter.Stop()
		} else {
			t.Errorf("UNEXPECTED FAILURE: Restart from existing state should succeed: %v", err)
		}
	} else {
		t.Errorf("UNEXPECTED FAILURE: Bootstrap should succeed for single node: %v", err)
	}
}

func TestUninitializedRaftOperations(t *testing.T) {
	config := DefaultRaftConfig("uninit-test", "127.0.0.1:8002", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()

	cmd := domain.NewPutCommand("test", []byte("data"))

	_, err = adapter.Apply(ctx, cmd)
	if err == nil {
		t.Error("VULNERABILITY: Uninitialized Raft adapter accepted commands - missing state validation")
	} else {
		t.Logf("EXPECTED: Uninitialized adapter rejected command: %v", err)
	}

	if adapter.IsLeader() {
		t.Error("VULNERABILITY: Uninitialized adapter claims to be leader")
	}

	nodeID, addr := adapter.GetLeader()
	if nodeID != "" || addr != "" {
		t.Errorf("VULNERABILITY: Uninitialized adapter returns leader info: %s@%s", nodeID, addr)
	}
}

func TestConcurrentStartStopRace(t *testing.T) {
	config := DefaultRaftConfig("race-test", "127.0.0.1:8003", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()

	var wg sync.WaitGroup
	startErrors := make(chan error, 10)
	stopErrors := make(chan error, 10)

	for i := 0; i < 5; i++ {
		wg.Add(2)

		go func() {
			defer wg.Done()
			err := adapter.Start(ctx, []ports.Peer{})
			startErrors <- err
		}()

		go func() {
			defer wg.Done()
			err := adapter.Stop()
			stopErrors <- err
		}()
	}

	wg.Wait()
	close(startErrors)
	close(stopErrors)

	startCount := 0
	for err := range startErrors {
		if err != nil {
			startCount++
			t.Logf("Start error: %v", err)
		}
	}

	stopCount := 0
	for err := range stopErrors {
		if err != nil {
			stopCount++
			t.Logf("Stop error: %v", err)
		}
	}

	t.Logf("Concurrent start/stop race generated %d start errors, %d stop errors", startCount, stopCount)

	// Proper concurrent operations should be handled gracefully
	// We expect some errors due to state transitions, but not crashes
	require.True(t, startCount > 0 || stopCount > 0, "Concurrent operations should have proper state validation")

	// Most importantly, verify the adapter is in a consistent state
	if adapter.started {
		// If adapter ended up started, it should be functional
		ctx := context.Background()
		cmd := domain.NewPutCommand("race-test", []byte("consistency-check"))
		_, err := adapter.Apply(ctx, cmd)
		require.Error(t, err, "Started adapter without leadership should reject commands")
	}
}

func TestMaliciousCommandInjection(t *testing.T) {
	t.Log("Testing command injection vulnerabilities...")

	maliciousCommands := []struct {
		name string
		cmd  *domain.Command
	}{
		{"nil-command", nil},
		{"invalid-type", &domain.Command{Type: domain.CommandType(255)}},
		{"huge-key", &domain.Command{Type: domain.CommandPut, Key: string(make([]byte, 10*1024*1024)), Value: []byte("small")}},
		{"huge-value", &domain.Command{Type: domain.CommandPut, Key: "small", Value: make([]byte, 100*1024*1024)}},
		{"empty-everything", &domain.Command{}},
		{"negative-timestamp", &domain.Command{Type: domain.CommandPut, Timestamp: time.Unix(-1000000, 0)}},
	}

	for _, tc := range maliciousCommands {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultRaftConfig(fmt.Sprintf("malicious-%s", tc.name), "127.0.0.1:0", t.TempDir())
			adapter, err := NewAdapter(config, slog.Default())
			if err != nil {
				t.Logf("Adapter creation failed: %v", err)
				return
			}

			ctx := context.Background()

			defer func() {
				if r := recover(); r != nil {
					t.Errorf("VULNERABILITY: Panic on malicious command %s: %v", tc.name, r)
				}
			}()

			_, maliciousErr := adapter.Apply(ctx, tc.cmd)
			require.Error(t, maliciousErr, "Malicious command should be rejected: %s", tc.name)
		})
	}
}

func TestMemoryExhaustionAttack(t *testing.T) {
	config := DefaultRaftConfig("memory-attack", "127.0.0.1:8004", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()
	err = adapter.Start(ctx, []ports.Peer{})
	require.NoError(t, err)
	defer func() {
		if adapter != nil && adapter.started {
			adapter.Stop()
			time.Sleep(500 * time.Millisecond)
		}
	}()

	time.Sleep(2 * time.Second)

	for i := 0; i < 50; i++ {
		if adapter.IsLeader() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 1000)

	t.Log("Launching memory exhaustion attack...")

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				massiveData := make([]byte, 200*1024*1024) // 200MB - exceeds our 100MB limit
				cmd := domain.NewPutCommand(fmt.Sprintf("attack-%d-%d", id, j), massiveData)

				_, err := adapter.Apply(ctx, cmd)
				if err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for range errors {
		errorCount++
	}

	require.Greater(t, errorCount, 900, "Most oversized operations should be rejected due to memory protection (got %d/%d)", errorCount, 1000)
}

func TestStateConsistencyBreak(t *testing.T) {
	config := DefaultRaftConfig("consistency-test", "127.0.0.1:8005", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	ctx := context.Background()

	var wg sync.WaitGroup
	results := make(chan bool, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			cmd := domain.NewPutCommand("shared-key", []byte(fmt.Sprintf("value-%d", id)))
			_, err := adapter.Apply(ctx, cmd)
			results <- (err == nil)
		}(i)
	}

	wg.Wait()
	close(results)

	successCount := 0
	for success := range results {
		if success {
			successCount++
		}
	}

	require.Equal(t, 0, successCount, "All operations should be rejected on uninitialized Raft (got %d/%d successful)", successCount, 50)
}

func TestClusterMembershipTampering(t *testing.T) {
	config := DefaultRaftConfig("membership-test", "127.0.0.1:8006", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	maliciousJoins := []struct {
		nodeID  string
		address string
	}{
		{"", "127.0.0.1:8000"},
		{"malicious-node", ""},
		{"malicious-node", "invalid-address"},
		{"malicious-node", "127.0.0.1:99999"},
		{string(make([]byte, 10000)), "127.0.0.1:8000"},
	}

	for i, join := range maliciousJoins {
		t.Run(fmt.Sprintf("malicious-join-%d", i), func(t *testing.T) {
			peer := ports.Peer{
				ID:       join.nodeID,
				Address:  join.address,
				Port:     8000,
				Metadata: make(map[string]string),
			}
			err := adapter.Join([]ports.Peer{peer})
			require.Error(t, err, "Malicious join should be rejected (nodeID='%s', addr='%s')",
				join.nodeID[:min(len(join.nodeID), 50)], join.address)
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestDoubleStopPanic(t *testing.T) {
	config := DefaultRaftConfig("double-stop", "127.0.0.1:8007", t.TempDir())
	adapter, err := NewAdapter(config, slog.Default())
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("VULNERABILITY: Double stop caused panic: %v", r)
		}
	}()

	err1 := adapter.Stop()
	require.Error(t, err1, "First stop should fail on unstarted adapter")

	err2 := adapter.Stop()
	require.Error(t, err2, "Second stop should also fail")

	// Both stops should fail because adapter was never started
}
