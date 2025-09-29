package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/examples/bootstrap-handoff/config"
	"github.com/eleven-am/graft/examples/bootstrap-handoff/harness"
)

func TestReadinessEndpoints(t *testing.T) {
	launcher := harness.NewNodeLauncher(nil)
	defer launcher.Cleanup()

	cfg := config.DefaultTestConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("WaitUntilReadyTimeout", func(t *testing.T) {
		node, err := launcher.LaunchNode(ctx, cfg.Nodes[0])
		if err != nil {
			t.Fatalf("failed to launch node: %v", err)
		}

		start := time.Now()
		err = launcher.WaitForReadiness(ctx, cfg.Nodes[0].NodeID, 10*time.Second)
		duration := time.Since(start)

		if err != nil {
			t.Errorf("WaitUntilReady failed: %v", err)
		}

		if duration > 10*time.Second {
			t.Errorf("WaitUntilReady took too long: %v", duration)
		}

		if !node.Manager.IsReady() {
			t.Error("node should be ready after WaitUntilReady succeeds")
		}

		launcher.StopAll()
	})

	t.Run("ReadinessStateTransitions", func(t *testing.T) {
		node1, err := launcher.LaunchNode(ctx, cfg.Nodes[0])
		if err != nil {
			t.Fatalf("failed to launch provisional node: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[0].NodeID, 10*time.Second); err != nil {
			t.Fatalf("provisional node failed to become ready: %v", err)
		}

		initialState := node1.Manager.GetReadinessState()
		if initialState != "ready" {
			t.Errorf("expected initial state 'ready', got '%s'", initialState)
		}

		time.Sleep(2 * time.Second)

		node2, err := launcher.LaunchNode(ctx, cfg.Nodes[1])
		if err != nil {
			t.Fatalf("failed to launch senior node: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[1].NodeID, 10*time.Second); err != nil {
			t.Fatalf("senior node failed to become ready: %v", err)
		}

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		stateChanges := make(map[string]bool)
		timeout := time.After(30 * time.Second)

		for {
			select {
			case <-timeout:
				t.Fatal("timeout waiting for state transitions")
			case <-ticker.C:
				currentState := node1.Manager.GetReadinessState()
				stateChanges[currentState] = true

				if currentState == "ready" && len(stateChanges) > 1 {
					finalClusterInfo := node1.Manager.GetClusterInfo()
					if len(finalClusterInfo.Peers) > 0 {
						goto stateValidation
					}
				}
			}
		}

	stateValidation:
		expectedStates := []string{"ready", "detecting"}
		for _, expectedState := range expectedStates {
			if !stateChanges[expectedState] {
				t.Errorf("expected to see state '%s' during transition", expectedState)
			}
		}

		launcher.StopAll()
	})

	t.Run("MultipleNodesReadiness", func(t *testing.T) {
		instances, err := launcher.LaunchStaggered(ctx, cfg.Nodes, 1*time.Second)
		if err != nil {
			t.Fatalf("failed to launch nodes: %v", err)
		}

		readinessTimes := make(map[string]time.Duration)

		for _, instance := range instances {
			start := time.Now()
			if err := launcher.WaitForReadiness(ctx, instance.Config.NodeID, 20*time.Second); err != nil {
				t.Errorf("node %s failed to become ready: %v", instance.Config.NodeID, err)
				continue
			}
			readinessTimes[instance.Config.NodeID] = time.Since(start)

			if !instance.Manager.IsReady() {
				t.Errorf("node %s reports not ready after WaitUntilReady", instance.Config.NodeID)
			}
		}

		for nodeID, readinessTime := range readinessTimes {
			t.Logf("Node %s became ready in %v", nodeID, readinessTime)
			if readinessTime > 20*time.Second {
				t.Errorf("node %s took too long to become ready: %v", nodeID, readinessTime)
			}
		}

		formationResult := harness.WaitForClusterFormation(ctx, instances, len(instances)-1, 30*time.Second)
		if !formationResult.Success {
			t.Errorf("cluster formation failed: %s", formationResult.Message)
		}

		launcher.StopAll()
	})

	t.Run("ReadinessUnderLoad", func(t *testing.T) {
		node, err := launcher.LaunchNode(ctx, cfg.Nodes[0])
		if err != nil {
			t.Fatalf("failed to launch node: %v", err)
		}

		concurrentWaiters := 10
		results := make(chan error, concurrentWaiters)

		for i := 0; i < concurrentWaiters; i++ {
			go func() {
				err := launcher.WaitForReadiness(ctx, cfg.Nodes[0].NodeID, 15*time.Second)
				results <- err
			}()
		}

		for i := 0; i < concurrentWaiters; i++ {
			select {
			case err := <-results:
				if err != nil {
					t.Errorf("concurrent WaitUntilReady failed: %v", err)
				}
			case <-time.After(20 * time.Second):
				t.Error("timeout waiting for concurrent readiness checks")
			}
		}

		launcher.StopAll()
	})
}
