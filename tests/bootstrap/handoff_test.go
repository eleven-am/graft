package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/examples/bootstrap-handoff/config"
	"github.com/eleven-am/graft/examples/bootstrap-handoff/harness"
)

func TestHandoffBehaviorValidation(t *testing.T) {
	launcher := harness.NewNodeLauncher(nil)
	defer launcher.Cleanup()

	cfg := config.DefaultTestConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("ProvisionalLeaderAssertions", func(t *testing.T) {
		node1, err := launcher.LaunchNode(ctx, cfg.Nodes[0])
		if err != nil {
			t.Fatalf("failed to launch node: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[0].NodeID, 10*time.Second); err != nil {
			t.Fatalf("node failed to become ready: %v", err)
		}

		result := harness.AssertHandoffBehavior(ctx, node1, harness.HandoffAssertion{
			ExpectedState:       "ready",
			ExpectedProvisional: true,
		})

		if !result.Success {
			t.Errorf("provisional leader assertion failed: %s", result.Message)
		}

		if !result.ActualData["is_ready"].(bool) {
			t.Error("node should be ready")
		}

		if !result.ActualData["is_provisional"].(bool) {
			t.Error("node should be provisional")
		}

		launcher.StopAll()
	})

	t.Run("MetadataPropagation", func(t *testing.T) {
		instances, err := launcher.LaunchStaggered(ctx, cfg.Nodes[:2], 2*time.Second)
		if err != nil {
			t.Fatalf("failed to launch nodes: %v", err)
		}

		for _, instance := range instances {
			if err := launcher.WaitForReadiness(ctx, instance.Config.NodeID, 15*time.Second); err != nil {
				t.Fatalf("node %s failed to become ready: %v", instance.Config.NodeID, err)
			}
		}

		result := harness.AssertMetadataPropagation(ctx, instances)
		if !result.Success {
			t.Errorf("metadata propagation failed: %s", result.Message)
		}

		bootIDs := result.ActualData["boot_ids"].(map[string]string)
		if len(bootIDs) != 2 {
			t.Errorf("expected 2 boot IDs, got %d", len(bootIDs))
		}

		for nodeID, bootID := range bootIDs {
			if bootID == "" {
				t.Errorf("node %s has empty boot ID", nodeID)
			}
		}

		launcher.StopAll()
	})

	t.Run("DemotionSequence", func(t *testing.T) {
		node1, err := launcher.LaunchNode(ctx, cfg.Nodes[0])
		if err != nil {
			t.Fatalf("failed to launch provisional node: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[0].NodeID, 10*time.Second); err != nil {
			t.Fatalf("provisional node failed to become ready: %v", err)
		}

		time.Sleep(2 * time.Second)

		node2, err := launcher.LaunchNode(ctx, cfg.Nodes[1])
		if err != nil {
			t.Fatalf("failed to launch senior node: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[1].NodeID, 10*time.Second); err != nil {
			t.Fatalf("senior node failed to become ready: %v", err)
		}

		result := harness.AssertDemotionSequence(ctx, node1, node2, 30*time.Second)
		if !result.Success {
			t.Errorf("demotion sequence failed: %s", result.Message)
		}

		duration, ok := result.ActualData["duration_ms"].(int64)
		if !ok || duration <= 0 {
			t.Errorf("invalid demotion duration: %v", duration)
		}

		if duration > 30000 {
			t.Errorf("demotion took too long: %dms", duration)
		}

		finalPeerCount, ok := result.ActualData["final_peer_count"].(int)
		if !ok || finalPeerCount != 1 {
			t.Errorf("expected 1 peer after demotion, got %v", finalPeerCount)
		}

		launcher.StopAll()
	})
}
