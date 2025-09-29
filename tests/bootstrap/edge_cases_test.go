package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/examples/bootstrap-handoff/config"
	"github.com/eleven-am/graft/examples/bootstrap-handoff/harness"
)

func TestEdgeCasesAndFailureScenarios(t *testing.T) {
	launcher := harness.NewNodeLauncher(nil)
	defer launcher.Cleanup()

	cfg := config.DefaultTestConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Run("RapidNodeLaunches", func(t *testing.T) {
		nodeConfigs := cfg.Nodes[:2]

		instances, err := launcher.LaunchStaggered(ctx, nodeConfigs, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("rapid launch failed: %v", err)
		}

		for _, instance := range instances {
			if err := launcher.WaitForReadiness(ctx, instance.Config.NodeID, 15*time.Second); err != nil {
				t.Errorf("node %s failed to become ready: %v", instance.Config.NodeID, err)
			}
		}

		formationResult := harness.WaitForClusterFormation(ctx, instances, 1, 20*time.Second)
		if !formationResult.Success {
			t.Errorf("rapid launch cluster formation failed: %s", formationResult.Message)
		}

		metadataResult := harness.AssertMetadataPropagation(ctx, instances)
		if !metadataResult.Success {
			t.Errorf("rapid launch metadata propagation failed: %s", metadataResult.Message)
		}

		launcher.StopAll()
	})

	t.Run("NodeStartupTimeout", func(t *testing.T) {
		node, err := launcher.LaunchNode(ctx, cfg.Nodes[0])
		if err != nil {
			t.Fatalf("failed to launch node: %v", err)
		}

		shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		err = launcher.WaitForReadiness(shortCtx, cfg.Nodes[0].NodeID, 100*time.Millisecond)
		if err == nil {
			t.Error("expected timeout error for very short readiness wait")
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[0].NodeID, 15*time.Second); err != nil {
			t.Errorf("node should still become ready with longer timeout: %v", err)
		}

		launcher.StopAll()
	})

	t.Run("SimultaneousProvisionalNodes", func(t *testing.T) {
		node1, err := launcher.LaunchNode(ctx, cfg.Nodes[0])
		if err != nil {
			t.Fatalf("failed to launch first node: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[0].NodeID, 10*time.Second); err != nil {
			t.Fatalf("first node failed to become ready: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		node2, err := launcher.LaunchNode(ctx, cfg.Nodes[1])
		if err != nil {
			t.Fatalf("failed to launch second node: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[1].NodeID, 10*time.Second); err != nil {
			t.Fatalf("second node failed to become ready: %v", err)
		}

		time.Sleep(5 * time.Second)

		leaderCount := 0
		for _, node := range []*harness.NodeInstance{node1, node2} {
			clusterInfo := node.Manager.GetClusterInfo()
			if clusterInfo.IsLeader {
				leaderCount++
			}
		}

		if leaderCount != 1 {
			t.Errorf("expected exactly 1 leader, found %d", leaderCount)
		}

		formationResult := harness.WaitForClusterFormation(ctx, []*harness.NodeInstance{node1, node2}, 1, 20*time.Second)
		if !formationResult.Success {
			t.Errorf("cluster formation failed with simultaneous provisionals: %s", formationResult.Message)
		}

		launcher.StopAll()
	})

	t.Run("HandoffWithWorkflowLoad", func(t *testing.T) {
		node1, err := launcher.LaunchNode(ctx, cfg.Nodes[0])
		if err != nil {
			t.Fatalf("failed to launch provisional node: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[0].NodeID, 10*time.Second); err != nil {
			t.Fatalf("provisional node failed to become ready: %v", err)
		}

		monitor := harness.NewWorkflowMonitor()
		if err := monitor.AttachToNode(node1); err != nil {
			t.Fatalf("failed to attach monitor: %v", err)
		}

		workflowCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go harness.StartContinuousWorkflows(workflowCtx, node1, "load-test", 500*time.Millisecond, monitor)

		time.Sleep(3 * time.Second)

		preHandoffEvents := len(monitor.GetEvents())
		if preHandoffEvents == 0 {
			t.Fatal("no workflows started before handoff")
		}

		node2, err := launcher.LaunchNode(ctx, cfg.Nodes[1])
		if err != nil {
			t.Fatalf("failed to launch senior node: %v", err)
		}

		if err := monitor.AttachToNode(node2); err != nil {
			t.Fatalf("failed to attach monitor to node2: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, cfg.Nodes[1].NodeID, 10*time.Second); err != nil {
			t.Fatalf("senior node failed to become ready: %v", err)
		}

		demotionResult := harness.AssertDemotionSequence(ctx, node1, node2, 30*time.Second)
		if !demotionResult.Success {
			t.Errorf("demotion under load failed: %s", demotionResult.Message)
		}

		time.Sleep(3 * time.Second)

		totalEvents := len(monitor.GetEvents())
		postHandoffEvents := totalEvents - preHandoffEvents

		if postHandoffEvents == 0 {
			t.Error("no workflows processed after handoff under load")
		}

		t.Logf("Workflows under load: pre=%d, post=%d, total=%d",
			preHandoffEvents, postHandoffEvents, totalEvents)

		launcher.StopAll()
	})

	t.Run("RepeatedHandoffs", func(t *testing.T) {
		nodeConfigs := cfg.Nodes[:3]

		node1, err := launcher.LaunchNode(ctx, nodeConfigs[0])
		if err != nil {
			t.Fatalf("failed to launch first node: %v", err)
		}

		if err := launcher.WaitForReadiness(ctx, nodeConfigs[0].NodeID, 10*time.Second); err != nil {
			t.Fatalf("first node failed to become ready: %v", err)
		}

		for i := 1; i < len(nodeConfigs); i++ {
			time.Sleep(2 * time.Second)

			nextNode, err := launcher.LaunchNode(ctx, nodeConfigs[i])
			if err != nil {
				t.Fatalf("failed to launch node %d: %v", i+1, err)
			}

			if err := launcher.WaitForReadiness(ctx, nodeConfigs[i].NodeID, 15*time.Second); err != nil {
				t.Fatalf("node %d failed to become ready: %v", i+1, err)
			}

			instances := launcher.GetAllInstances()
			formationResult := harness.WaitForClusterFormation(ctx, instances, i, 20*time.Second)
			if !formationResult.Success {
				t.Errorf("cluster formation failed after adding node %d: %s", i+1, formationResult.Message)
			}
		}

		allInstances := launcher.GetAllInstances()
		if len(allInstances) != 3 {
			t.Errorf("expected 3 instances, got %d", len(allInstances))
		}

		leaderCount := 0
		for _, instance := range allInstances {
			clusterInfo := instance.Manager.GetClusterInfo()
			if clusterInfo.IsLeader {
				leaderCount++
			}
			if len(clusterInfo.Peers) != 2 {
				t.Errorf("node %s should have 2 peers, has %d",
					instance.Config.NodeID, len(clusterInfo.Peers))
			}
		}

		if leaderCount != 1 {
			t.Errorf("expected exactly 1 leader after multiple handoffs, found %d", leaderCount)
		}

		launcher.StopAll()
	})
}
