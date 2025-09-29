package scenarios

import (
	"context"
	"fmt"
	"time"

	"bootstrap-handoff-test/config"
	"bootstrap-handoff-test/harness"
)

func RunBasicHandoff(ctx context.Context, launcher *harness.NodeLauncher) error {
	cfg := config.DefaultTestConfig()
	metrics := harness.NewMetricsCollector()

	fmt.Println("  📋 Starting basic 2-node handoff test")

	node1Config := cfg.Nodes[0]
	node2Config := cfg.Nodes[1]

	fmt.Printf("  🚀 Launching provisional leader: %s\n", node1Config.NodeID)
	node1, err := launcher.LaunchNode(ctx, node1Config)
	if err != nil {
		return fmt.Errorf("failed to launch node1: %w", err)
	}

	if err := launcher.WaitForReadiness(ctx, node1Config.NodeID, cfg.ReadinessTimeout); err != nil {
		return fmt.Errorf("node1 failed to become ready: %w", err)
	}

	result := harness.AssertHandoffBehavior(ctx, node1, harness.HandoffAssertion{
		ExpectedState:       "ready",
		ExpectedProvisional: true,
	})
	if !result.Success {
		return fmt.Errorf("node1 provisional assertion failed: %s", result.Message)
	}

	fmt.Printf("  ✅ Node1 ready as provisional leader (state: %s)\n",
		node1.Manager.GetReadinessState())

	time.Sleep(2 * time.Second)

	fmt.Printf("  🚀 Launching senior node: %s\n", node2Config.NodeID)
	metrics.StartHandoffTracking(node1Config.NodeID, node2Config.NodeID)

	node2, err := launcher.LaunchNode(ctx, node2Config)
	if err != nil {
		return fmt.Errorf("failed to launch node2: %w", err)
	}

	if err := launcher.WaitForReadiness(ctx, node2Config.NodeID, cfg.ReadinessTimeout); err != nil {
		return fmt.Errorf("node2 failed to become ready: %w", err)
	}

	fmt.Printf("  ✅ Node2 ready as senior node (state: %s)\n",
		node2.Manager.GetReadinessState())

	fmt.Println("  🔄 Waiting for handoff to complete...")

	demotionResult := harness.AssertDemotionSequence(ctx, node1, node2, cfg.HandoffTimeout)
	if !demotionResult.Success {
		return fmt.Errorf("demotion sequence failed: %s", demotionResult.Message)
	}

	metrics.CompleteHandoffTracking()

	fmt.Printf("  ✅ Handoff completed in %vms\n",
		demotionResult.ActualData["duration_ms"])

	formationResult := harness.WaitForClusterFormation(ctx, []*harness.NodeInstance{node1, node2}, 1, 10*time.Second)
	if !formationResult.Success {
		return fmt.Errorf("cluster formation failed: %s", formationResult.Message)
	}

	metadataResult := harness.AssertMetadataPropagation(ctx, []*harness.NodeInstance{node1, node2})
	if !metadataResult.Success {
		return fmt.Errorf("metadata propagation failed: %s", metadataResult.Message)
	}

	fmt.Println("  ✅ Basic handoff scenario completed successfully")

	report := metrics.GenerateReport()
	if handoffData, ok := report["handoff"].(map[string]interface{}); ok {
		fmt.Printf("  📊 Metrics: duration=%vms, transitions=%v\n",
			handoffData["duration_ms"], handoffData["state_transitions"])
	}

	return nil
}
