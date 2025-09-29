package scenarios

import (
	"context"
	"fmt"
	"time"

	"bootstrap-handoff-test/config"
	"bootstrap-handoff-test/harness"
)

func RunStaggeredLaunch(ctx context.Context, launcher *harness.NodeLauncher) error {
	cfg := config.DefaultTestConfig()
	metrics := harness.NewMetricsCollector()

	fmt.Println("  📋 Starting 3-node staggered launch test")

	nodeConfigs := cfg.Nodes

	fmt.Printf("  🚀 Launching %d nodes with %v stagger delay\n", len(nodeConfigs), cfg.StaggerDelay)

	startTime := time.Now()
	instances, err := launcher.LaunchStaggered(ctx, nodeConfigs, cfg.StaggerDelay)
	if err != nil {
		return fmt.Errorf("staggered launch failed: %w", err)
	}

	launchDuration := time.Since(startTime)
	fmt.Printf("  ⏱️  All nodes launched in %v\n", launchDuration)

	fmt.Println("  🔍 Waiting for all nodes to achieve readiness...")

	for _, instance := range instances {
		if err := launcher.WaitForReadiness(ctx, instance.Config.NodeID, cfg.ReadinessTimeout); err != nil {
			return fmt.Errorf("node %s failed to become ready: %w", instance.Config.NodeID, err)
		}
		fmt.Printf("  ✅ %s ready (state: %s)\n",
			instance.Config.NodeID, instance.Manager.GetReadinessState())
	}

	fmt.Println("  🔄 Analyzing handoff patterns...")

	expectedPeers := len(instances) - 1
	formationResult := harness.WaitForClusterFormation(ctx, instances, expectedPeers, 30*time.Second)
	if !formationResult.Success {
		return fmt.Errorf("cluster formation failed: %s", formationResult.Message)
	}

	formationTime := formationResult.ActualData["formation_time_ms"]
	fmt.Printf("  ✅ Cluster formation completed in %vms\n", formationTime)

	metadataResult := harness.AssertMetadataPropagation(ctx, instances)
	if !metadataResult.Success {
		return fmt.Errorf("metadata propagation failed: %s", metadataResult.Message)
	}

	fmt.Println("  ✅ Metadata propagation verified across all nodes")

	leaderFound := false
	followerCount := 0

	for _, instance := range instances {
		clusterInfo := instance.Manager.GetClusterInfo()
		if clusterInfo.IsLeader {
			if leaderFound {
				return fmt.Errorf("multiple leaders detected")
			}
			leaderFound = true
			fmt.Printf("  👑 Leader: %s (peers: %d)\n", instance.Config.NodeID, len(clusterInfo.Peers))
		} else {
			followerCount++
		}
	}

	if !leaderFound {
		return fmt.Errorf("no leader found in cluster")
	}

	expectedFollowers := len(instances) - 1
	if followerCount != expectedFollowers {
		return fmt.Errorf("expected %d followers, found %d", expectedFollowers, followerCount)
	}

	fmt.Printf("  ✅ Cluster topology validated: 1 leader, %d followers\n", followerCount)

	performanceMetrics := harness.PerformanceMetrics{
		ClusterFormationTime: time.Duration(formationTime.(int64)) * time.Millisecond,
		HandoffLatency:       launchDuration,
	}
	metrics.SetPerformanceMetrics(performanceMetrics)

	fmt.Println("  ✅ Staggered launch scenario completed successfully")

	report := metrics.GenerateReport()
	if perfData, ok := report["performance"].(map[string]interface{}); ok {
		fmt.Printf("  📊 Performance: formation=%vms, launch_latency=%vms\n",
			perfData["cluster_formation_ms"], perfData["handoff_latency_ms"])
	}

	return nil
}
