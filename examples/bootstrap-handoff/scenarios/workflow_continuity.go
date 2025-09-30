package scenarios

import (
	"context"
	"fmt"
	"time"

	"bootstrap-handoff-test/config"
	"bootstrap-handoff-test/harness"
)

func RunWorkflowContinuity(ctx context.Context, launcher *harness.NodeLauncher) error {
	cfg := config.DefaultTestConfig()
	metrics := harness.NewMetricsCollector()
	monitor := harness.NewWorkflowMonitor()

	fmt.Println("  📋 Starting workflow continuity during handoff test")

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

	if err := monitor.AttachToNode(node1); err != nil {
		return fmt.Errorf("failed to attach monitor to node1: %w", err)
	}

	fmt.Println("  🔄 Starting continuous workflow processing...")

	workflowCtx, cancelWorkflows := context.WithCancel(ctx)
	defer cancelWorkflows()

	go harness.StartContinuousWorkflows(workflowCtx, launcher, node1, "continuity-test", 2*time.Second, monitor)

	time.Sleep(5 * time.Second)

	preHandoffEvents := len(monitor.GetEvents())
	fmt.Printf("  📊 Pre-handoff: %d workflow events recorded\n", preHandoffEvents)

	if preHandoffEvents == 0 {
		return fmt.Errorf("no workflows started before handoff")
	}

	fmt.Printf("  🚀 Launching senior node: %s (handoff should begin)\n", node2Config.NodeID)
	handoffStartTime := time.Now()
	metrics.StartHandoffTracking(node1Config.NodeID, node2Config.NodeID)

	node2, err := launcher.LaunchNode(ctx, node2Config)
	if err != nil {
		return fmt.Errorf("failed to launch node2: %w", err)
	}

	if err := monitor.AttachToNode(node2); err != nil {
		return fmt.Errorf("failed to attach monitor to node2: %w", err)
	}

	if err := launcher.WaitForReadiness(ctx, node2Config.NodeID, cfg.ReadinessTimeout); err != nil {
		return fmt.Errorf("node2 failed to become ready: %w", err)
	}

	fmt.Println("  🔄 Waiting for handoff to complete...")

	demotionResult := harness.AssertDemotionSequence(ctx, node1, node2, cfg.HandoffTimeout)
	if !demotionResult.Success {
		return fmt.Errorf("demotion sequence failed: %s", demotionResult.Message)
	}

	handoffEndTime := time.Now()
	metrics.CompleteHandoffTracking()

	fmt.Printf("  ✅ Handoff completed in %vms\n",
		demotionResult.ActualData["duration_ms"])

	time.Sleep(5 * time.Second)

	totalEvents := len(monitor.GetEvents())
	postHandoffEvents := totalEvents - preHandoffEvents

	fmt.Printf("  📊 Post-handoff: %d additional workflow events recorded\n", postHandoffEvents)
	fmt.Printf("  📊 Total events: %d\n", totalEvents)

	if postHandoffEvents == 0 {
		return fmt.Errorf("no workflows processed after handoff - continuity broken")
	}

	testWorkflowID := "continuity-validation-test"
	fmt.Printf("  🧪 Starting post-handoff test workflow: %s\n", testWorkflowID)

	if err := harness.StartTestWorkflow(ctx, launcher, node1, testWorkflowID); err != nil {
		return fmt.Errorf("failed to start test workflow: %w", err)
	}

	time.Sleep(3 * time.Second)

	testWorkflowEvents := monitor.GetEventsForWorkflow(testWorkflowID)
	if len(testWorkflowEvents) == 0 {
		return fmt.Errorf("test workflow did not produce any events")
	}

	fmt.Printf("  ✅ Post-handoff workflow completed successfully (%d events)\n", len(testWorkflowEvents))

	eventsBeforeHandoff := 0
	eventsDuringHandoff := 0
	eventsAfterHandoff := 0

	for _, event := range monitor.GetEvents() {
		if event.Timestamp.Before(handoffStartTime) {
			eventsBeforeHandoff++
		} else if event.Timestamp.After(handoffEndTime) {
			eventsAfterHandoff++
		} else {
			eventsDuringHandoff++
		}
	}

	fmt.Printf("  📊 Event distribution:\n")
	fmt.Printf("    - Before handoff: %d events\n", eventsBeforeHandoff)
	fmt.Printf("    - During handoff: %d events\n", eventsDuringHandoff)
	fmt.Printf("    - After handoff: %d events\n", eventsAfterHandoff)

	if eventsBeforeHandoff == 0 || eventsAfterHandoff == 0 {
		return fmt.Errorf("workflow continuity broken: before=%d, after=%d",
			eventsBeforeHandoff, eventsAfterHandoff)
	}

	throughputBefore := metrics.CalculateWorkflowThroughput(eventsBeforeHandoff, handoffStartTime.Sub(time.Now().Add(-10*time.Second)))
	throughputAfter := metrics.CalculateWorkflowThroughput(eventsAfterHandoff, time.Since(handoffEndTime))

	performanceMetrics := harness.PerformanceMetrics{
		WorkflowThroughput: (throughputBefore + throughputAfter) / 2,
		HandoffLatency:     handoffEndTime.Sub(handoffStartTime),
	}
	metrics.SetPerformanceMetrics(performanceMetrics)

	fmt.Println("  ✅ Workflow continuity scenario completed successfully")

	report := metrics.GenerateReport()
	if perfData, ok := report["performance"].(map[string]interface{}); ok {
		fmt.Printf("  📊 Performance: throughput=%.2f workflows/sec, handoff_latency=%vms\n",
			perfData["workflow_throughput"], perfData["handoff_latency_ms"])
	}

	return nil
}
