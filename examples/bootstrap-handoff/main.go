package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bootstrap-handoff-test/harness"
	"bootstrap-handoff-test/scenarios"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	fmt.Println("🚀 Bootstrap Handoff Validation Suite")
	fmt.Println("====================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logger.Info("shutdown signal received")
		cancel()
	}()

	launcher := harness.NewNodeLauncher(logger)
	defer launcher.Cleanup()

	testScenarios := []struct {
		name string
		run  func(context.Context, *harness.NodeLauncher) error
	}{
		{"Basic Handoff", scenarios.RunBasicHandoff},
		{"Staggered Launch", scenarios.RunStaggeredLaunch},
		{"Workflow Continuity", scenarios.RunWorkflowContinuity},
	}

	for i, scenario := range testScenarios {
		fmt.Printf("\n🔄 [%d/%d] Running: %s\n", i+1, len(testScenarios), scenario.name)

		scenarioCtx, scenarioCancel := context.WithTimeout(ctx, 2*time.Minute)

		if err := scenario.run(scenarioCtx, launcher); err != nil {
			log.Printf("❌ Scenario '%s' failed: %v", scenario.name, err)
			scenarioCancel()
			continue
		}

		fmt.Printf("✅ Scenario '%s' completed successfully\n", scenario.name)
		scenarioCancel()
		launcher.StopAll()
		time.Sleep(1 * time.Second)
	}

	fmt.Println("\n🎉 Bootstrap Handoff Validation Complete!")
}
