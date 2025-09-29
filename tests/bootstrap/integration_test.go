package bootstrap

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/examples/bootstrap-handoff/config"
	"github.com/eleven-am/graft/examples/bootstrap-handoff/harness"
	"github.com/eleven-am/graft/examples/bootstrap-handoff/scenarios"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestBootstrapHandoffIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	launcher := harness.NewNodeLauncher(nil)
	defer launcher.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Run("BasicHandoff", func(t *testing.T) {
		if err := scenarios.RunBasicHandoff(ctx, launcher); err != nil {
			t.Errorf("basic handoff failed: %v", err)
		}
		launcher.StopAll()
		time.Sleep(1 * time.Second)
	})

	t.Run("StaggeredLaunch", func(t *testing.T) {
		if err := scenarios.RunStaggeredLaunch(ctx, launcher); err != nil {
			t.Errorf("staggered launch failed: %v", err)
		}
		launcher.StopAll()
		time.Sleep(1 * time.Second)
	})

	t.Run("WorkflowContinuity", func(t *testing.T) {
		if err := scenarios.RunWorkflowContinuity(ctx, launcher); err != nil {
			t.Errorf("workflow continuity failed: %v", err)
		}
		launcher.StopAll()
		time.Sleep(1 * time.Second)
	})
}
