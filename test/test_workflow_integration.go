package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/eleven-am/graft"
)

// Simple test node
type TestNode struct {
	Message string
}

func (t *TestNode) GetName() string {
	return "TestNode"
}

func (t *TestNode) Execute(ctx context.Context, state map[string]interface{}, config map[string]interface{}) (graft.NodeResult, error) {
	// Test accessing workflow context
	if workflowCtx, ok := graft.GetWorkflowContext(ctx); ok {
		slog.Info("Node executing with workflow context",
			"workflow_id", workflowCtx.WorkflowID,
			"node_name", workflowCtx.NodeName,
			"is_leader", workflowCtx.ClusterInfo.IsLeader)
	}

	return graft.NodeResult{
		NextNodes:   []graft.NextNode{}, // Terminal node
		GlobalState: map[string]interface{}{"result": t.Message},
	}, nil
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Test workflow execution with advanced config
	logger.Info("Testing workflow execution with advanced config...")
	config := graft.NewConfigBuilder("workflow-test", "localhost:7003", "./data-workflow").
		WithMDNS("_graft._tcp", "local.", "").
		WithResourceLimits(50, 5, nil).
		WithEngineSettings(10, 2*time.Minute, 2).
		Build()
	config.Logger = logger

	manager := graft.NewWithConfig(config)
	if manager == nil {
		logger.Error("Failed to create manager")
		os.Exit(1)
	}

	// Register a test node
	err := manager.RegisterNode(&TestNode{Message: "Hello from advanced config!"})
	if err != nil {
		logger.Error("Failed to register node", "error", err)
		os.Exit(1)
	}
	logger.Info("Test node registered successfully")

	// Test discovery configuration
	discoveryInfo := manager.Discovery()
	if discoveryInfo == nil {
		logger.Error("Discovery manager not available")
		os.Exit(1)
	}
	logger.Info("Discovery manager available")

	// Test cluster info with advanced config
	info := manager.GetClusterInfo()
	logger.Info("Cluster info",
		"node_id", info.NodeID,
		"status", info.Status,
		"is_leader", info.IsLeader)

	logger.Info("Workflow integration test passed successfully!")
}
