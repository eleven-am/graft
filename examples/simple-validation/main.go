package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/eleven-am/graft"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	fmt.Println("🚀 Starting Simple Validation Workflow Example")

	nodeID := "simple-node-1"
	raftAddr := "127.0.0.1:7100"
	dataDir := "./data"
	grpcPort := 8010

	manager := graft.New(nodeID, raftAddr, dataDir, logger)
	if manager == nil {
		log.Fatal("failed to create Graft manager")
	}

	if err := registerNodes(manager); err != nil {
		log.Fatalf("failed to register workflow nodes: %v", err)
	}

	ctx := context.Background()
	if err := manager.Start(ctx, grpcPort); err != nil {
		log.Fatalf("failed to start Graft: %v", err)
	}

	time.Sleep(2 * time.Second)

	scenarios := []struct {
		name   string
		doc    WorkflowDocument
		config WorkflowConfig
	}{
		{
			name:   "document requiring repair",
			doc:    WorkflowDocument{ID: "doc-001", Status: "new", NeedsRepair: true},
			config: WorkflowConfig{ProcessorName: nodeID, AllowRepair: true},
		},
		{
			name:   "document passing validation",
			doc:    WorkflowDocument{ID: "doc-002", Status: "new", NeedsRepair: false},
			config: WorkflowConfig{ProcessorName: nodeID, AllowRepair: true},
		},
	}

	done := make(chan struct{}, 1)
	var mu sync.Mutex
	completed := 0

	manager.OnWorkflowCompleted(func(event *graft.WorkflowCompletedEvent) {
		mu.Lock()
		completed++
		fmt.Printf("✅ Workflow %s completed in %v\n", event.WorkflowID, event.Duration.Round(time.Millisecond))
		if final, ok := event.FinalState.(map[string]interface{}); ok {
			if status, ok := final["status"].(string); ok {
				fmt.Printf("   final status: %s\n", status)
			}
			if history, ok := final["history"].([]interface{}); ok {
				fmt.Printf("   history: %v\n", history)
			}
		}
		if completed == len(scenarios) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		mu.Unlock()
	})

	for _, scenario := range scenarios {
		workflowID := fmt.Sprintf("simple-%d", time.Now().UnixNano())
		trigger := graft.WorkflowTrigger{
			WorkflowID:   workflowID,
			InitialNodes: []graft.NodeConfig{{Name: "intake_node", Config: scenario.config}},
			InitialState: scenario.doc,
		}
		fmt.Printf("▶️  Starting %s (workflow %s)\n", scenario.name, workflowID)
		if err := manager.StartWorkflow(trigger); err != nil {
			log.Fatalf("failed to start workflow %s: %v", workflowID, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	select {
	case <-done:
		fmt.Println("🎉 All simple workflows completed")
	case <-time.After(15 * time.Second):
		fmt.Println("⚠️ Timeout waiting for workflows to finish")
	}

	if err := manager.Stop(); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
