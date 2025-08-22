package main

import (
	"context"
	"fmt"
	"github.com/eleven-am/graft"
	"log"
	"log/slog"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type ProcessorConfig struct {
	DelayMS int `json:"delay_ms"`
}

type ProcessorState struct {
	Input string `json:"input"`
}

type ProcessorResult struct {
	ProcessedData string `json:"processed_data"`
	Timestamp     string `json:"timestamp"`
	NodeName      string `json:"node_name"`
}

type ProcessorNode struct{}

func (n *ProcessorNode) GetName() string {
	return "data-processor"
}

func (n *ProcessorNode) Execute(ctx context.Context, state ProcessorState, config ProcessorConfig) (*graft.NodeResult, error) {
	delay := config.DelayMS
	if delay == 0 {
		delay = 100
	}

	time.Sleep(time.Duration(delay) * time.Millisecond)
	result := ProcessorResult{
		ProcessedData: fmt.Sprintf("%s (processed)", state.Input),
		Timestamp:     time.Now().Format(time.RFC3339),
		NodeName:      n.GetName(),
	}

	return &graft.NodeResult{
		GlobalState: result,
		NextNodes:   nil,
	}, nil
}

func loadConfig() (graft.Config, error) {
	var config graft.Config

	if data, err := os.ReadFile("config.yaml"); err == nil {
		if err := yaml.Unmarshal(data, &config); err != nil {
			return config, fmt.Errorf("failed to parse config.yaml: %w", err)
		}
	} else {
		config = graft.DefaultConfig()
		config.NodeID = "node-1"
		config.ServiceName = "basic-example"
		config.Storage.DataDir = "./data/basic"
		config.Queue.DataDir = "./data/basic/queue"
	}

	return config, nil
}

func main() {
	fmt.Println("Starting Graft Basic Example...")

	config, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	debugLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	config.Logger = debugLogger

	cluster, err := graft.New(config)
	if err != nil {
		log.Fatalf("Failed to create cluster: %v", err)
	}

	ctx := context.Background()
	if err := cluster.Start(ctx); err != nil {
		log.Fatalf("Failed to start cluster: %v", err)
	}
	defer func() {
		fmt.Println("Stopping cluster...")
		if err := cluster.Stop(); err != nil {
			log.Printf("Error stopping cluster: %v", err)
		}
	}()

	fmt.Println("Cluster started successfully")

	processor := &ProcessorNode{}
	if err := cluster.RegisterNode(processor); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}

	fmt.Printf("Node registered: %s\n", processor.GetName())

	cluster.OnComplete(func(ctx context.Context, data graft.WorkflowCompletionData) error {
		fmt.Printf("COMPLETION HANDLER CALLED for workflow: %s\n", data.WorkflowID)
		fmt.Printf("   Status: %s\n", data.Status)
		fmt.Printf("   Final State: %+v\n", data.FinalState)
		return nil
	})

	cluster.OnError(func(workflowID string, finalState interface{}, err error) {
		fmt.Printf("ERROR HANDLER CALLED for workflow: %s\n", workflowID)
		fmt.Printf("   Error: %v\n", err)
	})

	workflowID := "workflow-001"
	trigger := graft.WorkflowTrigger{
		WorkflowID: workflowID,
		InitialState: ProcessorState{
			Input: "Hello, World!",
		},
		InitialNodes: []graft.NodeConfig{
			{
				Name:   "data-processor",
				Config: ProcessorConfig{DelayMS: 100},
			},
		},
		Metadata: map[string]string{
			"example": "basic",
			"version": "1.0",
		},
	}

	if err := cluster.StartWorkflow(workflowID, trigger); err != nil {
		log.Fatalf("Failed to start workflow: %v", err)
	}

	fmt.Printf("Workflow started: %s\n", workflowID)

	for {
		time.Sleep(1 * time.Second)

		state, err := cluster.GetWorkflowState(workflowID)
		if err != nil {
			log.Printf("Error getting workflow state: %v", err)
			break
		}

		fmt.Printf("Workflow status: %s\n", state.Status)

		if state.Status == "completed" {
			fmt.Printf("Workflow completed successfully!\n")
			fmt.Printf("Final state: %+v\n", state.CurrentState)

			if len(state.ExecutedNodes) > 0 {
				fmt.Println("Executed nodes:")
				for _, node := range state.ExecutedNodes {
					fmt.Printf("  - %s: %s\n", node.NodeName, node.Status)
				}
			}
			break
		} else if state.Status == "failed" {
			fmt.Printf("Workflow failed!\n")
			if state.LastError != nil {
				fmt.Printf("Error: %s\n", *state.LastError)
			}
			break
		}
	}

	info := cluster.GetClusterInfo()
	fmt.Printf("\nCluster Info:\n")
	fmt.Printf("  Node ID: %s\n", info.NodeID)
	fmt.Printf("  Registered Nodes: %v\n", info.RegisteredNodes)
	fmt.Printf("  Active Workflows: %d\n", info.ActiveWorkflows)
	fmt.Printf("  Resource Usage: %d/%d\n", info.ExecutionStats.TotalExecuting, info.ExecutionStats.TotalCapacity)

	fmt.Println("Basic example completed!")
}
