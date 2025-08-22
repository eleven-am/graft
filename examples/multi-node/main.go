package main

import (
	"context"
	"fmt"
	"github.com/eleven-am/graft"
	"log"
	"time"

	"gopkg.in/yaml.v3"
	"os"
)

type DataProcessor struct{}

func (n *DataProcessor) GetName() string {
	return "data-processor"
}

func (n *DataProcessor) Execute(ctx context.Context, config map[string]interface{}, state map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(200 * time.Millisecond)

	input, exists := state["raw_data"]
	if !exists {
		return nil, fmt.Errorf("missing raw_data in state")
	}

	inputStr, ok := input.(string)
	if !ok {
		return nil, fmt.Errorf("raw_data must be a string")
	}

	processed := fmt.Sprintf("processed_%s", inputStr)

	return map[string]interface{}{
		"processed_data": processed,
		"data_size":      len(inputStr),
		"processed_at":   time.Now().Format(time.RFC3339),
		"processor_id":   n.GetName(),
	}, nil
}

func (n *DataProcessor) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"raw_data": "string",
	}
}

func (n *DataProcessor) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"processed_data": "string",
		"data_size":      "number",
		"processed_at":   "string",
		"processor_id":   "string",
	}
}

type DataValidator struct{}

func (n *DataValidator) GetName() string {
	return "data-validator"
}

func (n *DataValidator) Execute(ctx context.Context, config map[string]interface{}, state map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(100 * time.Millisecond)

	processed, exists := state["processed_data"]
	if !exists {
		return nil, fmt.Errorf("missing processed_data in state")
	}

	dataSize, sizeExists := state["data_size"]
	if !sizeExists {
		return nil, fmt.Errorf("missing data_size in state")
	}

	size, ok := dataSize.(int)
	if !ok {
		return nil, fmt.Errorf("data_size must be a number")
	}

	minSize, exists := config["min_size"]
	if !exists {
	}

	minSizeInt, ok := minSize.(int)
	if !ok {
		return nil, fmt.Errorf("min_size config must be a number")
	}

	isValid := size >= minSizeInt
	status := "valid"
	if !isValid {
		status = "invalid"
	}

	return map[string]interface{}{
		"validation_status": status,
		"is_valid":          isValid,
		"validated_data":    processed,
		"validation_rules":  fmt.Sprintf("min_size=%d", minSizeInt),
		"validated_at":      time.Now().Format(time.RFC3339),
	}, nil
}

func (n *DataValidator) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"processed_data": "string",
		"data_size":      "number",
	}
}

func (n *DataValidator) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"validation_status": "string",
		"is_valid":          "boolean",
		"validated_data":    "string",
		"validation_rules":  "string",
		"validated_at":      "string",
	}
}

type DataStore struct{}

func (n *DataStore) GetName() string {
	return "data-store"
}

func (n *DataStore) Execute(ctx context.Context, config map[string]interface{}, state map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(150 * time.Millisecond)

	isValid, exists := state["is_valid"]
	if !exists {
		return nil, fmt.Errorf("missing is_valid in state")
	}

	valid, ok := isValid.(bool)
	if !ok {
		return nil, fmt.Errorf("is_valid must be boolean")
	}

	if !valid {
		return map[string]interface{}{
			"storage_status": "rejected",
			"reason":         "data validation failed",
			"stored_at":      time.Now().Format(time.RFC3339),
		}, nil
	}

	validatedData := state["validated_data"]

	storageID := fmt.Sprintf("store_%d", time.Now().Unix())

	return map[string]interface{}{
		"storage_status": "stored",
		"storage_id":     storageID,
		"stored_data":    validatedData,
		"stored_at":      time.Now().Format(time.RFC3339),
		"storage_path":   fmt.Sprintf("/data/%s", storageID),
	}, nil
}

func (n *DataStore) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"is_valid":       "boolean",
		"validated_data": "string",
	}
}

func (n *DataStore) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"storage_status": "string",
		"storage_id":     "string",
		"stored_data":    "string",
		"stored_at":      "string",
		"storage_path":   "string",
		"reason":         "string",
	}
}

func loadConfig() (graft.Config, error) {
	var config graft.Config

	if data, err := os.ReadFile("config.yaml"); err == nil {
		if err := yaml.Unmarshal(data, &config); err != nil {
			return config, fmt.Errorf("failed to parse config.yaml: %w", err)
		}
	} else {
		config = graft.DefaultConfig()
		config.NodeID = "multi-node-example"
		config.ServiceName = "multi-node-pipeline"
		config.Storage.DataDir = "./data/multi-node"
		config.Queue.DataDir = "./data/multi-node/queue"
		config.Resources.MaxConcurrentTotal = 20
		config.Engine.MaxConcurrentWorkflows = 10
	}

	return config, nil
}

func main() {
	fmt.Println("Starting Graft Multi-Node Pipeline Example...")

	config, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

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

	processor := &DataProcessor{}
	validator := &DataValidator{}
	store := &DataStore{}

	if err := cluster.RegisterNode(processor); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}
	if err := cluster.RegisterNode(validator); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}
	if err := cluster.RegisterNode(store); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}

	fmt.Printf("Registered %d nodes in pipeline\n", 3)

	workflows := []struct {
		id   string
		data string
	}{
		{"workflow-001", "sample_data_large"},
		{"workflow-002", "small"},
		{"workflow-003", "medium_sized_data"},
		{"workflow-004", "huge_dataset_for_processing"},
	}

	for _, wf := range workflows {
		trigger := graft.WorkflowTrigger{
			WorkflowID: wf.id,
			InitialState: map[string]interface{}{
				"raw_data": wf.data,
			},
			InitialNodes: []graft.NodeConfig{
				{
					Name:   "data-processor",
					Config: map[string]interface{}{},
				},
			},
			Metadata: map[string]string{
				"example":   "multi-node",
				"data_type": "sample",
				"pipeline":  "data-processing",
			},
		}

		if err := cluster.StartWorkflow(wf.id, trigger); err != nil {
			log.Printf("Failed to start workflow %s: %v", wf.id, err)
			continue
		}

		fmt.Printf("Started workflow: %s\n", wf.id)
	}

	completed := make(map[string]bool)
	maxWaitTime := 60 * time.Second
	start := time.Now()

	for len(completed) < len(workflows) && time.Since(start) < maxWaitTime {
		time.Sleep(2 * time.Second)

		for _, wf := range workflows {
			if completed[wf.id] {
				continue
			}

			state, err := cluster.GetWorkflowState(wf.id)
			if err != nil {
				log.Printf("Error getting workflow state for %s: %v", wf.id, err)
				continue
			}

			fmt.Printf("Workflow %s status: %s\n", wf.id, state.Status)

			if state.Status == "completed" {
				completed[wf.id] = true
				fmt.Printf("Workflow %s completed successfully!\n", wf.id)
				fmt.Printf("Final state: %+v\n", state.CurrentState)

				if len(state.ExecutedNodes) > 0 {
					fmt.Printf("Executed nodes for %s:\n", wf.id)
					for _, node := range state.ExecutedNodes {
						fmt.Printf("  - %s: %s\n", node.NodeName, node.Status)
					}
				}
				fmt.Println()
			} else if state.Status == "failed" {
				completed[wf.id] = true
				fmt.Printf("Workflow %s failed!\n", wf.id)
				if state.LastError != nil {
					fmt.Printf("Error: %s\n", *state.LastError)
				}
				fmt.Println()
			}
		}
	}

	info := cluster.GetClusterInfo()
	fmt.Printf("Final Cluster Statistics:\n")
	fmt.Printf("  Node ID: %s\n", info.NodeID)
	fmt.Printf("  Registered Nodes: %v\n", info.RegisteredNodes)
	fmt.Printf("  Active Workflows: %d\n", info.ActiveWorkflows)
	fmt.Printf("  Resource Usage: %d/%d\n", info.ExecutionStats.TotalExecuting, info.ExecutionStats.TotalCapacity)

	completedCount := len(completed)
	fmt.Printf("  Workflows Completed: %d/%d\n", completedCount, len(workflows))

	fmt.Println("Multi-node pipeline example completed!")
}
