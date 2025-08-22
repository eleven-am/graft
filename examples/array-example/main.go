package main

import (
	"context"
	"fmt"
	"github.com/eleven-am/graft"
	"log"
	"time"
)

// ArrayProcessorNode demonstrates using arrays as both config and state
type ArrayProcessorNode struct{}

func (n *ArrayProcessorNode) GetName() string {
	return "array-processor"
}

func (n *ArrayProcessorNode) Execute(ctx context.Context, state interface{}, config interface{}) (graft.NodeResult, error) {
	fmt.Printf("Array Processor executing with state: %+v, config: %+v\n", state, config)

	// Handle array state - could be array of strings, integers, etc.
	var processedItems []string

	switch s := state.(type) {
	case []string:
		for _, item := range s {
			processedItems = append(processedItems, fmt.Sprintf("processed-%s", item))
		}
	case []int:
		for _, item := range s {
			processedItems = append(processedItems, fmt.Sprintf("processed-%d", item))
		}
	case []interface{}:
		for _, item := range s {
			processedItems = append(processedItems, fmt.Sprintf("processed-%v", item))
		}
	default:
		processedItems = []string{"processed-unknown-type"}
	}

	// Use array config to determine delay
	var delay time.Duration = 100 * time.Millisecond
	if configArray, ok := config.([]int); ok && len(configArray) > 0 {
		delay = time.Duration(configArray[0]) * time.Millisecond
	}

	time.Sleep(delay)

	// Return processed array as next state
	return graft.NodeResult{
		Data: processedItems,
	}, nil
}

func (n *ArrayProcessorNode) CanStart(ctx context.Context, state interface{}, config interface{}) bool {
	// Can start if state is some kind of array/slice
	switch state.(type) {
	case []string, []int, []interface{}:
		return true
	}
	return false
}

func (n *ArrayProcessorNode) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"type":        "array",
		"description": "Array of items to process",
	}
}

func (n *ArrayProcessorNode) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"type":        "array",
		"description": "Array of processed items",
	}
}

// StringConcatNode demonstrates primitive string processing
type StringConcatNode struct{}

func (n *StringConcatNode) GetName() string {
	return "string-concat"
}

func (n *StringConcatNode) Execute(ctx context.Context, state interface{}, config interface{}) (graft.NodeResult, error) {
	fmt.Printf("String Concat executing with state: %+v, config: %+v\n", state, config)

	var result string = "concat:"

	// Join array elements into a single string
	if stateArray, ok := state.([]string); ok {
		for _, item := range stateArray {
			result += " " + item
		}
	}

	// Use string config as suffix
	if configStr, ok := config.(string); ok {
		result += " " + configStr
	}

	time.Sleep(50 * time.Millisecond)

	// Return primitive string
	return graft.NodeResult{
		Data: result,
	}, nil
}

func (n *StringConcatNode) CanStart(ctx context.Context, state interface{}, config interface{}) bool {
	_, ok := state.([]string)
	return ok
}

func (n *StringConcatNode) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"type":  "array",
		"items": map[string]interface{}{"type": "string"},
	}
}

func (n *StringConcatNode) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "string",
	}
}

func main() {
	fmt.Println("Starting Graft Array Example...")

	// Use basic configuration
	config := graft.DefaultConfig()
	config.NodeID = "array-node-1"
	config.ServiceName = "array-example"
	config.Storage.DataDir = "./data/array-example"
	config.Queue.DataDir = "./data/array-example/queue"

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

	// Register array processor node
	arrayProcessor := &ArrayProcessorNode{}
	if err := cluster.RegisterNode(arrayProcessor); err != nil {
		log.Fatalf("Failed to register array processor: %v", err)
	}

	// Register string concat node
	stringConcat := &StringConcatNode{}
	if err := cluster.RegisterNode(stringConcat); err != nil {
		log.Fatalf("Failed to register string concat: %v", err)
	}

	fmt.Printf("Nodes registered: %s, %s\n", arrayProcessor.GetName(), stringConcat.GetName())

	// Test 1: Array of strings with array config
	workflowID1 := "array-strings-workflow"
	trigger1 := graft.WorkflowTrigger{
		WorkflowID:   workflowID1,
		InitialState: []string{"apple", "banana", "cherry"}, // Array state
		InitialNodes: []graft.NodeConfig{
			{
				Name:   "array-processor",
				Config: []int{200, 300}, // Array config - delay 200ms, extra param 300
			},
		},
		Metadata: map[string]string{
			"example": "array-strings",
			"test":    "1",
		},
	}

	fmt.Println("\n--- Test 1: Array of Strings ---")
	if err := cluster.StartWorkflow(workflowID1, trigger1); err != nil {
		log.Fatalf("Failed to start workflow 1: %v", err)
	}

	// Monitor workflow 1
	if err := monitorWorkflow(cluster, workflowID1); err != nil {
		log.Printf("Workflow 1 monitoring error: %v", err)
	}

	// Test 2: Array of integers with primitive config
	workflowID2 := "array-integers-workflow"
	trigger2 := graft.WorkflowTrigger{
		WorkflowID:   workflowID2,
		InitialState: []int{1, 2, 3, 4, 5}, // Array of integers
		InitialNodes: []graft.NodeConfig{
			{
				Name:   "array-processor",
				Config: []int{100}, // Array config - delay 100ms
			},
		},
		Metadata: map[string]string{
			"example": "array-integers",
			"test":    "2",
		},
	}

	fmt.Println("\n--- Test 2: Array of Integers ---")
	if err := cluster.StartWorkflow(workflowID2, trigger2); err != nil {
		log.Fatalf("Failed to start workflow 2: %v", err)
	}

	// Monitor workflow 2
	if err := monitorWorkflow(cluster, workflowID2); err != nil {
		log.Printf("Workflow 2 monitoring error: %v", err)
	}

	// Test 3: Multi-node workflow with array -> string processing
	workflowID3 := "array-to-string-workflow"
	trigger3 := graft.WorkflowTrigger{
		WorkflowID:   workflowID3,
		InitialState: []string{"hello", "world", "from", "arrays"}, // Array state
		InitialNodes: []graft.NodeConfig{
			{
				Name:   "array-processor",
				Config: []int{50}, // Array config
			},
		},
		Metadata: map[string]string{
			"example": "array-to-string",
			"test":    "3",
		},
	}

	fmt.Println("\n--- Test 3: Array to String Processing ---")
	if err := cluster.StartWorkflow(workflowID3, trigger3); err != nil {
		log.Fatalf("Failed to start workflow 3: %v", err)
	}

	// Monitor workflow 3
	if err := monitorWorkflow(cluster, workflowID3); err != nil {
		log.Printf("Workflow 3 monitoring error: %v", err)
	}

	// Show cluster info
	info := cluster.GetClusterInfo()
	fmt.Printf("\nCluster Info:\n")
	fmt.Printf("  Node ID: %s\n", info.NodeID)
	fmt.Printf("  Registered Nodes: %v\n", info.RegisteredNodes)
	fmt.Printf("  Active Workflows: %d\n", info.ActiveWorkflows)

	fmt.Println("Array example completed!")
}

func monitorWorkflow(cluster graft.Cluster, workflowID string) error {
	fmt.Printf("Starting workflow: %s\n", workflowID)

	for i := 0; i < 30; i++ { // Max 30 seconds
		time.Sleep(1 * time.Second)

		state, err := cluster.GetWorkflowState(workflowID)
		if err != nil {
			return fmt.Errorf("error getting workflow state: %v", err)
		}

		fmt.Printf("  Status: %s\n", state.Status)

		if state.Status == "completed" {
			fmt.Printf("  Workflow completed successfully!\n")
			fmt.Printf("  Final state type: %T\n", state.CurrentState)
			fmt.Printf("  Final state: %+v\n", state.CurrentState)

			if len(state.ExecutedNodes) > 0 {
				fmt.Println("  Executed nodes:")
				for _, node := range state.ExecutedNodes {
					fmt.Printf("    - %s: %s\n", node.NodeName, node.Status)
					if node.Output != nil {
						fmt.Printf("      Output type: %T\n", node.Output)
						fmt.Printf("      Output: %+v\n", node.Output)
					}
				}
			}
			return nil
		} else if state.Status == "failed" {
			fmt.Printf("  Workflow failed!\n")
			if state.LastError != nil {
				fmt.Printf("  Error: %s\n", *state.LastError)
			}
			return fmt.Errorf("workflow failed")
		}
	}

	return fmt.Errorf("workflow timed out")
}
