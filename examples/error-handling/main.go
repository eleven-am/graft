package main

import (
	"context"
	"fmt"
	"github.com/eleven-am/graft"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"gopkg.in/yaml.v3"
)

type UnreliableConfig struct {
	FailureRate float64 `json:"failure_rate"`
}

type ProcessorState struct {
	Data string `json:"data"`
}

type ProcessorResult struct {
	ProcessedData  string `json:"processed_data"`
	ProcessingNode string `json:"processing_node"`
	ProcessedAt    string `json:"processed_at"`
}

type UnreliableProcessor struct{}

func (n *UnreliableProcessor) GetName() string {
	return "unreliable-processor"
}

func (n *UnreliableProcessor) Execute(ctx context.Context, state ProcessorState, config UnreliableConfig) (graft.NodeResult, error) {
	time.Sleep(100 * time.Millisecond)

	failureRate := config.FailureRate
	if failureRate == 0 {
	}

	if rand.Float64() < failureRate {
		errorTypes := []string{
			"network timeout",
			"service unavailable",
			"data corruption detected",
			"resource exhaustion",
			"invalid input format",
		}
		errorType := errorTypes[rand.IntN(len(errorTypes))]
		return graft.NodeResult{}, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("processing failed: %s", errorType),
			Details: map[string]interface{}{
				"error_type":   errorType,
				"failure_rate": failureRate,
				"processor":    "unreliable-processor",
			},
		}
	}

	result := ProcessorResult{
		ProcessedData:  fmt.Sprintf("processed_%v", state.Data),
		ProcessingNode: "unreliable-processor",
		ProcessedAt:    time.Now().Format(time.RFC3339),
	}

	return graft.NodeResult{
		GlobalState: result,
	}, nil
}

type RetryConfig struct {
	MaxRetries int `json:"max_retries"`
}

type RetryResult struct {
	ProcessedData  string `json:"processed_data"`
	ProcessingNode string `json:"processing_node"`
	RetryCount     int    `json:"retry_count"`
	ProcessedAt    string `json:"processed_at"`
}

type RetryProcessor struct{}

func (n *RetryProcessor) GetName() string {
	return "retry-processor"
}

func (n *RetryProcessor) Execute(ctx context.Context, state ProcessorState, config RetryConfig) (graft.NodeResult, error) {
	time.Sleep(150 * time.Millisecond)

	retryCount := 0

	maxRetries := config.MaxRetries
	if maxRetries == 0 {
		maxRetries = 2
	}

	if retryCount < maxRetries && rand.Float64() < 0.7 {
		return graft.NodeResult{}, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: fmt.Sprintf("temporary failure (retry %d)", retryCount),
			Details: map[string]interface{}{
				"retry_count": retryCount,
				"max_retries": maxRetries,
				"processor":   "retry-processor",
			},
		}
	}

	result := RetryResult{
		ProcessedData:  fmt.Sprintf("retry_success_%v", state.Data),
		ProcessingNode: "retry-processor",
		RetryCount:     retryCount,
		ProcessedAt:    time.Now().Format(time.RFC3339),
	}

	return graft.NodeResult{
		GlobalState: result,
	}, nil
}

type TimeoutProcessor struct{}

func (n *TimeoutProcessor) GetName() string {
	return "timeout-processor"
}

func (n *TimeoutProcessor) Execute(ctx context.Context, config map[string]interface{}, state map[string]interface{}) (map[string]interface{}, error) {
	input, exists := state["data"]
	if !exists {
		return nil, domain.NewValidationError("data", "required field missing from state")
	}

	duration := 3 * time.Second
	if dur, exists := config["duration_ms"]; exists {
		if durInt, ok := dur.(int); ok {
			duration = time.Duration(durInt) * time.Millisecond
		}
	}

	select {
	case <-time.After(duration):
		return map[string]interface{}{
			"processed_data":  fmt.Sprintf("timeout_success_%v", input),
			"processing_node": "timeout-processor",
			"duration_ms":     duration.Milliseconds(),
			"processed_at":    time.Now().Format(time.RFC3339),
		}, nil

	case <-ctx.Done():
		return nil, domain.Error{
			Type:    domain.ErrorTypeTimeout,
			Message: "processing cancelled due to timeout",
			Details: map[string]interface{}{
				"context_error": ctx.Err().Error(),
				"processor":     "timeout-processor",
			},
		}
	}
}

func (n *TimeoutProcessor) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"data": "string",
	}
}

func (n *TimeoutProcessor) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"processed_data":  "string",
		"processing_node": "string",
		"duration_ms":     "number",
		"processed_at":    "string",
	}
}

func loadConfig() (graft.Config, error) {
	var config graft.Config

	if data, err := os.ReadFile("config.yaml"); err == nil {
		if err := yaml.Unmarshal(data, &config); err != nil {
			return config, domain.NewConfigurationError("config.yaml", "YAML parsing failed", "Ensure config.yaml contains valid YAML syntax")
		}
	} else {
		config = graft.DefaultConfig()
		config.NodeID = "error-handling-node"
		config.ServiceName = "error-handling-cluster"
		config.Storage.DataDir = "./data/error-handling"
		config.Queue.DataDir = "./data/error-handling/queue"
	}

	config.Resources.MaxConcurrentTotal = 15
	config.Resources.DefaultPerTypeLimit = 5
	config.Engine.MaxConcurrentWorkflows = 8
	config.Engine.RetryAttempts = 3
	config.Engine.RetryBackoff = "1s"

	return config, nil
}

func main() {
	fmt.Println("Starting Graft Error Handling Example...")

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

	unreliable := &UnreliableProcessor{}
	retry := &RetryProcessor{}
	timeout := &TimeoutProcessor{}

	if err := cluster.RegisterNode(unreliable); err != nil {
		log.Fatalf("Failed to register unreliable node: %v", err)
	}
	if err := cluster.RegisterNode(retry); err != nil {
		log.Fatalf("Failed to register retry node: %v", err)
	}
	if err := cluster.RegisterNode(timeout); err != nil {
		log.Fatalf("Failed to register timeout node: %v", err)
	}

	fmt.Printf("Registered %d error-handling nodes\n", 3)

	cluster.OnError(func(workflowID string, finalState interface{}, err error) {
		fmt.Printf("[ERROR] Workflow %s failed: %v\n", workflowID, err)
	})

	cluster.OnComplete(func(ctx context.Context, data graft.WorkflowCompletionData) error {
		fmt.Printf("[SUCCESS] Workflow %s completed with state: %+v\n",
			data.WorkflowID, data.FinalState)
		fmt.Printf("  Duration: %v\n", data.Duration)
		fmt.Printf("  Executed nodes: %d\n", len(data.ExecutedNodes))
		return nil
	})

	workflows := []struct {
		id       string
		nodeType string
		config   interface{}
		data     string
		desc     string
	}{
		{
			id:       "unreliable-001",
			nodeType: "unreliable-processor",
			config:   "reliable_data",
			data:     "reliable_data",
			desc:     "Low failure rate processor",
		},
		{
			id:       "unreliable-002",
			nodeType: "unreliable-processor",
			config:   "unreliable_data",
			data:     "unreliable_data",
			desc:     "High failure rate processor",
		},
		{
			id:       "retry-001",
			nodeType: "retry-processor",
			config:   RetryConfig{MaxRetries: 3},
			data:     "retry_test",
			desc:     "Processor with retry logic",
		},
		{
			id:       "timeout-fast",
			nodeType: "timeout-processor",
			config:   "fast_data",
			data:     "fast_data",
			desc:     "Fast processing (under timeout)",
		},
		{
			id:       "timeout-slow",
			nodeType: "timeout-processor",
			config:   "slow_data",
			data:     "slow_data",
			desc:     "Slow processing (will timeout)",
		},
	}

	fmt.Println("\nStarting error handling demonstrations...")

	for _, wf := range workflows {
		trigger := graft.WorkflowTrigger{
			WorkflowID: wf.id,
			InitialState: ProcessorState{
				Data: wf.data,
			},
			InitialNodes: []graft.NodeConfig{
				{
					Name:   wf.nodeType,
					Config: wf.config,
				},
			},
			Metadata: map[string]string{
				"example":     "error-handling",
				"description": wf.desc,
			},
		}

		if err := cluster.StartWorkflow(wf.id, trigger); err != nil {
			log.Printf("Failed to start workflow %s: %v", wf.id, err)
			continue
		}

		fmt.Printf("Started workflow: %s (%s)\n", wf.id, wf.desc)
	}

	completed := make(map[string]bool)
	maxWait := 45 * time.Second
	start := time.Now()

	fmt.Printf("\nMonitoring %d workflows for up to %v...\n", len(workflows), maxWait)

	for len(completed) < len(workflows) && time.Since(start) < maxWait {
		time.Sleep(3 * time.Second)

		for _, wf := range workflows {
			if completed[wf.id] {
				continue
			}

			state, err := cluster.GetWorkflowState(wf.id)
			if err != nil {
				log.Printf("Error getting workflow state for %s: %v", wf.id, err)
				continue
			}

			fmt.Printf("[%s] Workflow %s (%s) status: %s\n",
				time.Now().Format("15:04:05"), wf.id, wf.desc, state.Status)

			if state.Status == "completed" {
				completed[wf.id] = true
				fmt.Printf("  ✓ SUCCESS: Final state: %+v\n", state.CurrentState)

				if len(state.ExecutedNodes) > 0 {
					for _, node := range state.ExecutedNodes {
						fmt.Printf("  - Node %s: %s\n", node.NodeName, node.Status)
					}
				}

			} else if state.Status == "failed" {
				completed[wf.id] = true
				fmt.Printf("  ✗ FAILED: Workflow failed permanently\n")
				if state.LastError != nil {
					fmt.Printf("  - Final error: %s\n", *state.LastError)
				}

				if len(state.ExecutedNodes) > 0 {
					fmt.Printf("  - Node execution history:\n")
					for _, node := range state.ExecutedNodes {
						fmt.Printf("    %s: %s\n", node.NodeName, node.Status)
						if node.Error != nil {
							fmt.Printf("      Error: %s\n", *node.Error)
						}
					}
				}
			}
			fmt.Println()
		}
	}

	info := cluster.GetClusterInfo()
	successCount := 0
	failureCount := 0

	for workflowID := range completed {
		state, _ := cluster.GetWorkflowState(workflowID)
		if state.Status == "completed" {
			successCount++
		} else {
			failureCount++
		}
	}

	fmt.Printf("Error Handling Demo Summary:\n")
	fmt.Printf("  Total Workflows: %d\n", len(workflows))
	fmt.Printf("  Successful: %d\n", successCount)
	fmt.Printf("  Failed: %d\n", failureCount)
	fmt.Printf("  Timed Out: %d\n", len(workflows)-len(completed))
	fmt.Printf("  Active Workflows: %d\n", info.ActiveWorkflows)
	fmt.Printf("  Resource Usage: %d/%d\n", info.ExecutionStats.TotalExecuting, info.ExecutionStats.TotalCapacity)

	fmt.Println("Error handling example completed!")
}
