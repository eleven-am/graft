package main

import (
	"context"
	"fmt"
	"github.com/eleven-am/graft"
	"log"
	"strings"
	"time"
)

type ArrayConfig []string

type ListProcessorState struct {
	Items []string `json:"items"`
}

type ListResult struct {
	ProcessedItems []string `json:"processed_items"`
	Count          int      `json:"count"`
}

type ArrayConfigProcessor struct{}

func (n *ArrayConfigProcessor) GetName() string {
	return "array-config-processor"
}

func (n *ArrayConfigProcessor) Execute(ctx context.Context, state ListProcessorState, config ArrayConfig) (*graft.NodeResult, error) {
	var processedItems []string

	for _, item := range state.Items {
		processed := item
		for _, rule := range config {
			switch rule {
			case "uppercase":
				processed = strings.ToUpper(processed)
			case "prefix":
				processed = "PROCESSED_" + processed
			case "suffix":
				processed = processed + "_DONE"
			}
		}
		processedItems = append(processedItems, processed)
	}

	return &graft.NodeResult{
		GlobalState: ListResult{
			ProcessedItems: processedItems,
			Count:          len(processedItems),
		},
	}, nil
}

type StringConfig string

type MessageState struct {
	Text string `json:"text"`
}

type MessageResult struct {
	FormattedMessage string `json:"formatted_message"`
	Template         string `json:"template"`
}

type StringConfigProcessor struct{}

func (n *StringConfigProcessor) GetName() string {
	return "string-config-processor"
}

func (n *StringConfigProcessor) Execute(ctx context.Context, state MessageState, config StringConfig) (*graft.NodeResult, error) {
	template := string(config)
	formatted := strings.ReplaceAll(template, "{text}", state.Text)

	return &graft.NodeResult{
		GlobalState: MessageResult{
			FormattedMessage: formatted,
			Template:         template,
		},
	}, nil
}

type NestedConfig struct {
	Database struct {
		Host     string                 `json:"host"`
		Port     int                    `json:"port"`
		Settings map[string]interface{} `json:"settings"`
	} `json:"database"`
	Processing struct {
		Workers   int      `json:"workers"`
		Timeout   string   `json:"timeout"`
		Pipelines []string `json:"pipelines"`
	} `json:"processing"`
}

type WorkloadState struct {
	JobID string `json:"job_id"`
	Data  []byte `json:"data"`
}

type ProcessingResult struct {
	JobID       string `json:"job_id"`
	ProcessedBy string `json:"processed_by"`
	DataSize    int    `json:"data_size"`
	Duration    string `json:"duration"`
}

type NestedConfigProcessor struct{}

func (n *NestedConfigProcessor) GetName() string {
	return "nested-config-processor"
}

func (n *NestedConfigProcessor) Execute(ctx context.Context, state WorkloadState, config NestedConfig) (*graft.NodeResult, error) {
	start := time.Now()

	timeout, _ := time.ParseDuration(config.Processing.Timeout)
	select {
	case <-time.After(timeout):
	case <-ctx.Done():
		return &graft.NodeResult{}, fmt.Errorf("processing cancelled")
	}

	processingInfo := fmt.Sprintf("%s:%d/%d_workers",
		config.Database.Host,
		config.Database.Port,
		config.Processing.Workers)

	return &graft.NodeResult{
		GlobalState: ProcessingResult{
			JobID:       state.JobID,
			ProcessedBy: processingInfo,
			DataSize:    len(state.Data),
			Duration:    time.Since(start).String(),
		},
	}, nil
}

type MinimalProcessor struct{}

func (n *MinimalProcessor) GetName() string {
	return "minimal-processor"
}

func (n *MinimalProcessor) Execute(ctx context.Context) (*graft.NodeResult, error) {
	return &graft.NodeResult{
		GlobalState: map[string]interface{}{
			"message":   "Minimal processing completed",
			"timestamp": time.Now().Format(time.RFC3339),
		},
	}, nil
}

func main() {
	fmt.Println("Starting Advanced Types Example - Demonstrating Pure interface{} Backend...")

	config := graft.DefaultConfig()
	config.NodeID = "advanced-types-node"
	config.ServiceName = "advanced-types-cluster"
	config.Storage.DataDir = "./data/advanced-types"
	config.Queue.DataDir = "./data/advanced-types/queue"

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

	nodes := []interface{}{
		&ArrayConfigProcessor{},
		&StringConfigProcessor{},
		&NestedConfigProcessor{},
		&MinimalProcessor{},
	}

	for _, node := range nodes {
		if err := cluster.RegisterNode(node); err != nil {
			log.Fatalf("Failed to register node %T: %v", node, err)
		}
		fmt.Printf("Registered node: %T\n", node)
	}

	cluster.OnComplete(func(ctx context.Context, data graft.WorkflowCompletionData) error {
		fmt.Printf("[SUCCESS] Workflow %s completed with state: %+v\n", data.WorkflowID, data.FinalState)
		fmt.Printf("  Duration: %v\n", data.Duration)
		fmt.Printf("  Executed nodes: %d\n", len(data.ExecutedNodes))
		return nil
	})

	cluster.OnError(func(workflowID string, finalState interface{}, err error) {
		fmt.Printf("[ERROR] Workflow %s failed: %v\n", workflowID, err)
	})

	workflows := []struct {
		id          string
		description string
		trigger     graft.WorkflowTrigger
	}{
		{
			"array-config-demo",
			"Array Config: Processing rules as []string",
			graft.WorkflowTrigger{
				WorkflowID: "array-config-demo",
				InitialState: ListProcessorState{
					Items: []string{"hello", "world", "graft"},
				},
				InitialNodes: []graft.NodeConfig{
					{
						Name: "array-config-processor",
					},
				},
			},
		},
		{
			"string-config-demo",
			"Primitive Config: Template as string",
			graft.WorkflowTrigger{
				WorkflowID: "string-config-demo",
				InitialState: MessageState{
					Text: "Hello from Graft!",
				},
				InitialNodes: []graft.NodeConfig{
					{
						Name: "string-config-processor",
					},
				},
			},
		},
		{
			"nested-config-demo",
			"Complex Config: Nested struct configuration",
			graft.WorkflowTrigger{
				WorkflowID: "nested-config-demo",
				InitialState: WorkloadState{
					JobID: "job-12345",
					Data:  []byte("sample data for processing"),
				},
				InitialNodes: []graft.NodeConfig{
					{
						Name: "nested-config-processor",
						Config: NestedConfig{
							Database: struct {
								Host     string                 `json:"host"`
								Port     int                    `json:"port"`
								Settings map[string]interface{} `json:"settings"`
							}{
								Host: "localhost",
								Port: 5432,
								Settings: map[string]interface{}{
									"pool_size": 10,
									"timeout":   "30s",
								},
							},
							Processing: struct {
								Workers   int      `json:"workers"`
								Timeout   string   `json:"timeout"`
								Pipelines []string `json:"pipelines"`
							}{
								Workers:   4,
								Timeout:   "2s",
								Pipelines: []string{"validation", "transformation", "storage"},
							},
						},
					},
				},
			},
		},
		{
			id:          "minimal-demo",
			description: "Minimal Config: No config needed",
			trigger: graft.WorkflowTrigger{
				WorkflowID: "minimal-demo",
				InitialNodes: []graft.NodeConfig{
					{
						Name: "minimal-processor",
					},
				},
			},
		},
	}

	fmt.Println("\nStarting advanced type demonstrations...")

	for _, wf := range workflows {
		fmt.Printf("\n=== %s ===\n", wf.description)
		fmt.Printf("Config type: %T\n", wf.trigger.InitialNodes[0].Config)

		if err := cluster.StartWorkflow(wf.id, wf.trigger); err != nil {
			log.Printf("Failed to start workflow %s: %v", wf.id, err)
			continue
		}

		fmt.Printf("Started workflow: %s\n", wf.id)
	}

	completed := make(map[string]bool)
	for len(completed) < len(workflows) {
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

			if state.Status == "completed" || state.Status == "failed" {
				completed[wf.id] = true
				fmt.Printf("✓ Workflow %s: %s\n", wf.id, state.Status)
			}
		}
	}

	fmt.Printf("\n🎉 Advanced Types Demo Complete!\n")
	fmt.Printf("Successfully demonstrated:\n")
	fmt.Printf("  ✓ Array configs: []string\n")
	fmt.Printf("  ✓ Primitive configs: string\n")
	fmt.Printf("  ✓ Complex nested struct configs\n")
	fmt.Printf("  ✓ Variadic method signatures\n")
	fmt.Printf("  ✓ Pure interface{} backend (no map[string]interface{} constraints!)\n")
}
