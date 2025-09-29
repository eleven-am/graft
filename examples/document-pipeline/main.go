package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/eleven-am/graft"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	fmt.Println("🚀 Starting Graft Document Processing Pipeline Example")
	fmt.Println(strings.Repeat("=", 60))

	nodeID := "doc-processor-1"
	raftAddr := "127.0.0.1:7002"
	dataDir := "./data"
	grpcPort := 8002

	manager := graft.New(nodeID, raftAddr, dataDir, logger)
	if manager == nil {
		log.Fatal("❌ Failed to create Graft manager")
	}

	fmt.Printf("✅ Created Graft manager (Node: %s, Raft: %s, gRPC: %d)\n",
		nodeID, raftAddr, grpcPort)

	if err := registerWorkflowNodes(manager); err != nil {
		log.Fatalf("❌ Failed to register workflow nodes: %v", err)
	}

	ctx := context.Background()

	fmt.Println("🔄 Starting Graft cluster...")
	if err := manager.Start(ctx, grpcPort); err != nil {
		log.Fatalf("❌ Failed to start Graft: %v", err)
	}

	fmt.Println("✅ Graft cluster started successfully!")
	fmt.Println("📊 Cluster Info:", formatClusterInfo(manager.GetClusterInfo()))

	time.Sleep(2 * time.Second)

	fmt.Println("\n🔥 Running Document Processing Scenarios")
	fmt.Println(strings.Repeat("=", 60))

	completedWorkflows := 0

	scenarios := []DocumentScenario{
		{
			Name: "Simple Document Processing",
			Doc: Document{
				ID:      "doc-001",
				Content: "Hello world! This is a simple document that needs processing. Thank you for your attention.",
				Type:    "text",
			},
			Config: ProcessingConfig{
				MaxRetries:    3,
				Timeout:       30 * time.Second,
				EnableOCR:     false,
				EnableNLP:     true,
				QualityGate:   0.7,
				ProcessorName: nodeID,
			},
		},
	}

	manager.OnWorkflowCompleted(func(event *graft.WorkflowCompletedEvent) {
		completedWorkflows++

		fmt.Printf("\n[%d/%d] ✅ Scenario-%d\n", completedWorkflows, len(scenarios), completedWorkflows)
		fmt.Printf("    Workflow ID: %s\n", event.WorkflowID)
		fmt.Printf("    Duration: %v\n", event.Duration.Round(time.Millisecond))
		fmt.Printf("    Status: completed\n")
		fmt.Printf("    Nodes Executed: %d\n", len(event.ExecutedNodes))

		if stateMap, ok := event.FinalState.(map[string]interface{}); ok {
			status, _ := stateMap["status"].(string)
			processedBy, _ := stateMap["processed_by"].([]interface{})
			wordCount, _ := stateMap["word_count"].(float64)
			language, _ := stateMap["language"].(string)
			priority, _ := stateMap["priority"].(float64)

			fmt.Printf("    📄 Final Document Status: %s\n", status)
			fmt.Printf("    📊 Processing Chain: %v\n", processedBy)
			fmt.Printf("    📈 Word Count: %.0f | Language: %s | Priority: %.0f\n",
				wordCount, language, priority)
		}

		if completedWorkflows == len(scenarios) {
			fmt.Println("\n🎉 All workflows completed! Shutting down...")

			go func() {
				time.Sleep(100 * time.Millisecond)
				fmt.Println("\n📊 Final Cluster Statistics")
				fmt.Println(strings.Repeat("=", 40))
				fmt.Println(formatClusterInfo(manager.GetClusterInfo()))

				fmt.Println("\n🔄 Shutting down Graft cluster...")
				if err := manager.Stop(); err != nil {
					log.Printf("⚠️  Error during shutdown: %v", err)
					os.Exit(1)
				}
				fmt.Println("✅ Graft cluster stopped successfully")
				fmt.Println("\n🎉 Document Processing Pipeline Example Complete!")
				os.Exit(0)
			}()
		}
	})

	for i, scenario := range scenarios {
		fmt.Printf("\n🔄 [%d] Processing: %s\n", i+1, scenario.Name)

		workflowID := fmt.Sprintf("workflow-%d-%d", i+1, time.Now().Unix())

		fmt.Printf("🔍 DEBUG: Creating trigger for workflow %s\n", workflowID)
		contentPreview := scenario.Doc.Content
		if len(contentPreview) > 50 {
			contentPreview = contentPreview[:50] + "..."
		}
		fmt.Printf("🔍 DEBUG: Initial document - ID: %s, Status: %s, Content: %s\n", scenario.Doc.ID, scenario.Doc.Status, contentPreview)
		fmt.Printf("🔍 DEBUG: Config - MaxRetries: %d, EnableNLP: %t, ProcessorName: %s\n", scenario.Config.MaxRetries, scenario.Config.EnableNLP, scenario.Config.ProcessorName)

		trigger := graft.WorkflowTrigger{
			WorkflowID: workflowID,
			InitialNodes: []graft.NodeConfig{
				{
					Name:   "document_ingest",
					Config: scenario.Config,
				},
			},
			InitialState: scenario.Doc,
		}

		fmt.Printf("🔍 DEBUG: About to call StartWorkflow...\n")
		if err := manager.StartWorkflow(trigger); err != nil {
			log.Fatalf("❌ Failed to start workflow %s: %v", workflowID, err)
		}

		fmt.Printf("✅ Started workflow: %s\n", workflowID)

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n⏳ Workflows started. Completion will be handled automatically...")
	fmt.Printf("🔄 Processing %d workflow(s). Waiting for completion callbacks...\n", len(scenarios))

	// Wait up to 30 seconds for workflows to complete
	timeout := time.After(30 * time.Second)
	completed := make(chan struct{})

	go func() {
		for completedWorkflows < len(scenarios) {
			time.Sleep(100 * time.Millisecond)
		}
		close(completed)
	}()

	select {
	case <-completed:
		fmt.Println("✅ All workflows completed successfully!")
	case <-timeout:
		fmt.Println("⚠️ Timeout: Some workflows didn't complete within 30 seconds")
		fmt.Printf("📊 Completed: %d/%d workflows\n", completedWorkflows, len(scenarios))
	}
}

type DocumentScenario struct {
	Name   string
	Doc    Document
	Config ProcessingConfig
}

type WorkflowResult struct {
	ScenarioName string
	WorkflowID   string
	Status       *graft.WorkflowStatus
	Error        error
	Duration     time.Duration
}

func registerWorkflowNodes(manager *graft.Manager) error {
	nodes := []interface{}{
		&DocumentIngestNode{},
		&DocumentValidatorNode{},
		&ContentAnalyzerNode{},
		&ContentProcessorNode{},
		&LanguageProcessorNode{},
		&OCRProcessorNode{},
		&NLPProcessorNode{},
		&QualityCheckerNode{},
		&QualityEnhancerNode{},
		&PriorityHandlerNode{},
		&DocumentRepairNode{},
		&NotificationSenderNode{},
		&DocumentFinalizerNode{},
	}

	for _, node := range nodes {
		if err := manager.RegisterNode(node); err != nil {
			return fmt.Errorf("failed to register node %T: %w", node, err)
		}
	}

	fmt.Printf("✅ Registered %d workflow nodes\n", len(nodes))
	return nil
}

func monitorWorkflow(manager *graft.Manager, workflowID, scenarioName string, timeout time.Duration) WorkflowResult {
	start := time.Now()

	statusChan, unsubscribe, err := manager.SubscribeToWorkflowState(workflowID)
	if err != nil {

		return monitorWorkflowPolling(manager, workflowID, scenarioName, timeout)
	}
	defer unsubscribe()

	timeoutCh := time.After(timeout)

	for {
		select {
		case status := <-statusChan:
			if status == nil {

				continue
			}

			if status.Status == "completed" || status.Status == "failed" {
				return WorkflowResult{
					ScenarioName: scenarioName,
					WorkflowID:   workflowID,
					Status:       status,
					Duration:     time.Since(start),
				}
			}

		case <-timeoutCh:
			return WorkflowResult{
				ScenarioName: scenarioName,
				WorkflowID:   workflowID,
				Error:        fmt.Errorf("workflow timeout after %v", timeout),
				Duration:     time.Since(start),
			}
		}
	}
}

// Fallback polling implementation for cases where subscription is not available
func monitorWorkflowPolling(manager *graft.Manager, workflowID, scenarioName string, timeout time.Duration) WorkflowResult {
	start := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeoutCh := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			status, err := manager.GetWorkflowStatus(workflowID)
			if err != nil {
				return WorkflowResult{
					ScenarioName: scenarioName,
					WorkflowID:   workflowID,
					Error:        err,
					Duration:     time.Since(start),
				}
			}

			if status.Status == "completed" || status.Status == "failed" {
				return WorkflowResult{
					ScenarioName: scenarioName,
					WorkflowID:   workflowID,
					Status:       status,
					Duration:     time.Since(start),
				}
			}

		case <-timeoutCh:
			return WorkflowResult{
				ScenarioName: scenarioName,
				WorkflowID:   workflowID,
				Error:        fmt.Errorf("workflow timeout after %v", timeout),
				Duration:     time.Since(start),
			}
		}
	}
}

func printWorkflowResult(result WorkflowResult, completed, total int) {
	fmt.Printf("\n[%d/%d] ✅ %s\n", completed, total, result.ScenarioName)
	fmt.Printf("    Workflow ID: %s\n", result.WorkflowID)
	fmt.Printf("    Duration: %v\n", result.Duration.Round(time.Millisecond))

	if result.Error != nil {
		fmt.Printf("    ❌ Error: %v\n", result.Error)
		return
	}

	if result.Status != nil {
		fmt.Printf("    Status: %s\n", result.Status.Status)
		fmt.Printf("    Nodes Executed: %d\n", len(result.Status.ExecutedNodes))
		fmt.Printf("    Pending Nodes: %d\n", len(result.Status.PendingNodes))

		if result.Status.LastError != nil {
			fmt.Printf("    ⚠️  Workflow Error: %s\n", *result.Status.LastError)
		}

		if finalDoc, ok := result.Status.CurrentState.(Document); ok {
			fmt.Printf("    📄 Final Document Status: %s\n", finalDoc.Status)
			fmt.Printf("    📊 Processing Chain: %v\n", finalDoc.ProcessedBy)
			fmt.Printf("    📈 Word Count: %d | Language: %s | Priority: %d\n",
				finalDoc.WordCount, finalDoc.Language, finalDoc.Priority)
		}
	}
}

func formatClusterInfo(info graft.ClusterInfo) string {
	return fmt.Sprintf(`
    Node ID: %s
    Status: %s
    Is Leader: %t
    Peers: %v
    Metrics:
      Total Workflows: %d
      Active Workflows: %d
      Completed Workflows: %d
      Failed Workflows: %d
      Nodes Executed: %d`,
		info.NodeID,
		info.Status,
		info.IsLeader,
		info.Peers,
		info.Metrics.TotalWorkflows,
		info.Metrics.ActiveWorkflows,
		info.Metrics.CompletedWorkflows,
		info.Metrics.FailedWorkflows,
		info.Metrics.NodesExecuted,
	)
}

func init() {
	fmt.Println(`
🔥 Graft Document Processing Pipeline 🔥

This example demonstrates a complex, distributed workflow for document processing
that showcases Graft's key capabilities:

📋 WORKFLOW FEATURES:
   • Parallel Processing: Multiple nodes execute simultaneously
   • Conditional Logic: Nodes execute based on document properties
   • Error Recovery: Failed operations trigger repair workflows  
   • Quality Gates: Documents must meet quality thresholds
   • Priority Handling: High-priority documents get expedited processing
   • Multi-language Support: Automatic translation capabilities

🔧 TECHNICAL DEMONSTRATION:
   • 13 different node types with complex interactions
   • State management across distributed processing steps
   • Automatic cluster formation and peer discovery
   • Real-time workflow monitoring via event subscriptions
   • Comprehensive error handling and retry logic
   • Scalable architecture supporting multiple processing scenarios

🌟 PROCESSING PIPELINE:
   Ingest → Validate → [Analyze + Process] → Quality Check → Finalize
   
   With conditional branches for:
   - OCR processing (images)
   - Language processing (translation) 
   - NLP analysis (sentiment, keywords)
   - Priority handling (urgent docs)
   - Error recovery (corrupted docs)`)
}
