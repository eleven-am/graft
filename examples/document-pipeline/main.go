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
		Level: slog.LevelInfo,
	}))

	fmt.Println("🚀 Starting Graft Document Processing Pipeline Example")
	fmt.Println(strings.Repeat("=", 60))

	nodeID := "doc-processor-1"
	raftAddr := "127.0.0.1:7001"
	dataDir := "./data"
	grpcPort := 8001

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
		{
			Name: "High Priority Urgent Document",
			Doc: Document{
				ID:      "doc-002",
				Content: "URGENT: This is a high-priority document that requires immediate attention and processing. Please handle with care and ensure quality processing.",
				Type:    "document",
			},
			Config: ProcessingConfig{
				MaxRetries:    5,
				Timeout:       60 * time.Second,
				EnableOCR:     false,
				EnableNLP:     true,
				QualityGate:   0.8,
				ProcessorName: nodeID,
			},
		},
		{
			Name: "Image Document with OCR",
			Doc: Document{
				ID:      "doc-003",
				Content: "IMAGE_DATA: This represents image content that needs OCR processing to extract text.",
				Type:    "image",
			},
			Config: ProcessingConfig{
				MaxRetries:    3,
				Timeout:       45 * time.Second,
				EnableOCR:     true,
				EnableNLP:     true,
				QualityGate:   0.6,
				ProcessorName: nodeID,
			},
		},
		{
			Name: "Corrupted Document (Error Recovery)",
			Doc: Document{
				ID:      "doc-004",
				Content: "ERROR: This document has been corrupted and needs repair before processing can continue.",
				Type:    "corrupted",
			},
			Config: ProcessingConfig{
				MaxRetries:    3,
				Timeout:       30 * time.Second,
				EnableOCR:     false,
				EnableNLP:     false,
				QualityGate:   0.5,
				ProcessorName: nodeID,
			},
		},
		{
			Name: "Multilingual Document",
			Doc: Document{
				ID:      "doc-005",
				Content: "Hola mundo! Este es un documento en español que necesita ser procesado y traducido. Gracias por su atención.",
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

	results := make(chan WorkflowResult, len(scenarios))

	for i, scenario := range scenarios {
		go func(idx int, s DocumentScenario) {
			fmt.Printf("\n🔄 [%d] Processing: %s\n", idx+1, s.Name)

			workflowID := fmt.Sprintf("workflow-%d-%d", idx+1, time.Now().Unix())

			trigger := graft.WorkflowTrigger{
				WorkflowID: workflowID,
				InitialNodes: []graft.NodeConfig{
					{
						Name:   "document_ingest",
						Config: s.Config,
					},
				},
				InitialState: s.Doc,
			}

			if err := manager.StartWorkflow(trigger); err != nil {
				results <- WorkflowResult{
					ScenarioName: s.Name,
					WorkflowID:   workflowID,
					Error:        err,
				}
				return
			}

			result := monitorWorkflow(manager, workflowID, s.Name, 2*time.Minute)
			results <- result
		}(i, scenario)

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n⏳ Monitoring workflow executions...")

	completedCount := 0
	for completedCount < len(scenarios) {
		select {
		case result := <-results:
			completedCount++
			printWorkflowResult(result, completedCount, len(scenarios))
		case <-time.After(3 * time.Minute):
			fmt.Println("⚠️  Timeout waiting for workflows to complete")
			break
		}
	}

	fmt.Println("\n📊 Final Cluster Statistics")
	fmt.Println(strings.Repeat("=", 40))
	fmt.Println(formatClusterInfo(manager.GetClusterInfo()))

	fmt.Println("\n🔄 Shutting down Graft cluster...")
	if err := manager.Stop(ctx); err != nil {
		log.Printf("⚠️  Error during shutdown: %v", err)
	} else {
		fmt.Println("✅ Graft cluster stopped successfully")
	}

	fmt.Println("\n🎉 Document Processing Pipeline Example Complete!")
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
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeoutCh := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			status, err := manager.GetWorkflowState(workflowID)
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
		fmt.Printf("    Ready Nodes: %d\n", len(result.Status.ReadyNodes))

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
    Workflow Metrics:
      - Total Workflows: %d
      - Active Workflows: %d
      - Completed Workflows: %d
      - Failed Workflows: %d
      - Nodes Executed: %d`,
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
   • Real-time workflow monitoring and status reporting
   • Comprehensive error handling and retry logic
   • Scalable architecture supporting multiple processing scenarios

🌟 PROCESSING PIPELINE:
   Ingest → Validate → [Analyze + Process] → Quality Check → Finalize
   
   With conditional branches for:
   - OCR processing (images)
   - Language processing (translation) 
   - NLP analysis (sentiment, keywords)
   - Priority handling (urgent docs)
   - Error recovery (corrupted docs)
`)
}
