package main

import (
	"context"
	"fmt"
	"github.com/eleven-am/graft"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

type CriticalConfig struct{}

type CriticalState struct {
	TaskData string `json:"task_data"`
	Priority string `json:"priority"`
}

type CriticalResult struct {
	ProcessedData  string `json:"processed_data"`
	ProcessingNode string `json:"processing_node"`
	PriorityLevel  string `json:"priority_level"`
	ProcessedAt    string `json:"processed_at"`
	ProcessingTime string `json:"processing_time"`
}

type CriticalProcessor struct{}

func (n *CriticalProcessor) GetName() string {
	return "critical-processor"
}

func (n *CriticalProcessor) Execute(ctx context.Context, state CriticalState, config CriticalConfig) (graft.NodeResult, error) {
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return graft.NodeResult{}, fmt.Errorf("critical processing cancelled: %v", ctx.Err())
	}

	if state.TaskData == "" {
		return graft.NodeResult{}, fmt.Errorf("missing task_data in state")
	}

	priority := state.Priority
	if priority == "" {
		priority = "normal"
	}

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "unknown"
	}

	result := CriticalResult{
		ProcessedData:  fmt.Sprintf("critical_%s", state.TaskData),
		ProcessingNode: nodeID,
		PriorityLevel:  priority,
		ProcessedAt:    time.Now().Format(time.RFC3339),
		ProcessingTime: "500ms",
	}

	return graft.NodeResult{
		Data: result,
	}, nil
}

type BatchConfig struct {
	BatchSize int `json:"batch_size"`
}

type BatchState struct {
	BatchData string `json:"batch_data"`
}

type BatchResult struct {
	ProcessedBatch string `json:"processed_batch"`
	ProcessingNode string `json:"processing_node"`
	BatchSize      int    `json:"batch_size"`
	ProcessedAt    string `json:"processed_at"`
	ProcessingTime string `json:"processing_time"`
}

type BatchProcessor struct{}

func (n *BatchProcessor) GetName() string {
	return "batch-processor"
}

func (n *BatchProcessor) Execute(ctx context.Context, state BatchState, config BatchConfig) (graft.NodeResult, error) {
	batchSize := config.BatchSize
	if batchSize == 0 {
		batchSize = 1
	}

	processingTime := time.Duration(batchSize*100) * time.Millisecond
	select {
	case <-time.After(processingTime):
	case <-ctx.Done():
		return graft.NodeResult{}, fmt.Errorf("batch processing cancelled: %v", ctx.Err())
	}

	if state.BatchData == "" {
		return graft.NodeResult{}, fmt.Errorf("missing batch_data in state")
	}

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "unknown"
	}

	result := BatchResult{
		ProcessedBatch: fmt.Sprintf("batch_%s", state.BatchData),
		ProcessingNode: nodeID,
		BatchSize:      batchSize,
		ProcessedAt:    time.Now().Format(time.RFC3339),
		ProcessingTime: processingTime.String(),
	}

	return graft.NodeResult{
		Data: result,
	}, nil
}

func loadConfigFromEnv() (graft.Config, error) {
	config := graft.DefaultConfig()

	if data, err := os.ReadFile("config.yaml"); err == nil {
		if err := yaml.Unmarshal(data, &config); err != nil {
			return config, fmt.Errorf("failed to parse config.yaml: %w", err)
		}
	}

	if nodeID := os.Getenv("NODE_ID"); nodeID != "" {
		config.NodeID = nodeID
	}

	if serviceName := os.Getenv("SERVICE_NAME"); serviceName != "" {
		config.ServiceName = serviceName
	}

	if servicePort := os.Getenv("SERVICE_PORT"); servicePort != "" {
		if port, err := strconv.Atoi(servicePort); err == nil {
			config.ServicePort = port
		}
	}

	if transportPort := os.Getenv("TRANSPORT_PORT"); transportPort != "" {
		if port, err := strconv.Atoi(transportPort); err == nil {
			config.Transport.ListenPort = port
		}
	}

	if dataDir := os.Getenv("DATA_DIR"); dataDir != "" {
		config.Storage.DataDir = dataDir
		config.Queue.DataDir = dataDir + "/queue"
	}

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		config.LogLevel = logLevel
	}

	if enableTLS := os.Getenv("ENABLE_TLS"); enableTLS == "true" {
		config.Transport.EnableTLS = true
		config.Transport.TLSCertFile = "/certs/cert.pem"
		config.Transport.TLSKeyFile = "/certs/key.pem"
	}

	var peers []string
	for i := 1; i <= 3; i++ {
		if peer := os.Getenv(fmt.Sprintf("PEER_NODE_%d", i)); peer != "" && !strings.Contains(peer, config.NodeID) {
			peers = append(peers, peer)
		}
	}

	if len(peers) > 0 {
		config.Discovery.Strategy = graft.StrategyStatic
		config.Discovery.Peers = peers
	}

	config.Resources.MaxConcurrentTotal = 100
	config.Resources.DefaultPerTypeLimit = 20
	config.Resources.MaxConcurrentPerType = map[string]int{
		"critical-processor": 30,
		"batch-processor":    10,
	}
	config.Engine.MaxConcurrentWorkflows = 50
	config.Engine.NodeExecutionTimeout = "60s"

	return config, nil
}

func main() {
	fmt.Println("Starting Graft Production Cluster Node...")

	config, err := loadConfigFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	fmt.Printf("Node ID: %s\n", config.NodeID)
	fmt.Printf("Service: %s:%d\n", config.ServiceName, config.ServicePort)
	fmt.Printf("Transport: %s:%d\n", config.Transport.ListenAddress, config.Transport.ListenPort)
	fmt.Printf("Peers: %v\n", config.Discovery.Peers)

	cluster, err := graft.New(config)
	if err != nil {
		log.Fatalf("Failed to create cluster: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	criticalProcessor := &CriticalProcessor{}
	batchProcessor := &BatchProcessor{}

	if err := cluster.RegisterNode(criticalProcessor); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}
	if err := cluster.RegisterNode(batchProcessor); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}

	fmt.Printf("Registered %d node types\n", 2)

	if config.NodeID == "node-1" {

		workflows := []struct {
			id       string
			nodeType string
			state    interface{}
			config   interface{}
		}{
			{
				"critical-001",
				"critical-processor",
				CriticalState{TaskData: "urgent_task", Priority: "high"},
				CriticalConfig{},
			},
			{
				"batch-001",
				"batch-processor",
				BatchState{BatchData: "large_dataset"},
				BatchConfig{BatchSize: 10},
			},
			{
				"critical-002",
				"critical-processor",
				CriticalState{TaskData: "emergency_task", Priority: "critical"},
				CriticalConfig{},
			},
		}

		for _, wf := range workflows {
			trigger := graft.WorkflowTrigger{
				WorkflowID:   wf.id,
				InitialState: wf.state,
				InitialNodes: []graft.NodeConfig{
					{
						Name:   wf.nodeType,
						Config: wf.config,
					},
				},
				Metadata: map[string]string{
					"environment": "production",
					"node_id":     config.NodeID,
					"started_at":  time.Now().Format(time.RFC3339),
				},
			}

			if err := cluster.StartWorkflow(wf.id, trigger); err != nil {
				log.Printf("Failed to start workflow %s: %v", wf.id, err)
			} else {
				fmt.Printf("Started production workflow: %s\n", wf.id)
			}
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	fmt.Println("Production cluster is running. Press Ctrl+C to shutdown gracefully.")

	for {
		select {
		case <-sigChan:
			fmt.Println("\nReceived shutdown signal. Initiating graceful shutdown...")
			cancel()
			return

		case <-ticker.C:
			info := cluster.GetClusterInfo()
			fmt.Printf("[%s] Status - Active Workflows: %d, Resource Usage: %d/%d, Registered Nodes: %v\n",
				time.Now().Format("15:04:05"),
				info.ActiveWorkflows,
				info.ExecutionStats.TotalExecuting,
				info.ExecutionStats.TotalCapacity,
				info.RegisteredNodes,
			)

		case <-ctx.Done():
			return
		}
	}
}
