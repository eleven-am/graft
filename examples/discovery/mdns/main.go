package main

import (
	"context"
	"fmt"
	"github.com/eleven-am/graft"
	"log"
	"os"
	"strconv"
	"time"
)

type MDNSProcessor struct{}

func (n *MDNSProcessor) GetName() string {
	return "mdns-processor"
}

func (n *MDNSProcessor) Execute(ctx context.Context, config map[string]interface{}, state map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(200 * time.Millisecond)

	input, exists := state["data"]
	if !exists {
		return nil, fmt.Errorf("missing data in state")
	}

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "unknown"
	}

	return map[string]interface{}{
		"processed_data":  fmt.Sprintf("mdns_%v", input),
		"processing_node": nodeID,
		"discovery_type":  "mdns",
		"processed_at":    time.Now().Format(time.RFC3339),
	}, nil
}

func (n *MDNSProcessor) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"data": "string",
	}
}

func (n *MDNSProcessor) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"processed_data":  "string",
		"processing_node": "string",
		"discovery_type":  "string",
		"processed_at":    "string",
	}
}

func createMDNSConfig() graft.Config {
	config := graft.DefaultConfig()

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = fmt.Sprintf("mdns-node-%d", time.Now().Unix()%1000)
	}
	config.NodeID = nodeID

	servicePort := 8080
	if port := os.Getenv("SERVICE_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			servicePort = p
		}
	}
	config.ServicePort = servicePort

	transportPort := 9090
	if port := os.Getenv("TRANSPORT_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			transportPort = p
		}
	}
	config.Transport.ListenPort = transportPort

	config.Discovery.Strategy = graft.StrategyMDNS
	config.Discovery.ServiceName = "graft-mdns-example"

	config.ServiceName = "mdns-discovery-cluster"
	config.Storage.DataDir = fmt.Sprintf("./data/mdns/%s", nodeID)
	config.Queue.DataDir = fmt.Sprintf("./data/mdns/%s/queue", nodeID)

	config.Resources.MaxConcurrentTotal = 15
	config.Resources.DefaultPerTypeLimit = 8
	config.Engine.MaxConcurrentWorkflows = 8

	return config
}

func main() {
	fmt.Println("Starting Graft mDNS Discovery Example...")

	config := createMDNSConfig()

	fmt.Printf("Node ID: %s\n", config.NodeID)
	fmt.Printf("Service Port: %d\n", config.ServicePort)
	fmt.Printf("Transport Port: %d\n", config.Transport.ListenPort)
	fmt.Printf("Discovery Strategy: %s\n", config.Discovery.Strategy)
	fmt.Printf("mDNS Service: %s\n", config.Discovery.ServiceName)

	cluster, err := graft.New(config)
	if err != nil {
		log.Fatalf("Failed to create cluster: %v", err)
	}

	ctx := context.Background()
	if err := cluster.Start(ctx); err != nil {
		log.Fatalf("Failed to start cluster: %v", err)
	}
	defer func() {
		fmt.Println("Stopping cluster and cleaning up mDNS...")
		if err := cluster.Stop(); err != nil {
			log.Printf("Error stopping cluster: %v", err)
		}
	}()

	fmt.Println("Cluster started successfully - advertising via mDNS")

	processor := &MDNSProcessor{}
	if err := cluster.RegisterNode(processor); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}

	fmt.Printf("Node registered: %s\n", processor.GetName())

	fmt.Println("Waiting for mDNS discovery (30 seconds)...")
	time.Sleep(30 * time.Second)

	info := cluster.GetClusterInfo()
	fmt.Printf("Discovered cluster members: %v\n", info.RegisteredNodes)

	fmt.Println("Starting test workflows...")

	workflows := []struct {
		id   string
		data string
	}{
		{"mdns-workflow-001", "zeroconf_data_1"},
		{"mdns-workflow-002", "zeroconf_data_2"},
		{"mdns-workflow-003", "zeroconf_data_3"},
	}

	for _, wf := range workflows {
		trigger := graft.WorkflowTrigger{
			WorkflowID: wf.id,
			InitialState: map[string]interface{}{
				"data": wf.data,
			},
			InitialNodes: []graft.NodeConfig{
				{
					Name:   "mdns-processor",
					Config: map[string]interface{}{},
				},
			},
			Metadata: map[string]string{
				"discovery":  "mdns",
				"example":    "zero-config",
				"started_by": config.NodeID,
			},
		}

		if err := cluster.StartWorkflow(wf.id, trigger); err != nil {
			log.Printf("Failed to start workflow %s: %v", wf.id, err)
			continue
		}

		fmt.Printf("Started workflow: %s\n", wf.id)
	}

	completed := make(map[string]bool)
	maxWait := 60 * time.Second
	start := time.Now()

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

			fmt.Printf("Workflow %s status: %s\n", wf.id, state.Status)

			if state.Status == "completed" {
				completed[wf.id] = true
				processingNode := "unknown"
				if stateMap, ok := state.CurrentState.(map[string]interface{}); ok {
					if node, exists := stateMap["processing_node"]; exists {
						processingNode = fmt.Sprintf("%v", node)
					}
				}
				fmt.Printf("Workflow %s completed on node: %v\n", wf.id, processingNode)
			} else if state.Status == "failed" {
				completed[wf.id] = true
				fmt.Printf("Workflow %s failed\n", wf.id)
				if state.LastError != nil {
					fmt.Printf("Error: %s\n", *state.LastError)
				}
			}
		}
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	fmt.Println("mDNS discovery example running. Press Ctrl+C to exit...")
	fmt.Println("Try starting additional instances in other terminals!")

	for {
		<-ticker.C

		info := cluster.GetClusterInfo()
		fmt.Printf("[%s] mDNS Cluster Status:\n", time.Now().Format("15:04:05"))
		fmt.Printf("  Active Workflows: %d\n", info.ActiveWorkflows)
		fmt.Printf("  Resource Usage: %d/%d\n", info.ExecutionStats.TotalExecuting, info.ExecutionStats.TotalCapacity)
		fmt.Printf("  Current Cluster Members: %v\n", info.RegisteredNodes)
		fmt.Println()
	}

	fmt.Println("mDNS discovery example completed!")
}
