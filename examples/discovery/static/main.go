package main

import (
	"context"
	"fmt"
	"github.com/eleven-am/graft"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type SimpleProcessor struct{}

func (n *SimpleProcessor) GetName() string {
	return "static-processor"
}

func (n *SimpleProcessor) Execute(ctx context.Context, config map[string]interface{}, state map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(300 * time.Millisecond)

	input, exists := state["data"]
	if !exists {
		return nil, fmt.Errorf("missing data in state")
	}

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "unknown"
	}

	return map[string]interface{}{
		"processed_data":  fmt.Sprintf("static_%v", input),
		"processing_node": nodeID,
		"discovery_type":  "static",
		"processed_at":    time.Now().Format(time.RFC3339),
	}, nil
}

func (n *SimpleProcessor) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"data": "string",
	}
}

func (n *SimpleProcessor) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"processed_data":  "string",
		"processing_node": "string",
		"discovery_type":  "string",
		"processed_at":    "string",
	}
}

func createStaticConfig() graft.Config {
	config := graft.DefaultConfig()

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "static-node-1"
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

	config.Discovery.Strategy = graft.StrategyStatic

	var peers []string
	if peerList := os.Getenv("PEERS"); peerList != "" {
		peers = strings.Split(peerList, ",")
		for i, peer := range peers {
			peers[i] = strings.TrimSpace(peer)
		}
	}
	config.Discovery.Peers = peers

	config.ServiceName = "static-discovery-cluster"
	config.Storage.DataDir = fmt.Sprintf("./data/static/%s", nodeID)
	config.Queue.DataDir = fmt.Sprintf("./data/static/%s/queue", nodeID)

	config.Resources.MaxConcurrentTotal = 20
	config.Resources.DefaultPerTypeLimit = 10
	config.Engine.MaxConcurrentWorkflows = 10

	return config
}

func main() {
	fmt.Println("Starting Graft Static Discovery Example...")

	config := createStaticConfig()

	fmt.Printf("Node ID: %s\n", config.NodeID)
	fmt.Printf("Service Port: %d\n", config.ServicePort)
	fmt.Printf("Transport Port: %d\n", config.Transport.ListenPort)
	fmt.Printf("Discovery Strategy: %s\n", config.Discovery.Strategy)
	fmt.Printf("Peers: %v\n", config.Discovery.Peers)

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

	processor := &SimpleProcessor{}
	if err := cluster.RegisterNode(processor); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}

	fmt.Printf("Node registered: %s\n", processor.GetName())

	time.Sleep(5 * time.Second)

	if config.NodeID == "static-node-1" || len(config.Discovery.Peers) == 0 {
		fmt.Println("Starting test workflows...")

		workflows := []struct {
			id   string
			data string
		}{
			{"static-workflow-001", "test_data_1"},
			{"static-workflow-002", "test_data_2"},
			{"static-workflow-003", "test_data_3"},
		}

		for _, wf := range workflows {
			trigger := graft.WorkflowTrigger{
				WorkflowID: wf.id,
				InitialState: map[string]interface{}{
					"data": wf.data,
				},
				InitialNodes: []graft.NodeConfig{
					{
						Name:   "static-processor",
						Config: map[string]interface{}{},
					},
				},
				Metadata: map[string]string{
					"discovery": "static",
					"example":   "static-discovery",
				},
			}

			if err := cluster.StartWorkflow(wf.id, trigger); err != nil {
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
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	fmt.Println("Static discovery example running. Press Ctrl+C to exit...")

	for {
		<-ticker.C

		info := cluster.GetClusterInfo()
		fmt.Printf("[%s] Cluster Status - Active Workflows: %d, Resource Usage: %d/%d\n",
			time.Now().Format("15:04:05"),
			info.ActiveWorkflows,
			info.ExecutionStats.TotalExecuting,
			info.ExecutionStats.TotalCapacity,
		)
	}

	fmt.Println("Static discovery example completed!")
}
