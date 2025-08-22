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

type KubernetesProcessor struct{}

func (n *KubernetesProcessor) GetName() string {
	return "k8s-processor"
}

func (n *KubernetesProcessor) Execute(ctx context.Context, config map[string]interface{}, state map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(250 * time.Millisecond)

	input, exists := state["data"]
	if !exists {
		return nil, fmt.Errorf("missing data in state")
	}

	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		podName = "unknown-pod"
	}

	namespace := os.Getenv("NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	return map[string]interface{}{
		"processed_data": fmt.Sprintf("k8s_%v", input),
		"processing_pod": podName,
		"namespace":      namespace,
		"discovery_type": "kubernetes",
		"processed_at":   time.Now().Format(time.RFC3339),
	}, nil
}

func (n *KubernetesProcessor) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{
		"data": "string",
	}
}

func (n *KubernetesProcessor) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{
		"processed_data": "string",
		"processing_pod": "string",
		"namespace":      "string",
		"discovery_type": "string",
		"processed_at":   "string",
	}
}

func createKubernetesConfig() graft.Config {
	config := graft.DefaultConfig()

	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		podName = fmt.Sprintf("k8s-node-%d", time.Now().Unix()%1000)
	}
	config.NodeID = podName

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

	config.Discovery.Strategy = graft.StrategyKubernetes

	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "graft-k8s-cluster"
	}
	config.Discovery.ServiceName = serviceName

	config.ServiceName = serviceName

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/data"
	}
	config.Storage.DataDir = fmt.Sprintf("%s/%s", dataDir, podName)
	config.Queue.DataDir = fmt.Sprintf("%s/%s/queue", dataDir, podName)

	config.Resources.MaxConcurrentTotal = 50
	config.Resources.DefaultPerTypeLimit = 20
	config.Engine.MaxConcurrentWorkflows = 25
	config.Engine.NodeExecutionTimeout = "45s"

	return config
}

func main() {
	fmt.Println("Starting Graft Kubernetes Discovery Example...")

	config := createKubernetesConfig()

	fmt.Printf("Pod Name: %s\n", config.NodeID)
	fmt.Printf("Service Port: %d\n", config.ServicePort)
	fmt.Printf("Transport Port: %d\n", config.Transport.ListenPort)
	fmt.Printf("Discovery Strategy: %s\n", config.Discovery.Strategy)
	fmt.Printf("Kubernetes Service: %s\n", config.Discovery.ServiceName)

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

	fmt.Println("Cluster started successfully - discovering via Kubernetes API")

	processor := &KubernetesProcessor{}
	if err := cluster.RegisterNode(processor); err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}

	fmt.Printf("Node registered: %s\n", processor.GetName())

	fmt.Println("Waiting for Kubernetes service discovery (20 seconds)...")
	time.Sleep(20 * time.Second)

	info := cluster.GetClusterInfo()
	fmt.Printf("Discovered Kubernetes cluster members: %v\n", info.RegisteredNodes)

	fmt.Println("Starting Kubernetes workflows...")

	workflows := []struct {
		id   string
		data string
	}{
		{"k8s-workflow-001", "pod_data_1"},
		{"k8s-workflow-002", "pod_data_2"},
		{"k8s-workflow-003", "pod_data_3"},
		{"k8s-workflow-004", "pod_data_4"},
	}

	for _, wf := range workflows {
		trigger := graft.WorkflowTrigger{
			WorkflowID: wf.id,
			InitialState: map[string]interface{}{
				"data": wf.data,
			},
			InitialNodes: []graft.NodeConfig{
				{
					Name:   "k8s-processor",
					Config: map[string]interface{}{},
				},
			},
			Metadata: map[string]string{
				"discovery":      "kubernetes",
				"example":        "k8s-native",
				"started_by_pod": config.NodeID,
			},
		}

		if err := cluster.StartWorkflow(wf.id, trigger); err != nil {
			log.Printf("Failed to start workflow %s: %v", wf.id, err)
			continue
		}

		fmt.Printf("Started workflow: %s\n", wf.id)
	}

	completed := make(map[string]bool)
	maxWait := 90 * time.Second
	start := time.Now()

	for len(completed) < len(workflows) && time.Since(start) < maxWait {
		time.Sleep(5 * time.Second)

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
				processingPod := "unknown"
				if stateMap, ok := state.CurrentState.(map[string]interface{}); ok {
					if pod, exists := stateMap["processing_pod"]; exists {
						processingPod = fmt.Sprintf("%v", pod)
					}
				}
				fmt.Printf("Workflow %s completed on pod: %v\n", wf.id, processingPod)
			} else if state.Status == "failed" {
				completed[wf.id] = true
				fmt.Printf("Workflow %s failed\n", wf.id)
				if state.LastError != nil {
					fmt.Printf("Error: %s\n", *state.LastError)
				}
			}
		}
	}

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	fmt.Println("Kubernetes discovery example running...")
	fmt.Println("Scale the deployment to see dynamic cluster changes!")

	for {
		<-ticker.C

		info := cluster.GetClusterInfo()
		fmt.Printf("[%s] Kubernetes Cluster Status:\n", time.Now().Format("15:04:05"))
		fmt.Printf("  Pod: %s\n", config.NodeID)
		fmt.Printf("  Active Workflows: %d\n", info.ActiveWorkflows)
		fmt.Printf("  Resource Usage: %d/%d\n", info.ExecutionStats.TotalExecuting, info.ExecutionStats.TotalCapacity)
		fmt.Printf("  Current Cluster Members: %v\n", info.RegisteredNodes)
		fmt.Println()
	}

	fmt.Println("Kubernetes discovery example completed!")
}
