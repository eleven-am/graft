package config

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/eleven-am/graft"
)

type NodeConfig struct {
	NodeID    string
	RaftAddr  string
	GRPCPort  int
	DataDir   string
	Discovery DiscoveryConfig
}

func (nc NodeConfig) RaftPort() int {
	_, portStr, err := net.SplitHostPort(nc.RaftAddr)
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0
	}
	return port
}

type DiscoveryConfig struct {
	Type    string
	Service string
	Domain  string
}

type TestConfig struct {
	Nodes              []NodeConfig
	HandoffTimeout     time.Duration
	WorkflowTimeout    time.Duration
	ReadinessTimeout   time.Duration
	StaggerDelay       time.Duration
	CleanupDelay       time.Duration
	ValidationInterval time.Duration
}

func DefaultTestConfig() TestConfig {
	return TestConfig{
		Nodes: []NodeConfig{
			{
				NodeID:   "node-1",
				RaftAddr: "127.0.0.1:7201",
				GRPCPort: 8201,
				DataDir:  "./test-data/node-1",
				Discovery: DiscoveryConfig{
					Type: "inmemory",
				},
			},
			{
				NodeID:   "node-2",
				RaftAddr: "127.0.0.1:7202",
				GRPCPort: 8202,
				DataDir:  "./test-data/node-2",
				Discovery: DiscoveryConfig{
					Type: "inmemory",
				},
			},
			{
				NodeID:   "node-3",
				RaftAddr: "127.0.0.1:7203",
				GRPCPort: 8203,
				DataDir:  "./test-data/node-3",
				Discovery: DiscoveryConfig{
					Type: "inmemory",
				},
			},
		},
		HandoffTimeout:     30 * time.Second,
		WorkflowTimeout:    15 * time.Second,
		ReadinessTimeout:   10 * time.Second,
		StaggerDelay:       2 * time.Second,
		CleanupDelay:       1 * time.Second,
		ValidationInterval: 500 * time.Millisecond,
	}
}

func (nc NodeConfig) CreateManager(logger *slog.Logger) *graft.Manager {
	manager := graft.New(nc.NodeID, nc.RaftAddr, nc.DataDir, logger)
	if manager == nil {
		return nil
	}

	switch nc.Discovery.Type {
	case "mdns":
		manager.MDNS(nc.Discovery.Service, nc.Discovery.Domain)
	case "inmemory":
		// Provider is injected at launch time by the harness.
	default:
		logger.Warn("unknown discovery type, defaulting to mdns", "type", nc.Discovery.Type)
		manager.MDNS(nc.Discovery.Service, nc.Discovery.Domain)
	}

	if err := manager.RegisterNode(&TestWorkflowNode{}); err != nil {
		logger.Error("failed to register test workflow node", "error", err)
		return nil
	}

	return manager
}

type TestWorkflowNode struct{}

func (n *TestWorkflowNode) GetName() string {
	return "test_node"
}

func (n *TestWorkflowNode) CanStart(ctx context.Context, state interface{}, config interface{}) bool {
	return true
}

func (n *TestWorkflowNode) Execute(ctx context.Context, state interface{}, config interface{}) (*graft.NodeResult, error) {
	configMap, ok := config.(map[string]interface{})
	if !ok {
		configMap = make(map[string]interface{})
	}

	processor, _ := configMap["processor"].(string)
	duration, _ := configMap["duration"].(float64)

	if duration > 0 {
		time.Sleep(time.Duration(duration) * time.Millisecond)
	}

	stateMap, ok := state.(map[string]interface{})
	if !ok {
		stateMap = make(map[string]interface{})
	}

	result := map[string]interface{}{
		"processed_by": processor,
		"processed_at": time.Now().Unix(),
		"status":       "completed",
		"test_result":  "success",
	}

	for k, v := range stateMap {
		result[k] = v
	}

	return &graft.NodeResult{
		GlobalState: result,
		NextNodes:   nil,
	}, nil
}
