package ports

import "context"

type LoadBalancer interface {
	Start(ctx context.Context) error
	Stop() error

	ShouldExecuteNode(nodeID string, workflowID string, nodeName string) (bool, error)
	GetClusterLoad() (map[string]int, error)
	GetNodeLoad(nodeID string) (int, error)
}

type NodeLoad struct {
	NodeID          string `json:"node_id"`
	ActiveWorkflows int    `json:"active_workflows"`
	LastUpdated     int64  `json:"last_updated"`
}