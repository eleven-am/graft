package ports

import "context"

type ClusterManager interface {
	GetActiveNodes() []string
	IsNodeActive(nodeID string) bool
	GetClusterHealth() ClusterHealthStatus
	WaitForHealthyCluster(ctx context.Context) error
}

type ClusterHealthStatus struct {
	IsHealthy       bool     `json:"is_healthy"`
	TotalNodes      int      `json:"total_nodes"`
	HealthyNodes    int      `json:"healthy_nodes"`
	LeaderID        string   `json:"leader_id,omitempty"`
	UnhealthyNodes  []string `json:"unhealthy_nodes,omitempty"`
	MinimumNodes    int      `json:"minimum_nodes"`
	IsMinimumViable bool     `json:"is_minimum_viable"`
}
