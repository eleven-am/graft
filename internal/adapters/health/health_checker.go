package health

import (
	"encoding/json"
	"log/slog"

	"github.com/eleven-am/graft/internal/ports"
)

type HealthChecker struct {
	loadBalancer   ports.LoadBalancer
	clusterManager ports.ClusterManager
	raftNode       ports.RaftNode
	logger         *slog.Logger
}

type HealthStatus struct {
	Healthy        bool                        `json:"healthy"`
	Status         string                      `json:"status"`
	NodeID         string                      `json:"node_id"`
	IsDraining     bool                        `json:"is_draining"`
	ClusterHealth  *ports.ClusterHealthStatus  `json:"cluster_health,omitempty"`
	RaftHealth     *ports.HealthStatus         `json:"raft_health,omitempty"`
}

func NewHealthChecker(loadBalancer ports.LoadBalancer, clusterManager ports.ClusterManager, raftNode ports.RaftNode, nodeID string, logger *slog.Logger) *HealthChecker {
	if logger == nil {
		logger = slog.Default()
	}

	return &HealthChecker{
		loadBalancer:   loadBalancer,
		clusterManager: clusterManager,
		raftNode:       raftNode,
		logger:         logger.With("component", "health-checker"),
	}
}

func (hc *HealthChecker) GetHealth() *HealthStatus {
	status := &HealthStatus{
		Healthy: true,
		Status:  "healthy",
	}

	if hc.raftNode != nil {
		clusterInfo := hc.raftNode.GetClusterInfo()
		status.NodeID = clusterInfo.NodeID
		
		raftHealth := hc.raftNode.GetHealth()
		status.RaftHealth = &raftHealth
		
		if !raftHealth.Healthy {
			status.Healthy = false
			status.Status = "unhealthy"
		}
	}

	if hc.loadBalancer != nil {
		isDraining := hc.loadBalancer.IsDraining()
		status.IsDraining = isDraining
		
		if isDraining {
			status.Status = "draining"
		}
	}

	if hc.clusterManager != nil {
		clusterHealth := hc.clusterManager.GetClusterHealth()
		status.ClusterHealth = &clusterHealth
		
		if !clusterHealth.IsHealthy {
			status.Healthy = false
			if status.Status == "healthy" {
				status.Status = "cluster_unhealthy"
			}
		}
	}

	return status
}

func (hc *HealthChecker) IsReady() bool {
	health := hc.GetHealth()
	
	if health.IsDraining {
		return false
	}
	
	if hc.clusterManager != nil {
		clusterHealth := hc.clusterManager.GetClusterHealth()
		return clusterHealth.IsMinimumViable
	}
	
	return health.Healthy
}

func (hc *HealthChecker) GetHealthJSON() ([]byte, error) {
	health := hc.GetHealth()
	return json.Marshal(health)
}