package cluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type RaftClusterManager struct {
	raftNode        ports.RaftNode
	logger          *slog.Logger
	minimumNodes    int
	healthThreshold time.Duration
}

func NewRaftClusterManager(raftNode ports.RaftNode, minimumNodes int, logger *slog.Logger) *RaftClusterManager {
	if logger == nil {
		logger = slog.Default()
	}

	return &RaftClusterManager{
		raftNode:        raftNode,
		logger:          logger.With("component", "cluster-manager"),
		minimumNodes:    minimumNodes,
		healthThreshold: 10 * time.Second,
	}
}

func (rcm *RaftClusterManager) GetActiveNodes() []string {
	clusterInfo := rcm.raftNode.GetClusterInfo()

	activeNodes := make([]string, 0, len(clusterInfo.Members))
	for _, member := range clusterInfo.Members {
		if member.State != ports.NodeFollower && member.State != ports.NodeLeader && member.State != ports.NodeCandidate {
			continue
		}
		activeNodes = append(activeNodes, member.ID)
	}

	return activeNodes
}

func (rcm *RaftClusterManager) IsNodeActive(nodeID string) bool {
	activeNodes := rcm.GetActiveNodes()
	for _, active := range activeNodes {
		if active == nodeID {
			return true
		}
	}
	return false
}

func (rcm *RaftClusterManager) GetClusterHealth() ports.ClusterHealthStatus {
	clusterInfo := rcm.raftNode.GetClusterInfo()
	health := rcm.raftNode.GetHealth()

	activeNodes := rcm.GetActiveNodes()
	totalNodes := len(clusterInfo.Members)
	healthyNodes := len(activeNodes)

	unhealthy := make([]string, 0)
	for _, member := range clusterInfo.Members {
		isActive := false
		for _, active := range activeNodes {
			if active == member.ID {
				isActive = true
				break
			}
		}
		if !isActive {
			unhealthy = append(unhealthy, member.ID)
		}
	}

	leaderID := ""
	if clusterInfo.Leader != nil {
		leaderID = clusterInfo.Leader.ID
	}

	isMinimumViable := healthyNodes >= rcm.minimumNodes
	isHealthy := health.Healthy && isMinimumViable

	return ports.ClusterHealthStatus{
		IsHealthy:       isHealthy,
		TotalNodes:      totalNodes,
		HealthyNodes:    healthyNodes,
		LeaderID:        leaderID,
		UnhealthyNodes:  unhealthy,
		MinimumNodes:    rcm.minimumNodes,
		IsMinimumViable: isMinimumViable,
	}
}

func (rcm *RaftClusterManager) WaitForHealthyCluster(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			health := rcm.GetClusterHealth()
			if health.IsHealthy {
				rcm.logger.Info("cluster is healthy",
					"healthy_nodes", health.HealthyNodes,
					"total_nodes", health.TotalNodes,
					"leader_id", health.LeaderID)
				return nil
			}
			rcm.logger.Debug("waiting for healthy cluster",
				"healthy_nodes", health.HealthyNodes,
				"minimum_nodes", health.MinimumNodes,
				"total_nodes", health.TotalNodes)
		}
	}
}
