package shutdown

import (
	"context"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type GracefulShutdownManager struct {
	loadBalancer   ports.LoadBalancer
	clusterManager ports.ClusterManager
	raftNode       ports.RaftNode
	logger         *slog.Logger
	drainTimeout   time.Duration
}

func NewGracefulShutdownManager(loadBalancer ports.LoadBalancer, clusterManager ports.ClusterManager, raftNode ports.RaftNode, logger *slog.Logger) *GracefulShutdownManager {
	if logger == nil {
		logger = slog.Default()
	}

	return &GracefulShutdownManager{
		loadBalancer:   loadBalancer,
		clusterManager: clusterManager,
		raftNode:       raftNode,
		logger:         logger.With("component", "graceful-shutdown"),
		drainTimeout:   30 * time.Second,
	}
}

func (gsm *GracefulShutdownManager) InitiateGracefulShutdown(ctx context.Context) error {
	gsm.logger.Info("initiating graceful shutdown")

	if gsm.loadBalancer != nil {
		gsm.logger.Info("starting work draining")
		if err := gsm.loadBalancer.StartDraining(); err != nil {
			gsm.logger.Error("failed to start draining", "error", err)
			return domain.NewDiscoveryError("shutdown", "start_draining", err)
		}

		drainCtx, cancel := context.WithTimeout(ctx, gsm.drainTimeout)
		defer cancel()

		gsm.logger.Info("waiting for work to complete", "timeout", gsm.drainTimeout)
		if err := gsm.loadBalancer.WaitForDraining(drainCtx); err != nil {
			gsm.logger.Warn("drain timeout reached, continuing with shutdown", "error", err)
		}
	}

	if gsm.raftNode != nil && gsm.raftNode.IsLeader() {
		gsm.logger.Info("transferring leadership before shutdown")
		if err := gsm.raftNode.TransferLeadership(); err != nil {
			gsm.logger.Warn("failed to transfer leadership", "error", err)
		} else {
			time.Sleep(2 * time.Second)
		}
	}

	gsm.logger.Info("graceful shutdown preparation complete")
	return nil
}

func (gsm *GracefulShutdownManager) SetDrainTimeout(timeout time.Duration) {
	gsm.drainTimeout = timeout
}
