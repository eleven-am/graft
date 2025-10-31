package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eleven-am/graft"
)

func main() {
	nodeID := getEnv("GRAFT_NODE_ID", "node-1")
	raftAddr := getEnv("GRAFT_RAFT_ADDR", "127.0.0.1:7001")
	grpcPort := getEnv("GRAFT_GRPC_PORT", "9001")
	dataDir := getEnv("GRAFT_DATA_DIR", "./data/node-1")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("starting graft node with static discovery",
		"node_id", nodeID,
		"raft_addr", raftAddr,
		"grpc_port", grpcPort,
		"data_dir", dataDir)

	clusterID := getEnv("GRAFT_CLUSTER_ID", "static-cluster-example")

	config := graft.NewConfigBuilder(nodeID, raftAddr, dataDir).
		WithClusterID(clusterID).
		WithStaticPeers(
			graft.StaticPeer{
				ID:      "node-1",
				Address: "127.0.0.1",
				Port:    7001,
				Metadata: map[string]string{
					"grpc_port": "9001",
				},
			},
			graft.StaticPeer{
				ID:      "node-2",
				Address: "127.0.0.1",
				Port:    7002,
				Metadata: map[string]string{
					"grpc_port": "9002",
				},
			},
			graft.StaticPeer{
				ID:      "node-3",
				Address: "127.0.0.1",
				Port:    7003,
				Metadata: map[string]string{
					"grpc_port": "9003",
				},
			},
		).
		Build()

	config.Logger = logger

	manager := graft.NewWithConfig(config)
	if manager == nil {
		logger.Error("failed to create manager")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	startErr := make(chan error, 1)
	go func() {
		port := 9001
		if grpcPort == "9002" {
			port = 9002
		} else if grpcPort == "9003" {
			port = 9003
		}
		startErr <- manager.Start(ctx, port)
	}()

	select {
	case err := <-startErr:
		if err != nil {
			logger.Error("manager start failed", "error", err)
			os.Exit(1)
		}
	case <-time.After(5 * time.Second):
		logger.Info("manager starting...")
	}

	readyCtx, readyCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer readyCancel()

	if err := manager.WaitUntilReady(readyCtx); err != nil {
		logger.Error("manager failed to become ready", "error", err)
		_ = manager.Stop()
		os.Exit(1)
	}

	go monitorCluster(ctx, manager, logger)

	logger.Info("node is ready", "node_id", nodeID)

	<-ctx.Done()

	logger.Info("shutting down node", "node_id", nodeID)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := manager.Stop(); err != nil {
		logger.Error("error during shutdown", "error", err)
	}

	<-shutdownCtx.Done()
	logger.Info("node stopped", "node_id", nodeID)
}

func monitorCluster(ctx context.Context, manager *graft.Manager, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info := manager.GetClusterInfo()
			logger.Info("cluster status",
				"node_id", info.NodeID,
				"status", info.Status,
				"is_leader", info.IsLeader,
				"peers", len(info.Peers))
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
