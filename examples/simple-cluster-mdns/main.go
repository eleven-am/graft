package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
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
	mdnsService := getEnv("GRAFT_MDNS_SERVICE", "graft-example")
	mdnsDomain := getEnv("GRAFT_MDNS_DOMAIN", "local.")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("starting graft node with mDNS discovery",
		"node_id", nodeID,
		"raft_addr", raftAddr,
		"grpc_port", grpcPort,
		"data_dir", dataDir,
		"mdns_service", mdnsService,
		"mdns_domain", mdnsDomain)

	normalizedRaftAddr, err := resolveAdvertiseAddress(raftAddr)
	if err != nil {
		logger.Error("failed to resolve raft address", "raft_addr", raftAddr, "error", err)
		os.Exit(1)
	}
	if normalizedRaftAddr != raftAddr {
		logger.Info("resolved raft advertise address",
			"original", raftAddr,
			"resolved", normalizedRaftAddr)
		raftAddr = normalizedRaftAddr
	}

	clusterID := getEnv("GRAFT_CLUSTER_ID", "mdns-cluster-example")

	config := graft.NewConfigBuilder(nodeID, raftAddr, dataDir).
		WithClusterID(clusterID).
		WithMDNS(mdnsService, mdnsDomain, "", true).
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

func resolveAdvertiseAddress(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, nil
	}

	if ip := net.ParseIP(host); ip != nil && !ip.IsUnspecified() {
		return addr, nil
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return addr, err
	}
	if len(ips) == 0 {
		return addr, errors.New("no IP addresses returned for host")
	}

	var chosen net.IP
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			chosen = v4
			break
		}
	}
	if chosen == nil {
		chosen = ips[0]
	}

	return net.JoinHostPort(chosen.String(), port), nil
}
