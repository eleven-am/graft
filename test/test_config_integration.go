package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/eleven-am/graft"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	logger.Info("Testing simple configuration...")
	simpleManager := graft.New("test-simple", "localhost:7001", "./data-simple", logger)
	if simpleManager == nil {
		logger.Error("Failed to create simple manager")
		os.Exit(1)
	}

	logger.Info("Simple manager created successfully")
	logger.Info("Testing advanced configuration...")
	config := graft.NewConfigBuilder("test-advanced", "localhost:7002", "./data-advanced").
		WithMDNS("_graft._tcp", "local.", "").
		WithResourceLimits(100, 10, map[string]int{
			"test": 5,
		}).
		WithEngineSettings(50, 5*time.Minute, 3).
		Build()

	config.Logger = logger
	advancedManager := graft.NewWithConfig(config)
	if advancedManager == nil {
		logger.Error("Failed to create advanced manager")
		os.Exit(1)
	}
	logger.Info("Advanced manager created successfully")

	logger.Info("Testing cluster info...")
	info := simpleManager.GetClusterInfo()
	logger.Info("Cluster info retrieved", "node_id", info.NodeID, "status", info.Status)

	info2 := advancedManager.GetClusterInfo()
	logger.Info("Advanced cluster info retrieved", "node_id", info2.NodeID, "status", info2.Status)
	logger.Info("All configuration tests passed successfully!")
}
