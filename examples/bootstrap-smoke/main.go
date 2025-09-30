package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/eleven-am/graft"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dataDir, err := os.MkdirTemp("", "graft-bootstrap-smoke-")
	if err != nil {
		logger.Error("failed to create temp data dir", "error", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dataDir)

	logger.Info("bootstrap smoke test", "data_dir", dataDir)

	manager := graft.New("bootstrap-smoke", "127.0.0.1:0", dataDir, logger)
	if manager == nil {
		logger.Error("failed to create manager instance")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	startErr := make(chan error, 1)
	go func() {
		startErr <- manager.Start(ctx, 0)
	}()

	select {
	case err := <-startErr:
		if err != nil {
			logger.Error("manager start failed", "error", err)
			os.Exit(1)
		}
	case <-time.After(5 * time.Second):
		logger.Info("manager still starting after 5s")
	}

	readyCtx, readyCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer readyCancel()

	if err := manager.WaitUntilReady(readyCtx); err != nil {
		logger.Error("manager failed to report ready", "error", err)
		_ = manager.Stop()
		os.Exit(1)
	}

	info := manager.GetClusterInfo()
	logger.Info("manager ready", "is_leader", info.IsLeader, "peers", info.Peers)

	time.Sleep(2 * time.Second)

	if err := manager.Stop(); err != nil {
		logger.Warn("manager stop returned error", "error", err)
	}

	logger.Info("bootstrap smoke test complete")
}
