package harness

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"bootstrap-handoff-test/config"
	"github.com/eleven-am/graft"
	"github.com/eleven-am/graft/testsupport/localdiscovery"
)

type NodeInstance struct {
	Manager   *graft.Manager
	Config    config.NodeConfig
	StartedAt time.Time
	Ready     bool
	mu        sync.RWMutex
}

func (n *NodeInstance) IsReady() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Ready
}

func (n *NodeInstance) SetReady(ready bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Ready = ready
}

type NodeLauncher struct {
	logger    *slog.Logger
	instances map[string]*NodeInstance
	mu        sync.RWMutex
	hub       *localdiscovery.Hub
	launches  int
}

func NewNodeLauncher(logger *slog.Logger) *NodeLauncher {
	return &NodeLauncher{
		logger:    logger,
		instances: make(map[string]*NodeInstance),
		hub:       localdiscovery.NewHub(logger),
	}
}

func (nl *NodeLauncher) LaunchNode(ctx context.Context, cfg config.NodeConfig) (*NodeInstance, error) {
	nl.mu.Lock()
	defer nl.mu.Unlock()

	if _, exists := nl.instances[cfg.NodeID]; exists {
		return nil, fmt.Errorf("node %s already launched", cfg.NodeID)
	}

	uniqueDataDir := filepath.Clean(fmt.Sprintf("%s-%d", cfg.DataDir, time.Now().UnixNano()))
	if err := os.MkdirAll(uniqueDataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	actualCfg := cfg
	nl.launches++
	offset := nl.launches * 10
	actualCfg.RaftAddr = fmt.Sprintf("127.0.0.1:%d", cfg.RaftPort()+offset)
	actualCfg.GRPCPort = cfg.GRPCPort + offset
	actualCfg.DataDir = uniqueDataDir

	if actualCfg.Discovery.Type == "inmemory" {
		graft.ResetBootstrapMetadataForTesting()
	}

	manager := actualCfg.CreateManager(nl.logger)
	if manager == nil {
		return nil, fmt.Errorf("failed to create manager for node %s", actualCfg.NodeID)
	}

	if actualCfg.Discovery.Type == "inmemory" {
		manager.AddProvider(nl.hub.NewProvider())
	}

	instance := &NodeInstance{
		Manager:   manager,
		Config:    actualCfg,
		StartedAt: time.Now(),
		Ready:     false,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Start(ctx, actualCfg.GRPCPort)
	}()

	var startErr error
	select {
	case err := <-errCh:
		startErr = err
	case <-time.After(15 * time.Second):
		startErr = fmt.Errorf("node %s start timed out", actualCfg.NodeID)
		_ = manager.Stop()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
		}
	}

	if startErr != nil {
		return nil, startErr
	}

	nl.instances[cfg.NodeID] = instance

	go nl.monitorReadiness(ctx, instance)

	nl.logger.Info("node launched", "node_id", actualCfg.NodeID, "raft_addr", actualCfg.RaftAddr, "grpc_port", actualCfg.GRPCPort)
	return instance, nil
}

func (nl *NodeLauncher) LaunchStaggered(ctx context.Context, configs []config.NodeConfig, delay time.Duration) ([]*NodeInstance, error) {
	var instances []*NodeInstance

	for i, cfg := range configs {
		instance, err := nl.LaunchNode(ctx, cfg)
		if err != nil {
			return instances, fmt.Errorf("failed to launch node %s: %w", cfg.NodeID, err)
		}
		instances = append(instances, instance)

		if i < len(configs)-1 {
			nl.logger.Info("waiting before launching next node", "delay", delay)
			time.Sleep(delay)
		}
	}

	return instances, nil
}

func (nl *NodeLauncher) GetInstance(nodeID string) *NodeInstance {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	return nl.instances[nodeID]
}

func (nl *NodeLauncher) GetAllInstances() []*NodeInstance {
	nl.mu.RLock()
	defer nl.mu.RUnlock()

	instances := make([]*NodeInstance, 0, len(nl.instances))
	for _, instance := range nl.instances {
		instances = append(instances, instance)
	}
	return instances
}

func (nl *NodeLauncher) StopNode(nodeID string) error {
	nl.mu.Lock()
	defer nl.mu.Unlock()

	instance, exists := nl.instances[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if err := instance.Manager.Stop(); err != nil {
		nl.logger.Error("failed to stop node", "node_id", nodeID, "error", err)
		return err
	}

	delete(nl.instances, nodeID)

	if err := os.RemoveAll(instance.Config.DataDir); err != nil {
		nl.logger.Warn("failed to cleanup data dir", "node_id", nodeID, "error", err)
	}

	nl.logger.Info("node stopped and cleaned up", "node_id", nodeID)
	return nil
}

func (nl *NodeLauncher) StopAll() {
	nl.mu.RLock()
	nodeIDs := make([]string, 0, len(nl.instances))
	for nodeID := range nl.instances {
		nodeIDs = append(nodeIDs, nodeID)
	}
	nl.mu.RUnlock()

	for _, nodeID := range nodeIDs {
		if err := nl.StopNode(nodeID); err != nil {
			nl.logger.Error("failed to stop node during cleanup", "node_id", nodeID, "error", err)
		}
	}
}

func (nl *NodeLauncher) Cleanup() {
	nl.StopAll()
	if err := os.RemoveAll("./test-data"); err != nil {
		nl.logger.Warn("failed to cleanup test data directory", "error", err)
	}
}

func (nl *NodeLauncher) WaitForReadiness(ctx context.Context, nodeID string, timeout time.Duration) error {
	instance := nl.GetInstance(nodeID)
	if instance == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return instance.Manager.WaitUntilReady(readyCtx)
}

func (nl *NodeLauncher) monitorReadiness(ctx context.Context, instance *NodeInstance) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ready := instance.Manager.IsReady()
			if ready != instance.IsReady() {
				instance.SetReady(ready)
				nl.logger.Info("node readiness changed",
					"node_id", instance.Config.NodeID,
					"ready", ready,
					"readiness_state", instance.Manager.GetReadinessState())
			}
		}
	}
}
