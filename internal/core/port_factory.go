package core

import (
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/adapters/engine"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func createWorkflowEngine(
	storage ports.StoragePort,
	readyQueue ports.QueuePort,
	pendingQueue ports.QueuePort,
	nodeRegistry ports.NodeRegistryPort,
	resourceManager ports.ResourceManagerPort,
	semaphore ports.SemaphorePort,
	raft ports.RaftPort,
	engineConfig *domain.EngineConfig,
	logger *slog.Logger,
) ports.EnginePort {
	var config engine.Config
	if engineConfig != nil {
		config = engine.Config{
			MaxConcurrentWorkflows: engineConfig.MaxConcurrentWorkflows,
			NodeExecutionTimeout:   engineConfig.NodeExecutionTimeout,
			StateUpdateInterval:    engineConfig.StateUpdateInterval,
			RetryAttempts:          engineConfig.RetryAttempts,
			RetryBackoff:           engineConfig.RetryBackoff,
		}
	} else {
		config = engine.Config{
			MaxConcurrentWorkflows: 10,
			NodeExecutionTimeout:   5 * time.Minute,
			StateUpdateInterval:    30 * time.Second,
			RetryAttempts:          3,
			RetryBackoff:           1 * time.Second,
		}
	}

	eng := engine.NewEngine(config, logger)
	eng.SetStorage(storage)
	eng.SetReadyQueue(readyQueue)
	eng.SetPendingQueue(pendingQueue)
	eng.SetNodeRegistry(nodeRegistry)
	eng.SetResourceManager(resourceManager)
	eng.SetSemaphore(semaphore)
	eng.SetRaft(raft)

	return eng
}
