package core

import (
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/adapters/engine"
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
	logger *slog.Logger,
) ports.EnginePort {
	config := engine.Config{
		MaxConcurrentWorkflows: 10,
		NodeExecutionTimeout:   5 * time.Minute,
		StateUpdateInterval:    30 * time.Second,
		RetryAttempts:          3,
		RetryBackoff:           1 * time.Second,
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
