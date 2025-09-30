package raft2

import (
	"context"
	"fmt"
	"path/filepath"

	"log/slog"

	"github.com/eleven-am/graft/internal/domain"
)

// BadgerStorageProvider provisions persistent storage for raft2 nodes.
type BadgerStorageProvider struct {
	Logger *slog.Logger
}

// Create initialises storage resources in the controller's data directory.
func (p *BadgerStorageProvider) Create(_ context.Context, opts domain.RaftControllerOptions) (*StorageResources, error) {
	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}

	dataDir := opts.DataDir
	if dataDir == "" {
		return nil, fmt.Errorf("raft2: data directory not provided")
	}

	storage, err := NewStorage(StorageConfig{DataDir: filepath.Join(dataDir, "raft2")}, logger)
	if err != nil {
		return nil, fmt.Errorf("raft2: storage init failed: %w", err)
	}

	return storage.resources(), nil
}
