package raft

import (
	"context"
	"errors"

	"log/slog"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
)

// FSMFactory provides raft FSM instances compatible with the domain command
// model for the raft runtime.
type FSMFactory struct {
	EventManager ports.EventManager
	Logger       *slog.Logger
}

// Create implements the raft FSMFactory interface.
func (f *FSMFactory) Create(_ context.Context, opts domain.RaftControllerOptions, storage *StorageResources) (raft.FSM, error) {
	if storage == nil {
		return nil, errors.New("raft: storage resources required for FSM creation")
	}

	if storage.StateDB == nil {
		return nil, errors.New("raft: storage missing state database")
	}

	logger := f.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return NewFSM(storage.StateDB, f.EventManager, opts.NodeID, opts.RuntimeConfig.ClusterID, opts.RuntimeConfig.ClusterPolicy, logger), nil
}
