package engine

import (
	"context"
	json "github.com/goccy/go-json"
	"fmt"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type StateManager struct {
	storage ports.StoragePort
	logger  *slog.Logger
}

func NewStateManager(storage ports.StoragePort, logger *slog.Logger) *StateManager {
	return &StateManager{
		storage: storage,
		logger:  logger.With("component", "state_manager"),
	}
}

func (sm *StateManager) SaveWorkflowState(ctx context.Context, workflow *domain.WorkflowInstance) error {
	sm.logger.Debug("saving workflow state", 
		"workflow_id", workflow.ID, 
		"status", workflow.Status,
		"version", workflow.Version)

	key := fmt.Sprintf("workflow:state:%s", workflow.ID)
	
	workflowBytes, err := json.Marshal(workflow)
	if err != nil {
		return domain.NewDiscoveryError("state_manager", "marshal_workflow", err)
	}

	if err := sm.storage.Put(key, workflowBytes, workflow.Version); err != nil {
		return domain.NewDiscoveryError("state_manager", "save_workflow_state", err)
	}

	sm.logger.Debug("workflow state saved successfully", 
		"workflow_id", workflow.ID,
		"version", workflow.Version)
	
	return nil
}

func (sm *StateManager) LoadWorkflowState(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {
	sm.logger.Debug("loading workflow state", "workflow_id", workflowID)
	
	key := fmt.Sprintf("workflow:state:%s", workflowID)
	
	data, _, exists, err := sm.storage.Get(key)
	if err != nil {
		return nil, domain.NewDiscoveryError("state_manager", "load_workflow_state", err)
	}
	if !exists {
		return nil, domain.ErrNotFound
	}

	var workflow domain.WorkflowInstance
	if err := json.Unmarshal(data, &workflow); err != nil {
		return nil, domain.NewDiscoveryError("state_manager", "unmarshal_workflow", err)
	}

	sm.logger.Debug("workflow state loaded successfully", 
		"workflow_id", workflowID,
		"status", workflow.Status,
		"version", workflow.Version)

	return &workflow, nil
}

func (sm *StateManager) UpdateWorkflowState(ctx context.Context, workflowID string, updateFn func(*domain.WorkflowInstance) error) error {
	sm.logger.Debug("updating workflow state with versioning", "workflow_id", workflowID)
	
	key := fmt.Sprintf("workflow:state:%s", workflowID)

	for retries := 0; retries < 10; retries++ {
		workflow, err := sm.LoadWorkflowState(ctx, workflowID)
		if err != nil {
			return domain.NewDiscoveryError("state_manager", "load_workflow_for_update", err)
		}

		oldVersion := workflow.Version
		
		if err := updateFn(workflow); err != nil {
			return domain.NewDiscoveryError("state_manager", "update_function", err)
		}

		workflow.Version++

		newBytes, err := json.Marshal(workflow)
		if err != nil {
			return domain.NewDiscoveryError("state_manager", "marshal_updated_workflow", err)
		}

		err = sm.storage.Put(key, newBytes, oldVersion+1)
		if err == nil {
			sm.logger.Debug("workflow state updated successfully", 
				"workflow_id", workflowID,
				"old_version", oldVersion,
				"new_version", workflow.Version,
				"retries", retries)
			return nil
		}

		if retries == 9 {
			return domain.NewDiscoveryError("state_manager", "update_workflow_state_retries", err)
		}

		sm.logger.Debug("update failed, retrying", 
			"workflow_id", workflowID, 
			"retry", retries,
			"error", err)
		
		time.Sleep(time.Duration(retries) * 10 * time.Millisecond)
	}

	return domain.ErrTimeout
}