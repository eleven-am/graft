package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type StateManager struct {
	engine *Engine
}

func NewStateManager(engine *Engine) *StateManager {
	return &StateManager{engine: engine}
}

type WorkflowStateData struct {
	ID           string            `json:"id"`
	Status       string            `json:"status"`
	CurrentState interface{}       `json:"current_state"`
	StartedAt    time.Time         `json:"started_at"`
	CompletedAt  *time.Time        `json:"completed_at,omitempty"`
	Metadata     map[string]string `json:"metadata"`
	LastError    *string           `json:"last_error,omitempty"`
	Version      int               `json:"version"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

func (sm *StateManager) SaveWorkflowState(ctx context.Context, workflow *WorkflowInstance) error {
	workflow.mu.RLock()
	defer workflow.mu.RUnlock()

	stateData := sm.createStateData(workflow)

	serializedData, err := json.Marshal(stateData)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to serialize workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflow.ID,
				"error":       err.Error(),
			},
		}
	}

	stateKey := sm.generateStateKey(workflow.ID)
	if err := sm.engine.storage.Put(ctx, stateKey, serializedData); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to persist workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflow.ID,
				"error":       err.Error(),
			},
		}
	}

	stateSize := 0
	if stateMap, ok := workflow.CurrentState.(map[string]interface{}); ok {
		stateSize = len(stateMap)
	}

	sm.engine.logger.Debug("workflow state saved",
		"workflow_id", workflow.ID,
		"status", workflow.Status,
		"state_size", stateSize,
	)

	go sm.triggerEvaluation(ctx, workflow)

	return nil
}

func (sm *StateManager) LoadWorkflowState(ctx context.Context, workflowID string) (*WorkflowInstance, error) {
	stateKey := sm.generateStateKey(workflowID)

	data, err := sm.engine.storage.Get(ctx, stateKey)
	if err != nil {
		sm.engine.logger.Error("storage get failed for workflow state",
			"workflow_id", workflowID,
			"state_key", stateKey,
			"error", err.Error())
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to load workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	// Check for empty or nil data before attempting to unmarshal
	if data == nil {
		sm.engine.logger.Warn("workflow state data is nil, key not found",
			"workflow_id", workflowID,
			"state_key", stateKey)
		return nil, nil
	}

	if len(data) == 0 {
		sm.engine.logger.Warn("workflow state data is empty (zero length)",
			"workflow_id", workflowID,
			"state_key", stateKey)
		return nil, nil
	}

	var stateData WorkflowStateData
	if err := json.Unmarshal(data, &stateData); err != nil {
		sm.engine.logger.Error("failed to unmarshal workflow state data",
			"workflow_id", workflowID,
			"data_length", len(data),
			"data_preview", string(data[:min(50, len(data))]),
			"error", err.Error())
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to deserialize workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
				"data_length": len(data),
			},
		}
	}

	workflow := sm.createWorkflowInstance(stateData)

	stateSize := 0
	if stateMap, ok := workflow.CurrentState.(map[string]interface{}); ok {
		stateSize = len(stateMap)
	}

	sm.engine.logger.Debug("workflow state loaded",
		"workflow_id", workflowID,
		"status", workflow.Status,
		"state_size", stateSize,
	)

	return workflow, nil
}

func (sm *StateManager) DeleteWorkflowState(ctx context.Context, workflowID string) error {
	stateKey := sm.generateStateKey(workflowID)

	if err := sm.engine.storage.Delete(ctx, stateKey); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to delete workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	sm.engine.logger.Debug("workflow state deleted",
		"workflow_id", workflowID,
	)

	return nil
}

func (sm *StateManager) ListWorkflowStates(ctx context.Context) ([]*WorkflowInstance, error) {
	if sm.engine.storage == nil {
		sm.engine.logger.Debug("storage not available, returning empty workflow states list")
		return []*WorkflowInstance{}, nil
	}

	prefix := "workflow:state:"

	items, err := sm.engine.storage.List(ctx, prefix)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list workflow states",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	workflows := make([]*WorkflowInstance, 0, len(items))

	for _, item := range items {
		var stateData WorkflowStateData
		if err := json.Unmarshal(item.Value, &stateData); err != nil {
			sm.engine.logger.Error("failed to deserialize workflow state",
				"key", item.Key,
				"error", err.Error(),
			)
			continue
		}

		workflow := sm.createWorkflowInstance(stateData)

		workflows = append(workflows, workflow)
	}

	sm.engine.logger.Debug("workflow states listed",
		"count", len(workflows),
	)

	return workflows, nil
}

func (sm *StateManager) UpdateWorkflowState(ctx context.Context, workflowID string, updates map[string]interface{}) error {
	workflow, err := sm.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return err
	}

	if workflow == nil {
		return domain.NewNotFoundError("workflow", workflowID)
	}

	var stateMap map[string]interface{}
	if workflow.CurrentState == nil {
		stateMap = make(map[string]interface{})
		workflow.CurrentState = stateMap
	} else if existingMap, ok := workflow.CurrentState.(map[string]interface{}); ok {
		stateMap = existingMap
	} else {
		stateMap = make(map[string]interface{})
		workflow.CurrentState = stateMap
	}

	for key, value := range updates {
		stateMap[key] = value
	}

	totalStateKeys := 0
	if stateMap, ok := workflow.CurrentState.(map[string]interface{}); ok {
		totalStateKeys = len(stateMap)
	}

	sm.engine.logger.Debug("workflow state updated",
		"workflow_id", workflowID,
		"updates", len(updates),
		"total_state_keys", totalStateKeys,
	)

	return sm.SaveWorkflowState(ctx, workflow)
}

func (sm *StateManager) GetWorkflowState(ctx context.Context, workflowID string) (map[string]interface{}, error) {
	workflow, err := sm.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	workflow.mu.RLock()
	defer workflow.mu.RUnlock()

	state := make(map[string]interface{})
	if stateMap, ok := workflow.CurrentState.(map[string]interface{}); ok {
		for k, v := range stateMap {
			state[k] = v
		}
	}

	return state, nil
}

func (sm *StateManager) CleanupCompletedWorkflows(ctx context.Context, olderThan time.Duration) error {
	workflows, err := sm.ListWorkflowStates(ctx)
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-olderThan)
	cleaned := 0

	for _, workflow := range workflows {
		if workflow.Status == ports.WorkflowStateCompleted || workflow.Status == ports.WorkflowStateFailed {
			if workflow.CompletedAt != nil && workflow.CompletedAt.Before(cutoff) {
				if err := sm.DeleteWorkflowState(ctx, workflow.ID); err != nil {
					sm.engine.logger.Error("failed to cleanup workflow state",
						"workflow_id", workflow.ID,
						"error", err.Error(),
					)
					continue
				}
				cleaned++
			}
		}
	}

	sm.engine.logger.Debug("completed workflow cleanup",
		"total_workflows", len(workflows),
		"cleaned_count", cleaned,
		"cutoff_time", cutoff,
	)

	return nil
}

func (sm *StateManager) generateStateKey(workflowID string) string {
	return fmt.Sprintf("workflow:state:%s", workflowID)
}

func (sm *StateManager) CreateCheckpoint(ctx context.Context, workflowID string) error {
	workflow, err := sm.LoadWorkflowState(ctx, workflowID)
	if err != nil {
		return err
	}

	if workflow == nil {
		return domain.NewNotFoundError("workflow", workflowID)
	}

	checkpointKey := fmt.Sprintf("workflow:checkpoint:%s:%d", workflowID, time.Now().Unix())

	workflow.mu.RLock()
	checkpointData := sm.createStateData(workflow)
	workflow.mu.RUnlock()

	serializedData, err := json.Marshal(checkpointData)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to serialize checkpoint data",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	if err := sm.engine.storage.Put(ctx, checkpointKey, serializedData); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create checkpoint",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	sm.engine.logger.Debug("workflow checkpoint created",
		"workflow_id", workflowID,
		"checkpoint_key", checkpointKey,
	)

	return nil
}

func (sm *StateManager) createStateData(workflow *WorkflowInstance) WorkflowStateData {
	return WorkflowStateData{
		ID:           workflow.ID,
		Status:       string(workflow.Status),
		CurrentState: workflow.CurrentState,
		StartedAt:    workflow.StartedAt,
		CompletedAt:  workflow.CompletedAt,
		Metadata:     workflow.Metadata,
		LastError:    workflow.LastError,
		Version:      1,
		UpdatedAt:    time.Now(),
	}
}

func (sm *StateManager) createWorkflowInstance(stateData WorkflowStateData) *WorkflowInstance {
	status := ports.WorkflowState(stateData.Status)
	return &WorkflowInstance{
		ID:           stateData.ID,
		Status:       status,
		CurrentState: stateData.CurrentState,
		StartedAt:    stateData.StartedAt,
		CompletedAt:  stateData.CompletedAt,
		Metadata:     stateData.Metadata,
		LastError:    stateData.LastError,
	}
}

func (sm *StateManager) triggerEvaluation(ctx context.Context, workflow *WorkflowInstance) {
	if sm.engine.evaluationTrigger == nil {
		return
	}

	event := StateChangeEvent{
		WorkflowID: workflow.ID,
		ChangedBy:  "state_manager",
		NewState:   workflow.CurrentState,
		Timestamp:  time.Now(),
		EventType:  EventTypeStateUpdated,
	}

	if err := sm.engine.evaluationTrigger.TriggerEvaluation(ctx, event); err != nil {
		sm.engine.logger.Error("failed to trigger evaluation",
			"workflow_id", workflow.ID,
			"error", err.Error(),
		)
	}
}
