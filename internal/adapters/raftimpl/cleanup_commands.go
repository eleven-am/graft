package raftimpl

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type CleanupOp struct {
	Target string `json:"target"`
	Action string `json:"action"`
}

type WorkflowCleanupCommand struct {
	WorkflowID string      `json:"workflow_id"`
	Operations []CleanupOp `json:"operations"`
	Timestamp  time.Time   `json:"timestamp"`
}

type CleanupStatus struct {
	WorkflowID    string            `json:"workflow_id"`
	Status        string            `json:"status"`
	StartedAt     time.Time         `json:"started_at"`
	CompletedAt   *time.Time        `json:"completed_at,omitempty"`
	Operations    []CleanupOp       `json:"operations"`
	Results       map[string]string `json:"results,omitempty"`
	Error         *string           `json:"error,omitempty"`
}

func NewWorkflowCleanupCommand(workflowID string, operations []CleanupOp) *WorkflowCleanupCommand {
	return &WorkflowCleanupCommand{
		WorkflowID: workflowID,
		Operations: operations,
		Timestamp:  time.Now(),
	}
}

func (wcc *WorkflowCleanupCommand) Validate() error {
	if wcc.WorkflowID == "" {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "workflow ID cannot be empty",
			Details: map[string]interface{}{},
		}
	}
	
	if len(wcc.Operations) == 0 {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "cleanup operations cannot be empty",
			Details: map[string]interface{}{
				"workflow_id": wcc.WorkflowID,
			},
		}
	}
	
	validTargets := map[string]bool{
		"state": true,
		"queue": true,
		"claims": true,
		"audit": true,
	}
	
	validActions := map[string]bool{
		"delete": true,
		"archive": true,
		"compress": true,
	}
	
	for i, op := range wcc.Operations {
		if !validTargets[op.Target] {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "invalid cleanup target",
				Details: map[string]interface{}{
					"operation_index": i,
					"target":          op.Target,
					"valid_targets":   []string{"state", "queue", "claims", "audit"},
					"workflow_id":     wcc.WorkflowID,
				},
			}
		}
		if !validActions[op.Action] {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "invalid cleanup action",
				Details: map[string]interface{}{
					"operation_index": i,
					"action":          op.Action,
					"valid_actions":   []string{"delete", "archive", "compress"},
					"workflow_id":     wcc.WorkflowID,
				},
			}
		}
	}
	
	return nil
}

func (wcc *WorkflowCleanupCommand) Marshal() ([]byte, error) {
	return json.Marshal(wcc)
}

func UnmarshalWorkflowCleanupCommand(data []byte) (*WorkflowCleanupCommand, error) {
	var cmd WorkflowCleanupCommand
	err := json.Unmarshal(data, &cmd)
	return &cmd, err
}

func NewCleanupCommand(workflowID string, operations []CleanupOp) *Command {
	cleanupCmd := NewWorkflowCleanupCommand(workflowID, operations)
	data, _ := cleanupCmd.Marshal()
	
	return &Command{
		Type:      CommandCleanupWorkflow,
		Key:       fmt.Sprintf("cleanup:%s", workflowID),
		Value:     data,
		Timestamp: time.Now(),
	}
}