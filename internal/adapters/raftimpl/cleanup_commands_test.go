package raftimpl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorkflowCleanupCommand_Creation(t *testing.T) {
	workflowID := "test-workflow-123"
	operations := []CleanupOp{
		{Target: "state", Action: "delete"},
		{Target: "queue", Action: "delete"},
		{Target: "claims", Action: "delete"},
		{Target: "audit", Action: "archive"},
	}

	cmd := NewWorkflowCleanupCommand(workflowID, operations)

	assert.Equal(t, workflowID, cmd.WorkflowID)
	assert.Equal(t, operations, cmd.Operations)
	assert.True(t, time.Since(cmd.Timestamp) < time.Second)
}

func TestWorkflowCleanupCommand_Validation(t *testing.T) {
	t.Run("Valid command", func(t *testing.T) {
		operations := []CleanupOp{
			{Target: "state", Action: "delete"},
			{Target: "queue", Action: "delete"},
		}
		
		cmd := NewWorkflowCleanupCommand("test-workflow", operations)
		err := cmd.Validate()
		assert.NoError(t, err)
	})

	t.Run("Empty workflow ID", func(t *testing.T) {
		operations := []CleanupOp{
			{Target: "state", Action: "delete"},
		}
		
		cmd := NewWorkflowCleanupCommand("", operations)
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "workflow ID cannot be empty")
	})

	t.Run("Empty operations", func(t *testing.T) {
		cmd := NewWorkflowCleanupCommand("test-workflow", []CleanupOp{})
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cleanup operations cannot be empty")
	})

	t.Run("Invalid target", func(t *testing.T) {
		operations := []CleanupOp{
			{Target: "invalid-target", Action: "delete"},
		}
		
		cmd := NewWorkflowCleanupCommand("test-workflow", operations)
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cleanup target")
	})

	t.Run("Invalid action", func(t *testing.T) {
		operations := []CleanupOp{
			{Target: "state", Action: "invalid-action"},
		}
		
		cmd := NewWorkflowCleanupCommand("test-workflow", operations)
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cleanup action")
	})

	t.Run("Multiple operations with mixed validity", func(t *testing.T) {
		operations := []CleanupOp{
			{Target: "state", Action: "delete"},
			{Target: "invalid", Action: "delete"},
			{Target: "queue", Action: "archive"},
		}
		
		cmd := NewWorkflowCleanupCommand("test-workflow", operations)
		err := cmd.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cleanup target")
	})

	t.Run("All valid targets and actions", func(t *testing.T) {
		operations := []CleanupOp{
			{Target: "state", Action: "delete"},
			{Target: "queue", Action: "delete"},
			{Target: "claims", Action: "delete"},
			{Target: "audit", Action: "archive"},
			{Target: "state", Action: "compress"},
		}
		
		cmd := NewWorkflowCleanupCommand("test-workflow", operations)
		err := cmd.Validate()
		assert.NoError(t, err)
	})
}

func TestWorkflowCleanupCommand_MarshalUnmarshal(t *testing.T) {
	original := &WorkflowCleanupCommand{
		WorkflowID: "test-workflow-456",
		Operations: []CleanupOp{
			{Target: "state", Action: "archive"},
			{Target: "queue", Action: "delete"},
		},
		Timestamp: time.Now().Round(time.Second),
	}

	data, err := original.Marshal()
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	unmarshaled, err := UnmarshalWorkflowCleanupCommand(data)
	assert.NoError(t, err)
	assert.Equal(t, original.WorkflowID, unmarshaled.WorkflowID)
	assert.Equal(t, original.Operations, unmarshaled.Operations)
	assert.True(t, original.Timestamp.Equal(unmarshaled.Timestamp))
}

func TestNewCleanupCommand(t *testing.T) {
	workflowID := "test-workflow-789"
	operations := []CleanupOp{
		{Target: "state", Action: "delete"},
		{Target: "audit", Action: "archive"},
	}

	cmd := NewCleanupCommand(workflowID, operations)

	assert.Equal(t, CommandCleanupWorkflow, cmd.Type)
	assert.Equal(t, "cleanup:"+workflowID, cmd.Key)
	assert.NotEmpty(t, cmd.Value)
	assert.True(t, time.Since(cmd.Timestamp) < time.Second)

	cleanupCmd, err := UnmarshalWorkflowCleanupCommand(cmd.Value)
	assert.NoError(t, err)
	assert.Equal(t, workflowID, cleanupCmd.WorkflowID)
	assert.Equal(t, operations, cleanupCmd.Operations)
}

func TestCleanupStatus(t *testing.T) {
	status := &CleanupStatus{
		WorkflowID: "test-workflow",
		Status:     "completed",
		StartedAt:  time.Now().Add(-time.Minute),
		Operations: []CleanupOp{
			{Target: "state", Action: "delete"},
			{Target: "queue", Action: "delete"},
		},
		Results: map[string]string{
			"state": "deleted 5 items",
			"queue": "deleted 10 items",
		},
	}
	
	now := time.Now()
	status.CompletedAt = &now

	assert.Equal(t, "test-workflow", status.WorkflowID)
	assert.Equal(t, "completed", status.Status)
	assert.NotNil(t, status.CompletedAt)
	assert.Equal(t, 2, len(status.Operations))
	assert.Equal(t, 2, len(status.Results))
	assert.Equal(t, "deleted 5 items", status.Results["state"])
	assert.Equal(t, "deleted 10 items", status.Results["queue"])
}

func TestCleanupOp_ValidTargetsAndActions(t *testing.T) {
	validTargets := []string{"state", "queue", "claims", "audit"}
	validActions := []string{"delete", "archive", "compress"}

	for _, target := range validTargets {
		for _, action := range validActions {
			op := CleanupOp{Target: target, Action: action}
			operations := []CleanupOp{op}
			
			cmd := NewWorkflowCleanupCommand("test-workflow", operations)
			err := cmd.Validate()
			assert.NoError(t, err, "Target: %s, Action: %s should be valid", target, action)
		}
	}
}

func TestCleanupCommand_Integration(t *testing.T) {
	workflowID := "integration-test-workflow"
	operations := []CleanupOp{
		{Target: "state", Action: "archive"},
		{Target: "queue", Action: "delete"},
		{Target: "claims", Action: "delete"},
		{Target: "audit", Action: "archive"},
	}

	cleanupCmd := NewWorkflowCleanupCommand(workflowID, operations)
	
	err := cleanupCmd.Validate()
	assert.NoError(t, err)

	cmd := NewCleanupCommand(workflowID, operations)
	
	assert.Equal(t, CommandCleanupWorkflow, cmd.Type)
	assert.Contains(t, cmd.Key, workflowID)

	data, err := cmd.Marshal()
	assert.NoError(t, err)

	unmarshaledCmd, err := UnmarshalCommand(data)
	assert.NoError(t, err)
	assert.Equal(t, CommandCleanupWorkflow, unmarshaledCmd.Type)

	unmarshaledCleanupCmd, err := UnmarshalWorkflowCleanupCommand(unmarshaledCmd.Value)
	assert.NoError(t, err)
	assert.Equal(t, workflowID, unmarshaledCleanupCmd.WorkflowID)
	assert.Equal(t, operations, unmarshaledCleanupCmd.Operations)
}