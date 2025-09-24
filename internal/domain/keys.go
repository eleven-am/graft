package domain

import "fmt"

const (
	WorkflowStatePrefix    = "workflow:state:"
	WorkflowBatchPrefix    = "workflow:batch:"
	WorkflowSnapshotPrefix = "workflow:snapshot:"
)

// WorkflowStateKey builds the canonical key for workflow state storage
func WorkflowStateKey(id string) string {
	return fmt.Sprintf("%s%s", WorkflowStatePrefix, id)
}

// WorkflowBatchKey builds the canonical key for workflow batch storage
func WorkflowBatchKey(batchID string) string {
	return fmt.Sprintf("%s%s", WorkflowBatchPrefix, batchID)
}

// WorkflowSnapshotKey builds the key for workflow snapshots (full or incremental)
func WorkflowSnapshotKey(id string, version int64) string {
	return fmt.Sprintf("%s%s:%d", WorkflowSnapshotPrefix, id, version)
}
