package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type StateValidator struct {
	logger *slog.Logger
}

func NewStateValidator(logger *slog.Logger) *StateValidator {
	return &StateValidator{
		logger: logger.With("component", "state-validator"),
	}
}

func (sv *StateValidator) ValidateState(ctx context.Context, state *domain.CompleteWorkflowState, level domain.ValidationLevel) (*domain.ValidationResult, error) {
	sv.logger.Debug("starting workflow state validation",
		"workflow_id", state.WorkflowID,
		"validation_level", level,
		"version", state.Version)

	result := &domain.ValidationResult{
		Valid:               true,
		Errors:              []domain.ValidationError{},
		Warnings:            []domain.ValidationWarning{},
		RecommendedActions:  []domain.RecommendedAction{},
		StateConsistency:    domain.ConsistencyReport{},
		ResumptionReadiness: domain.ResumptionReadiness{},
	}

	if level == domain.ValidationSkip {
		sv.logger.Debug("skipping validation per validation level", "workflow_id", state.WorkflowID)
		return result, nil
	}

	sv.validateBasicStructure(state, result)
	sv.validateDAGConsistency(state, result)
	sv.validateTemporalConsistency(state, result)
	sv.validateResourceConsistency(state, result)
	sv.validateDataIntegrity(state, result)
	sv.assessResumptionReadiness(state, result)

	if level == domain.ValidationLenient {
		sv.filterLenientValidation(result)
	} else if level == domain.ValidationStrict {
		sv.enhanceStrictValidation(state, result)
	}

	result.Valid = len(result.Errors) == 0
	result.StateConsistency = sv.buildConsistencyReport(result)

	sv.logger.Debug("workflow state validation completed",
		"workflow_id", state.WorkflowID,
		"valid", result.Valid,
		"errors", len(result.Errors),
		"warnings", len(result.Warnings))

	return result, nil
}

func (sv *StateValidator) validateBasicStructure(state *domain.CompleteWorkflowState, result *domain.ValidationResult) {
	if state.WorkflowID == "" {
		sv.addError(result, "MISSING_WORKFLOW_ID", "WorkflowID is required", "", "", domain.SeverityCritical, nil)
	}

	if state.Status == "" {
		sv.addError(result, "MISSING_STATUS", "Workflow status is required", "", "", domain.SeverityCritical, nil)
	}

	if state.StartedAt.IsZero() {
		sv.addError(result, "MISSING_START_TIME", "StartedAt timestamp is required", "", "", domain.SeverityHigh, nil)
	}

	if state.Version <= 0 {
		sv.addError(result, "INVALID_VERSION", "Version must be positive", "", "", domain.SeverityMedium, nil)
	}

	if state.ExecutionDAG.Nodes == nil || len(state.ExecutionDAG.Nodes) == 0 {
		sv.addError(result, "MISSING_DAG", "ExecutionDAG must contain nodes", "", "", domain.SeverityCritical, nil)
	}
}

func (sv *StateValidator) validateDAGConsistency(state *domain.CompleteWorkflowState, result *domain.ValidationResult) {
	nodeMap := make(map[string]*domain.DAGNode)
	for i, node := range state.ExecutionDAG.Nodes {
		if _, exists := nodeMap[node.ID]; exists {
			sv.addError(result, "DUPLICATE_NODE", fmt.Sprintf("Duplicate node ID: %s", node.ID), "", node.ID, domain.SeverityCritical, nil)
			continue
		}
		nodeMap[node.ID] = &state.ExecutionDAG.Nodes[i]
	}

	executedNodeMap := make(map[string]bool)
	for _, executed := range state.ExecutedNodes {
		executedNodeMap[executed.NodeName] = true
	}

	for _, node := range state.ExecutionDAG.Nodes {
		for _, depID := range node.Dependencies {
			if _, exists := nodeMap[depID]; !exists {
				sv.addError(result, "MISSING_DEPENDENCY", fmt.Sprintf("Node %s depends on non-existent node %s", node.ID, depID), "", node.ID, domain.SeverityHigh, nil)
				continue
			}

			if node.Status == domain.NodeStatusCompleted {
				if !executedNodeMap[depID] {
					sv.addError(result, "DEPENDENCY_NOT_EXECUTED", fmt.Sprintf("Completed node %s has unexecuted dependency %s", node.ID, depID), "", node.ID, domain.SeverityCritical, nil)
				}
			}
		}
	}

	if sv.hasCircularDependencies(state.ExecutionDAG) {
		sv.addError(result, "CIRCULAR_DEPENDENCY", "DAG contains circular dependencies", "", "", domain.SeverityCritical, nil)
	}
}

func (sv *StateValidator) validateTemporalConsistency(state *domain.CompleteWorkflowState, result *domain.ValidationResult) {
	if len(state.ExecutedNodes) == 0 {
		return
	}

	executionTimes := make([]time.Time, 0, len(state.ExecutedNodes))
	for _, node := range state.ExecutedNodes {
		executionTimes = append(executionTimes, node.ExecutedAt)

		if node.ExecutedAt.Before(state.StartedAt) {
			sv.addError(result, "NODE_BEFORE_WORKFLOW", fmt.Sprintf("Node %s executed before workflow start", node.NodeName), "", node.NodeName, domain.SeverityHigh, nil)
		}

		if node.Duration < 0 {
			sv.addError(result, "NEGATIVE_DURATION", fmt.Sprintf("Node %s has negative duration", node.NodeName), "", node.NodeName, domain.SeverityMedium, nil)
		}

		if node.Duration > 24*time.Hour {
			sv.addWarning(result, "LONG_EXECUTION", fmt.Sprintf("Node %s executed for over 24 hours", node.NodeName), "", node.NodeName, "Consider investigating long-running node execution", nil)
		}
	}

	sort.Slice(executionTimes, func(i, j int) bool {
		return executionTimes[i].Before(executionTimes[j])
	})

	if state.CompletedAt != nil {
		lastExecution := executionTimes[len(executionTimes)-1]
		if state.CompletedAt.Before(lastExecution) {
			sv.addError(result, "COMPLETION_BEFORE_LAST_NODE", "Workflow completed before last node execution", "", "", domain.SeverityHigh, nil)
		}
	}
}

func (sv *StateValidator) validateResourceConsistency(state *domain.CompleteWorkflowState, result *domain.ValidationResult) {
	now := time.Now()

	for _, claim := range state.ClaimsData {
		if claim.ExpiresAt.Before(now) {
			sv.addWarning(result, "EXPIRED_CLAIM", fmt.Sprintf("Resource claim %s has expired", claim.ID), "", claim.NodeID, "Consider refreshing or releasing expired claims", nil)
		}

		if claim.ClaimedAt.After(claim.ExpiresAt) {
			sv.addError(result, "INVALID_CLAIM_TIME", fmt.Sprintf("Claim %s has claim time after expiration", claim.ID), "", claim.NodeID, domain.SeverityMedium, nil)
		}
	}

	claimedResources := make(map[string]bool)
	for _, claim := range state.ClaimsData {
		if claimedResources[claim.ResourceID] {
			sv.addError(result, "DUPLICATE_RESOURCE_CLAIM", fmt.Sprintf("Resource %s is claimed multiple times", claim.ResourceID), "", claim.NodeID, domain.SeverityHigh, nil)
		}
		claimedResources[claim.ResourceID] = true
	}

	for _, resourceState := range state.ResourceStates {
		if resourceState.ClaimedBy != "" && !claimedResources[resourceState.ID] {
			sv.addWarning(result, "UNCLAIMED_RESOURCE_STATE", fmt.Sprintf("Resource %s shows as claimed but no claim record exists", resourceState.ID), "", "", "Verify resource claim consistency", nil)
		}
	}
}

func (sv *StateValidator) validateDataIntegrity(state *domain.CompleteWorkflowState, result *domain.ValidationResult) {
	if state.StateHash != "" {
		computedHash := sv.computeStateHash(state)
		if computedHash != state.StateHash {
			sv.addError(result, "HASH_MISMATCH", "State hash does not match computed hash", "", "", domain.SeverityCritical, map[string]interface{}{
				"expected": state.StateHash,
				"computed": computedHash,
			})
		}
	}

	for _, idempotencyKey := range state.IdempotencyKeys {
		if idempotencyKey.ExpiresAt.Before(time.Now()) {
			sv.addWarning(result, "EXPIRED_IDEMPOTENCY_KEY", fmt.Sprintf("Idempotency key %s has expired", idempotencyKey.Key), "", idempotencyKey.NodeID, "Consider cleaning up expired keys", nil)
		}

		if idempotencyKey.CreatedAt.After(idempotencyKey.ExpiresAt) {
			sv.addError(result, "INVALID_IDEMPOTENCY_TIME", fmt.Sprintf("Idempotency key %s has creation time after expiration", idempotencyKey.Key), "", idempotencyKey.NodeID, domain.SeverityMedium, nil)
		}
	}

	queueNodeMap := make(map[string]bool)
	for _, item := range state.QueueItems {
		queueNodeMap[item.NodeName] = true

		if item.WorkflowID != state.WorkflowID {
			sv.addError(result, "QUEUE_WORKFLOW_MISMATCH", fmt.Sprintf("Queue item %s has mismatched workflow ID", item.ID), "", item.NodeName, domain.SeverityHigh, nil)
		}

		if item.RetryCount > item.MaxRetries {
			sv.addError(result, "EXCESSIVE_RETRIES", fmt.Sprintf("Queue item %s exceeds max retries", item.ID), "", item.NodeName, domain.SeverityMedium, nil)
		}
	}
}

func (sv *StateValidator) assessResumptionReadiness(state *domain.CompleteWorkflowState, result *domain.ValidationResult) {
	readiness := domain.ResumptionReadiness{
		Ready:                true,
		MissingPrerequisites: []string{},
		InvalidResources:     []string{},
		StateInconsistencies: []string{},
		Recommendations:      []string{},
	}

	if state.ResumptionPoint != nil {
		nodeExists := false
		for _, node := range state.ExecutionDAG.Nodes {
			if node.ID == state.ResumptionPoint.NodeID {
				nodeExists = true
				for _, prereq := range state.ResumptionPoint.Prerequisites {
					prereqCompleted := false
					for _, executed := range state.ExecutedNodes {
						if executed.NodeName == prereq && executed.Status == string(domain.NodeStatusCompleted) {
							prereqCompleted = true
							break
						}
					}
					if !prereqCompleted {
						readiness.MissingPrerequisites = append(readiness.MissingPrerequisites, prereq)
						readiness.Ready = false
					}
				}
				break
			}
		}

		if !nodeExists {
			readiness.StateInconsistencies = append(readiness.StateInconsistencies, fmt.Sprintf("Resumption point node %s does not exist in DAG", state.ResumptionPoint.NodeID))
			readiness.Ready = false
		}
	}

	now := time.Now()
	for _, claim := range state.ClaimsData {
		if claim.ExpiresAt.Before(now) {
			readiness.InvalidResources = append(readiness.InvalidResources, claim.ResourceID)
			readiness.Ready = false
		}
	}

	if len(result.Errors) > 0 {
		readiness.Ready = false
		readiness.StateInconsistencies = append(readiness.StateInconsistencies, "Validation errors must be resolved before resumption")
	}

	if !readiness.Ready {
		if len(readiness.MissingPrerequisites) > 0 {
			readiness.Recommendations = append(readiness.Recommendations, "Complete missing prerequisite nodes before resumption")
		}
		if len(readiness.InvalidResources) > 0 {
			readiness.Recommendations = append(readiness.Recommendations, "Refresh expired resource claims")
		}
		if len(readiness.StateInconsistencies) > 0 {
			readiness.Recommendations = append(readiness.Recommendations, "Resolve state inconsistencies")
		}
	}

	result.ResumptionReadiness = readiness
}

func (sv *StateValidator) filterLenientValidation(result *domain.ValidationResult) {
	criticalErrors := []domain.ValidationError{}
	for _, err := range result.Errors {
		if err.Severity == domain.SeverityCritical || err.Severity == domain.SeverityHigh {
			criticalErrors = append(criticalErrors, err)
		}
	}
	result.Errors = criticalErrors
}

func (sv *StateValidator) enhanceStrictValidation(state *domain.CompleteWorkflowState, result *domain.ValidationResult) {
	if state.Metadata == nil {
		sv.addWarning(result, "MISSING_METADATA", "Workflow metadata is empty", "", "", "Consider adding metadata for better traceability", nil)
	}

	for _, node := range state.ExecutedNodes {
		if node.Results == nil && node.Status == string(domain.NodeStatusCompleted) {
			sv.addWarning(result, "MISSING_NODE_RESULTS", fmt.Sprintf("Completed node %s has no results", node.NodeName), "", node.NodeName, "Verify node execution captured results properly", nil)
		}
	}

	if len(state.Checkpoints) == 0 {
		sv.addWarning(result, "NO_CHECKPOINTS", "No checkpoints found in workflow state", "", "", "Consider adding checkpoints for better resumption capabilities", nil)
	}

	// Validate edge consistency in DAG
	edgeMap := make(map[string]map[string]bool)
	for _, edge := range state.ExecutionDAG.Edges {
		if edgeMap[edge.From] == nil {
			edgeMap[edge.From] = make(map[string]bool)
		}
		if edgeMap[edge.From][edge.To] {
			sv.addError(result, "DUPLICATE_EDGE", fmt.Sprintf("Duplicate edge from %s to %s", edge.From, edge.To), "", "", domain.SeverityMedium, nil)
		}
		edgeMap[edge.From][edge.To] = true

		// Verify edge endpoints exist
		fromExists := false
		toExists := false
		for _, node := range state.ExecutionDAG.Nodes {
			if node.ID == edge.From {
				fromExists = true
			}
			if node.ID == edge.To {
				toExists = true
			}
		}
		if !fromExists {
			sv.addError(result, "EDGE_INVALID_SOURCE", fmt.Sprintf("Edge from non-existent node %s", edge.From), "", edge.From, domain.SeverityHigh, nil)
		}
		if !toExists {
			sv.addError(result, "EDGE_INVALID_TARGET", fmt.Sprintf("Edge to non-existent node %s", edge.To), "", edge.To, domain.SeverityHigh, nil)
		}
	}

	// Validate root and leaf consistency
	for _, root := range state.ExecutionDAG.Roots {
		found := false
		for _, node := range state.ExecutionDAG.Nodes {
			if node.ID == root {
				found = true
				// Verify it's actually a root (no incoming edges)
				for _, edge := range state.ExecutionDAG.Edges {
					if edge.To == root {
						sv.addError(result, "INVALID_ROOT", fmt.Sprintf("Node %s marked as root but has incoming edge from %s", root, edge.From), "", root, domain.SeverityMedium, nil)
					}
				}
				break
			}
		}
		if !found {
			sv.addError(result, "ROOT_NOT_IN_DAG", fmt.Sprintf("Root node %s not found in DAG", root), "", root, domain.SeverityHigh, nil)
		}
	}

	for _, leaf := range state.ExecutionDAG.Leaves {
		found := false
		for _, node := range state.ExecutionDAG.Nodes {
			if node.ID == leaf {
				found = true
				// Verify it's actually a leaf (no outgoing edges)
				for _, edge := range state.ExecutionDAG.Edges {
					if edge.From == leaf {
						sv.addError(result, "INVALID_LEAF", fmt.Sprintf("Node %s marked as leaf but has outgoing edge to %s", leaf, edge.To), "", leaf, domain.SeverityMedium, nil)
					}
				}
				break
			}
		}
		if !found {
			sv.addError(result, "LEAF_NOT_IN_DAG", fmt.Sprintf("Leaf node %s not found in DAG", leaf), "", leaf, domain.SeverityHigh, nil)
		}
	}

	// Validate dependency consistency with edges
	for _, node := range state.ExecutionDAG.Nodes {
		for _, dep := range node.Dependencies {
			// Check if there's a corresponding edge
			edgeExists := false
			for _, edge := range state.ExecutionDAG.Edges {
				if edge.From == dep && edge.To == node.ID {
					edgeExists = true
					break
				}
			}
			if !edgeExists {
				sv.addWarning(result, "DEPENDENCY_WITHOUT_EDGE", fmt.Sprintf("Node %s depends on %s but no edge exists", node.ID, dep), "", node.ID, "Consider adding edge to DAG for consistency", nil)
			}
		}
	}

	// Validate queue item consistency
	nodeExistsMap := make(map[string]bool)
	for _, node := range state.ExecutionDAG.Nodes {
		nodeExistsMap[node.ID] = true
	}

	for _, item := range state.QueueItems {
		if !nodeExistsMap[item.NodeName] {
			sv.addError(result, "QUEUE_ITEM_UNKNOWN_NODE", fmt.Sprintf("Queue item %s references unknown node %s", item.ID, item.NodeName), "", item.NodeName, domain.SeverityMedium, nil)
		}
	}

	// Validate idempotency key uniqueness
	keyMap := make(map[string]bool)
	for _, key := range state.IdempotencyKeys {
		if keyMap[key.Key] {
			sv.addError(result, "DUPLICATE_IDEMPOTENCY_KEY", fmt.Sprintf("Duplicate idempotency key: %s", key.Key), "", key.NodeID, domain.SeverityHigh, nil)
		}
		keyMap[key.Key] = true
	}

	// Validate execution order consistency
	nodeExecutionMap := make(map[string]time.Time)
	for _, executed := range state.ExecutedNodes {
		nodeExecutionMap[executed.NodeName] = executed.ExecutedAt
	}

	for _, node := range state.ExecutionDAG.Nodes {
		if executionTime, executed := nodeExecutionMap[node.ID]; executed {
			// Check all dependencies were executed before this node
			for _, dep := range node.Dependencies {
				if depTime, depExecuted := nodeExecutionMap[dep]; depExecuted {
					if !depTime.Before(executionTime) && !depTime.Equal(executionTime) {
						sv.addError(result, "EXECUTION_ORDER_VIOLATION", fmt.Sprintf("Node %s executed before its dependency %s", node.ID, dep), "", node.ID, domain.SeverityCritical, map[string]interface{}{
							"node_time": executionTime,
							"dep_time":  depTime,
						})
					}
				}
			}
		}
	}

	// Validate triggered_by consistency
	for _, executed := range state.ExecutedNodes {
		if executed.TriggeredBy != "" {
			// Verify the triggering node was actually executed
			triggerFound := false
			for _, other := range state.ExecutedNodes {
				if other.NodeName == executed.TriggeredBy {
					triggerFound = true
					// Verify trigger executed before this node
					if !other.ExecutedAt.Before(executed.ExecutedAt) {
						sv.addError(result, "TRIGGER_TIMING_VIOLATION", fmt.Sprintf("Node %s triggered by %s but trigger executed after", executed.NodeName, executed.TriggeredBy), "", executed.NodeName, domain.SeverityHigh, nil)
					}
					break
				}
			}
			if !triggerFound {
				sv.addWarning(result, "TRIGGER_NOT_EXECUTED", fmt.Sprintf("Node %s triggered by %s but trigger not in executed nodes", executed.NodeName, executed.TriggeredBy), "", executed.NodeName, "Verify trigger execution history", nil)
			}
		}
	}
}

func (sv *StateValidator) hasCircularDependencies(dag domain.WorkflowDAG) bool {
	visited := make(map[string]bool)
	recursionStack := make(map[string]bool)

	for _, node := range dag.Nodes {
		if !visited[node.ID] {
			if sv.detectCycle(node.ID, dag, visited, recursionStack) {
				return true
			}
		}
	}
	return false
}

func (sv *StateValidator) detectCycle(nodeID string, dag domain.WorkflowDAG, visited, recursionStack map[string]bool) bool {
	visited[nodeID] = true
	recursionStack[nodeID] = true

	for _, node := range dag.Nodes {
		if node.ID == nodeID {
			for _, depID := range node.Dependencies {
				if !visited[depID] {
					if sv.detectCycle(depID, dag, visited, recursionStack) {
						return true
					}
				} else if recursionStack[depID] {
					return true
				}
			}
		}
	}

	recursionStack[nodeID] = false
	return false
}

func (sv *StateValidator) computeStateHash(state *domain.CompleteWorkflowState) string {
	hasher := sha256.New()

	// Core workflow state
	hasher.Write([]byte(state.WorkflowID))
	hasher.Write([]byte(state.Status))
	hasher.Write([]byte(state.StartedAt.Format(time.RFC3339)))
	if state.CompletedAt != nil {
		hasher.Write([]byte(state.CompletedAt.Format(time.RFC3339)))
	}
	hasher.Write([]byte(fmt.Sprintf("%d", state.Version)))

	// Current state data
	if state.CurrentState != nil {
		if stateBytes, err := json.Marshal(state.CurrentState); err == nil {
			hasher.Write(stateBytes)
		}
	}

	// Executed nodes with all details
	for _, node := range state.ExecutedNodes {
		hasher.Write([]byte(node.NodeName))
		hasher.Write([]byte(string(node.Status)))
		hasher.Write([]byte(node.ExecutedAt.Format(time.RFC3339)))
		hasher.Write([]byte(fmt.Sprintf("%d", node.Duration.Nanoseconds())))
		if node.Error != nil {
			hasher.Write([]byte(*node.Error))
		}
		if node.TriggeredBy != "" {
			hasher.Write([]byte(node.TriggeredBy))
		}
	}

	// Pending nodes
	for _, node := range state.PendingNodes {
		hasher.Write([]byte(node.NodeName))
		hasher.Write([]byte(node.QueuedAt.Format(time.RFC3339)))
		hasher.Write([]byte(fmt.Sprintf("%d", node.Priority)))
		if node.Reason != "" {
			hasher.Write([]byte(node.Reason))
		}
		if node.TriggeredBy != "" {
			hasher.Write([]byte(node.TriggeredBy))
		}
	}

	// Ready nodes
	for _, node := range state.ReadyNodes {
		hasher.Write([]byte(node.NodeName))
		hasher.Write([]byte(node.QueuedAt.Format(time.RFC3339)))
		hasher.Write([]byte(fmt.Sprintf("%d", node.Priority)))
		if node.IdempotencyKey != "" {
			hasher.Write([]byte(node.IdempotencyKey))
		}
		if node.TriggeredBy != "" {
			hasher.Write([]byte(node.TriggeredBy))
		}
	}

	// Queue items
	for _, item := range state.QueueItems {
		hasher.Write([]byte(item.ID))
		hasher.Write([]byte(item.WorkflowID))
		hasher.Write([]byte(item.NodeName))
		hasher.Write([]byte(fmt.Sprintf("%d", item.Priority)))
		hasher.Write([]byte(item.IdempotencyKey))
		hasher.Write([]byte(item.QueueType))
	}

	// Idempotency keys
	for _, key := range state.IdempotencyKeys {
		hasher.Write([]byte(key.Key))
		hasher.Write([]byte(key.WorkflowID))
		hasher.Write([]byte(key.NodeID))
		hasher.Write([]byte(key.CreatedAt.Format(time.RFC3339)))
		hasher.Write([]byte(key.ExpiresAt.Format(time.RFC3339)))
	}

	// Checkpoints
	for _, checkpoint := range state.Checkpoints {
		hasher.Write([]byte(checkpoint.ID))
		hasher.Write([]byte(checkpoint.WorkflowID))
		hasher.Write([]byte(checkpoint.NodeID))
		hasher.Write([]byte(checkpoint.CreatedAt.Format(time.RFC3339)))
	}

	// DAG structure
	if len(state.ExecutionDAG.Nodes) > 0 {
		// Sort for consistent ordering
		dagNodes := make([]domain.DAGNode, len(state.ExecutionDAG.Nodes))
		copy(dagNodes, state.ExecutionDAG.Nodes)
		sort.Slice(dagNodes, func(i, j int) bool {
			return dagNodes[i].ID < dagNodes[j].ID
		})

		for _, node := range dagNodes {
			hasher.Write([]byte(node.ID))
			hasher.Write([]byte(node.Type))
			hasher.Write([]byte(string(node.Status)))
			for _, dep := range node.Dependencies {
				hasher.Write([]byte(dep))
			}
		}

		// Sort edges for consistent ordering
		dagEdges := make([]domain.DAGEdge, len(state.ExecutionDAG.Edges))
		copy(dagEdges, state.ExecutionDAG.Edges)
		sort.Slice(dagEdges, func(i, j int) bool {
			if dagEdges[i].From == dagEdges[j].From {
				return dagEdges[i].To < dagEdges[j].To
			}
			return dagEdges[i].From < dagEdges[j].From
		})

		for _, edge := range dagEdges {
			hasher.Write([]byte(edge.From))
			hasher.Write([]byte(edge.To))
			hasher.Write([]byte(edge.Type))
		}

		// Roots and leaves
		for _, root := range state.ExecutionDAG.Roots {
			hasher.Write([]byte("root:" + root))
		}
		for _, leaf := range state.ExecutionDAG.Leaves {
			hasher.Write([]byte("leaf:" + leaf))
		}
	}

	// Resource states
	for _, resource := range state.ResourceStates {
		hasher.Write([]byte(resource.ID))
		hasher.Write([]byte(resource.Type))
		if resource.ClaimedBy != "" {
			hasher.Write([]byte(resource.ClaimedBy))
		}
		if resource.ClaimedAt != nil {
			hasher.Write([]byte(resource.ClaimedAt.Format(time.RFC3339)))
		}
		if resource.ExpiresAt != nil {
			hasher.Write([]byte(resource.ExpiresAt.Format(time.RFC3339)))
		}
	}

	// Dependencies
	for _, dep := range state.Dependencies {
		hasher.Write([]byte(dep.SourceNode))
		hasher.Write([]byte(dep.TargetNode))
		hasher.Write([]byte(dep.DependencyType))
		hasher.Write([]byte(dep.CreatedAt.Format(time.RFC3339)))
	}

	// Claims
	for _, claim := range state.ClaimsData {
		hasher.Write([]byte(claim.ID))
		hasher.Write([]byte(claim.WorkflowID))
		hasher.Write([]byte(claim.NodeID))
		hasher.Write([]byte(claim.ResourceID))
		hasher.Write([]byte(claim.ClaimedAt.Format(time.RFC3339)))
		hasher.Write([]byte(claim.ClaimedBy))
	}

	// Last executed node
	if state.LastExecutedNode != "" {
		hasher.Write([]byte("last:" + state.LastExecutedNode))
	}

	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (sv *StateValidator) buildConsistencyReport(result *domain.ValidationResult) domain.ConsistencyReport {
	report := domain.ConsistencyReport{
		DAGConsistent:      true,
		TemporalConsistent: true,
		ResourceConsistent: true,
		DataIntegrity:      true,
		Issues:             []domain.ConsistencyIssue{},
		Recommendations:    []string{},
	}

	for _, err := range result.Errors {
		issue := domain.ConsistencyIssue{
			Type:        err.Code,
			Description: err.Message,
			NodeID:      err.NodeID,
			Severity:    err.Severity,
			Metadata:    err.Metadata,
		}
		report.Issues = append(report.Issues, issue)

		switch {
		case err.Code == "CIRCULAR_DEPENDENCY" || err.Code == "MISSING_DEPENDENCY" || err.Code == "DEPENDENCY_NOT_EXECUTED":
			report.DAGConsistent = false
		case err.Code == "NODE_BEFORE_WORKFLOW" || err.Code == "COMPLETION_BEFORE_LAST_NODE" || err.Code == "NEGATIVE_DURATION":
			report.TemporalConsistent = false
		case err.Code == "DUPLICATE_RESOURCE_CLAIM" || err.Code == "INVALID_CLAIM_TIME":
			report.ResourceConsistent = false
		case err.Code == "HASH_MISMATCH" || err.Code == "INVALID_IDEMPOTENCY_TIME":
			report.DataIntegrity = false
		}
	}

	if len(report.Issues) > 0 {
		report.Recommendations = append(report.Recommendations, "Resolve consistency issues before attempting workflow resumption")
	}

	return report
}

func (sv *StateValidator) addError(result *domain.ValidationResult, code, message, field, nodeID string, severity domain.ValidationSeverity, metadata map[string]interface{}) {
	result.Errors = append(result.Errors, domain.ValidationError{
		Code:     code,
		Message:  message,
		Field:    field,
		NodeID:   nodeID,
		Severity: severity,
		Metadata: metadata,
	})
}

func (sv *StateValidator) addWarning(result *domain.ValidationResult, code, message, field, nodeID, suggestion string, metadata map[string]interface{}) {
	result.Warnings = append(result.Warnings, domain.ValidationWarning{
		Code:       code,
		Message:    message,
		Field:      field,
		NodeID:     nodeID,
		Suggestion: suggestion,
		Metadata:   metadata,
	})
}

func (sv *StateValidator) addRecommendation(result *domain.ValidationResult, action, description, nodeID string, priority domain.ActionPriority, metadata map[string]interface{}) {
	result.RecommendedActions = append(result.RecommendedActions, domain.RecommendedAction{
		Action:      action,
		Description: description,
		NodeID:      nodeID,
		Priority:    priority,
		Metadata:    metadata,
	})
}
