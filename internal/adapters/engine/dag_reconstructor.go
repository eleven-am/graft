package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type DAGReconstructor struct {
	engine     *Engine
	validator  *StateValidator
	dagManager *DAGManager
	logger     *slog.Logger
}

func NewDAGReconstructor(engine *Engine, validator *StateValidator, logger *slog.Logger) *DAGReconstructor {
	return &DAGReconstructor{
		engine:     engine,
		validator:  validator,
		dagManager: NewDAGManager(logger),
		logger:     logger.With("component", "dag-reconstructor"),
	}
}

func (d *DAGReconstructor) ReconstructExecutionPath(
	ctx context.Context,
	state *domain.CompleteWorkflowState,
	resumptionPoint *domain.ResumptionPoint,
) (*domain.ExecutionPlan, error) {
	d.logger.Debug("starting execution path reconstruction",
		"workflow_id", state.WorkflowID,
		"resumption_node", resumptionPoint.NodeID)

	// Build the DAG using the proper library
	_, err := d.dagManager.BuildDAGFromState(state)
	if err != nil {
		return nil, fmt.Errorf("failed to build DAG from state: %w", err)
	}

	// Validate the DAG
	if err := d.dagManager.ValidateDAG(state.WorkflowID); err != nil {
		return nil, fmt.Errorf("DAG validation failed: %w", err)
	}

	completedNodes := d.buildCompletedNodeMap(state.ExecutedNodes)

	var nextExecutable []string

	if resumptionPoint.NodeID != "" {
		// Check if resumption point has all dependencies completed
		deps, err := d.dagManager.GetNodeDependencies(state.WorkflowID, resumptionPoint.NodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to get dependencies for resumption node: %w", err)
		}

		allDepsCompleted := true
		for _, dep := range deps {
			if !completedNodes[dep] {
				allDepsCompleted = false
				break
			}
		}

		if !allDepsCompleted {
			return nil, fmt.Errorf("resumption node %s has incomplete dependencies", resumptionPoint.NodeID)
		}

		nextExecutable = []string{resumptionPoint.NodeID}

		// Also get other executable nodes
		otherExecutable, err := d.dagManager.GetNextExecutableNodes(state.WorkflowID, completedNodes)
		if err != nil {
			d.logger.Warn("failed to get other executable nodes", "error", err)
		} else {
			// Add other executable nodes if not already included
			for _, node := range otherExecutable {
				if node != resumptionPoint.NodeID {
					nextExecutable = append(nextExecutable, node)
				}
			}
		}
	} else {
		// Get all next executable nodes
		nextExecutable, err = d.dagManager.GetNextExecutableNodes(state.WorkflowID, completedNodes)
		if err != nil {
			return nil, fmt.Errorf("failed to get next executable nodes: %w", err)
		}
	}

	// Analyze execution path if we have a starting point
	var pathAnalysis *ExecutionPathAnalysis
	if resumptionPoint.NodeID != "" {
		pathAnalysis, err = d.dagManager.AnalyzeExecutionPath(state.WorkflowID, resumptionPoint.NodeID, completedNodes)
		if err != nil {
			d.logger.Warn("failed to analyze execution path", "error", err)
		}
	}

	requiredResources := d.analyzeRequiredResources(nextExecutable, state)
	estimatedDuration := d.estimateExecutionDuration(nextExecutable, state)

	metadata := map[string]interface{}{
		"completed_nodes":     len(completedNodes),
		"total_nodes":         len(state.ExecutionDAG.Nodes),
		"remaining_nodes":     len(state.ExecutionDAG.Nodes) - len(completedNodes),
		"reconstruction_time": time.Now(),
	}

	if pathAnalysis != nil {
		metadata["reachable_nodes"] = len(pathAnalysis.ReachableNodes)
		metadata["blocked_nodes"] = len(pathAnalysis.BlockedNodes)
		metadata["estimated_steps"] = pathAnalysis.EstimatedSteps
	}

	plan := &domain.ExecutionPlan{
		WorkflowID:        state.WorkflowID,
		ResumptionPoint:   *resumptionPoint,
		NextExecutable:    nextExecutable,
		RequiredResources: requiredResources,
		EstimatedDuration: estimatedDuration,
		Metadata:          metadata,
	}

	d.logger.Debug("execution path reconstruction completed",
		"workflow_id", state.WorkflowID,
		"next_executable", len(nextExecutable),
		"required_resources", len(requiredResources),
		"estimated_duration", estimatedDuration)

	return plan, nil
}

func (d *DAGReconstructor) RestoreWorkflowState(
	ctx context.Context,
	state *domain.CompleteWorkflowState,
) error {
	d.logger.Debug("starting workflow state restoration", "workflow_id", state.WorkflowID)

	if err := d.restoreQueueStates(ctx, state); err != nil {
		return fmt.Errorf("failed to restore queue states: %w", err)
	}

	if err := d.restoreResourceClaims(ctx, state); err != nil {
		return fmt.Errorf("failed to restore resource claims: %w", err)
	}

	if err := d.restoreIdempotencyTracking(ctx, state); err != nil {
		return fmt.Errorf("failed to restore idempotency tracking: %w", err)
	}

	if err := d.restoreWorkflowInstance(ctx, state); err != nil {
		return fmt.Errorf("failed to restore workflow instance: %w", err)
	}

	d.logger.Debug("workflow state restoration completed", "workflow_id", state.WorkflowID)
	return nil
}

func (d *DAGReconstructor) buildCompletedNodeMap(executedNodes []domain.ExecutedNodeData) map[string]bool {
	completed := make(map[string]bool)
	for _, node := range executedNodes {
		if string(node.Status) == string(ports.NodeExecutionStatusCompleted) {
			completed[node.NodeName] = true
		}
	}
	return completed
}

func (d *DAGReconstructor) buildDAGNodeMap(dag domain.WorkflowDAG) map[string]*domain.DAGNode {
	nodeMap := make(map[string]*domain.DAGNode)
	for i, node := range dag.Nodes {
		nodeMap[node.ID] = &dag.Nodes[i]
	}
	return nodeMap
}

func (d *DAGReconstructor) identifyNextExecutableNodes(
	dag domain.WorkflowDAG,
	completedNodes map[string]bool,
	resumptionPoint *domain.ResumptionPoint,
	nodeMap map[string]*domain.DAGNode,
) ([]string, error) {
	nextExecutable := []string{}

	if resumptionPoint.NodeID != "" {
		if node, exists := nodeMap[resumptionPoint.NodeID]; exists {
			if d.arePrerequisitesCompleted(node.Dependencies, completedNodes) {
				nextExecutable = append(nextExecutable, resumptionPoint.NodeID)
			} else {
				return nil, fmt.Errorf("resumption node %s has incomplete prerequisites", resumptionPoint.NodeID)
			}
		} else {
			return nil, fmt.Errorf("resumption node %s not found in DAG", resumptionPoint.NodeID)
		}
	} else {
		for _, node := range dag.Nodes {
			if completedNodes[node.ID] {
				continue
			}

			if node.Status == domain.NodeStatusCompleted {
				continue
			}

			if d.arePrerequisitesCompleted(node.Dependencies, completedNodes) {
				nextExecutable = append(nextExecutable, node.ID)
			}
		}
	}

	sort.Strings(nextExecutable)
	return nextExecutable, nil
}

func (d *DAGReconstructor) arePrerequisitesCompleted(dependencies []string, completedNodes map[string]bool) bool {
	for _, dep := range dependencies {
		if !completedNodes[dep] {
			return false
		}
	}
	return true
}

func (d *DAGReconstructor) analyzeRequiredResources(nextExecutable []string, state *domain.CompleteWorkflowState) []string {
	resourceSet := make(map[string]bool)

	for _, nodeID := range nextExecutable {
		for _, resource := range state.ResourceStates {
			if resource.ClaimedBy == nodeID {
				resourceSet[resource.ID] = true
			}
		}
	}

	resources := make([]string, 0, len(resourceSet))
	for resource := range resourceSet {
		resources = append(resources, resource)
	}

	sort.Strings(resources)
	return resources
}

func (d *DAGReconstructor) estimateExecutionDuration(nextExecutable []string, state *domain.CompleteWorkflowState) time.Duration {
	if len(nextExecutable) == 0 {
		return 0
	}

	avgDuration := d.calculateAverageNodeDuration(state.ExecutedNodes)
	if avgDuration == 0 {
		avgDuration = 30 * time.Second
	}

	return time.Duration(len(nextExecutable)) * avgDuration
}

func (d *DAGReconstructor) calculateAverageNodeDuration(executedNodes []domain.ExecutedNodeData) time.Duration {
	if len(executedNodes) == 0 {
		return 0
	}

	var totalDuration time.Duration
	validCount := 0

	for _, node := range executedNodes {
		if node.Duration > 0 && string(node.Status) == string(ports.NodeExecutionStatusCompleted) {
			totalDuration += node.Duration
			validCount++
		}
	}

	if validCount == 0 {
		return 0
	}

	return totalDuration / time.Duration(validCount)
}

func (d *DAGReconstructor) restoreQueueStates(ctx context.Context, state *domain.CompleteWorkflowState) error {
	if d.engine.readyQueue == nil || d.engine.pendingQueue == nil {
		return fmt.Errorf("queue adapters not available for state restoration")
	}

	for _, queueItem := range state.QueueItems {
		item := ports.QueueItem{
			ID:             queueItem.ID,
			WorkflowID:     queueItem.WorkflowID,
			NodeName:       queueItem.NodeName,
			Config:         queueItem.Config,
			Priority:       queueItem.Priority,
			EnqueuedAt:     queueItem.EnqueuedAt,
			PartitionKey:   queueItem.PartitionKey,
			RetryCount:     queueItem.RetryCount,
			MaxRetries:     queueItem.MaxRetries,
			Checksum:       queueItem.Checksum,
			IdempotencyKey: queueItem.IdempotencyKey,
		}

		switch queueItem.QueueType {
		case "ready":
			if err := d.engine.readyQueue.Enqueue(ctx, item); err != nil {
				d.logger.Error("failed to restore ready queue item",
					"item_id", item.ID,
					"workflow_id", item.WorkflowID,
					"error", err)
				return err
			}
		case "pending":
			if err := d.engine.pendingQueue.Enqueue(ctx, item); err != nil {
				d.logger.Error("failed to restore pending queue item",
					"item_id", item.ID,
					"workflow_id", item.WorkflowID,
					"error", err)
				return err
			}
		default:
			d.logger.Warn("unknown queue type for item",
				"item_id", item.ID,
				"queue_type", queueItem.QueueType)
		}
	}

	d.logger.Debug("queue states restored",
		"workflow_id", state.WorkflowID,
		"queue_items", len(state.QueueItems))

	return nil
}

func (d *DAGReconstructor) restoreResourceClaims(ctx context.Context, state *domain.CompleteWorkflowState) error {
	if d.engine.resourceManager == nil {
		return fmt.Errorf("resource manager not available for state restoration")
	}

	for _, claim := range state.ClaimsData {
		if claim.ExpiresAt.Before(time.Now()) {
			d.logger.Debug("skipping expired resource claim",
				"claim_id", claim.ID,
				"resource_id", claim.ResourceID,
				"expired_at", claim.ExpiresAt)
			continue
		}

		if err := d.engine.resourceManager.AcquireNode(claim.ResourceID); err != nil {
			d.logger.Error("failed to restore resource claim",
				"claim_id", claim.ID,
				"resource_id", claim.ResourceID,
				"error", err)
			return err
		}

		d.logger.Debug("resource claim restored",
			"claim_id", claim.ID,
			"resource_id", claim.ResourceID,
			"workflow_id", claim.WorkflowID)
	}

	return nil
}

func (d *DAGReconstructor) restoreIdempotencyTracking(ctx context.Context, state *domain.CompleteWorkflowState) error {
	if d.engine.storage == nil {
		return fmt.Errorf("storage adapter not available for idempotency tracking restoration")
	}

	if len(state.IdempotencyKeys) == 0 {
		d.logger.Debug("no idempotency keys to restore",
			"workflow_id", state.WorkflowID)
		return nil
	}

	d.logger.Debug("restoring idempotency tracking",
		"workflow_id", state.WorkflowID,
		"idempotency_keys", len(state.IdempotencyKeys))

	// Group idempotency keys by status for batch processing
	activeKeys := []domain.IdempotencyKeyData{}
	expiredKeys := []domain.IdempotencyKeyData{}
	now := time.Now()

	for _, key := range state.IdempotencyKeys {
		if key.ExpiresAt.After(now) {
			activeKeys = append(activeKeys, key)
		} else {
			expiredKeys = append(expiredKeys, key)
		}
	}

	// Log expired keys but don't restore them
	if len(expiredKeys) > 0 {
		d.logger.Debug("skipping expired idempotency keys",
			"workflow_id", state.WorkflowID,
			"expired_count", len(expiredKeys))
	}

	// Restore active idempotency keys to storage
	for _, key := range activeKeys {
		// Store the idempotency key with its metadata
		idempotencyKey := fmt.Sprintf("workflow:idempotency:%s:%s", state.WorkflowID, key.Key)

		keyData := map[string]interface{}{
			"key":         key.Key,
			"node_id":     key.NodeID,
			"workflow_id": key.WorkflowID,
			"created_at":  key.CreatedAt,
			"expires_at":  key.ExpiresAt,
			"result":      key.Result,
			"metadata":    key.Metadata,
		}

		keyBytes, err := json.Marshal(keyData)
		if err != nil {
			d.logger.Error("failed to marshal idempotency key",
				"key", key.Key,
				"workflow_id", state.WorkflowID,
				"error", err)
			continue
		}

		if err := d.engine.storage.Put(ctx, idempotencyKey, keyBytes); err != nil {
			d.logger.Error("failed to restore idempotency key",
				"key", key.Key,
				"workflow_id", state.WorkflowID,
				"error", err)
			// Continue with other keys even if one fails
			continue
		}

		// Also maintain a reverse lookup for quick validation
		nodeKeyLookup := fmt.Sprintf("workflow:idempotency:node:%s:%s", key.NodeID, key.Key)
		lookupData := map[string]interface{}{
			"workflow_id": state.WorkflowID,
			"key":         key.Key,
			"node_id":     key.NodeID,
			"created_at":  key.CreatedAt,
		}

		lookupBytes, err := json.Marshal(lookupData)
		if err != nil {
			d.logger.Error("failed to marshal node lookup",
				"key", key.Key,
				"node_id", key.NodeID,
				"error", err)
			continue
		}

		if err := d.engine.storage.Put(ctx, nodeKeyLookup, lookupBytes); err != nil {
			d.logger.Error("failed to create node lookup for idempotency key",
				"key", key.Key,
				"node_id", key.NodeID,
				"error", err)
			// Not critical, continue
		}

		d.logger.Debug("idempotency key restored",
			"key", key.Key,
			"node_id", key.NodeID,
			"workflow_id", state.WorkflowID,
			"expires_at", key.ExpiresAt)
	}

	d.logger.Debug("idempotency tracking restored",
		"workflow_id", state.WorkflowID,
		"active_keys", len(activeKeys),
		"expired_keys", len(expiredKeys))

	return nil
}

func (d *DAGReconstructor) restoreWorkflowInstance(ctx context.Context, state *domain.CompleteWorkflowState) error {
	if d.engine.storage == nil {
		return fmt.Errorf("storage adapter not available for workflow instance restoration")
	}

	instance := &ports.WorkflowInstance{
		ID:           state.WorkflowID,
		Status:       ports.WorkflowState(state.Status),
		CurrentState: state.CurrentState,
		StartedAt:    state.StartedAt,
		CompletedAt:  state.CompletedAt,
		Metadata:     state.Metadata,
		LastError:    nil,
	}

	workflowKey := fmt.Sprintf("workflow:state:%s", state.WorkflowID)
	instanceBytes, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("failed to marshal workflow instance: %w", err)
	}
	if err := d.engine.storage.Put(ctx, workflowKey, instanceBytes); err != nil {
		return fmt.Errorf("failed to restore workflow instance: %w", err)
	}

	for _, executed := range state.ExecutedNodes {
		executedKey := fmt.Sprintf("workflow:executed_node:%s:%s", state.WorkflowID, executed.NodeName)
		executedBytes, err := json.Marshal(executed)
		if err != nil {
			d.logger.Error("failed to marshal executed node", "node_name", executed.NodeName, "error", err)
			continue
		}
		if err := d.engine.storage.Put(ctx, executedKey, executedBytes); err != nil {
			d.logger.Error("failed to restore executed node",
				"workflow_id", state.WorkflowID,
				"node_name", executed.NodeName,
				"error", err)
		}
	}

	for _, checkpoint := range state.Checkpoints {
		checkpointKey := fmt.Sprintf("workflow:checkpoint:%s:%s", state.WorkflowID, checkpoint.ID)
		checkpointBytes, err := json.Marshal(checkpoint)
		if err != nil {
			d.logger.Error("failed to marshal checkpoint", "checkpoint_id", checkpoint.ID, "error", err)
			continue
		}
		if err := d.engine.storage.Put(ctx, checkpointKey, checkpointBytes); err != nil {
			d.logger.Error("failed to restore checkpoint",
				"workflow_id", state.WorkflowID,
				"checkpoint_id", checkpoint.ID,
				"error", err)
		}
	}

	d.logger.Debug("workflow instance restored",
		"workflow_id", state.WorkflowID,
		"executed_nodes", len(state.ExecutedNodes),
		"checkpoints", len(state.Checkpoints))

	return nil
}

func (d *DAGReconstructor) ValidateResumptionDependencies(
	ctx context.Context,
	state *domain.CompleteWorkflowState,
	resumptionPoint *domain.ResumptionPoint,
) error {
	nodeMap := d.buildDAGNodeMap(state.ExecutionDAG)
	completedNodes := d.buildCompletedNodeMap(state.ExecutedNodes)

	node, exists := nodeMap[resumptionPoint.NodeID]
	if !exists {
		return fmt.Errorf("resumption node %s not found in DAG", resumptionPoint.NodeID)
	}

	missingDeps := []string{}
	for _, dep := range node.Dependencies {
		if !completedNodes[dep] {
			missingDeps = append(missingDeps, dep)
		}
	}

	if len(missingDeps) > 0 {
		return fmt.Errorf("resumption node %s has unmet dependencies: %v", resumptionPoint.NodeID, missingDeps)
	}

	for _, prereq := range resumptionPoint.Prerequisites {
		if !completedNodes[prereq] {
			return fmt.Errorf("resumption point prerequisite %s is not completed", prereq)
		}
	}

	return nil
}
