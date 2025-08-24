package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type WorkflowDataCollector struct {
	storage         ports.StoragePort
	pendingQueue    ports.QueuePort
	readyQueue      ports.QueuePort
	nodeRegistry    ports.NodeRegistryPort
	resourceManager ports.ResourceManagerPort
	metricsTracker  *MetricsTracker
	nodeID          string
	logger          *slog.Logger
}

func NewWorkflowDataCollector(storage ports.StoragePort, pendingQueue ports.QueuePort, readyQueue ports.QueuePort, nodeRegistry ports.NodeRegistryPort, resourceManager ports.ResourceManagerPort, metricsTracker *MetricsTracker, nodeID string, logger *slog.Logger) *WorkflowDataCollector {
	return &WorkflowDataCollector{
		storage:         storage,
		pendingQueue:    pendingQueue,
		readyQueue:      readyQueue,
		nodeRegistry:    nodeRegistry,
		resourceManager: resourceManager,
		metricsTracker:  metricsTracker,
		nodeID:          nodeID,
		logger:          logger.With("component", "workflow-data-collector"),
	}
}

func (wdc *WorkflowDataCollector) CollectWorkflowData(ctx context.Context, workflow *WorkflowInstance) (*domain.WorkflowCompletionData, error) {
	workflow.mu.RLock()
	workflowID := workflow.ID
	finalState := workflow.CurrentState
	status := string(workflow.Status)
	startedAt := workflow.StartedAt
	completedAt := time.Now()
	if workflow.CompletedAt != nil {
		completedAt = *workflow.CompletedAt
	}
	metadata := workflow.Metadata
	workflow.mu.RUnlock()

	data := &domain.WorkflowCompletionData{
		WorkflowID:  workflowID,
		FinalState:  finalState,
		Status:      status,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		Duration:    completedAt.Sub(startedAt),
		Metadata:    metadata,
	}

	var err error

	data.ExecutedNodes, err = wdc.collectExecutedNodes(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect executed nodes: %w", err)
	}

	data.Checkpoints, err = wdc.collectCheckpoints(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect checkpoints: %w", err)
	}

	data.QueueItems, err = wdc.collectQueueItems(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect queue items: %w", err)
	}

	data.IdempotencyKeys, err = wdc.collectIdempotencyKeys(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect idempotency keys: %w", err)
	}

	data.ClaimsData, err = wdc.collectClaimsData(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect claims data: %w", err)
	}

	data.PendingNodes, err = wdc.collectPendingNodes(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect pending nodes: %w", err)
	}

	data.ReadyNodes, err = wdc.collectReadyNodes(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect ready nodes: %w", err)
	}

	data.ResourceStates, err = wdc.collectResourceStates(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect resource states: %w", err)
	}

	data.Dependencies, err = wdc.collectDependencies(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect dependencies: %w", err)
	}

	data.ExecutionMetrics = wdc.collectExecutionMetrics(ctx, workflowID, data.ExecutedNodes)

	data.SystemContext = wdc.collectSystemContext()

	return data, nil
}

func (wdc *WorkflowDataCollector) collectExecutedNodes(ctx context.Context, workflowID string) ([]domain.ExecutedNodeData, error) {
	prefix := fmt.Sprintf("workflow:execution:%s:", workflowID)

	items, err := wdc.storage.List(ctx, prefix)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list executed nodes",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	result := make([]domain.ExecutedNodeData, 0, len(items))

	for _, item := range items {
		var nodeData map[string]interface{}
		if err := json.Unmarshal(item.Value, &nodeData); err != nil {
			continue
		}

		executedAtStr, ok := nodeData["executed_at"].(string)
		if !ok {
			continue
		}

		executedAt, err := time.Parse(time.RFC3339Nano, executedAtStr)
		if err != nil {
			continue
		}

		durationNanos, ok := nodeData["duration"].(float64)
		if !ok {
			continue
		}

		duration := time.Duration(int64(durationNanos))

		nodeResult := domain.ExecutedNodeData{
			NodeName:   nodeData["node_name"].(string),
			ExecutedAt: executedAt,
			Duration:   duration,
			Status:     nodeData["status"].(string),
			Config:     nodeData["config"],
			Results:    nodeData["results"],
		}

		if errorMsg, exists := nodeData["error"].(string); exists {
			nodeResult.Error = &errorMsg
		}

		result = append(result, nodeResult)
	}

	return result, nil
}

func (wdc *WorkflowDataCollector) collectCheckpoints(ctx context.Context, workflowID string) ([]domain.CheckpointData, error) {
	prefix := fmt.Sprintf("workflow:checkpoint:%s:", workflowID)

	items, err := wdc.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list checkpoints: %w", err)
	}

	result := make([]domain.CheckpointData, 0, len(items))

	for _, item := range items {
		var state interface{}
		if err := json.Unmarshal(item.Value, &state); err != nil {
			continue
		}

		timestampStr := strings.TrimPrefix(item.Key, prefix)
		timestamp, err := time.Parse("20060102150405", timestampStr)
		if err != nil {
			continue
		}

		result = append(result, domain.CheckpointData{
			ID:         item.Key,
			WorkflowID: workflowID,
			NodeID:     "",
			State:      state,
			CreatedAt:  timestamp,
			Metadata:   map[string]interface{}{},
		})
	}

	return result, nil
}

func (wdc *WorkflowDataCollector) CollectWorkflowErrorData(ctx context.Context, workflow *WorkflowInstance, err error, failedNode string, stackTrace string) (*domain.WorkflowErrorData, error) {
	workflow.mu.RLock()
	workflowID := workflow.ID
	currentState := workflow.CurrentState
	status := string(workflow.Status)
	startedAt := workflow.StartedAt
	failedAt := time.Now()
	if workflow.CompletedAt != nil {
		failedAt = *workflow.CompletedAt
	}
	metadata := workflow.Metadata
	workflow.mu.RUnlock()

	data := &domain.WorkflowErrorData{
		WorkflowID:    workflowID,
		CurrentState:  currentState,
		Status:        status,
		StartedAt:     startedAt,
		FailedAt:      failedAt,
		Duration:      failedAt.Sub(startedAt),
		Metadata:      metadata,
		Error:         err,
		ErrorType:     fmt.Sprintf("%T", err),
		FailedNode:    failedNode,
		StackTrace:    stackTrace,
		RecoveryHints: wdc.generateRecoveryHints(err, failedNode),
	}

	var collectErr error

	data.ExecutedNodes, collectErr = wdc.collectExecutedNodes(ctx, workflowID)
	if collectErr != nil {
		return nil, fmt.Errorf("failed to collect executed nodes: %w", collectErr)
	}

	data.PendingNodes, collectErr = wdc.collectPendingNodes(ctx, workflowID)
	if collectErr != nil {
		return nil, fmt.Errorf("failed to collect pending nodes: %w", collectErr)
	}

	data.ReadyNodes, collectErr = wdc.collectReadyNodes(ctx, workflowID)
	if collectErr != nil {
		return nil, fmt.Errorf("failed to collect ready nodes: %w", collectErr)
	}

	data.Checkpoints, collectErr = wdc.collectCheckpoints(ctx, workflowID)
	if collectErr != nil {
		return nil, fmt.Errorf("failed to collect checkpoints: %w", collectErr)
	}

	data.QueueItems, collectErr = wdc.collectQueueItems(ctx, workflowID)
	if collectErr != nil {
		return nil, fmt.Errorf("failed to collect queue items: %w", collectErr)
	}

	data.IdempotencyKeys, collectErr = wdc.collectIdempotencyKeys(ctx, workflowID)
	if collectErr != nil {
		return nil, fmt.Errorf("failed to collect idempotency keys: %w", collectErr)
	}

	data.ClaimsData, collectErr = wdc.collectClaimsData(ctx, workflowID)
	if collectErr != nil {
		return nil, fmt.Errorf("failed to collect claims data: %w", collectErr)
	}

	data.ResourceStates, collectErr = wdc.collectResourceStates(ctx, workflowID)
	if collectErr != nil {
		return nil, fmt.Errorf("failed to collect resource states: %w", collectErr)
	}

	data.Dependencies, collectErr = wdc.collectDependencies(ctx, workflowID)
	if collectErr != nil {
		return nil, fmt.Errorf("failed to collect dependencies: %w", collectErr)
	}

	data.ExecutionMetrics = wdc.collectExecutionMetrics(ctx, workflowID, data.ExecutedNodes)
	data.SystemContext = wdc.collectSystemContext()

	return data, nil
}

func (wdc *WorkflowDataCollector) collectPendingNodes(ctx context.Context, workflowID string) ([]domain.PendingNodeData, error) {
	result := make([]domain.PendingNodeData, 0)

	pendingItems, err := wdc.pendingQueue.GetItems(ctx)
	if err != nil {
		return result, nil
	}

	for _, item := range pendingItems {
		if item.WorkflowID == workflowID {
			result = append(result, domain.PendingNodeData{
				NodeName: item.NodeName,
				Config:   item.Config,
				QueuedAt: item.EnqueuedAt,
				Priority: item.Priority,
				Reason:   "awaiting_dependencies",
			})
		}
	}

	return result, nil
}

func (wdc *WorkflowDataCollector) collectReadyNodes(ctx context.Context, workflowID string) ([]domain.ReadyNodeData, error) {
	result := make([]domain.ReadyNodeData, 0)

	readyItems, err := wdc.readyQueue.GetItems(ctx)
	if err != nil {
		return result, nil
	}

	for _, item := range readyItems {
		if item.WorkflowID == workflowID {
			result = append(result, domain.ReadyNodeData{
				NodeName:       item.NodeName,
				Config:         item.Config,
				QueuedAt:       item.EnqueuedAt,
				Priority:       item.Priority,
				WorkflowID:     workflowID,
				IdempotencyKey: item.IdempotencyKey,
			})
		}
	}

	return result, nil
}

func (wdc *WorkflowDataCollector) collectResourceStates(ctx context.Context, workflowID string) ([]domain.ResourceStateData, error) {
	result := make([]domain.ResourceStateData, 0)

	if wdc.resourceManager != nil {
		resources, err := wdc.resourceManager.GetResourceStates(ctx, workflowID)
		if err != nil {
			wdc.logger.Warn("failed to get resource states from manager",
				"workflow_id", workflowID,
				"error", err)
			// Return empty result but don't fail - resource states may not be critical
			return result, nil
		}

		for _, resource := range resources {
			result = append(result, domain.ResourceStateData{
				ID:        resource.ID,
				Type:      resource.Type,
				State:     resource.State,
				ClaimedBy: resource.ClaimedBy,
				ClaimedAt: resource.ClaimedAt,
				ExpiresAt: resource.ExpiresAt,
				Metadata:  resource.Metadata,
			})
		}

		wdc.logger.Debug("collected resource states",
			"workflow_id", workflowID,
			"count", len(resources))
	}

	return result, nil
}

func (wdc *WorkflowDataCollector) collectExecutionMetrics(ctx context.Context, workflowID string, executedNodes []domain.ExecutedNodeData) domain.ExecutionMetricsData {
	metrics := domain.ExecutionMetricsData{
		NodesExecuted: len(executedNodes),
		NodeMetrics:   make(map[string]domain.NodeMetricsData),
	}

	if len(executedNodes) > 0 {
		var totalTime time.Duration
		for _, node := range executedNodes {
			totalTime += node.Duration

			nodeMetric := domain.NodeMetricsData{
				ExecutionCount: 1,
				TotalTime:      node.Duration,
				AverageTime:    node.Duration,
				SuccessRate:    1.0,
				LastExecutedAt: node.ExecutedAt,
			}

			if node.Error != nil {
				nodeMetric.SuccessRate = 0.0
			}

			metrics.NodeMetrics[node.NodeName] = nodeMetric
		}

		metrics.TotalExecutionTime = totalTime
		metrics.AverageNodeTime = time.Duration(int64(totalTime) / int64(len(executedNodes)))
	}

	if wdc.pendingQueue != nil {
		pendingItems, _ := wdc.pendingQueue.GetItems(ctx)
		metrics.QueueMetrics.PendingQueueSize = len(pendingItems)
	}

	if wdc.readyQueue != nil {
		readyItems, _ := wdc.readyQueue.GetItems(ctx)
		metrics.QueueMetrics.ReadyQueueSize = len(readyItems)
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	metrics.MemoryMetrics = domain.MemoryMetricsData{
		HeapSize:       memStats.HeapSys,
		HeapInUse:      memStats.HeapInuse,
		StackInUse:     memStats.StackInuse,
		GoroutineCount: runtime.NumGoroutine(),
		GCPauseTotal:   memStats.PauseTotalNs,
		NextGC:         memStats.NextGC,
	}

	return metrics
}

func (wdc *WorkflowDataCollector) collectSystemContext() domain.SystemContextData {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return domain.SystemContextData{
		EngineVersion: "1.0.0",
		NodeID:        wdc.nodeID,
		ClusterState:  "active",
		Timestamp:     time.Now(),
		RuntimeInfo: domain.RuntimeInfoData{
			GoVersion:     runtime.Version(),
			GOOS:          runtime.GOOS,
			GOARCH:        runtime.GOARCH,
			NumCPU:        runtime.NumCPU(),
			NumGoroutines: runtime.NumGoroutine(),
		},
	}
}

func (wdc *WorkflowDataCollector) generateRecoveryHints(err error, failedNode string) []string {
	hints := make([]string, 0)

	errStr := strings.ToLower(err.Error())

	if strings.Contains(errStr, "timeout") {
		hints = append(hints, "Consider increasing node execution timeout")
		hints = append(hints, "Check if external dependencies are responding")
	}

	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "network") {
		hints = append(hints, "Verify network connectivity to external services")
		hints = append(hints, "Check service discovery configuration")
	}

	if strings.Contains(errStr, "resource") {
		hints = append(hints, "Verify resource availability and limits")
		hints = append(hints, "Check for resource leaks in previous executions")
	}

	if strings.Contains(errStr, "permission") || strings.Contains(errStr, "unauthorized") {
		hints = append(hints, "Verify authentication credentials and permissions")
	}

	if failedNode != "" {
		hints = append(hints, fmt.Sprintf("Review node '%s' configuration and dependencies", failedNode))
	}

	hints = append(hints, "Check workflow state consistency before resuming")
	hints = append(hints, "Consider resuming from last stable checkpoint")

	return hints
}

func (wdc *WorkflowDataCollector) ExportCompleteWorkflowState(ctx context.Context, workflow *WorkflowInstance) (*domain.CompleteWorkflowState, error) {
	workflow.mu.RLock()
	workflowID := workflow.ID
	status := workflow.Status
	startedAt := workflow.StartedAt
	completedAt := workflow.CompletedAt
	currentState := workflow.CurrentState
	metadata := workflow.Metadata
	workflow.mu.RUnlock()

	state := &domain.CompleteWorkflowState{
		WorkflowID:   workflowID,
		Status:       domain.WorkflowState(status),
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		CurrentState: currentState,
		Metadata:     metadata,
		Version:      1,
	}

	if completedAt != nil {
		state.Duration = completedAt.Sub(startedAt)
	} else {
		state.Duration = time.Since(startedAt)
	}

	var err error

	state.ExecutedNodes, err = wdc.collectExecutedNodes(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect executed nodes: %w", err)
	}

	state.PendingNodes, err = wdc.collectPendingNodes(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect pending nodes: %w", err)
	}

	state.ReadyNodes, err = wdc.collectReadyNodes(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect ready nodes: %w", err)
	}

	state.QueueItems, err = wdc.collectQueueItems(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect queue items: %w", err)
	}

	state.ClaimsData, err = wdc.collectClaimsData(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect claims data: %w", err)
	}

	state.IdempotencyKeys, err = wdc.collectIdempotencyKeys(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect idempotency keys: %w", err)
	}

	state.Checkpoints, err = wdc.collectCheckpoints(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect checkpoints: %w", err)
	}

	state.ExecutionDAG, err = wdc.constructExecutionDAG(ctx, workflowID, state.ExecutedNodes, state.PendingNodes, state.ReadyNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to construct execution DAG: %w", err)
	}

	if wdc.resourceManager != nil {
		resourceStates, err := wdc.resourceManager.GetResourceStates(ctx, workflowID)
		if err != nil {
			return nil, fmt.Errorf("failed to collect resource states: %w", err)
		}
		state.ResourceStates = wdc.convertResourceStates(resourceStates)
	}

	state.Dependencies, err = wdc.collectDependencies(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect dependencies: %w", err)
	}

	if len(state.ExecutedNodes) > 0 {
		lastExecuted := state.ExecutedNodes[len(state.ExecutedNodes)-1]
		state.LastExecutedNode = lastExecuted.NodeName
	}

	state.StateHash = wdc.computeStateHash(state)

	return state, nil
}

func (wdc *WorkflowDataCollector) collectQueueItems(ctx context.Context, workflowID string) ([]domain.QueueItemData, error) {
	var allItems []domain.QueueItemData

	if wdc.readyQueue != nil {
		readyItems, err := wdc.readyQueue.GetItems(ctx)
		if err != nil && !domain.IsNotFoundError(err) {
			return nil, fmt.Errorf("failed to get ready queue items: %w", err)
		}
		for _, item := range readyItems {
			if item.WorkflowID == workflowID {
				allItems = append(allItems, wdc.convertQueueItem(item, "ready"))
			}
		}
	}

	if wdc.pendingQueue != nil {
		pendingItems, err := wdc.pendingQueue.GetItems(ctx)
		if err != nil && !domain.IsNotFoundError(err) {
			return nil, fmt.Errorf("failed to get pending queue items: %w", err)
		}
		for _, item := range pendingItems {
			if item.WorkflowID == workflowID {
				allItems = append(allItems, wdc.convertQueueItem(item, "pending"))
			}
		}
	}

	return allItems, nil
}

func (wdc *WorkflowDataCollector) convertQueueItem(item ports.QueueItem, queueType string) domain.QueueItemData {
	return domain.QueueItemData{
		ID:             item.ID,
		WorkflowID:     item.WorkflowID,
		NodeName:       item.NodeName,
		Config:         item.Config,
		Priority:       item.Priority,
		EnqueuedAt:     item.EnqueuedAt,
		PartitionKey:   item.PartitionKey,
		RetryCount:     item.RetryCount,
		MaxRetries:     item.MaxRetries,
		Checksum:       item.Checksum,
		IdempotencyKey: item.IdempotencyKey,
		QueueType:      queueType,
		Metadata:       map[string]interface{}{},
	}
}

func (wdc *WorkflowDataCollector) collectClaimsData(ctx context.Context, workflowID string) ([]domain.ClaimData, error) {
	if wdc.storage == nil {
		return []domain.ClaimData{}, nil
	}

	claimPrefix := fmt.Sprintf("claim:%s:", workflowID)
	claims, err := wdc.storage.List(ctx, claimPrefix)
	if err != nil && !domain.IsNotFoundError(err) {
		return nil, fmt.Errorf("failed to list claims: %w", err)
	}

	var claimsData []domain.ClaimData
	for _, kv := range claims {
		var claim domain.ClaimData
		if err := json.Unmarshal([]byte(fmt.Sprintf("%v", kv.Value)), &claim); err == nil {
			claimsData = append(claimsData, claim)
		}
	}

	return claimsData, nil
}

func (wdc *WorkflowDataCollector) collectIdempotencyKeys(ctx context.Context, workflowID string) ([]domain.IdempotencyKeyData, error) {
	if wdc.storage == nil {
		return []domain.IdempotencyKeyData{}, nil
	}

	idempotencyPrefix := fmt.Sprintf("workflow:idempotency:%s:", workflowID)
	keys, err := wdc.storage.List(ctx, idempotencyPrefix)
	if err != nil && !domain.IsNotFoundError(err) {
		return nil, fmt.Errorf("failed to list idempotency keys: %w", err)
	}

	var idempotencyData []domain.IdempotencyKeyData
	for _, kv := range keys {
		var idempotencyKey domain.IdempotencyKeyData
		if err := json.Unmarshal([]byte(fmt.Sprintf("%v", kv.Value)), &idempotencyKey); err == nil {
			idempotencyData = append(idempotencyData, idempotencyKey)
		}
	}

	return idempotencyData, nil
}

func (wdc *WorkflowDataCollector) constructExecutionDAG(ctx context.Context, workflowID string, executedNodes []domain.ExecutedNodeData, pendingNodes []domain.PendingNodeData, readyNodes []domain.ReadyNodeData) (domain.WorkflowDAG, error) {
	// Use the DAG manager to build a proper DAG
	dagManager := NewDAGManager(wdc.logger)

	// First build the basic structure
	dag := domain.WorkflowDAG{
		Nodes:  []domain.DAGNode{},
		Edges:  []domain.DAGEdge{},
		Roots:  []string{},
		Leaves: []string{},
	}

	nodeMap := make(map[string]domain.DAGNode)
	dependencies := make(map[string][]string)

	// Build nodes from executed nodes and track their next nodes as edges
	for _, executed := range executedNodes {
		node := domain.DAGNode{
			ID:           executed.NodeName,
			Type:         "executed",
			Status:       domain.NodeStatusCompleted,
			Dependencies: []string{},
			Metadata: map[string]interface{}{
				"executed_at": executed.ExecutedAt,
				"duration":    executed.Duration,
				"results":     executed.Results,
			},
		}

		// Track dependencies based on TriggeredBy field
		if executed.TriggeredBy != "" {
			node.Dependencies = append(node.Dependencies, executed.TriggeredBy)
			dependencies[executed.NodeName] = append(dependencies[executed.NodeName], executed.TriggeredBy)
		}

		// Extract next nodes from results to build edges
		if results, ok := executed.Results.(map[string]interface{}); ok {
			if nextNodes, ok := results["next_nodes"].([]interface{}); ok {
				for _, next := range nextNodes {
					if nextMap, ok := next.(map[string]interface{}); ok {
						if nextName, ok := nextMap["node_name"].(string); ok {
							// Create edge from this node to next node
							edge := domain.DAGEdge{
								From: executed.NodeName,
								To:   nextName,
								Type: "execution",
								Metadata: map[string]interface{}{
									"triggered_at": executed.ExecutedAt,
								},
							}
							dag.Edges = append(dag.Edges, edge)
							// Track that the next node depends on this one
							if dependencies[nextName] == nil {
								dependencies[nextName] = []string{}
							}
							dependencies[nextName] = append(dependencies[nextName], executed.NodeName)
						}
					}
				}
			}
		}

		nodeMap[executed.NodeName] = node
	}

	// Build nodes from pending nodes
	for _, pending := range pendingNodes {
		deps := dependencies[pending.NodeName]
		if pending.TriggeredBy != "" && !contains(deps, pending.TriggeredBy) {
			deps = append(deps, pending.TriggeredBy)
		}

		node := domain.DAGNode{
			ID:           pending.NodeName,
			Type:         "pending",
			Status:       domain.NodeStatusPending,
			Dependencies: deps,
			Metadata: map[string]interface{}{
				"queued_at": pending.QueuedAt,
				"priority":  pending.Priority,
				"reason":    pending.Reason,
			},
		}

		// If triggered by another node, add edge
		if pending.TriggeredBy != "" {
			edge := domain.DAGEdge{
				From: pending.TriggeredBy,
				To:   pending.NodeName,
				Type: "pending",
				Metadata: map[string]interface{}{
					"queued_at": pending.QueuedAt,
				},
			}
			dag.Edges = append(dag.Edges, edge)
		}

		nodeMap[pending.NodeName] = node
	}

	// Build nodes from ready nodes
	for _, ready := range readyNodes {
		deps := dependencies[ready.NodeName]
		if ready.TriggeredBy != "" && !contains(deps, ready.TriggeredBy) {
			deps = append(deps, ready.TriggeredBy)
		}

		node := domain.DAGNode{
			ID:           ready.NodeName,
			Type:         "ready",
			Status:       domain.NodeStatusReady,
			Dependencies: deps,
			Metadata: map[string]interface{}{
				"queued_at":       ready.QueuedAt,
				"priority":        ready.Priority,
				"idempotency_key": ready.IdempotencyKey,
			},
		}

		// Ready nodes might have been triggered by completed nodes
		if ready.TriggeredBy != "" {
			edge := domain.DAGEdge{
				From: ready.TriggeredBy,
				To:   ready.NodeName,
				Type: "ready",
				Metadata: map[string]interface{}{
					"queued_at": ready.QueuedAt,
				},
			}
			dag.Edges = append(dag.Edges, edge)
		}

		nodeMap[ready.NodeName] = node
	}

	// Add all nodes to the DAG
	for _, node := range nodeMap {
		dag.Nodes = append(dag.Nodes, node)
	}

	// Sort nodes for consistent ordering
	sort.Slice(dag.Nodes, func(i, j int) bool {
		return dag.Nodes[i].ID < dag.Nodes[j].ID
	})

	// Now use the DAG manager to validate and identify roots/leaves
	// Build the DAG using heimdalr/dag for proper analysis
	_, err := dagManager.BuildDAGFromState(&domain.CompleteWorkflowState{
		WorkflowID:   workflowID,
		ExecutionDAG: dag,
	})
	if err != nil {
		// If there's a cycle or other issue, log it but continue
		wdc.logger.Warn("DAG validation found issues", "error", err)
	} else {
		// Get roots and leaves from the proper DAG
		if roots, err := dagManager.GetRoots(workflowID); err == nil {
			dag.Roots = roots
		}
		if leaves, err := dagManager.GetLeaves(workflowID); err == nil {
			dag.Leaves = leaves
		}
	}

	// Fallback to simple detection if DAG manager fails
	if len(dag.Roots) == 0 || len(dag.Leaves) == 0 {
		hasIncomingEdge := make(map[string]bool)
		hasOutgoingEdge := make(map[string]bool)

		for _, edge := range dag.Edges {
			hasOutgoingEdge[edge.From] = true
			hasIncomingEdge[edge.To] = true
		}

		// Identify root nodes (nodes with no incoming edges)
		if len(dag.Roots) == 0 {
			for nodeID := range nodeMap {
				if !hasIncomingEdge[nodeID] {
					dag.Roots = append(dag.Roots, nodeID)
				}
			}
		}

		// Identify leaf nodes (nodes with no outgoing edges)
		if len(dag.Leaves) == 0 {
			for nodeID := range nodeMap {
				if !hasOutgoingEdge[nodeID] {
					dag.Leaves = append(dag.Leaves, nodeID)
				}
			}
		}

		sort.Strings(dag.Roots)
		sort.Strings(dag.Leaves)
	}

	return dag, nil
}

// Helper function to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (wdc *WorkflowDataCollector) convertResourceStates(resourceStates []ports.ResourceState) []domain.ResourceStateData {
	var converted []domain.ResourceStateData
	for _, rs := range resourceStates {
		converted = append(converted, domain.ResourceStateData{
			ID:        rs.ID,
			Type:      rs.Type,
			State:     rs.State,
			ClaimedBy: rs.ClaimedBy,
			ClaimedAt: rs.ClaimedAt,
			ExpiresAt: rs.ExpiresAt,
			Metadata:  rs.Metadata,
		})
	}
	return converted
}

func (wdc *WorkflowDataCollector) collectDependencies(ctx context.Context, workflowID string) ([]domain.DependencyData, error) {
	var dependencies []domain.DependencyData

	if wdc.storage == nil {
		return dependencies, nil
	}

	// First, try to collect stored dependencies
	dependencyPrefix := fmt.Sprintf("workflow:dependency:%s:", workflowID)
	deps, err := wdc.storage.List(ctx, dependencyPrefix)
	if err != nil && !domain.IsNotFoundError(err) {
		wdc.logger.Warn("failed to list stored dependencies, will derive from execution data",
			"workflow_id", workflowID,
			"error", err)
	} else {
		for _, kv := range deps {
			var dep domain.DependencyData
			if err := json.Unmarshal(kv.Value, &dep); err == nil {
				dependencies = append(dependencies, dep)
			}
		}

		// If we found stored dependencies, return them
		if len(dependencies) > 0 {
			return dependencies, nil
		}
	}

	// If no stored dependencies, derive them from executed nodes
	executedNodes, err := wdc.collectExecutedNodes(ctx, workflowID)
	if err != nil {
		return dependencies, nil // Return empty if we can't collect executed nodes
	}

	// Build dependencies from the execution flow
	dependencyMap := make(map[string]bool) // Use map to avoid duplicates

	for _, executed := range executedNodes {
		// If this node was triggered by another, that's a dependency
		if executed.TriggeredBy != "" {
			depKey := fmt.Sprintf("%s->%s", executed.TriggeredBy, executed.NodeName)
			if !dependencyMap[depKey] {
				dependency := domain.DependencyData{
					SourceNode:     executed.TriggeredBy,
					TargetNode:     executed.NodeName,
					DependencyType: "execution",
					CreatedAt:      executed.ExecutedAt,
					Metadata: map[string]interface{}{
						"derived_from": "execution_history",
					},
				}
				dependencies = append(dependencies, dependency)
				dependencyMap[depKey] = true
			}
		}

		// Extract dependencies from node results (next_nodes)
		if results, ok := executed.Results.(map[string]interface{}); ok {
			if nextNodes, ok := results["next_nodes"].([]interface{}); ok {
				for _, next := range nextNodes {
					if nextMap, ok := next.(map[string]interface{}); ok {
						if nextName, ok := nextMap["node_name"].(string); ok {
							depKey := fmt.Sprintf("%s->%s", executed.NodeName, nextName)
							if !dependencyMap[depKey] {
								dependency := domain.DependencyData{
									SourceNode:     executed.NodeName,
									TargetNode:     nextName,
									DependencyType: "trigger",
									CreatedAt:      executed.ExecutedAt,
									Metadata: map[string]interface{}{
										"derived_from": "next_nodes",
									},
								}

								// Add any config data passed to the next node
								if config, ok := nextMap["config"]; ok {
									dependency.Data = config
								}

								dependencies = append(dependencies, dependency)
								dependencyMap[depKey] = true
							}
						}
					}
				}
			}
		}
	}

	// Also check pending and ready nodes for dependencies
	pendingNodes, _ := wdc.collectPendingNodes(ctx, workflowID)
	for _, pending := range pendingNodes {
		if pending.TriggeredBy != "" {
			depKey := fmt.Sprintf("%s->%s", pending.TriggeredBy, pending.NodeName)
			if !dependencyMap[depKey] {
				dependency := domain.DependencyData{
					SourceNode:     pending.TriggeredBy,
					TargetNode:     pending.NodeName,
					DependencyType: "pending",
					CreatedAt:      pending.QueuedAt,
					Metadata: map[string]interface{}{
						"status": "pending",
						"reason": pending.Reason,
					},
				}
				dependencies = append(dependencies, dependency)
				dependencyMap[depKey] = true
			}
		}
	}

	readyNodes, _ := wdc.collectReadyNodes(ctx, workflowID)
	for _, ready := range readyNodes {
		if ready.TriggeredBy != "" {
			depKey := fmt.Sprintf("%s->%s", ready.TriggeredBy, ready.NodeName)
			if !dependencyMap[depKey] {
				dependency := domain.DependencyData{
					SourceNode:     ready.TriggeredBy,
					TargetNode:     ready.NodeName,
					DependencyType: "ready",
					CreatedAt:      ready.QueuedAt,
					Metadata: map[string]interface{}{
						"status":      "ready",
						"workflow_id": ready.WorkflowID,
					},
				}
				dependencies = append(dependencies, dependency)
				dependencyMap[depKey] = true
			}
		}
	}

	// Sort dependencies by creation time for consistent ordering
	sort.Slice(dependencies, func(i, j int) bool {
		return dependencies[i].CreatedAt.Before(dependencies[j].CreatedAt)
	})

	wdc.logger.Debug("dependencies collected",
		"workflow_id", workflowID,
		"count", len(dependencies),
		"derived", len(dependencyMap))

	return dependencies, nil
}

func (wdc *WorkflowDataCollector) computeStateHash(state *domain.CompleteWorkflowState) string {
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
