package engine

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/heimdalr/dag"
)

// DAGManager manages workflow DAGs using the heimdalr/dag library
type DAGManager struct {
	dags   map[string]*dag.DAG
	mu     sync.RWMutex
	logger *slog.Logger
}

// NewDAGManager creates a new DAG manager
func NewDAGManager(logger *slog.Logger) *DAGManager {
	return &DAGManager{
		dags:   make(map[string]*dag.DAG),
		logger: logger.With("component", "dag-manager"),
	}
}

// BuildDAGFromState constructs a proper DAG from workflow state
func (dm *DAGManager) BuildDAGFromState(state *domain.CompleteWorkflowState) (*dag.DAG, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Create a new DAG instance
	workflowDAG := dag.NewDAG()

	// First, add all nodes as vertices
	nodeMap := make(map[string]interface{})
	for _, node := range state.ExecutionDAG.Nodes {
		nodeData := &WorkflowNode{
			ID:           node.ID,
			Type:         node.Type,
			Status:       node.Status,
			Dependencies: node.Dependencies,
			Metadata:     node.Metadata,
		}
		nodeMap[node.ID] = nodeData

		// Add vertex to DAG
		err := workflowDAG.AddVertexByID(node.ID, nodeData)
		if err != nil {
			dm.logger.Error("failed to add vertex to DAG",
				"node_id", node.ID,
				"error", err)
			return nil, fmt.Errorf("failed to add vertex %s: %w", node.ID, err)
		}
	}

	// Add edges based on explicit edge definitions
	for _, edge := range state.ExecutionDAG.Edges {
		err := workflowDAG.AddEdge(edge.From, edge.To)
		if err != nil {
			// Check if it's a cycle/loop error
			if _, ok := err.(dag.EdgeLoopError); ok {
				dm.logger.Error("edge would create cycle in DAG",
					"from", edge.From,
					"to", edge.To)
				return nil, fmt.Errorf("edge from %s to %s would create cycle", edge.From, edge.To)
			}
			dm.logger.Error("failed to add edge to DAG",
				"from", edge.From,
				"to", edge.To,
				"error", err)
			return nil, fmt.Errorf("failed to add edge from %s to %s: %w", edge.From, edge.To, err)
		}
	}

	// Add edges based on node dependencies (validate dependencies exist)
	for _, node := range state.ExecutionDAG.Nodes {
		for _, depID := range node.Dependencies {
			// Validate that the dependency node exists
			if _, exists := nodeMap[depID]; !exists {
				return nil, fmt.Errorf("node %s depends on non-existent node %s", node.ID, depID)
			}

			// Check if edge already exists (from explicit edges)
			children, err := workflowDAG.GetChildren(depID)
			if err == nil && children != nil {
				if _, exists := children[node.ID]; exists {
					// Edge already exists, skip
					continue
				}
			}

			// Add edge from dependency to node
			err = workflowDAG.AddEdge(depID, node.ID)
			if err != nil {
				// Check if it's a cycle/loop error
				if _, ok := err.(dag.EdgeLoopError); ok {
					dm.logger.Error("dependency would create cycle in DAG",
						"from", depID,
						"to", node.ID)
					return nil, fmt.Errorf("dependency from %s to %s would create cycle", depID, node.ID)
				}
				dm.logger.Error("failed to add dependency edge to DAG",
					"from", depID,
					"to", node.ID,
					"error", err)
				return nil, fmt.Errorf("failed to add dependency edge from %s to %s: %w", depID, node.ID, err)
			}
		}
	}

	// Store the DAG for future reference
	dm.dags[state.WorkflowID] = workflowDAG

	dm.logger.Debug("DAG built successfully",
		"workflow_id", state.WorkflowID,
		"vertices", workflowDAG.GetOrder(),
		"edges", len(state.ExecutionDAG.Edges))

	return workflowDAG, nil
}

// GetTopologicalOrder returns nodes in topological order
func (dm *DAGManager) GetTopologicalOrder(workflowID string) ([]string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	workflowDAG, exists := dm.dags[workflowID]
	if !exists {
		return nil, fmt.Errorf("DAG not found for workflow %s", workflowID)
	}

	// Get all roots and traverse from there
	roots := workflowDAG.GetRoots()
	var order []string
	visited := make(map[string]bool)

	// Process each root and its descendants
	for rootID := range roots {
		if !visited[rootID] {
			order = append(order, rootID)
			visited[rootID] = true

			// Get descendants in order
			descendants, err := workflowDAG.GetOrderedDescendants(rootID)
			if err == nil {
				for _, descID := range descendants {
					if !visited[descID] {
						order = append(order, descID)
						visited[descID] = true
					}
				}
			}
		}
	}

	return order, nil
}

// GetNextExecutableNodes returns nodes that are ready to execute
func (dm *DAGManager) GetNextExecutableNodes(workflowID string, completedNodes map[string]bool) ([]string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	workflowDAG, exists := dm.dags[workflowID]
	if !exists {
		return nil, fmt.Errorf("DAG not found for workflow %s", workflowID)
	}

	var executableNodes []string

	// Get all vertices
	vertices := workflowDAG.GetVertices()

	for id := range vertices {
		// Skip if already completed
		if completedNodes[id] {
			continue
		}

		// Check if all ancestors (dependencies) are completed
		ancestors, err := workflowDAG.GetAncestors(id)
		if err != nil {
			dm.logger.Warn("failed to get ancestors for node",
				"node_id", id,
				"error", err)
			continue
		}

		allAncestorsCompleted := true
		for ancestorID := range ancestors {
			if !completedNodes[ancestorID] {
				allAncestorsCompleted = false
				break
			}
		}

		if allAncestorsCompleted {
			executableNodes = append(executableNodes, id)
		}
	}

	return executableNodes, nil
}

// GetRoots returns all root nodes (nodes with no dependencies)
func (dm *DAGManager) GetRoots(workflowID string) ([]string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	workflowDAG, exists := dm.dags[workflowID]
	if !exists {
		return nil, fmt.Errorf("DAG not found for workflow %s", workflowID)
	}

	roots := workflowDAG.GetRoots()
	var rootIDs []string
	for id := range roots {
		rootIDs = append(rootIDs, id)
	}

	return rootIDs, nil
}

// GetLeaves returns all leaf nodes (nodes with no dependents)
func (dm *DAGManager) GetLeaves(workflowID string) ([]string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	workflowDAG, exists := dm.dags[workflowID]
	if !exists {
		return nil, fmt.Errorf("DAG not found for workflow %s", workflowID)
	}

	leaves := workflowDAG.GetLeaves()
	var leafIDs []string
	for id := range leaves {
		leafIDs = append(leafIDs, id)
	}

	return leafIDs, nil
}

// ValidateDAG checks if the DAG is valid (no cycles, etc.)
func (dm *DAGManager) ValidateDAG(workflowID string) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	workflowDAG, exists := dm.dags[workflowID]
	if !exists {
		return fmt.Errorf("DAG not found for workflow %s", workflowID)
	}

	// Check if DAG has any vertices
	if workflowDAG.GetOrder() == 0 {
		return fmt.Errorf("DAG is empty")
	}

	// The heimdalr/dag library ensures no cycles exist
	// Additional validation can be added here

	return nil
}

// GetNodeDependencies returns direct dependencies of a node
func (dm *DAGManager) GetNodeDependencies(workflowID string, nodeID string) ([]string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	workflowDAG, exists := dm.dags[workflowID]
	if !exists {
		return nil, fmt.Errorf("DAG not found for workflow %s", workflowID)
	}

	ancestors, err := workflowDAG.GetParents(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get dependencies for node %s: %w", nodeID, err)
	}

	var deps []string
	for id := range ancestors {
		deps = append(deps, id)
	}

	return deps, nil
}

// GetNodeDependents returns nodes that depend on this node
func (dm *DAGManager) GetNodeDependents(workflowID string, nodeID string) ([]string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	workflowDAG, exists := dm.dags[workflowID]
	if !exists {
		return nil, fmt.Errorf("DAG not found for workflow %s", workflowID)
	}

	descendants, err := workflowDAG.GetChildren(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get dependents for node %s: %w", nodeID, err)
	}

	var deps []string
	for id := range descendants {
		deps = append(deps, id)
	}

	return deps, nil
}

// RemoveDAG removes a DAG from memory
func (dm *DAGManager) RemoveDAG(workflowID string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	delete(dm.dags, workflowID)
	dm.logger.Debug("DAG removed from memory", "workflow_id", workflowID)
}

// WorkflowNode represents a node in the workflow DAG
type WorkflowNode struct {
	ID           string
	Type         string
	Status       domain.NodeExecutionStatus
	Dependencies []string
	Metadata     map[string]interface{}
}

// ConvertToExecutionDAG converts heimdalr DAG back to domain.WorkflowDAG
func (dm *DAGManager) ConvertToExecutionDAG(workflowID string) (*domain.WorkflowDAG, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	workflowDAG, exists := dm.dags[workflowID]
	if !exists {
		return nil, fmt.Errorf("DAG not found for workflow %s", workflowID)
	}

	executionDAG := &domain.WorkflowDAG{
		Nodes:  []domain.DAGNode{},
		Edges:  []domain.DAGEdge{},
		Roots:  []string{},
		Leaves: []string{},
	}

	// Convert vertices to nodes
	vertices := workflowDAG.GetVertices()
	for id, vertex := range vertices {
		if nodeData, ok := vertex.(*WorkflowNode); ok {
			node := domain.DAGNode{
				ID:           id,
				Type:         nodeData.Type,
				Status:       nodeData.Status,
				Dependencies: nodeData.Dependencies,
				Metadata:     nodeData.Metadata,
			}
			executionDAG.Nodes = append(executionDAG.Nodes, node)
		}
	}

	// Get all edges
	for id := range vertices {
		children, _ := workflowDAG.GetChildren(id)
		for childID := range children {
			edge := domain.DAGEdge{
				From: id,
				To:   childID,
				Type: "execution",
			}
			executionDAG.Edges = append(executionDAG.Edges, edge)
		}
	}

	// Get roots and leaves
	roots := workflowDAG.GetRoots()
	for id := range roots {
		executionDAG.Roots = append(executionDAG.Roots, id)
	}

	leaves := workflowDAG.GetLeaves()
	for id := range leaves {
		executionDAG.Leaves = append(executionDAG.Leaves, id)
	}

	return executionDAG, nil
}

// AnalyzeExecutionPath analyzes the execution path from a specific node
func (dm *DAGManager) AnalyzeExecutionPath(workflowID string, fromNodeID string, completedNodes map[string]bool) (*ExecutionPathAnalysis, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	workflowDAG, exists := dm.dags[workflowID]
	if !exists {
		return nil, fmt.Errorf("DAG not found for workflow %s", workflowID)
	}

	analysis := &ExecutionPathAnalysis{
		StartNode:           fromNodeID,
		ReachableNodes:      []string{},
		BlockedNodes:        []string{},
		NextExecutableNodes: []string{},
		EstimatedSteps:      0,
	}

	// Get all descendants (nodes reachable from this node)
	descendants, err := workflowDAG.GetDescendants(fromNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get descendants: %w", err)
	}

	for id := range descendants {
		if !completedNodes[id] {
			analysis.ReachableNodes = append(analysis.ReachableNodes, id)

			// Check if this node is ready to execute
			ancestors, _ := workflowDAG.GetAncestors(id)
			allAncestorsCompleted := true
			for ancestorID := range ancestors {
				if !completedNodes[ancestorID] {
					allAncestorsCompleted = false
					break
				}
			}

			if allAncestorsCompleted {
				analysis.NextExecutableNodes = append(analysis.NextExecutableNodes, id)
			} else {
				analysis.BlockedNodes = append(analysis.BlockedNodes, id)
			}
		}
	}

	// Estimate steps based on longest path
	analysis.EstimatedSteps = len(analysis.ReachableNodes)

	return analysis, nil
}

// ExecutionPathAnalysis contains analysis of execution path
type ExecutionPathAnalysis struct {
	StartNode           string
	ReachableNodes      []string
	BlockedNodes        []string
	NextExecutableNodes []string
	EstimatedSteps      int
}
