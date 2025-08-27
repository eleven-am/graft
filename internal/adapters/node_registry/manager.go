package node_registry

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Manager struct {
	nodes  map[string]ports.NodePort
	mu     sync.RWMutex
	logger *slog.Logger
}

func NewManager(logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		nodes:  make(map[string]ports.NodePort),
		logger: logger.With("component", "node-registry"),
	}
}

func (r *Manager) RegisterNode(node interface{}) error {
	if node == nil {
		r.logger.Error("attempted to register nil node")
		return &ports.NodeRegistrationError{
			NodeName: "<nil>",
			Reason:   "node cannot be nil",
		}
	}

	fmt.Printf("🔍 Registering node type: %T\n", node)

	// First check if it already implements ports.NodePort
	portNode, ok := node.(ports.NodePort)
	if !ok {
		fmt.Printf("🔧 Node doesn't implement ports.NodePort, creating adapter...\n")
		// Try to create an adapter
		adapter, err := NewNodeAdapter(node)
		if err != nil {
			fmt.Printf("❌ Failed to create adapter: %v\n", err)
			return &ports.NodeRegistrationError{
				NodeName: "<unknown>",
				Reason:   fmt.Sprintf("failed to adapt node: %v", err),
			}
		}
		portNode = adapter
		fmt.Printf("✅ Successfully created adapter for node\n")
	}

	nodeName := portNode.GetName()
	r.logger.Debug("attempting to register node", "node_name", nodeName)

	if nodeName == "" {
		r.logger.Error("attempted to register node with empty name")
		return &ports.NodeRegistrationError{
			NodeName: nodeName,
			Reason:   "node name cannot be empty",
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeName]; exists {
		r.logger.Debug("node registration failed - already exists", "node_name", nodeName)
		return &ports.NodeRegistrationError{
			NodeName: nodeName,
			Reason:   "node already registered",
		}
	}

	r.nodes[nodeName] = portNode
	r.logger.Debug("node registered successfully", "node_name", nodeName, "total_nodes", len(r.nodes))
	return nil
}

func (r *Manager) GetNode(nodeName string) (ports.NodePort, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logger.Debug("retrieving node", "node_name", nodeName)

	node, exists := r.nodes[nodeName]
	if !exists {
		r.logger.Debug("node not found", "node_name", nodeName)
		return nil, domain.NewNotFoundError("node", nodeName)
	}

	r.logger.Debug("node retrieved successfully", "node_name", nodeName)
	return node, nil
}

func (r *Manager) ListNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logger.Debug("listing all nodes", "total_nodes", len(r.nodes))

	nodeNames := make([]string, 0, len(r.nodes))
	for nodeName := range r.nodes {
		nodeNames = append(nodeNames, nodeName)
	}

	return nodeNames
}

func (r *Manager) UnregisterNode(nodeName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logger.Debug("attempting to unregister node", "node_name", nodeName)

	if _, exists := r.nodes[nodeName]; !exists {
		r.logger.Debug("node unregistration failed - not found", "node_name", nodeName)
		return domain.NewNotFoundError("node", nodeName)
	}

	delete(r.nodes, nodeName)
	r.logger.Debug("node unregistered successfully", "node_name", nodeName, "remaining_nodes", len(r.nodes))
	return nil
}

func (r *Manager) HasNode(nodeName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logger.Debug("checking if node exists", "node_name", nodeName)

	_, exists := r.nodes[nodeName]
	r.logger.Debug("node existence check completed", "node_name", nodeName, "exists", exists)
	return exists
}

func (r *Manager) GetNodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := len(r.nodes)
	r.logger.Debug("node count requested", "count", count)
	return count
}
