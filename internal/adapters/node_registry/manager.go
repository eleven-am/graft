package node_registry

import (
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

	nodeName := extractNodeName(node)
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

	wrapper, err := NewNodeWrapper(node)
	if err != nil {
		r.logger.Error("failed to create node wrapper", "node_name", nodeName, "error", err)
		return &ports.NodeRegistrationError{
			NodeName: nodeName,
			Reason:   "failed to wrap node: " + err.Error(),
		}
	}

	r.nodes[nodeName] = wrapper
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

