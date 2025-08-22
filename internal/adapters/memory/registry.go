package memory

import (
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type MemoryNodeRegistry struct {
	nodes  map[string]ports.NodePort
	mu     sync.RWMutex
	logger *slog.Logger
}

func NewMemoryNodeRegistry(logger *slog.Logger) *MemoryNodeRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	
	return &MemoryNodeRegistry{
		nodes:  make(map[string]ports.NodePort),
		logger: logger.With("component", "registry", "type", "memory"),
	}
}

func (r *MemoryNodeRegistry) RegisterNode(node ports.NodePort) error {
	if node == nil {
		return &ports.NodeRegistrationError{
			NodeName: "<nil>",
			Reason:   "node cannot be nil",
		}
	}

	nodeName := node.GetName()
	if nodeName == "" {
		return &ports.NodeRegistrationError{
			NodeName: nodeName,
			Reason:   "node name cannot be empty",
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeName]; exists {
		r.logger.Warn("node registration conflict detected", "nodeName", nodeName)
		return &ports.NodeRegistrationError{
			NodeName: nodeName,
			Reason:   "node already registered",
		}
	}

	r.nodes[nodeName] = node
	r.logger.Info("node registered successfully", "nodeName", nodeName)
	return nil
}

func (r *MemoryNodeRegistry) GetNode(nodeName string) (ports.NodePort, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node, exists := r.nodes[nodeName]
	if !exists {
		r.logger.Debug("node not found", "nodeName", nodeName)
		return nil, domain.Error{
			Type:    domain.ErrorTypeNotFound,
			Message: "node not found in registry",
			Details: map[string]interface{}{
				"node_name": nodeName,
			},
		}
	}

	r.logger.Debug("node retrieved successfully", "nodeName", nodeName)
	return node, nil
}

func (r *MemoryNodeRegistry) ListNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodeNames := make([]string, 0, len(r.nodes))
	for nodeName := range r.nodes {
		nodeNames = append(nodeNames, nodeName)
	}

	r.logger.Debug("listed nodes", "count", len(nodeNames))
	return nodeNames
}

func (r *MemoryNodeRegistry) UnregisterNode(nodeName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeName]; !exists {
		r.logger.Warn("attempt to unregister non-existent node", "nodeName", nodeName)
		return domain.Error{
			Type:    domain.ErrorTypeNotFound,
			Message: "node not found for unregistration",
			Details: map[string]interface{}{
				"node_name": nodeName,
			},
		}
	}

	delete(r.nodes, nodeName)
	r.logger.Info("node unregistered successfully", "nodeName", nodeName)
	return nil
}

func (r *MemoryNodeRegistry) HasNode(nodeName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.nodes[nodeName]
	r.logger.Debug("node existence check", "nodeName", nodeName, "exists", exists)
	return exists
}

func (r *MemoryNodeRegistry) GetNodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := len(r.nodes)
	r.logger.Debug("node count retrieved", "count", count)
	return count
}