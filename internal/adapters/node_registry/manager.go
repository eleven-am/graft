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

	portNode, ok := node.(ports.NodePort)
	if !ok {
		adapter, err := NewNodeAdapter(node)
		if err != nil {
			return &ports.NodeRegistrationError{
				NodeName: "<unknown>",
				Reason:   fmt.Sprintf("failed to adapt node: %v", err),
			}
		}
		portNode = adapter
	}

	nodeName := portNode.GetName()

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
		return &ports.NodeRegistrationError{
			NodeName: nodeName,
			Reason:   "node already registered",
		}
	}

	r.nodes[nodeName] = portNode
	return nil
}

func (r *Manager) GetNode(nodeName string) (ports.NodePort, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node, exists := r.nodes[nodeName]
	if !exists {
		return nil, domain.NewNotFoundError("node", nodeName)
	}

	return node, nil
}

func (r *Manager) ListNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodeNames := make([]string, 0, len(r.nodes))
	for nodeName := range r.nodes {
		nodeNames = append(nodeNames, nodeName)
	}

	return nodeNames
}

func (r *Manager) UnregisterNode(nodeName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeName]; !exists {
		return domain.NewNotFoundError("node", nodeName)
	}

	delete(r.nodes, nodeName)
	return nil
}

func (r *Manager) HasNode(nodeName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.nodes[nodeName]
	return exists
}

func (r *Manager) GetNodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.nodes)
}
