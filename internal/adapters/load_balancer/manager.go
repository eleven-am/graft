package load_balancer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Manager struct {
	storage ports.StoragePort
	events  ports.EventManager
	nodeID  string
	logger  *slog.Logger

	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	localActiveCount int
}

func NewManager(storage ports.StoragePort, events ports.EventManager, nodeID string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		storage: storage,
		events:  events,
		nodeID:  nodeID,
		logger:  logger.With("component", "load-balancer"),
	}
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return domain.NewDiscoveryError("load-balancer", "start", domain.ErrAlreadyStarted)
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.running = true

	if err := m.subscribeToEvents(); err != nil {
		m.cancel()
		m.running = false
		return domain.NewDiscoveryError("load-balancer", "subscribe", err)
	}

	if err := m.initializeNodeLoad(); err != nil {
		m.cancel()
		m.running = false
		return domain.NewDiscoveryError("load-balancer", "initialize", err)
	}

	return nil
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return domain.NewDiscoveryError("load-balancer", "stop", domain.ErrNotStarted)
	}

	m.cancel()
	m.running = false

	return nil
}

func (m *Manager) subscribeToEvents() error {
	if err := m.events.OnNodeStarted(m.onNodeStarted); err != nil {
		return domain.NewDiscoveryError("load-balancer", "subscribe-started", err)
	}

	if err := m.events.OnNodeCompleted(m.onNodeCompleted); err != nil {
		return domain.NewDiscoveryError("load-balancer", "subscribe-completed", err)
	}

	if err := m.events.OnNodeError(m.onNodeError); err != nil {
		return domain.NewDiscoveryError("load-balancer", "subscribe-error", err)
	}

	return nil
}

func (m *Manager) initializeNodeLoad() error {
	load := &ports.NodeLoad{
		NodeID:          m.nodeID,
		ActiveWorkflows: 0,
		LastUpdated:     time.Now().Unix(),
	}

	return m.updateNodeLoad(load)
}

func (m *Manager) onNodeStarted(event *domain.NodeStartedEvent) {
	if event.NodeID != m.nodeID {
		return
	}

	m.mu.Lock()
	m.localActiveCount++
	activeCount := m.localActiveCount
	m.mu.Unlock()

	load := &ports.NodeLoad{
		NodeID:          m.nodeID,
		ActiveWorkflows: activeCount,
		LastUpdated:     time.Now().Unix(),
	}

	if err := m.updateNodeLoad(load); err != nil {
		m.logger.Error("failed to update node load on start", "error", err)
	}
}

func (m *Manager) onNodeCompleted(event *domain.NodeCompletedEvent) {
	m.decrementLoad()
}

func (m *Manager) onNodeError(event *domain.NodeErrorEvent) {
	m.decrementLoad()
}

func (m *Manager) decrementLoad() {
	m.mu.Lock()
	if m.localActiveCount > 0 {
		m.localActiveCount--
	}
	activeCount := m.localActiveCount
	m.mu.Unlock()

	load := &ports.NodeLoad{
		NodeID:          m.nodeID,
		ActiveWorkflows: activeCount,
		LastUpdated:     time.Now().Unix(),
	}

	if err := m.updateNodeLoad(load); err != nil {
		m.logger.Error("failed to update node load on completion", "error", err)
	}
}

func (m *Manager) updateNodeLoad(load *ports.NodeLoad) error {
	data, err := json.Marshal(load)
	if err != nil {
		return domain.ErrInvalidInput
	}

	key := fmt.Sprintf("cluster:load:%s", load.NodeID)
	return m.storage.Put(key, data, 0)
}

func (m *Manager) ShouldExecuteNode(nodeID string, workflowID string, nodeName string) (bool, error) {
	clusterLoad, err := m.GetClusterLoad()
	if err != nil {
		m.logger.Warn("failed to get cluster load, defaulting to execute", "error", err)
		return true, nil
	}

	currentNodeLoad := clusterLoad[m.nodeID]

	for otherNodeID, otherNodeLoad := range clusterLoad {
		if otherNodeID == m.nodeID {
			continue
		}

		if otherNodeLoad < currentNodeLoad {
			return false, nil
		}
	}

	return true, nil
}

func (m *Manager) GetClusterLoad() (map[string]int, error) {
	keys, err := m.storage.ListByPrefix("cluster:load:")
	if err != nil {
		return nil, domain.ErrConnection
	}

	clusterLoad := make(map[string]int)
	for _, kv := range keys {
		var load ports.NodeLoad
		if err := json.Unmarshal(kv.Value, &load); err != nil {
			m.logger.Warn("failed to unmarshal node load", "key", kv.Key, "error", err)
			continue
		}

		clusterLoad[load.NodeID] = load.ActiveWorkflows
	}

	return clusterLoad, nil
}

func (m *Manager) GetNodeLoad(nodeID string) (int, error) {
	key := fmt.Sprintf("cluster:load:%s", nodeID)
	data, _, exists, err := m.storage.Get(key)
	if err != nil {
		return 0, domain.ErrConnection
	}
	if !exists {
		return 0, nil
	}

	var load ports.NodeLoad
	if err := json.Unmarshal(data, &load); err != nil {
		return 0, domain.ErrInvalidInput
	}

	return load.ActiveWorkflows, nil
}
