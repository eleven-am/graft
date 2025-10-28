package connector_registry

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Manager struct {
	connectors map[string]ports.ConnectorPort
	mu         sync.RWMutex
	logger     *slog.Logger
}

func NewManager(logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		connectors: make(map[string]ports.ConnectorPort),
		logger:     logger.With("component", "connector-registry"),
	}
}

func (m *Manager) RegisterConnector(connector interface{}) error {
	if connector == nil {
		m.logger.Error("attempted to register nil connector")
		return &ports.ConnectorRegistrationError{
			ConnectorName: "<nil>",
			Reason:        "connector cannot be nil",
		}
	}

	portConnector, ok := connector.(ports.ConnectorPort)
	if !ok {
		adapter, err := NewConnectorAdapter(connector)
		if err != nil {
			return &ports.ConnectorRegistrationError{
				ConnectorName: "<unknown>",
				Reason:        fmt.Sprintf("failed to adapt connector: %v", err),
			}
		}
		portConnector = adapter
	}

	name := portConnector.GetName()
	if name == "" {
		m.logger.Error("attempted to register connector with empty name")
		return &ports.ConnectorRegistrationError{
			ConnectorName: name,
			Reason:        "connector name cannot be empty",
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.connectors[name]; exists {
		return &ports.ConnectorRegistrationError{
			ConnectorName: name,
			Reason:        "connector already registered",
		}
	}

	m.connectors[name] = portConnector
	return nil
}

func (m *Manager) GetConnector(name string) (ports.ConnectorPort, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	connector, exists := m.connectors[name]
	if !exists {
		return nil, domain.NewNotFoundError("connector", name)
	}

	return connector, nil
}

func (m *Manager) ListConnectors() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.connectors))
	for name := range m.connectors {
		names = append(names, name)
	}

	return names
}

func (m *Manager) UnregisterConnector(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.connectors[name]; !exists {
		return domain.NewNotFoundError("connector", name)
	}

	delete(m.connectors, name)
	return nil
}

func (m *Manager) HasConnector(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.connectors[name]
	return exists
}

func (m *Manager) GetConnectorCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.connectors)
}
