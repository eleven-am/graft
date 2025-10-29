package connector_registry

import (
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Manager struct {
	factories map[string]ports.ConnectorFactory
	mu        sync.RWMutex
	logger    *slog.Logger
}

func NewManager(logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		factories: make(map[string]ports.ConnectorFactory),
		logger:    logger.With("component", "connector-registry"),
	}
}

func (m *Manager) RegisterConnectorFactory(name string, factory ports.ConnectorFactory) error {
	if name == "" {
		m.logger.Error("attempted to register connector factory with empty name")
		return &ports.ConnectorRegistrationError{
			ConnectorName: name,
			Reason:        "connector name cannot be empty",
		}
	}

	if factory == nil {
		m.logger.Error("attempted to register nil connector factory", "name", name)
		return &ports.ConnectorRegistrationError{
			ConnectorName: name,
			Reason:        "connector factory cannot be nil",
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.factories[name]; exists {
		return &ports.ConnectorRegistrationError{
			ConnectorName: name,
			Reason:        "connector factory already registered",
		}
	}

	m.factories[name] = factory
	return nil
}

func (m *Manager) GetConnectorFactory(name string) (ports.ConnectorFactory, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	factory, exists := m.factories[name]
	if !exists {
		return nil, domain.NewNotFoundError("connector factory", name)
	}

	return factory, nil
}

func (m *Manager) ListConnectors() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.factories))
	for name := range m.factories {
		names = append(names, name)
	}

	return names
}

func (m *Manager) UnregisterConnector(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.factories[name]; !exists {
		return domain.NewNotFoundError("connector factory", name)
	}

	delete(m.factories, name)
	return nil
}

func (m *Manager) HasConnector(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.factories[name]
	return exists
}

func (m *Manager) GetConnectorCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.factories)
}
