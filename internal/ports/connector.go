package ports

import "context"

type ConnectorConfig interface {
	GetID() string
}

type ConnectorPort interface {
	GetName() string
	GetConfig() ConnectorConfig
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type ConnectorFactory func(configJSON []byte) (ConnectorPort, error)

type ConnectorRegistryPort interface {
	RegisterConnectorFactory(name string, factory ConnectorFactory) error
	GetConnectorFactory(name string) (ConnectorFactory, error)
	ListConnectors() []string
	UnregisterConnector(name string) error
	HasConnector(name string) bool
	GetConnectorCount() int
}

// ConnectorLeaseCleaner exposes cleanup hooks for releasing connector leases when
// a node fails or is explicitly removed from the cluster.
type ConnectorLeaseCleaner interface {
	CleanupNodeLeases(nodeID string)
}

type ConnectorRegistrationError struct {
	ConnectorName string
	Reason        string
}

func (e ConnectorRegistrationError) Error() string {
	return "connector registration failed for '" + e.ConnectorName + "': " + e.Reason
}
