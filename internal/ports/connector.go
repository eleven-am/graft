package ports

import "context"

type ConnectorConfig interface {
	GetID() string
}

type ConnectorPort interface {
	GetName() string
	NewConfig() ConnectorConfig
	Start(ctx context.Context, config ConnectorConfig) error
	Stop(ctx context.Context, config ConnectorConfig) error
}

type ConnectorRegistryPort interface {
	RegisterConnector(connector interface{}) error
	GetConnector(name string) (ConnectorPort, error)
	ListConnectors() []string
	UnregisterConnector(name string) error
	HasConnector(name string) bool
	GetConnectorCount() int
}

type ConnectorRegistrationError struct {
	ConnectorName string
	Reason        string
}

func (e ConnectorRegistrationError) Error() string {
	return "connector registration failed for '" + e.ConnectorName + "': " + e.Reason
}
