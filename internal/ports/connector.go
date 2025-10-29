package ports

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type ConnectorState string

const (
	ConnectorStatePending ConnectorState = "pending"
	ConnectorStateRunning ConnectorState = "running"
	ConnectorStateError   ConnectorState = "error"
)

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

type ConnectorLeaseStatus struct {
	Owner      string            `json:"owner"`
	Generation int64             `json:"generation"`
	ExpiresAt  time.Time         `json:"expires_at"`
	RenewedAt  time.Time         `json:"renewed_at"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type ConnectorStatus struct {
	ID            string                `json:"id"`
	Name          string                `json:"name"`
	NodeID        string                `json:"node_id"`
	State         ConnectorState        `json:"state"`
	StartedAt     time.Time             `json:"started_at"`
	LastHeartbeat time.Time             `json:"last_heartbeat"`
	StoppedAt     time.Time             `json:"stopped_at"`
	LastError     string                `json:"last_error,omitempty"`
	Config        json.RawMessage       `json:"config,omitempty"`
	Metadata      map[string]any        `json:"metadata,omitempty"`
	Lease         *ConnectorLeaseStatus `json:"lease,omitempty"`
}

type ConnectorStatusReporter interface {
	Status() map[string]any
}

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
	return fmt.Sprintf("connector registration failed for '%s': %s", e.ConnectorName, e.Reason)
}
