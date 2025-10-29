package core

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubConnectorConfig struct {
	id string
}

func (c *stubConnectorConfig) GetID() string {
	return c.id
}

type stubConnector struct {
	cfg      *stubConnectorConfig
	metadata map[string]any
}

func (c *stubConnector) GetName() string {
	return "stub"
}

func (c *stubConnector) GetConfig() ports.ConnectorConfig {
	return c.cfg
}

func (c *stubConnector) Start(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (c *stubConnector) Stop(context.Context) error {
	return nil
}

func (c *stubConnector) Status() map[string]any {
	return c.metadata
}

type stubLeaseManager struct {
	record *ports.LeaseRecord
}

func (s *stubLeaseManager) Key(namespace, id string) string {
	return namespace + ":" + id
}

func (s *stubLeaseManager) TryAcquire(string, string, time.Duration, map[string]string) (*ports.LeaseRecord, bool, error) {
	return s.record, false, nil
}

func (s *stubLeaseManager) Renew(string, string, time.Duration) (*ports.LeaseRecord, error) {
	return s.record, nil
}

func (s *stubLeaseManager) Release(string, string) error {
	return nil
}

func (s *stubLeaseManager) ForceRelease(string) error {
	return nil
}

func (s *stubLeaseManager) Get(key string) (*ports.LeaseRecord, bool, error) {
	if s.record != nil && s.record.Key == key {
		return s.record, true, nil
	}
	return nil, false, nil
}

func TestManager_ListConnectorStatusesIncludesLeaseAndMetadata(t *testing.T) {
	now := time.Now()

	manager := &Manager{
		nodeID:     "node-1",
		connectors: make(map[string]*connectorHandle),
	}

	cfg := &stubConnectorConfig{id: "connector-1"}
	connector := &stubConnector{
		cfg:      cfg,
		metadata: map[string]any{"status": "ok"},
	}

	handle := &connectorHandle{
		manager:   manager,
		id:        cfg.id,
		name:      connector.GetName(),
		config:    cfg,
		configRaw: []byte(`{"id":"connector-1"}`),
		connector: connector,
		leaseWake: make(chan struct{}, 1),
		releaseCh: make(chan connectorReleaseRequest, 1),
		done:      make(chan struct{}),
		state:     ports.ConnectorStateRunning,
		startedAt: now.Add(-time.Minute),
	}
	handle.lastHeartbeat = now

	leaseManager := &stubLeaseManager{}
	manager.leaseManager = leaseManager

	handle.leaseKey = leaseManager.Key(connectorLeaseNamespace, cfg.id)
	leaseManager.record = &ports.LeaseRecord{
		Key:        handle.leaseKey,
		Owner:      "node-1",
		Generation: 3,
		ExpiresAt:  now.Add(2 * time.Minute),
		RenewedAt:  now,
		Metadata:   map[string]string{"lease": "meta"},
	}
	manager.connectors[cfg.id] = handle

	statuses, err := manager.ListConnectorStatuses()
	require.NoError(t, err)
	require.Len(t, statuses, 1)

	status := statuses[0]
	assert.Equal(t, cfg.id, status.ID)
	assert.Equal(t, connector.GetName(), status.Name)
	assert.Equal(t, ports.ConnectorStateRunning, status.State)
	assert.Equal(t, manager.nodeID, status.NodeID)
	assert.WithinDuration(t, handle.startedAt, status.StartedAt, time.Second)
	assert.WithinDuration(t, handle.lastHeartbeat, status.LastHeartbeat, time.Second)
	assert.Equal(t, connector.metadata, status.Metadata)
	require.NotNil(t, status.Lease)
	assert.Equal(t, leaseManager.record.Owner, status.Lease.Owner)
	assert.Equal(t, leaseManager.record.Generation, status.Lease.Generation)
	assert.WithinDuration(t, leaseManager.record.ExpiresAt, status.Lease.ExpiresAt, time.Second)
	assert.WithinDuration(t, leaseManager.record.RenewedAt, status.Lease.RenewedAt, time.Second)
	assert.Equal(t, leaseManager.record.Metadata, status.Lease.Metadata)
	require.NotEmpty(t, status.Config)
	assert.JSONEq(t, `{"id":"connector-1"}`, string(status.Config))
	assert.Empty(t, status.LastError)

	handles, err := manager.ListConnectorHandles()
	require.NoError(t, err)
	assert.Equal(t, statuses, handles)
}

func TestManager_GetConnectorStatus_NotFound(t *testing.T) {
	manager := &Manager{
		connectors: make(map[string]*connectorHandle),
	}

	_, err := manager.GetConnectorStatus("missing")
	require.Error(t, err)
	assert.True(t, domain.IsNotFoundError(err))
}

func TestManager_GetConnectorStatus_Validation(t *testing.T) {
	manager := &Manager{
		connectors: make(map[string]*connectorHandle),
	}

	_, err := manager.GetConnectorStatus("  ")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connector id cannot be empty")
}
