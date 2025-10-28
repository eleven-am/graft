package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

const (
	connectorLeaseKeyPrefix    = "connectors:lease:"
	connectorConfigKeyPrefix   = "connectors:config:"
	connectorLeaseDuration     = 30 * time.Second
	connectorHeartbeatInterval = 10 * time.Second

	connectorAcquireRetryInterval = 2 * time.Second
	connectorAcquireBaseBackoff   = time.Second
	connectorAcquireMaxBackoff    = 10 * time.Second
	connectorRebalanceMinDelay    = 500 * time.Millisecond
	connectorRebalanceMaxDelay    = 2 * time.Second
)

type connectorLeaseRecord struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expires_at"`
}

type connectorConfigRecord struct {
	ID     string          `json:"id"`
	Name   string          `json:"name"`
	Config json.RawMessage `json:"config"`
}

type connectorReleaseReason string

const (
	releaseReasonStop      connectorReleaseReason = "stop"
	releaseReasonRebalance connectorReleaseReason = "rebalance"
	releaseReasonShutdown  connectorReleaseReason = "shutdown"
	releaseReasonUpdated   connectorReleaseReason = "update"
)

type connectorReleaseRequest struct {
	reason connectorReleaseReason
	ack    chan struct{}
}

type connectorHandle struct {
	manager   *Manager
	id        string
	name      string
	config    ports.ConnectorConfig
	configRaw []byte
	connector ports.ConnectorPort
	logger    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	releaseCh chan connectorReleaseRequest
	done      chan struct{}

	leaseVersion int64

	mu            sync.Mutex
	rebalanceNext bool
}

var (
	errConnectorLeaseLost = errors.New("connector lease lost")
)

func (m *Manager) RegisterConnector(connector interface{}) error {
	if m.connectorRegistry == nil {
		return fmt.Errorf("connector registry not initialized")
	}
	if err := m.connectorRegistry.RegisterConnector(connector); err != nil {
		return err
	}

	if m.ctx != nil {
		if err := m.syncConnectorConfigs(); err != nil && m.logger != nil {
			m.logger.Warn("failed to sync connector configs after registration", "error", err)
		}
	}

	return nil
}

func (m *Manager) UnregisterConnector(name string) error {
	if m.connectorRegistry == nil {
		return fmt.Errorf("connector registry not initialized")
	}
	return m.connectorRegistry.UnregisterConnector(name)
}

func (m *Manager) StartConnector(connectorName string, config ports.ConnectorConfig) error {
	if m.connectorRegistry == nil {
		return fmt.Errorf("connector registry not initialized")
	}
	if m.ctx == nil {
		return fmt.Errorf("manager not started")
	}

	if connectorName == "" {
		return domain.NewValidationError("connector name cannot be empty", nil,
			domain.WithComponent("core.Manager.StartConnector"))
	}
	if config == nil {
		return domain.NewValidationError("connector config cannot be nil", nil,
			domain.WithComponent("core.Manager.StartConnector"),
			domain.WithContextDetail("connector_name", connectorName))
	}

	id := strings.TrimSpace(config.GetID())
	if id == "" {
		return domain.NewValidationError("connector config must provide a non-empty ID", nil,
			domain.WithComponent("core.Manager.StartConnector"),
			domain.WithContextDetail("connector_name", connectorName))
	}

	connector, err := m.connectorRegistry.GetConnector(connectorName)
	if err != nil {
		return err
	}

	m.startConnectorWatchers()

	payload, err := json.Marshal(config)
	if err != nil {
		return domain.NewValidationError(
			"failed to marshal connector config",
			err,
			domain.WithComponent("core.Manager.StartConnector"),
			domain.WithContextDetail("connector_name", connectorName),
		)
	}

	record, err := m.persistConnectorConfig(connectorName, id, payload)
	if err != nil {
		return err
	}

	if err := m.ensureConnectorHandleFromRecord(connector, record); err != nil {
		return err
	}

	if m.logger != nil {
		m.logger.Info("connector start requested", "connector", connectorName, "connector_id", id)
	}

	return nil
}

func (m *Manager) StopConnector(id string) error {
	record, _, exists, err := m.getConnectorConfig(id)
	if err != nil {
		return err
	}
	if !exists {
		return domain.NewNotFoundError("connector", id)
	}

	if m.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	if err := m.storage.Delete(m.connectorConfigKey(id)); err != nil && !domain.IsNotFound(err) {
		return err
	}

	m.stopConnectorHandle(id, releaseReasonStop)

	if m.logger != nil {
		m.logger.Info("connector stop requested", "connector", record.Name, "connector_id", id)
	}

	return nil
}

func (m *Manager) stopConnectorHandle(id string, reason connectorReleaseReason) {
	m.connectorMu.Lock()
	handle, exists := m.connectors[id]
	if exists {
		delete(m.connectors, id)
	}
	m.connectorMu.Unlock()

	if !exists {
		return
	}

	handle.requestRelease(reason)
	handle.stop()
}

func (m *Manager) stopAllConnectors() {
	m.connectorMu.Lock()
	handles := make([]*connectorHandle, 0, len(m.connectors))
	for id, handle := range m.connectors {
		handles = append(handles, handle)
		delete(m.connectors, id)
	}
	m.connectorMu.Unlock()

	for _, handle := range handles {
		handle.requestRelease(releaseReasonShutdown)
		handle.stop()
	}
}

func (m *Manager) rebalanceConnectors(reason connectorReleaseReason) {
	m.connectorMu.RLock()
	handles := make([]*connectorHandle, 0, len(m.connectors))
	for _, handle := range m.connectors {
		handles = append(handles, handle)
	}
	m.connectorMu.RUnlock()

	for _, handle := range handles {
		handle.requestRelease(reason)
	}
}

func newConnectorHandle(m *Manager, name, id string, connector ports.ConnectorPort, config ports.ConnectorConfig, configRaw []byte) *connectorHandle {
	parentCtx := m.ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(parentCtx)

	logger := m.logger
	if logger != nil {
		logger = logger.With("connector", name, "connector_id", id)
	}

	handle := &connectorHandle{
		manager:   m,
		id:        id,
		name:      name,
		config:    config,
		configRaw: configRaw,
		connector: connector,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		releaseCh: make(chan connectorReleaseRequest, 1),
		done:      make(chan struct{}),
	}

	go handle.run()
	return handle
}

func (h *connectorHandle) run() {
	defer close(h.done)

	backoff := connectorAcquireBaseBackoff

	for {
		if err := h.ctx.Err(); err != nil {
			return
		}

		acquired, version, err := h.manager.acquireConnectorLease(h)
		if err != nil {
			if h.logger != nil {
				h.logger.Error("failed to acquire connector lease", "error", err)
			}
			if !h.waitForNextAttempt(backoff) {
				return
			}
			if backoff < connectorAcquireMaxBackoff {
				backoff *= 2
				if backoff > connectorAcquireMaxBackoff {
					backoff = connectorAcquireMaxBackoff
				}
			}
			continue
		}

		if !acquired {
			if !h.waitForNextAttempt(connectorAcquireRetryInterval) {
				return
			}
			continue
		}

		backoff = connectorAcquireBaseBackoff
		h.leaseVersion = version

		if h.logger != nil {
			h.logger.Info("acquired connector lease")
		}

		shouldRun, err := h.manager.shouldRunConnector(h)
		if err != nil {
			if h.logger != nil {
				h.logger.Warn("load balancer decision failed", "error", err)
			}
			h.manager.releaseConnectorLease(h, false)
			if !h.waitForNextAttempt(connectorAcquireRetryInterval) {
				return
			}
			continue
		}

		if !shouldRun {
			if h.logger != nil {
				h.logger.Debug("deferring connector execution per load balancer decision")
			}
			h.manager.releaseConnectorLease(h, false)
			if !h.waitForNextAttempt(connectorAcquireRetryInterval) {
				return
			}
			continue
		}

		err = h.runWithLease()
		if errors.Is(err, context.Canceled) && h.ctx.Err() != nil {
			return
		}
		if err != nil && h.logger != nil {
			h.logger.Warn("connector run ended", "error", err)
		}

		if h.ctx.Err() != nil {
			return
		}

		if h.takeRebalanceFlag() {
			delay := h.manager.randomConnectorDelay(connectorRebalanceMinDelay, connectorRebalanceMaxDelay)
			if !h.waitForNextAttempt(delay) {
				return
			}
		}
	}
}

func (h *connectorHandle) runWithLease() error {
	if err := h.manager.registerConnectorLoad(h); err != nil {
		if h.logger != nil {
			h.logger.Warn("failed to register connector load", "error", err)
		}
		h.manager.releaseConnectorLease(h, false)
		return err
	}
	defer h.manager.unregisterConnectorLoad(h)

	runCtx, runCancel := context.WithCancel(h.ctx)
	errCh := make(chan error, 1)

	go func() {
		if h.logger != nil {
			h.logger.Info("starting connector")
		}
		err := h.connector.Start(runCtx, h.config)
		errCh <- err
	}()

	ticker := time.NewTicker(connectorHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			runCancel()
			err := <-errCh
			h.manager.releaseConnectorLease(h, true)
			if h.logger != nil {
				h.logger.Info("connector context cancelled", "error", err)
			}
			return context.Canceled
		case req := <-h.releaseCh:
			if req.reason == releaseReasonRebalance {
				h.markRebalance()
			}
			runCancel()
			err := <-errCh
			h.manager.releaseConnectorLease(h, false)
			if h.logger != nil {
				h.logger.Info("connector released", "reason", string(req.reason), "error", err)
			}
			close(req.ack)
			if err == nil {
				return fmt.Errorf("connector stopped without error")
			}
			return err
		case err := <-errCh:
			h.manager.releaseConnectorLease(h, false)
			if err == nil {
				return fmt.Errorf("connector stopped without error")
			}
			return err
		case <-ticker.C:
			newVersion, err := h.manager.renewConnectorLease(h)
			if err != nil {
				runCancel()
				runErr := <-errCh
				h.manager.releaseConnectorLease(h, true)
				if h.logger != nil {
					h.logger.Warn("failed to renew connector lease", "error", err)
				}
				if errors.Is(err, errConnectorLeaseLost) {
					h.markRebalance()
					return err
				}
				if runErr != nil {
					return fmt.Errorf("lease renewal failed: %w (connector error: %v)", err, runErr)
				}
				return fmt.Errorf("lease renewal failed: %w", err)
			}
			h.leaseVersion = newVersion
		}
	}
}

func (h *connectorHandle) waitForNextAttempt(delay time.Duration) bool {
	if delay <= 0 {
		delay = connectorAcquireRetryInterval
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-h.ctx.Done():
		return false
	case req := <-h.releaseCh:
		if req.reason == releaseReasonRebalance {
			h.markRebalance()
		}
		close(req.ack)
		return true
	case <-timer.C:
		return true
	}
}

func (h *connectorHandle) markRebalance() {
	h.mu.Lock()
	h.rebalanceNext = true
	h.mu.Unlock()
}

func (h *connectorHandle) takeRebalanceFlag() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	flagged := h.rebalanceNext
	h.rebalanceNext = false
	return flagged
}

func (h *connectorHandle) requestRelease(reason connectorReleaseReason) {
	req := connectorReleaseRequest{reason: reason, ack: make(chan struct{})}
	select {
	case <-h.ctx.Done():
		close(req.ack)
		return
	case h.releaseCh <- req:
	}
	<-req.ack
}

func (h *connectorHandle) stop() {
	h.cancel()
	<-h.done
}

func (m *Manager) ensureConnectorHandleFromRecord(connector ports.ConnectorPort, record *connectorConfigRecord) error {
	if record == nil {
		return nil
	}

	var toStop *connectorHandle

	m.connectorMu.Lock()
	existing, exists := m.connectors[record.ID]
	if exists {
		if bytes.Equal(existing.configRaw, record.Config) {
			m.connectorMu.Unlock()
			return nil
		}
		toStop = existing
		delete(m.connectors, record.ID)
	}
	m.connectorMu.Unlock()

	if toStop != nil {
		toStop.requestRelease(releaseReasonUpdated)
		toStop.stop()
	}

	config, err := m.instantiateConnectorConfig(connector, record)
	if err != nil {
		return err
	}

	handle := newConnectorHandle(m, record.Name, record.ID, connector, config, append([]byte(nil), record.Config...))

	m.connectorMu.Lock()
	if _, exists := m.connectors[record.ID]; exists {
		m.connectorMu.Unlock()
		handle.requestRelease(releaseReasonUpdated)
		handle.stop()
		return nil
	}
	m.connectors[record.ID] = handle
	m.connectorMu.Unlock()

	if m.logger != nil {
		m.logger.Info("connector handle ready", "connector", record.Name, "connector_id", record.ID)
	}

	return nil
}

func (m *Manager) instantiateConnectorConfig(connector ports.ConnectorPort, record *connectorConfigRecord) (ports.ConnectorConfig, error) {
	cfg := connector.NewConfig()
	if cfg == nil {
		return nil, domain.NewValidationError(
			"connector returned nil config",
			nil,
			domain.WithComponent("core.Manager"),
			domain.WithContextDetail("connector", connector.GetName()),
		)
	}

	if err := json.Unmarshal(record.Config, cfg); err != nil {
		return nil, domain.NewValidationError(
			"failed to unmarshal connector config",
			err,
			domain.WithComponent("core.Manager"),
			domain.WithContextDetail("connector", connector.GetName()),
			domain.WithContextDetail("connector_id", record.ID),
		)
	}

	configID := strings.TrimSpace(cfg.GetID())
	if configID == "" {
		return nil, domain.NewValidationError(
			"connector config must provide a non-empty ID",
			nil,
			domain.WithComponent("core.Manager"),
			domain.WithContextDetail("connector", connector.GetName()),
		)
	}

	if record.ID != "" && record.ID != configID {
		return nil, domain.NewValidationError(
			"connector config ID mismatch",
			nil,
			domain.WithComponent("core.Manager"),
			domain.WithContextDetail("connector", connector.GetName()),
			domain.WithContextDetail("expected_id", record.ID),
			domain.WithContextDetail("actual_id", configID),
		)
	}

	return cfg, nil
}

func (m *Manager) persistConnectorConfig(name, id string, payload []byte) (*connectorConfigRecord, error) {
	if m.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	key := m.connectorConfigKey(id)

	for {
		existing, version, exists, err := m.getConnectorConfig(id)
		if err != nil {
			return nil, err
		}

		if exists && existing.Name != name {
			return nil, domain.NewValidationError(
				"connector config already registered to a different connector",
				nil,
				domain.WithComponent("core.Manager.StartConnector"),
				domain.WithContextDetail("connector_id", id),
				domain.WithContextDetail("existing_connector", existing.Name),
				domain.WithContextDetail("requested_connector", name),
			)
		}

		if exists && bytes.Equal(existing.Config, payload) {
			return existing, nil
		}

		record := &connectorConfigRecord{ID: id, Name: name, Config: append([]byte(nil), payload...)}

		data, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}

		newVersion := int64(1)
		if exists {
			newVersion = version + 1
		}

		if err := m.storage.Put(key, data, newVersion); err != nil {
			if isVersionMismatch(err) {
				continue
			}
			return nil, err
		}

		return record, nil
	}
}

func (m *Manager) syncConnectorConfig(id string) error {
	record, _, exists, err := m.getConnectorConfig(id)
	if err != nil {
		return err
	}

	if !exists {
		m.stopConnectorHandle(id, releaseReasonStop)
		return nil
	}

	connector, err := m.connectorRegistry.GetConnector(record.Name)
	if err != nil {
		return err
	}

	return m.ensureConnectorHandleFromRecord(connector, record)
}

func (m *Manager) syncConnectorConfigs() error {
	if m.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	entries, err := m.storage.ListByPrefix(connectorConfigKeyPrefix)
	if err != nil {
		return err
	}

	active := make(map[string]struct{}, len(entries))

	for _, entry := range entries {
		var record connectorConfigRecord
		if err := json.Unmarshal(entry.Value, &record); err != nil {
			if m.logger != nil {
				m.logger.Warn("failed to decode connector config", "error", err)
			}
			continue
		}

		if record.ID == "" || record.Name == "" {
			continue
		}

		connector, err := m.connectorRegistry.GetConnector(record.Name)
		if err != nil {
			if m.logger != nil {
				m.logger.Warn("connector not registered for stored config", "connector", record.Name, "connector_id", record.ID)
			}
			continue
		}

		if err := m.ensureConnectorHandleFromRecord(connector, &record); err != nil {
			if m.logger != nil {
				m.logger.Warn("failed to ensure connector handle", "connector", record.Name, "connector_id", record.ID, "error", err)
			}
			continue
		}
		active[record.ID] = struct{}{}
	}

	stale := make([]*connectorHandle, 0)

	m.connectorMu.Lock()
	for id, handle := range m.connectors {
		if _, ok := active[id]; !ok {
			stale = append(stale, handle)
			delete(m.connectors, id)
		}
	}
	m.connectorMu.Unlock()

	for _, handle := range stale {
		handle.requestRelease(releaseReasonStop)
		handle.stop()
	}

	return nil
}

func (m *Manager) startConnectorWatchers() {
	m.connectorWatchersOnce.Do(func() {
		if m.eventManager == nil || m.storage == nil || m.ctx == nil {
			return
		}

		if err := m.syncConnectorConfigs(); err != nil && m.logger != nil {
			m.logger.Warn("failed to sync connector configs", "error", err)
		}

		ch, unsubscribe, err := m.eventManager.SubscribeToChannel(connectorConfigKeyPrefix)
		if err != nil {
			if m.logger != nil {
				m.logger.Warn("failed to subscribe to connector config events", "error", err)
			}
			return
		}

		m.setConnectorWatcherUnsub(unsubscribe)

		go func() {
			for {
				select {
				case <-m.ctx.Done():
					m.callConnectorWatcherUnsub()
					return
				case evt, ok := <-ch:
					if !ok {
						return
					}
					id := strings.TrimPrefix(evt.Key, connectorConfigKeyPrefix)
					if id == "" {
						continue
					}
					if err := m.syncConnectorConfig(id); err != nil {
						if m.logger != nil {
							m.logger.Warn("failed to sync connector config", "connector_id", id, "error", err)
						}
					}
				}
			}
		}()
	})
}

func (m *Manager) stopConnectorWatchers() {
	m.callConnectorWatcherUnsub()
}

func (m *Manager) callConnectorWatcherUnsub() {
	m.connectorWatcherMu.Lock()
	defer m.connectorWatcherMu.Unlock()
	if m.connectorWatcherUnsub != nil {
		m.connectorWatcherUnsub()
		m.connectorWatcherUnsub = nil
	}
}

func (m *Manager) setConnectorWatcherUnsub(unsub func()) {
	m.connectorWatcherMu.Lock()
	m.connectorWatcherUnsub = unsub
	m.connectorWatcherMu.Unlock()
}

func (m *Manager) acquireConnectorLease(handle *connectorHandle) (bool, int64, error) {
	if m.storage == nil {
		return false, 0, fmt.Errorf("storage not initialized")
	}

	record, version, exists, err := m.getConnectorLease(handle.id)
	if err != nil {
		return false, 0, err
	}

	now := time.Now()
	if exists && record.Owner != "" && record.Owner != m.nodeID && record.ExpiresAt.After(now) {
		return false, 0, nil
	}

	record.ID = handle.id
	record.Name = handle.name
	record.Owner = m.nodeID
	record.ExpiresAt = now.Add(connectorLeaseDuration)

	newVersion := int64(1)
	if exists {
		newVersion = version + 1
	}

	data, err := json.Marshal(record)
	if err != nil {
		return false, 0, err
	}

	if err := m.storage.Put(m.connectorLeaseKey(handle.id), data, newVersion); err != nil {
		if isVersionMismatch(err) {
			return false, 0, nil
		}
		return false, 0, err
	}

	return true, newVersion, nil
}

func (m *Manager) renewConnectorLease(handle *connectorHandle) (int64, error) {
	record, version, exists, err := m.getConnectorLease(handle.id)
	if err != nil {
		return 0, err
	}
	if !exists || record.Owner != m.nodeID {
		return 0, errConnectorLeaseLost
	}

	record.ExpiresAt = time.Now().Add(connectorLeaseDuration)
	newVersion := version + 1

	data, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}

	if err := m.storage.Put(m.connectorLeaseKey(handle.id), data, newVersion); err != nil {
		if isVersionMismatch(err) {
			return 0, errConnectorLeaseLost
		}
		return 0, err
	}

	return newVersion, nil
}

func (m *Manager) releaseConnectorLease(handle *connectorHandle, force bool) {
	record, version, exists, err := m.getConnectorLease(handle.id)
	if err != nil || !exists {
		return
	}

	if record.Owner != m.nodeID {
		if m.logger != nil {
			m.logger.Debug("skipping connector lease release; ownership moved", "connector", handle.name, "connector_id", handle.id, "current_owner", record.Owner)
		}
		return
	}

	record.Owner = ""
	record.ExpiresAt = time.Time{}

	newVersion := version + 1
	data, err := json.Marshal(record)
	if err != nil {
		return
	}

	if err := m.storage.Put(m.connectorLeaseKey(handle.id), data, newVersion); err != nil {
		if m.logger != nil && !isVersionMismatch(err) {
			m.logger.Warn("failed to release connector lease", "connector", handle.name, "connector_id", handle.id, "error", err)
		}
		return
	}

	handle.leaseVersion = 0
}

func (m *Manager) getConnectorLease(id string) (*connectorLeaseRecord, int64, bool, error) {
	if m.storage == nil {
		return nil, 0, false, fmt.Errorf("storage not initialized")
	}

	value, version, exists, err := m.storage.Get(m.connectorLeaseKey(id))
	if err != nil {
		return nil, 0, false, err
	}

	if !exists {
		return &connectorLeaseRecord{ID: id}, 0, false, nil
	}

	var record connectorLeaseRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return nil, version, true, err
	}

	return &record, version, true, nil
}

func (m *Manager) connectorLeaseKey(id string) string {
	return connectorLeaseKeyPrefix + id
}

func (m *Manager) getConnectorConfig(id string) (*connectorConfigRecord, int64, bool, error) {
	if m.storage == nil {
		return nil, 0, false, fmt.Errorf("storage not initialized")
	}

	value, version, exists, err := m.storage.Get(m.connectorConfigKey(id))
	if err != nil {
		return nil, 0, false, err
	}

	if !exists {
		return nil, 0, false, nil
	}

	var record connectorConfigRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return nil, version, true, err
	}

	return &record, version, true, nil
}

func (m *Manager) connectorConfigKey(id string) string {
	return connectorConfigKeyPrefix + id
}

func isVersionMismatch(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "version mismatch")
}

func (m *Manager) randomConnectorDelay(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	m.connectorRandMu.Lock()
	defer m.connectorRandMu.Unlock()

	if m.connectorRand == nil {
		m.connectorRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	delta := max - min
	return min + time.Duration(m.connectorRand.Int63n(int64(delta)))
}

func (m *Manager) initConnectorObservers() {
	m.connectorObserversOnce.Do(func() {
		if m.eventManager == nil {
			return
		}
		if err := m.eventManager.OnNodeJoined(func(*domain.NodeJoinedEvent) {
			m.rebalanceConnectors(releaseReasonRebalance)
		}); err != nil && m.logger != nil {
			m.logger.Warn("failed to register node join handler for connectors", "error", err)
		}
	})
}

func (m *Manager) shouldRunConnector(handle *connectorHandle) (bool, error) {
	if m.loadBalancer == nil {
		return true, nil
	}

	workflowID := m.connectorWorkflowKey(handle.id)
	return m.loadBalancer.ShouldExecuteNode(m.nodeID, workflowID, handle.name)
}

func (m *Manager) registerConnectorLoad(handle *connectorHandle) error {
	if m.loadBalancer == nil {
		return nil
	}

	return m.loadBalancer.RegisterConnectorLoad(handle.id, 1)
}

func (m *Manager) unregisterConnectorLoad(handle *connectorHandle) {
	if m.loadBalancer == nil {
		return
	}

	if err := m.loadBalancer.DeregisterConnectorLoad(handle.id); err != nil && m.logger != nil {
		m.logger.Warn("failed to deregister connector load", "connector", handle.name, "connector_id", handle.id, "error", err)
	}
}

func (m *Manager) connectorWorkflowKey(id string) string {
	return "connector::" + id
}
