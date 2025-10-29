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
	connectorLeaseNamespace    = "connector"
	connectorLeaseKeyPrefix    = "lease:connector:"
	connectorConfigKeyPrefix   = "connectors:config:"
	connectorLeaseDuration     = 30 * time.Second
	connectorHeartbeatInterval = 10 * time.Second

	connectorAcquireRetryInterval = 2 * time.Second
	connectorAcquireBaseBackoff   = time.Second
	connectorAcquireMaxBackoff    = 10 * time.Second
	connectorRebalanceMinDelay    = 500 * time.Millisecond
	connectorRebalanceMaxDelay    = 2 * time.Second
)

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
	leaseKey  string

	ctx    context.Context
	cancel context.CancelFunc

	releaseCh  chan connectorReleaseRequest
	leaseWake  chan struct{}
	leaseUnsub func()
	leaseCh    <-chan domain.Event
	done       chan struct{}

	mu            sync.Mutex
	rebalanceNext bool
}

var (
	errConnectorLeaseLost = errors.New("connector lease lost")
)

func (m *Manager) RegisterConnector(name string, factory ports.ConnectorFactory) error {
	if m.connectorRegistry == nil {
		return fmt.Errorf("connector registry not initialized")
	}
	if err := m.connectorRegistry.RegisterConnectorFactory(name, factory); err != nil {
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

	if !m.connectorRegistry.HasConnector(connectorName) {
		return domain.NewNotFoundError("connector factory", connectorName)
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

	if err := m.ensureConnectorHandleFromRecord(record); err != nil {
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

	leaseKey := m.connectorLeaseKey(id)

	handle := &connectorHandle{
		manager:   m,
		id:        id,
		name:      name,
		config:    config,
		configRaw: configRaw,
		connector: connector,
		logger:    logger,
		leaseKey:  leaseKey,
		ctx:       ctx,
		cancel:    cancel,
		releaseCh: make(chan connectorReleaseRequest, 1),
		leaseWake: make(chan struct{}, 1),
		done:      make(chan struct{}),
	}

	go handle.run()
	return handle
}

func (h *connectorHandle) run() {
	defer close(h.done)

	leaseBackoff := connectorAcquireRetryInterval
	for {
		if err := h.ctx.Err(); err != nil {
			return
		}

		h.ensureLeaseSubscription()

		acquired, err := h.manager.acquireConnectorLease(h)
		if err != nil {
			if h.logger != nil {
				h.logger.Error("failed to acquire connector lease", "error", err)
			}
			if !h.waitForNextAttempt(leaseBackoff) {
				return
			}
			if leaseBackoff < connectorAcquireMaxBackoff {
				leaseBackoff *= 2
				if leaseBackoff > connectorAcquireMaxBackoff {
					leaseBackoff = connectorAcquireMaxBackoff
				}
			}
			continue
		}

		if !acquired {
			if !h.waitForLeaseSignal(leaseBackoff) {
				return
			}
			continue
		}

		leaseBackoff = connectorAcquireBaseBackoff

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

		err := h.connector.Start(runCtx)
		errCh <- err
	}()

	ticker := time.NewTicker(connectorHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			runCancel()

			stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if stopErr := h.connector.Stop(stopCtx); stopErr != nil && h.logger != nil {
				h.logger.Warn("connector stop failed", "error", stopErr)
			}
			stopCancel()

			err := <-errCh
			h.manager.releaseConnectorLease(h, true)
			if h.logger != nil {
				h.logger.Info("connector context cancelled", "error", err)
			}
			select {
			case req := <-h.releaseCh:
				close(req.ack)
			default:
			}
			h.unsubscribeLeaseEvents()
			return context.Canceled
		case req := <-h.releaseCh:
			if req.reason == releaseReasonRebalance {
				h.markRebalance()
			}
			runCancel()

			stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if stopErr := h.connector.Stop(stopCtx); stopErr != nil && h.logger != nil {
				h.logger.Warn("connector stop failed", "error", stopErr)
			}
			stopCancel()

			err := <-errCh
			h.manager.releaseConnectorLease(h, false)
			if h.logger != nil {
				h.logger.Info("connector released", "reason", string(req.reason), "error", err)
			}
			close(req.ack)
			h.unsubscribeLeaseEvents()
			if err == nil {
				return fmt.Errorf("connector stopped without error")
			}
			return err
		case err := <-errCh:
			runCancel()

			stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if stopErr := h.connector.Stop(stopCtx); stopErr != nil && h.logger != nil {
				h.logger.Warn("connector stop failed", "error", stopErr)
			}
			stopCancel()

			h.manager.releaseConnectorLease(h, false)
			select {
			case req := <-h.releaseCh:
				close(req.ack)
			default:
			}
			h.unsubscribeLeaseEvents()
			if err == nil {
				return fmt.Errorf("connector stopped without error")
			}
			return err
		case <-ticker.C:
			err := h.manager.renewConnectorLease(h)
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
	case <-h.leaseWake:
		return true
	case <-timer.C:
		return true
	}
}

func (h *connectorHandle) waitForLeaseSignal(backoff time.Duration) bool {
	if backoff <= 0 {
		backoff = connectorAcquireRetryInterval
	}

	timer := time.NewTimer(backoff)
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
	case <-h.leaseWake:
		return true
	case <-timer.C:
		return true
	}
}

func (h *connectorHandle) signalLeaseWake() {
	select {
	case h.leaseWake <- struct{}{}:
	default:
	}
}

func (h *connectorHandle) ensureLeaseSubscription() {
	if h.manager == nil || h.manager.eventManager == nil {
		return
	}

	h.mu.Lock()
	needsSubscribe := h.leaseUnsub == nil
	h.mu.Unlock()

	if !needsSubscribe {
		return
	}

	h.subscribeToLeaseEvents()
}

func (h *connectorHandle) subscribeToLeaseEvents() {
	if h.manager == nil || h.manager.eventManager == nil {
		return
	}

	ch, unsub, err := h.manager.eventManager.SubscribeToChannel(h.leaseKey)
	if err != nil {
		if h.logger != nil {
			h.logger.Warn("failed to subscribe to connector lease events", "error", err)
		}
		return
	}

	h.mu.Lock()
	if h.leaseUnsub != nil {
		h.mu.Unlock()
		unsub()
		return
	}
	h.leaseUnsub = unsub
	h.leaseCh = ch
	h.mu.Unlock()

	go h.consumeLeaseEvents(ch, unsub)
}

func (h *connectorHandle) consumeLeaseEvents(ch <-chan domain.Event, unsub func()) {
	defer func() {
		h.mu.Lock()
		if h.leaseCh == ch {
			h.leaseUnsub = nil
			h.leaseCh = nil
		}
		h.mu.Unlock()
		unsub()
	}()

	for {
		select {
		case <-h.ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			switch evt.Type {
			case domain.EventDelete, domain.EventExpire:
				h.signalLeaseWake()
			}
		}
	}
}

func (h *connectorHandle) unsubscribeLeaseEvents() {
	h.mu.Lock()
	unsub := h.leaseUnsub
	h.leaseUnsub = nil
	h.leaseCh = nil
	h.mu.Unlock()

	if unsub != nil {
		unsub()
	}
	h.signalLeaseWake()
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
	select {
	case <-req.ack:
		return
	case <-time.After(5 * time.Second):
		if h.logger != nil {
			h.logger.Warn("timeout waiting for connector release acknowledgment", "connector", h.name, "connector_id", h.id, "reason", string(reason))
		}
		return
	}
}

func (h *connectorHandle) stop() {
	h.cancel()
	<-h.done
}

func (m *Manager) ensureConnectorHandleFromRecord(record *connectorConfigRecord) error {
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

	connector, config, err := m.instantiateConnectorFromRecord(record)
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

func (m *Manager) instantiateConnectorFromRecord(record *connectorConfigRecord) (ports.ConnectorPort, ports.ConnectorConfig, error) {
	factory, err := m.connectorRegistry.GetConnectorFactory(record.Name)
	if err != nil {
		return nil, nil, err
	}

	connector, err := factory(record.Config)
	if err != nil {
		return nil, nil, domain.NewValidationError(
			"failed to create connector from factory",
			err,
			domain.WithComponent("core.Manager"),
			domain.WithContextDetail("connector", record.Name),
			domain.WithContextDetail("connector_id", record.ID),
		)
	}

	if connector == nil {
		return nil, nil, domain.NewValidationError(
			"connector factory returned nil connector",
			nil,
			domain.WithComponent("core.Manager"),
			domain.WithContextDetail("connector", record.Name),
		)
	}

	cfg := connector.GetConfig()
	if cfg == nil {
		return nil, nil, domain.NewValidationError(
			"connector GetConfig() returned nil",
			nil,
			domain.WithComponent("core.Manager"),
			domain.WithContextDetail("connector", record.Name),
		)
	}

	configID := strings.TrimSpace(cfg.GetID())
	if configID == "" {
		return nil, nil, domain.NewValidationError(
			"connector config must provide a non-empty ID",
			nil,
			domain.WithComponent("core.Manager"),
			domain.WithContextDetail("connector", record.Name),
		)
	}

	if record.ID != "" && record.ID != configID {
		return nil, nil, domain.NewValidationError(
			"connector config ID mismatch",
			nil,
			domain.WithComponent("core.Manager"),
			domain.WithContextDetail("connector", record.Name),
			domain.WithContextDetail("expected_id", record.ID),
			domain.WithContextDetail("actual_id", configID),
		)
	}

	return connector, cfg, nil
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

	return m.ensureConnectorHandleFromRecord(record)
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

		if err := m.ensureConnectorHandleFromRecord(&record); err != nil {
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

func (m *Manager) acquireConnectorLease(handle *connectorHandle) (bool, error) {
	if m.leaseManager == nil {
		return false, fmt.Errorf("lease manager not initialized")
	}

	metadata := map[string]string{"name": handle.name}
	_, acquired, err := m.leaseManager.TryAcquire(handle.leaseKey, m.nodeID, connectorLeaseDuration, metadata)
	if err != nil {
		return false, err
	}

	return acquired, nil
}

func (m *Manager) renewConnectorLease(handle *connectorHandle) error {
	if m.leaseManager == nil {
		return fmt.Errorf("lease manager not initialized")
	}

	_, err := m.leaseManager.Renew(handle.leaseKey, m.nodeID, connectorLeaseDuration)
	if err == nil {
		return nil
	}

	if domain.IsLeaseOwnedByOther(err) || domain.IsLeaseNotFound(err) {
		return errConnectorLeaseLost
	}

	return err
}

func (m *Manager) releaseConnectorLease(handle *connectorHandle, force bool) {
	if m.leaseManager == nil {
		return
	}

	var err error
	if force {
		err = m.leaseManager.ForceRelease(handle.leaseKey)
	} else {
		err = m.leaseManager.Release(handle.leaseKey, m.nodeID)
		if domain.IsLeaseOwnedByOther(err) || domain.IsLeaseNotFound(err) {
			err = nil
		}
	}

	if err != nil && m.logger != nil {
		m.logger.Warn("failed to release connector lease", "connector", handle.name, "connector_id", handle.id, "error", err, "force", force)
	}
}

// CleanupNodeLeases force releases connector leases owned by the specified node. Only
// the raft leader should invoke this to avoid conflicting deletes.
func (m *Manager) CleanupNodeLeases(nodeID string) {
	if nodeID == "" || m == nil {
		return
	}
	if m.raftAdapter == nil || !m.raftAdapter.IsLeader() {
		return
	}
	if m.leaseManager == nil || m.storage == nil {
		return
	}
	if nodeID == m.nodeID {
		return
	}

	entries, err := m.storage.ListByPrefix(connectorLeaseKeyPrefix)
	if err != nil {
		if m.logger != nil {
			m.logger.Error("failed to list connector leases for cleanup", "node_id", nodeID, "error", err)
		}
		return
	}

	for _, kv := range entries {
		if len(kv.Value) == 0 {
			continue
		}

		var record ports.LeaseRecord
		if err := json.Unmarshal(kv.Value, &record); err != nil {
			if m.logger != nil {
				m.logger.Warn("failed to decode connector lease during cleanup", "node_id", nodeID, "error", err)
			}
			continue
		}

		if record.Owner != nodeID {
			continue
		}

		connectorID := strings.TrimPrefix(record.Key, connectorLeaseKeyPrefix)

		if err := m.leaseManager.ForceRelease(record.Key); err != nil {
			if m.logger != nil {
				m.logger.Error("failed to cleanup connector lease", "connector", connectorID, "owner", nodeID, "error", err)
			}
			continue
		}

		if m.logger != nil {
			m.logger.Info("cleaned up connector lease after node failure", "connector", connectorID, "owner", nodeID)
		}
	}
}

func (m *Manager) connectorLeaseKey(id string) string {
	if m.leaseManager != nil {
		return m.leaseManager.Key(connectorLeaseNamespace, id)
	}
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
