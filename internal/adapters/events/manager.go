package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/google/uuid"
)

type Manager struct {
	storage ports.StoragePort
	logger  *slog.Logger
	nodeID  string

	mu            sync.RWMutex
	subscriptions map[string]*subscription
	running       bool
	ctx           context.Context
	cancel        context.CancelFunc

	workflowStartedHandlers   []func(*domain.WorkflowStartedEvent)
	workflowCompletedHandlers []func(*domain.WorkflowCompletedEvent)
	workflowFailedHandlers    []func(*domain.WorkflowErrorEvent)
	workflowPausedHandlers    []func(*domain.WorkflowPausedEvent)
	workflowResumedHandlers   []func(*domain.WorkflowResumedEvent)
	nodeStartedHandlers       []func(*domain.NodeStartedEvent)
	nodeCompletedHandlers     []func(*domain.NodeCompletedEvent)
	nodeErrorHandlers         []func(*domain.NodeErrorEvent)
	genericHandlers           []genericSubscription
}

type genericSubscription struct {
	id      string
	pattern string
	handler func(string, interface{})
}

type subscription struct {
	id      string
	pattern string
	channel <-chan ports.StorageEvent
	cleanup func()
}

func NewManager(logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		logger:        logger.With("component", "event-manager"),
		subscriptions: make(map[string]*subscription),
	}
}

func NewManagerWithStorage(storage ports.StoragePort, nodeID string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		storage:       storage,
		nodeID:        nodeID,
		logger:        logger.With("component", "event-manager"),
		subscriptions: make(map[string]*subscription),
	}
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return domain.NewDiscoveryError("event-manager", "start", domain.ErrAlreadyStarted)
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.running = true

	if err := m.subscribeToEvents(); err != nil {
		m.cancel()
		m.running = false
		return domain.NewDiscoveryError("event-manager", "subscribe", err)
	}

	m.logger.Debug("event manager started")
	return nil
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return domain.NewDiscoveryError("event-manager", "stop", domain.ErrNotStarted)
	}

	m.cancel()

	for _, sub := range m.subscriptions {
		sub.cleanup()
	}
	m.subscriptions = make(map[string]*subscription)

	m.running = false
	m.logger.Debug("event manager stopped")
	return nil
}

func (m *Manager) subscribeToEvents() error {
	workflowChan, workflowCleanup, err := m.storage.Subscribe("workflow:")
	if err != nil {
		return domain.NewDiscoveryError("event-manager", "subscribe-workflow", err)
	}

	workflowSub := &subscription{
		id:      uuid.New().String(),
		pattern: "workflow:",
		channel: workflowChan,
		cleanup: workflowCleanup,
	}
	m.subscriptions["workflow"] = workflowSub

	go m.handleStorageEvents(workflowSub)

	nodeChan, nodeCleanup, err := m.storage.Subscribe("node:")
	if err != nil {
		workflowCleanup()
		return domain.NewDiscoveryError("event-manager", "subscribe-node", err)
	}

	nodeSub := &subscription{
		id:      uuid.New().String(),
		pattern: "node:",
		channel: nodeChan,
		cleanup: nodeCleanup,
	}
	m.subscriptions["node"] = nodeSub

	go m.handleStorageEvents(nodeSub)

	return nil
}

func (m *Manager) handleStorageEvents(sub *subscription) {
	for {
		select {
		case <-m.ctx.Done():
			return
		case storageEvent, ok := <-sub.channel:
			if !ok {
				m.logger.Warn("storage event channel closed", "pattern", sub.pattern)
				return
			}

			if err := m.processStorageEvent(&storageEvent); err != nil {
				m.logger.Error("failed to process storage event",
					"error", err,
					"key", storageEvent.Key,
					"type", storageEvent.Type)
			}
		}
	}
}

func (m *Manager) processStorageEvent(event *ports.StorageEvent) error {
	parts := strings.Split(event.Key, ":")
	if len(parts) < 2 {
		return nil
	}

	switch parts[0] {
	case "workflow":
		return m.processWorkflowEvent(event, parts)
	case "node":
		return m.processNodeEvent(event, parts)
	default:
		return nil
	}
}

func (m *Manager) processWorkflowEvent(event *ports.StorageEvent, keyParts []string) error {
	if len(keyParts) < 3 {
		return nil
	}

	workflowID := keyParts[1]
	eventType := keyParts[2]

	data, _, exists, err := m.storage.Get(event.Key)
	if err != nil {
		return domain.ErrInvalidInput
	}
	if !exists {
		return nil
	}

	switch eventType {
	case "started":
		var workflowEvent domain.WorkflowStartedEvent
		if err := json.Unmarshal(data, &workflowEvent); err != nil {
			return domain.ErrInvalidInput
		}
		m.notifyWorkflowStarted(&workflowEvent)

	case "completed":
		var workflowEvent domain.WorkflowCompletedEvent
		if err := json.Unmarshal(data, &workflowEvent); err != nil {
			return domain.ErrInvalidInput
		}
		m.notifyWorkflowCompleted(&workflowEvent)

	case "failed":
		var workflowEvent domain.WorkflowErrorEvent
		if err := json.Unmarshal(data, &workflowEvent); err != nil {
			return domain.ErrInvalidInput
		}
		m.notifyWorkflowFailed(&workflowEvent)

	case "paused":
		var workflowEvent domain.WorkflowPausedEvent
		if err := json.Unmarshal(data, &workflowEvent); err != nil {
			return domain.ErrInvalidInput
		}
		m.notifyWorkflowPaused(&workflowEvent)

	case "resumed":
		var workflowEvent domain.WorkflowResumedEvent
		if err := json.Unmarshal(data, &workflowEvent); err != nil {
			return domain.ErrInvalidInput
		}
		m.notifyWorkflowResumed(&workflowEvent)
	}

	m.notifyGenericHandlers(event.Key, workflowID)
	return nil
}

func (m *Manager) processNodeEvent(event *ports.StorageEvent, keyParts []string) error {
	if len(keyParts) < 4 {
		return nil
	}

	workflowID := keyParts[1]
	nodeName := keyParts[2]
	eventType := keyParts[3]

	data, _, exists, err := m.storage.Get(event.Key)
	if err != nil {
		return domain.ErrInvalidInput
	}

	if !exists {
		return nil
	}

	switch eventType {
	case "started":
		var nodeEvent domain.NodeStartedEvent
		if err := json.Unmarshal(data, &nodeEvent); err != nil {
			return domain.ErrInvalidInput
		}
		m.notifyNodeStarted(&nodeEvent)

	case "completed":
		var nodeEvent domain.NodeCompletedEvent
		if err := json.Unmarshal(data, &nodeEvent); err != nil {
			return domain.ErrInvalidInput
		}
		m.notifyNodeCompleted(&nodeEvent)

	case "error":
		var nodeEvent domain.NodeErrorEvent
		if err := json.Unmarshal(data, &nodeEvent); err != nil {
			return domain.ErrInvalidInput
		}
		m.notifyNodeError(&nodeEvent)
	}

	eventKey := fmt.Sprintf("node:%s:%s", workflowID, nodeName)
	m.notifyGenericHandlers(event.Key, eventKey)
	return nil
}

func (m *Manager) OnWorkflowStarted(handler func(*domain.WorkflowStartedEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workflowStartedHandlers = append(m.workflowStartedHandlers, handler)
	return nil
}

func (m *Manager) OnWorkflowCompleted(handler func(*domain.WorkflowCompletedEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workflowCompletedHandlers = append(m.workflowCompletedHandlers, handler)
	return nil
}

func (m *Manager) OnWorkflowFailed(handler func(*domain.WorkflowErrorEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workflowFailedHandlers = append(m.workflowFailedHandlers, handler)
	return nil
}

func (m *Manager) OnWorkflowPaused(handler func(*domain.WorkflowPausedEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workflowPausedHandlers = append(m.workflowPausedHandlers, handler)
	return nil
}

func (m *Manager) OnWorkflowResumed(handler func(*domain.WorkflowResumedEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workflowResumedHandlers = append(m.workflowResumedHandlers, handler)
	return nil
}

func (m *Manager) OnNodeStarted(handler func(*domain.NodeStartedEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeStartedHandlers = append(m.nodeStartedHandlers, handler)
	return nil
}

func (m *Manager) OnNodeCompleted(handler func(*domain.NodeCompletedEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeCompletedHandlers = append(m.nodeCompletedHandlers, handler)
	return nil
}

func (m *Manager) OnNodeError(handler func(*domain.NodeErrorEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeErrorHandlers = append(m.nodeErrorHandlers, handler)
	return nil
}

func (m *Manager) Subscribe(pattern string, handler func(string, interface{})) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sub := genericSubscription{
		id:      uuid.New().String(),
		pattern: pattern,
		handler: handler,
	}
	m.genericHandlers = append(m.genericHandlers, sub)
	return nil
}

func (m *Manager) Unsubscribe(pattern string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var filtered []genericSubscription
	for _, sub := range m.genericHandlers {
		if sub.pattern != pattern {
			filtered = append(filtered, sub)
		}
	}
	m.genericHandlers = filtered
	return nil
}

func (m *Manager) notifyWorkflowStarted(event *domain.WorkflowStartedEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.WorkflowStartedEvent), len(m.workflowStartedHandlers))
	copy(handlers, m.workflowStartedHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) notifyWorkflowCompleted(event *domain.WorkflowCompletedEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.WorkflowCompletedEvent), len(m.workflowCompletedHandlers))
	copy(handlers, m.workflowCompletedHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) notifyWorkflowFailed(event *domain.WorkflowErrorEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.WorkflowErrorEvent), len(m.workflowFailedHandlers))
	copy(handlers, m.workflowFailedHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) notifyWorkflowPaused(event *domain.WorkflowPausedEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.WorkflowPausedEvent), len(m.workflowPausedHandlers))
	copy(handlers, m.workflowPausedHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) notifyWorkflowResumed(event *domain.WorkflowResumedEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.WorkflowResumedEvent), len(m.workflowResumedHandlers))
	copy(handlers, m.workflowResumedHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) notifyNodeStarted(event *domain.NodeStartedEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.NodeStartedEvent), len(m.nodeStartedHandlers))
	copy(handlers, m.nodeStartedHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) notifyNodeCompleted(event *domain.NodeCompletedEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.NodeCompletedEvent), len(m.nodeCompletedHandlers))
	copy(handlers, m.nodeCompletedHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) notifyNodeError(event *domain.NodeErrorEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.NodeErrorEvent), len(m.nodeErrorHandlers))
	copy(handlers, m.nodeErrorHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) notifyGenericHandlers(key string, eventData interface{}) {
	m.mu.RLock()
	var matchingHandlers []func(string, interface{})
	for _, sub := range m.genericHandlers {
		if m.patternMatches(sub.pattern, key) {
			matchingHandlers = append(matchingHandlers, sub.handler)
		}
	}
	m.mu.RUnlock()

	for _, handler := range matchingHandlers {
		go m.safeCall(func() { handler(key, eventData) })
	}
}

func (m *Manager) patternMatches(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(key, prefix)
	}
	return pattern == key
}


func (m *Manager) safeCall(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("event handler panicked", "panic", r)
		}
	}()
	fn()
}
