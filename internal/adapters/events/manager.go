package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Manager struct {
	storage ports.StoragePort
	logger  *slog.Logger
	nodeID  string

	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	workflowStartedHandlers   []func(*domain.WorkflowStartedEvent)
	workflowCompletedHandlers []func(*domain.WorkflowCompletedEvent)
	workflowFailedHandlers    []func(*domain.WorkflowErrorEvent)
	workflowPausedHandlers    []func(*domain.WorkflowPausedEvent)
	workflowResumedHandlers   []func(*domain.WorkflowResumedEvent)
	nodeStartedHandlers       []func(*domain.NodeStartedEvent)
	nodeCompletedHandlers     []func(*domain.NodeCompletedEvent)
	nodeErrorHandlers         []func(*domain.NodeErrorEvent)

	nodeJoinedHandlers    []func(*domain.NodeJoinedEvent)
	nodeLeftHandlers      []func(*domain.NodeLeftEvent)
	leaderChangedHandlers []func(*domain.LeaderChangedEvent)

	commandHandlers      map[string]domain.CommandHandler
	channelSubscriptions map[string][]chan domain.Event
	processedEvents      map[string]time.Time
}

func NewManager(storage ports.StoragePort, nodeID string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		logger:               logger.With("component", "event-manager"),
		storage:              storage,
		nodeID:               nodeID,
		commandHandlers:      make(map[string]domain.CommandHandler),
		channelSubscriptions: make(map[string][]chan domain.Event),
		processedEvents:      make(map[string]time.Time),
	}
}

// PublishStorageEvents allows storage to publish persisted events to the event hub.
// It broadcasts to channel subscribers and triggers typed handlers.
func (m *Manager) PublishStorageEvents(events []domain.Event) error {
	for _, ev := range events {
		m.routeEvent(ev)
	}
	return nil
}

func (m *Manager) Broadcast(event domain.Event) error {
	m.routeEvent(event)
	return nil
}

// routeEvent fans out to channels and invokes typed handlers for any event origin
func (m *Manager) routeEvent(event domain.Event) {
	eventKey := m.eventKey(event)

	m.mu.Lock()
	if _, processed := m.processedEvents[eventKey]; processed {
		m.mu.Unlock()
		return
	}
	m.processedEvents[eventKey] = time.Now()
	m.mu.Unlock()

	m.broadcastToChannels(event)
	_ = m.processStorageEvent(&event)
}

// eventKey generates a unique key for an event to enable deduplication
func (m *Manager) eventKey(event domain.Event) string {
	return fmt.Sprintf("%s:%d:%s:%d", event.Key, event.Version, event.NodeID, event.Timestamp.UnixNano())
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return domain.NewDiscoveryError("event-manager", "start", domain.ErrAlreadyStarted)
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.running = true

	go m.cleanupProcessedEvents()

	return nil
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return domain.NewDiscoveryError("event-manager", "stop", domain.ErrNotStarted)
	}

	m.cancel()

	for _, subs := range m.channelSubscriptions {
		for _, ch := range subs {
			close(ch)
		}
	}
	m.channelSubscriptions = make(map[string][]chan domain.Event)
	m.processedEvents = make(map[string]time.Time)

	m.running = false
	return nil
}

func (m *Manager) processStorageEvent(event *domain.Event) error {
	parts := strings.Split(event.Key, ":")
	if len(parts) < 2 {
		return nil
	}

	switch parts[0] {
	case "workflow":
		return m.processWorkflowEvent(event, parts)
	case "node":
		return m.processNodeEvent(event, parts)
	case "dev-cmd":
		return m.processDevCommand(event, parts)
	default:
		return nil
	}
}

func (m *Manager) processWorkflowEvent(event *domain.Event, keyParts []string) error {
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

	if eventType == "node" && len(keyParts) >= 5 {
		nodeName := keyParts[3]
		nodeEventType := keyParts[4]

		switch nodeEventType {
		case "started":
			var nodeEvent domain.NodeStartedEvent
			if err := json.Unmarshal(data, &nodeEvent); err != nil {
				return domain.ErrInvalidInput
			}
			nodeEvent.WorkflowID = workflowID
			nodeEvent.NodeName = nodeName
			m.notifyNodeStarted(&nodeEvent)
		case "completed":
			var nodeEvent domain.NodeCompletedEvent
			if err := json.Unmarshal(data, &nodeEvent); err != nil {
				return domain.ErrInvalidInput
			}
			nodeEvent.WorkflowID = workflowID
			nodeEvent.NodeName = nodeName
			m.notifyNodeCompleted(&nodeEvent)
		case "error":
			var nodeEvent domain.NodeErrorEvent
			if err := json.Unmarshal(data, &nodeEvent); err != nil {
				return domain.ErrInvalidInput
			}
			nodeEvent.WorkflowID = workflowID
			nodeEvent.NodeName = nodeName
			m.notifyNodeError(&nodeEvent)
		}
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

	return nil
}

func (m *Manager) processNodeEvent(event *domain.Event, keyParts []string) error {
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
		nodeEvent.WorkflowID = workflowID
		nodeEvent.NodeName = nodeName
		m.notifyNodeStarted(&nodeEvent)

	case "completed":
		var nodeEvent domain.NodeCompletedEvent
		if err := json.Unmarshal(data, &nodeEvent); err != nil {
			return domain.ErrInvalidInput
		}
		nodeEvent.WorkflowID = workflowID
		nodeEvent.NodeName = nodeName
		m.notifyNodeCompleted(&nodeEvent)

	case "error":
		var nodeEvent domain.NodeErrorEvent
		if err := json.Unmarshal(data, &nodeEvent); err != nil {
			return domain.ErrInvalidInput
		}
		nodeEvent.WorkflowID = workflowID
		nodeEvent.NodeName = nodeName
		m.notifyNodeError(&nodeEvent)
	}

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

// Subscribe wraps SubscribeToChannel to provide a callback API.
// It listens to channel events for the given pattern (supports '*' suffix as prefix wildcard)
// and invokes the handler with the matching key. The event payload is not provided; consumers
// should fetch any needed data from storage.
// Callback-based Subscribe/Unsubscribe removed; use SubscribeToChannel instead

func (m *Manager) SubscribeToChannel(prefix string) (<-chan domain.Event, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan domain.Event, 100)
	m.channelSubscriptions[prefix] = append(m.channelSubscriptions[prefix], ch)

	unsubscribe := func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		subs := m.channelSubscriptions[prefix]
		for i, sub := range subs {
			if sub == ch {
				m.channelSubscriptions[prefix] = append(subs[:i], subs[i+1:]...)
				close(ch)
				break
			}
		}
	}

	return ch, unsubscribe, nil
}

func (m *Manager) broadcastToChannels(event domain.Event) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for prefix, subs := range m.channelSubscriptions {
		if len(event.Key) >= len(prefix) && event.Key[:len(prefix)] == prefix {
			for _, ch := range subs {
				select {
				case ch <- event:
				default:
				}
			}
		}
	}
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

func (m *Manager) BroadcastCommand(ctx context.Context, devCmd *domain.DevCommand) error {
	internalCmd := devCmd.ToInternalCommand()

	if m.storage == nil {
		return domain.NewDiscoveryError("event-manager", "broadcast-dev-command", domain.ErrNotStarted)
	}

	ttl := 60 * time.Second
	if err := m.storage.PutWithTTL(internalCmd.Key, internalCmd.Value, 0, ttl); err != nil {
		return domain.NewDiscoveryError("event-manager", "store-dev-command", err)
	}

	return nil
}

func (m *Manager) RegisterCommandHandler(cmdName string, handler domain.CommandHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commandHandlers[cmdName] = handler
	return nil
}

func (m *Manager) processDevCommand(event *domain.Event, keyParts []string) error {
	if len(keyParts) < 2 {
		return nil
	}

	cmdName := keyParts[1]

	data, _, exists, err := m.storage.Get(event.Key)
	if err != nil {
		return domain.ErrInvalidInput
	}
	if !exists {
		return nil
	}

	var devCmd domain.DevCommand
	if err := json.Unmarshal(data, &devCmd); err != nil {
		return domain.ErrInvalidInput
	}

	m.mu.RLock()
	handler, exists := m.commandHandlers[cmdName]
	m.mu.RUnlock()

	if !exists {
		m.logger.Warn("no handler registered for dev command", "command", cmdName)
		return nil
	}

	ctx := context.Background()
	go m.safeCall(func() {
		if err := handler(ctx, event.NodeID, devCmd.Params); err != nil {
			m.logger.Error("dev command handler failed", "command", cmdName, "error", err)
		}
	})

	return nil
}

func (m *Manager) OnNodeJoined(handler func(*domain.NodeJoinedEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeJoinedHandlers = append(m.nodeJoinedHandlers, handler)
	return nil
}

func (m *Manager) OnNodeLeft(handler func(*domain.NodeLeftEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeLeftHandlers = append(m.nodeLeftHandlers, handler)
	return nil
}

func (m *Manager) OnLeaderChanged(handler func(*domain.LeaderChangedEvent)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderChangedHandlers = append(m.leaderChangedHandlers, handler)
	return nil
}

func (m *Manager) NotifyNodeJoined(event *domain.NodeJoinedEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.NodeJoinedEvent), len(m.nodeJoinedHandlers))
	copy(handlers, m.nodeJoinedHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) NotifyNodeLeft(event *domain.NodeLeftEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.NodeLeftEvent), len(m.nodeLeftHandlers))
	copy(handlers, m.nodeLeftHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) NotifyLeaderChanged(event *domain.LeaderChangedEvent) {
	m.mu.RLock()
	handlers := make([]func(*domain.LeaderChangedEvent), len(m.leaderChangedHandlers))
	copy(handlers, m.leaderChangedHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		go m.safeCall(func() { handler(event) })
	}
}

func (m *Manager) safeCall(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("event handler panicked", "panic", r)
		}
	}()
	fn()
}

// cleanupProcessedEvents periodically removes old entries from processedEvents
func (m *Manager) cleanupProcessedEvents() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.mu.Lock()
			cutoff := time.Now().Add(-10 * time.Minute)
			for key, timestamp := range m.processedEvents {
				if timestamp.Before(cutoff) {
					delete(m.processedEvents, key)
				}
			}
			m.mu.Unlock()
		case <-m.ctx.Done():
			return
		}
	}
}
