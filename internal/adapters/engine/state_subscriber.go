package engine

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type WorkflowStateSubscription struct {
	WorkflowID   string
	Channel      chan *ports.WorkflowStatus
	LastUpdated  time.Time
	SubscriberID string
}

type StateSubscriptionManager struct {
	subscriptions map[string]map[string]*WorkflowStateSubscription
	engine        *Engine
	logger        *slog.Logger
	mu            sync.RWMutex
	nextID        int64
}

func NewStateSubscriptionManager(engine *Engine, logger *slog.Logger) *StateSubscriptionManager {
	return &StateSubscriptionManager{
		subscriptions: make(map[string]map[string]*WorkflowStateSubscription),
		engine:        engine,
		logger:        logger.With("component", "state_subscription_manager"),
	}
}

func (ssm *StateSubscriptionManager) Subscribe(workflowID string) (<-chan *ports.WorkflowStatus, func(), error) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	ssm.nextID++
	subscriberID := generateSubscriberID(ssm.nextID)

	channel := make(chan *ports.WorkflowStatus, 100)

	subscription := &WorkflowStateSubscription{
		WorkflowID:   workflowID,
		Channel:      channel,
		LastUpdated:  time.Now(),
		SubscriberID: subscriberID,
	}

	if ssm.subscriptions[workflowID] == nil {
		ssm.subscriptions[workflowID] = make(map[string]*WorkflowStateSubscription)
	}

	ssm.subscriptions[workflowID][subscriberID] = subscription

	unsubscribeFunc := func() {
		ssm.unsubscribe(workflowID, subscriberID)
	}

	ssm.logger.Debug("workflow state subscription created",
		"workflow_id", workflowID,
		"subscriber_id", subscriberID,
		"total_subscribers", len(ssm.subscriptions[workflowID]))

	currentStatus, err := ssm.engine.GetWorkflowStatus(workflowID)
	if err == nil && currentStatus != nil {
		select {
		case channel <- currentStatus:
		default:
		}
	}

	return channel, unsubscribeFunc, nil
}

func (ssm *StateSubscriptionManager) unsubscribe(workflowID, subscriberID string) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	if workflowSubs, exists := ssm.subscriptions[workflowID]; exists {
		if subscription, exists := workflowSubs[subscriberID]; exists {
			close(subscription.Channel)
			delete(workflowSubs, subscriberID)

			if len(workflowSubs) == 0 {
				delete(ssm.subscriptions, workflowID)
			}

			ssm.logger.Debug("workflow state subscription removed",
				"workflow_id", workflowID,
				"subscriber_id", subscriberID,
				"remaining_subscribers", len(workflowSubs))
		}
	}
}

func (ssm *StateSubscriptionManager) OnStateChange(event StateChangeEvent) {
	ssm.mu.RLock()
	workflowSubs, exists := ssm.subscriptions[event.WorkflowID]
	if !exists || len(workflowSubs) == 0 {
		ssm.mu.RUnlock()
		return
	}

	subscribers := make([]*WorkflowStateSubscription, 0, len(workflowSubs))
	for _, sub := range workflowSubs {
		subscribers = append(subscribers, sub)
	}
	ssm.mu.RUnlock()

	workflowStatus, err := ssm.engine.GetWorkflowStatus(event.WorkflowID)
	if err != nil {
		ssm.logger.Error("failed to get workflow status for state change notification",
			"workflow_id", event.WorkflowID,
			"error", err.Error())
		return
	}

	for _, subscription := range subscribers {
		select {
		case subscription.Channel <- workflowStatus:
			subscription.LastUpdated = time.Now()
			ssm.logger.Debug("workflow state update sent to subscriber",
				"workflow_id", event.WorkflowID,
				"subscriber_id", subscription.SubscriberID,
				"event_type", event.EventType)
		default:
			ssm.logger.Warn("subscriber channel full, dropping state update",
				"workflow_id", event.WorkflowID,
				"subscriber_id", subscription.SubscriberID)
		}
	}
}

func (ssm *StateSubscriptionManager) GetSubscriberCount(workflowID string) int {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()

	if workflowSubs, exists := ssm.subscriptions[workflowID]; exists {
		return len(workflowSubs)
	}
	return 0
}

func (ssm *StateSubscriptionManager) GetTotalSubscriberCount() int {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()

	total := 0
	for _, workflowSubs := range ssm.subscriptions {
		total += len(workflowSubs)
	}
	return total
}

func generateSubscriberID(id int64) string {
	return fmt.Sprintf("sub-%d-%d", time.Now().Unix(), id)
}
