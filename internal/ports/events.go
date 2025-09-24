package ports

import (
	"context"

	"github.com/eleven-am/graft/internal/domain"
)

type EventManager interface {
	Start(ctx context.Context) error
	Stop() error

	Broadcast(event domain.Event) error
	// PublishStorageEvents publishes storage-level events (post-persistence)
	PublishStorageEvents(events []domain.Event) error

	SubscribeToChannel(prefix string) (<-chan domain.Event, func(), error)

	OnWorkflowStarted(handler func(event *domain.WorkflowStartedEvent)) error
	OnWorkflowCompleted(handler func(event *domain.WorkflowCompletedEvent)) error
	OnWorkflowFailed(handler func(event *domain.WorkflowErrorEvent)) error
	OnWorkflowPaused(handler func(event *domain.WorkflowPausedEvent)) error
	OnWorkflowResumed(handler func(event *domain.WorkflowResumedEvent)) error

	OnNodeStarted(handler func(event *domain.NodeStartedEvent)) error
	OnNodeCompleted(handler func(event *domain.NodeCompletedEvent)) error
	OnNodeError(handler func(event *domain.NodeErrorEvent)) error

	// Callback-based Subscribe removed; use SubscribeToChannel instead

	BroadcastCommand(ctx context.Context, devCmd *domain.DevCommand) error
	RegisterCommandHandler(cmdName string, handler domain.CommandHandler) error

	OnNodeJoined(handler func(event *domain.NodeJoinedEvent)) error
	OnNodeLeft(handler func(event *domain.NodeLeftEvent)) error
	OnLeaderChanged(handler func(event *domain.LeaderChangedEvent)) error
}

type EventHandler func(interface{})

type EventSubscription struct {
	ID      string
	Pattern string
	Handler EventHandler
}
