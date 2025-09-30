package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"log/slog"

	"github.com/eleven-am/graft/internal/ports"
)

// Strategy drives bootstrap coordination. Implementations encapsulate discovery,
// peer negotiation, and leadership handoff logic, emitting high-level events via
// the supplied EventEmitter.
type Strategy interface {
	Run(ctx context.Context, emitter EventEmitter) error
}

// EventEmitter is used by strategies to publish bootstrap events without
// coupling to the coordinator implementation.
type EventEmitter interface {
	Emit(event ports.BootstrapEvent)
}

// CoordinatorConfig configures a Coordinator instance.
type CoordinatorConfig struct {
	NodeID      string
	EventBuffer int
	Logger      *slog.Logger
	Clock       func() time.Time
}

// Coordinator orchestrates the lifecycle of a bootstrap strategy and exposes
// the resulting event stream to consumers.
type Coordinator struct {
	mu       sync.Mutex
	strategy Strategy
	config   CoordinatorConfig

	events chan ports.BootstrapEvent

	cancel context.CancelFunc
	done   chan struct{}
	start  bool
}

// NewCoordinator constructs a Coordinator with the provided strategy and
// configuration.
func NewCoordinator(strategy Strategy, cfg CoordinatorConfig) *Coordinator {
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = 32
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}

	return &Coordinator{
		strategy: strategy,
		config:   cfg,
		events:   make(chan ports.BootstrapEvent, cfg.EventBuffer),
	}
}

// SetNodeID updates the coordinator configuration with the provided node ID.
func (c *Coordinator) SetNodeID(nodeID string) {
	c.mu.Lock()
	c.config.NodeID = nodeID
	c.mu.Unlock()
}

// Start launches the underlying strategy loop. It is safe to call once;
// subsequent calls result in an error.
func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.start {
		return errCoordinatorAlreadyStarted
	}
	if c.strategy == nil {
		return errNotImplemented
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.cancel = cancel
	c.done = done
	c.start = true

	go c.run(runCtx, done)

	c.emit(ports.BootstrapEvent{
		Type:      ports.BootstrapEventProvisional,
		Timestamp: c.config.Clock(),
		NodeID:    c.config.NodeID,
	})

	return nil
}

// Stop terminates the running strategy. It returns an error if the coordinator
// was never started.
func (c *Coordinator) Stop() error {
	c.mu.Lock()
	if !c.start {
		c.mu.Unlock()
		return errCoordinatorNotStarted
	}
	c.start = false
	cancel := c.cancel
	done := c.done
	c.cancel = nil
	c.done = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if done != nil {
		<-done
	}

	return nil
}

// Events returns the channel over which bootstrap events are published.
func (c *Coordinator) Events() <-chan ports.BootstrapEvent {
	return c.events
}

func (c *Coordinator) run(ctx context.Context, done chan struct{}) {
	defer close(done)

	emitter := &coordinatorEmitter{coord: c, ctx: ctx}
	if err := c.strategy.Run(ctx, emitter); err != nil && !errors.Is(err, context.Canceled) {
		c.emit(ports.BootstrapEvent{
			Type:      ports.BootstrapEventJoinFailed,
			Timestamp: c.config.Clock(),
			NodeID:    c.config.NodeID,
			Details:   map[string]string{"error": err.Error()},
		})
		if c.config.Logger != nil {
			c.config.Logger.Error("bootstrap strategy failed", "error", err)
		}
	}
}

func (c *Coordinator) emit(event ports.BootstrapEvent) {
	select {
	case c.events <- event:
	default:
		if c.config.Logger != nil {
			c.config.Logger.Warn("dropping bootstrap event", "event", event.Type, "node_id", event.NodeID)
		}
	}
}

type coordinatorEmitter struct {
	coord *Coordinator
	ctx   context.Context
}

func (e *coordinatorEmitter) Emit(event ports.BootstrapEvent) {
	if event.Timestamp.IsZero() {
		event.Timestamp = e.coord.config.Clock()
	}
	if event.NodeID == "" {
		event.NodeID = e.coord.config.NodeID
	}

	select {
	case <-e.ctx.Done():
		return
	default:
		e.coord.emit(event)
	}
}
