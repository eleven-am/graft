package raft

import (
	"context"
	"log/slog"
	"sync"

	"github.com/eleven-am/graft/internal/ports"
)

type EventEmitter interface {
	Emit(event ports.BootstrapEvent)
}

type Strategy interface {
	Run(ctx context.Context, emitter EventEmitter) error
}

type CoordinatorConfig struct {
	NodeID string
	Logger *slog.Logger
}

type Coordinator struct {
	strategy Strategy
	config   CoordinatorConfig
	logger   *slog.Logger
	eventsCh chan ports.BootstrapEvent

	mu      sync.RWMutex
	started bool
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
}

func NewCoordinator(strategy Strategy, config CoordinatorConfig) *Coordinator {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Coordinator{
		strategy: strategy,
		config:   config,
		logger:   logger.With("component", "raft.coordinator"),
		eventsCh: make(chan ports.BootstrapEvent, 16),
	}
}

func (c *Coordinator) SetNodeID(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.NodeID = nodeID
}

func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errCoordinatorAlreadyStarted
	}

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.done = make(chan struct{})
	c.started = true
	c.mu.Unlock()

	go c.run()
	return nil
}

func (c *Coordinator) run() {
	defer close(c.done)

	if c.strategy == nil {
		return
	}

	c.strategy.Run(c.ctx, c)
}

func (c *Coordinator) Stop() error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return errCoordinatorNotStarted
	}

	cancel := c.cancel
	done := c.done
	c.started = false
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

func (c *Coordinator) Events() <-chan ports.BootstrapEvent {
	return c.eventsCh
}

func (c *Coordinator) Emit(event ports.BootstrapEvent) {
	c.mu.RLock()
	if !c.started {
		c.mu.RUnlock()
		return
	}
	c.mu.RUnlock()

	event.NodeID = c.config.NodeID

	select {
	case c.eventsCh <- event:
	default:
		if c.logger != nil {
			c.logger.Warn("event channel full, dropping event", "event_type", event.Type)
		}
	}
}

var _ ports.BootstrapCoordinator = (*Coordinator)(nil)
