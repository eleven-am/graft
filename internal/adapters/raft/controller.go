package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"log/slog"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

// NodeRuntime abstracts the low-level raft node implementation controlled by
// the raft controller.
type NodeRuntime interface {
	Start(ctx context.Context, opts domain.RaftControllerOptions) error
	Stop() error
	Apply(cmd domain.Command, timeout time.Duration) (*domain.CommandResult, error)
	Demote(ctx context.Context, peer ports.RaftPeer) error
	LeadershipInfo() ports.RaftLeadershipInfo
	ClusterInfo() ports.ClusterInfo
}

// ControllerDeps groups external collaborators required by the controller.
type ControllerDeps struct {
	Coordinator ports.BootstrapCoordinator
	Readiness   ports.ReadinessGate
	Runtime     NodeRuntime
	Logger      *slog.Logger
	Clock       func() time.Time
}

// Controller coordinates the raft node runtime, bootstrap strategy, and
// readiness state machine.
type Controller struct {
	mu sync.RWMutex

	deps ControllerDeps

	started  bool
	ctx      context.Context
	cancel   context.CancelFunc
	runDone  chan struct{}
	leaderCh chan struct{}

	opts     domain.RaftControllerOptions
	metadata map[string]string

	leadership ports.RaftLeadershipInfo
	lastEvent  ports.BootstrapEvent
}

// NewController constructs a controller using the supplied dependencies.
func NewController(deps ControllerDeps) (*Controller, error) {
	if deps.Coordinator == nil {
		return nil, errMissingCoordinator
	}
	if deps.Runtime == nil {
		return nil, errMissingRuntime
	}
	if deps.Readiness == nil {
		deps.Readiness = NewReadiness()
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}

	return &Controller{
		deps:       deps,
		leaderCh:   make(chan struct{}),
		leadership: ports.RaftLeadershipInfo{State: ports.RaftLeadershipUnknown},
	}, nil
}

// Start brings the controller online using the provided options.
func (c *Controller) Start(ctx context.Context, opts domain.RaftControllerOptions) error {
	if opts.NodeID == "" {
		return errors.New("raft: node id is required")
	}

	c.mu.RLock()
	if c.started {
		c.mu.RUnlock()
		return errControllerAlreadyStarted
	}
	c.mu.RUnlock()

	runCtx, cancel := context.WithCancel(ctx)

	if err := c.deps.Runtime.Start(runCtx, opts); err != nil {
		if c.deps.Logger != nil {
			c.deps.Logger.Error("runtime start failed",
				"node_id", opts.NodeID,
				"bind_address", opts.BindAddress,
				"peer_count", len(opts.Peers),
				"bootstrap_multi_node", opts.BootstrapMultiNode,
				"error", err)
		}
		cancel()
		return err
	}

	if coord, ok := c.deps.Coordinator.(*Coordinator); ok {
		coord.SetNodeID(opts.NodeID)
	}

	if err := c.deps.Coordinator.Start(runCtx); err != nil {
		cancel()
		_ = c.deps.Runtime.Stop()
		return err
	}

	metadataCopy := cloneMetadata(opts.BootstrapMetadata)

	c.mu.Lock()
	c.started = true
	c.ctx = runCtx
	c.cancel = cancel
	c.runDone = make(chan struct{})
	c.opts = opts
	c.metadata = metadataCopy
	c.lastEvent = ports.BootstrapEvent{Type: ports.BootstrapEventProvisional, Timestamp: c.deps.Clock(), NodeID: opts.NodeID}
	c.updateLeadershipLocked(ports.RaftLeadershipInfo{
		State:         ports.RaftLeadershipProvisional,
		LeaderID:      opts.NodeID,
		LeaderAddress: opts.BindAddress,
	})
	c.mu.Unlock()

	c.deps.Readiness.SetState(ports.ReadinessStatePending)

	go c.consumeEvents(runCtx, c.runDone)

	return nil
}

// Stop terminates the controller and underlying runtime.
func (c *Controller) Stop() error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return errControllerNotStarted
	}

	cancel := c.cancel
	runDone := c.runDone
	c.started = false
	c.cancel = nil
	c.runDone = nil
	c.updateLeadershipLocked(ports.RaftLeadershipInfo{State: ports.RaftLeadershipUnknown})
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if runDone != nil {
		<-runDone
	}

	var stopErr error
	if err := c.deps.Coordinator.Stop(); err != nil && !errors.Is(err, errCoordinatorNotStarted) {
		stopErr = errors.Join(stopErr, err)
	}
	if err := c.deps.Runtime.Stop(); err != nil {
		stopErr = errors.Join(stopErr, err)
	}

	c.deps.Readiness.SetState(ports.ReadinessStatePaused)

	return stopErr
}

// WaitForLeadership blocks until the controller achieves leadership or the
// context is cancelled.
func (c *Controller) WaitForLeadership(ctx context.Context) error {
	c.mu.RLock()
	if !c.started {
		c.mu.RUnlock()
		return errControllerNotStarted
	}
	info := c.leadership
	leaderCh := c.leaderCh
	c.mu.RUnlock()

	if info.State == ports.RaftLeadershipLeader {
		return nil
	}

	select {
	case <-leaderCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LeadershipInfo returns the current leadership view.
func (c *Controller) LeadershipInfo() ports.RaftLeadershipInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leadership
}

// Apply delegates command application to the underlying runtime when the
// controller is the active leader.
func (c *Controller) Apply(cmd domain.Command, timeout time.Duration) (*domain.CommandResult, error) {
	c.mu.RLock()
	if !c.started {
		c.mu.RUnlock()
		return nil, errControllerNotStarted
	}
	if c.leadership.State != ports.RaftLeadershipLeader {
		c.mu.RUnlock()
		return nil, errNotLeader
	}
	c.mu.RUnlock()

	return c.deps.Runtime.Apply(cmd, timeout)
}

// Demote requests the underlying runtime to step down in favour of the
// supplied peer.
func (c *Controller) Demote(ctx context.Context, peer ports.RaftPeer) error {
	c.mu.RLock()
	if !c.started {
		c.mu.RUnlock()
		return errControllerNotStarted
	}
	c.mu.RUnlock()

	return c.deps.Runtime.Demote(ctx, peer)
}

// Metadata returns bootstrap metadata associated with the controller.
func (c *Controller) Metadata() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneMetadata(c.metadata)
}

func (c *Controller) consumeEvents(ctx context.Context, done chan struct{}) {
	defer close(done)

	events := c.deps.Coordinator.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			c.handleBootstrapEvent(ev)
		}
	}
}

func (c *Controller) handleBootstrapEvent(event ports.BootstrapEvent) {
	c.mu.Lock()
	c.lastEvent = event

	info := c.leadership
	switch event.Type {
	case ports.BootstrapEventProvisional:
		info.State = ports.RaftLeadershipProvisional
	case ports.BootstrapEventLeader:
		info.State = ports.RaftLeadershipLeader
		info.LeaderID = c.opts.NodeID
		info.LeaderAddress = c.opts.BindAddress
	case ports.BootstrapEventFollower, ports.BootstrapEventJoinSuccess:
		info.State = ports.RaftLeadershipFollower
		if leaderID, ok := event.Details["leader_id"]; ok {
			info.LeaderID = leaderID
		}
		if leaderAddr, ok := event.Details["leader_address"]; ok {
			info.LeaderAddress = leaderAddr
		}
	case ports.BootstrapEventJoinStart:
		info.State = ports.RaftLeadershipJoining
	case ports.BootstrapEventJoinFailed:
		info.State = ports.RaftLeadershipProvisional
	case ports.BootstrapEventDemotion:
		info.State = ports.RaftLeadershipDemoted
	default:
		c.mu.Unlock()
		return
	}

	c.updateLeadershipLocked(info)
	c.updateReadinessLocked(info)

	if c.deps.Logger != nil {
		c.deps.Logger.Debug("bootstrap event", "event", event.Type, "state", info.State)
	}

	c.mu.Unlock()
}

func (c *Controller) updateLeadershipLocked(info ports.RaftLeadershipInfo) {
	prev := c.leadership.State
	c.leadership = info

	if prev != ports.RaftLeadershipLeader && info.State == ports.RaftLeadershipLeader {
		select {
		case <-c.leaderCh:
		default:
			close(c.leaderCh)
		}
	}

	if prev == ports.RaftLeadershipLeader && info.State != ports.RaftLeadershipLeader {
		c.leaderCh = make(chan struct{})
	}
}

func (c *Controller) updateReadinessLocked(info ports.RaftLeadershipInfo) {
	switch info.State {
	case ports.RaftLeadershipLeader:
		c.deps.Readiness.SetState(ports.ReadinessStateReady)
	case ports.RaftLeadershipFollower:
		clusterInfo := c.deps.Runtime.ClusterInfo()
		memberCount := len(clusterInfo.Members)
		if memberCount >= 2 {
			c.deps.Logger.Debug("follower configuration ready", "members", memberCount)
			c.deps.Readiness.SetState(ports.ReadinessStateReady)
		} else {
			c.deps.Logger.Debug("follower waiting for configuration", "members", memberCount, "expected", 2)
			c.deps.Readiness.SetState(ports.ReadinessStatePending)
		}
	case ports.RaftLeadershipDemoted:
		c.deps.Readiness.SetState(ports.ReadinessStatePaused)
	default:
		c.deps.Readiness.SetState(ports.ReadinessStatePending)
	}
}

func cloneMetadata(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	clone := make(map[string]string, len(src))
	for k, v := range src {
		clone[k] = v
	}
	return clone
}
