package raft2

import (
	"context"
	"errors"
	"time"

	"log/slog"

	"github.com/eleven-am/graft/internal/ports"
)

// PollingStrategy observes leadership information from the runtime and emits
// coarse bootstrap events when transitions occur. It provides the baseline
// coordination loop for the new raft2 controller; more sophisticated discovery
// and negotiation can build on top of this mechanism.
type PollingStrategy struct {
	runtime  NodeRuntime
	logger   *slog.Logger
	interval time.Duration
	clock    func() time.Time
}

// NewPollingStrategy constructs a strategy with the provided runtime and
// optional configuration.
func NewPollingStrategy(runtime NodeRuntime, interval time.Duration, logger *slog.Logger) *PollingStrategy {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &PollingStrategy{
		runtime:  runtime,
		logger:   logger.With("component", "raft2.strategy"),
		interval: interval,
		clock:    time.Now,
	}
}

// Run implements the Strategy interface.
func (s *PollingStrategy) Run(ctx context.Context, emitter EventEmitter) error {
	if s.runtime == nil {
		return errors.New("raft2: polling strategy requires runtime")
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	last := ports.RaftLeadershipInfo{State: ports.RaftLeadershipUnknown}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			info := s.runtime.LeadershipInfo()
			if sameLeadership(last, info) {
				continue
			}

			event := mapLeadership(info, s.clock())
			emitter.Emit(event)
			last = info
			if s.logger != nil {
				s.logger.Debug("emitted bootstrap event", "event", event.Type, "state", info.State)
			}
		}
	}
}

func sameLeadership(prev, next ports.RaftLeadershipInfo) bool {
	return prev.State == next.State && prev.LeaderID == next.LeaderID && prev.LeaderAddress == next.LeaderAddress && prev.Term == next.Term
}

func mapLeadership(info ports.RaftLeadershipInfo, ts time.Time) ports.BootstrapEvent {
	event := ports.BootstrapEvent{Timestamp: ts}

	switch info.State {
	case ports.RaftLeadershipLeader:
		event.Type = ports.BootstrapEventLeader
		event.Details = map[string]string{
			"leader_id":      info.LeaderID,
			"leader_address": info.LeaderAddress,
		}
	case ports.RaftLeadershipFollower:
		event.Type = ports.BootstrapEventFollower
		event.Details = map[string]string{
			"leader_id":      info.LeaderID,
			"leader_address": info.LeaderAddress,
		}
	case ports.RaftLeadershipJoining:
		event.Type = ports.BootstrapEventJoinStart
	case ports.RaftLeadershipDemoted:
		event.Type = ports.BootstrapEventDemotion
	case ports.RaftLeadershipProvisional:
		event.Type = ports.BootstrapEventProvisional
	default:
		event.Type = ports.BootstrapEventProvisional
	}

	return event
}
