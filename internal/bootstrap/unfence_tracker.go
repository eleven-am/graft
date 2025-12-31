package bootstrap

import (
	"sync"
	"time"
)

const (
	DefaultSafetyWindowDuration   = 30 * time.Second
	DefaultUnfenceWindowDuration  = 5 * time.Minute
	DefaultUnfenceQuorumThreshold = 10
	DefaultProbeHistorySize       = 60
)

type ProbeRecord struct {
	Timestamp      time.Time
	ReachablePeers int
	ExpectedPeers  int
}

type UnfenceTracker struct {
	mu              sync.RWMutex
	probeHistory    []ProbeRecord
	maxHistorySize  int
	windowDuration  time.Duration
	quorumThreshold int
	clock           func() time.Time
}

type UnfenceTrackerOption func(*UnfenceTracker)

func WithHistorySize(size int) UnfenceTrackerOption {
	return func(t *UnfenceTracker) {
		if size > 0 {
			t.maxHistorySize = size
		}
	}
}

func WithWindowDuration(d time.Duration) UnfenceTrackerOption {
	return func(t *UnfenceTracker) {
		if d > 0 {
			t.windowDuration = d
		}
	}
}

func WithQuorumThreshold(threshold int) UnfenceTrackerOption {
	return func(t *UnfenceTracker) {
		if threshold > 0 {
			t.quorumThreshold = threshold
		}
	}
}

func WithClock(clock func() time.Time) UnfenceTrackerOption {
	return func(t *UnfenceTracker) {
		if clock != nil {
			t.clock = clock
		}
	}
}

func NewUnfenceTracker(opts ...UnfenceTrackerOption) *UnfenceTracker {
	t := &UnfenceTracker{
		probeHistory:    make([]ProbeRecord, 0, DefaultProbeHistorySize),
		maxHistorySize:  DefaultProbeHistorySize,
		windowDuration:  DefaultUnfenceWindowDuration,
		quorumThreshold: DefaultUnfenceQuorumThreshold,
		clock:           time.Now,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *UnfenceTracker) RecordProbe(reachable, expected int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	record := ProbeRecord{
		Timestamp:      t.clock(),
		ReachablePeers: reachable,
		ExpectedPeers:  expected,
	}

	t.probeHistory = append(t.probeHistory, record)

	if len(t.probeHistory) > t.maxHistorySize {
		t.probeHistory = t.probeHistory[1:]
	}
}

func (t *UnfenceTracker) CanAutoUnfence() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	windowStart := now.Add(-t.windowDuration)

	var recentProbes []ProbeRecord
	for _, p := range t.probeHistory {
		if p.Timestamp.After(windowStart) {
			recentProbes = append(recentProbes, p)
		}
	}

	if len(recentProbes) < t.quorumThreshold {
		return false
	}

	for _, p := range recentProbes {
		if p.ReachablePeers > 0 {
			return false
		}
	}

	return true
}

func (t *UnfenceTracker) ProbesInWindow() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	windowStart := now.Add(-t.windowDuration)

	count := 0
	for _, p := range t.probeHistory {
		if p.Timestamp.After(windowStart) {
			count++
		}
	}
	return count
}

func (t *UnfenceTracker) TotalProbes() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.probeHistory)
}

func (t *UnfenceTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.probeHistory = make([]ProbeRecord, 0, t.maxHistorySize)
}
