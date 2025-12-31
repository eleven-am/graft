package bootstrap

import (
	"testing"
	"time"
)

func TestUnfenceTracker_RecordProbe(t *testing.T) {
	tracker := NewUnfenceTracker()

	tracker.RecordProbe(0, 3)
	tracker.RecordProbe(1, 3)
	tracker.RecordProbe(2, 3)

	if tracker.TotalProbes() != 3 {
		t.Errorf("TotalProbes() = %d, want 3", tracker.TotalProbes())
	}
}

func TestUnfenceTracker_CanAutoUnfence_InsufficientProbes(t *testing.T) {
	tracker := NewUnfenceTracker(WithQuorumThreshold(10))

	for i := 0; i < 5; i++ {
		tracker.RecordProbe(0, 3)
	}

	if tracker.CanAutoUnfence() {
		t.Error("CanAutoUnfence() = true, want false (insufficient probes)")
	}
}

func TestUnfenceTracker_CanAutoUnfence_ReachablePeersPresent(t *testing.T) {
	now := time.Now()
	currentTime := now
	tracker := NewUnfenceTracker(
		WithQuorumThreshold(5),
		WithClock(func() time.Time { return currentTime }),
	)

	for i := 0; i < 4; i++ {
		tracker.RecordProbe(0, 3)
		currentTime = currentTime.Add(10 * time.Second)
	}

	tracker.RecordProbe(1, 3)
	currentTime = currentTime.Add(10 * time.Second)

	for i := 0; i < 5; i++ {
		tracker.RecordProbe(0, 3)
		currentTime = currentTime.Add(10 * time.Second)
	}

	if tracker.CanAutoUnfence() {
		t.Error("CanAutoUnfence() = true, want false (reachable peers detected in window)")
	}
}

func TestUnfenceTracker_CanAutoUnfence_Success(t *testing.T) {
	now := time.Now()
	currentTime := now
	tracker := NewUnfenceTracker(
		WithQuorumThreshold(10),
		WithWindowDuration(5*time.Minute),
		WithClock(func() time.Time { return currentTime }),
	)

	for i := 0; i < 12; i++ {
		tracker.RecordProbe(0, 3)
		currentTime = currentTime.Add(20 * time.Second)
	}

	if !tracker.CanAutoUnfence() {
		t.Error("CanAutoUnfence() = false, want true (all probes show 0 reachable)")
	}
}

func TestUnfenceTracker_WindowExpiry(t *testing.T) {
	now := time.Now()
	currentTime := now
	tracker := NewUnfenceTracker(
		WithQuorumThreshold(5),
		WithWindowDuration(2*time.Minute),
		WithClock(func() time.Time { return currentTime }),
	)

	for i := 0; i < 10; i++ {
		tracker.RecordProbe(0, 3)
		currentTime = currentTime.Add(10 * time.Second)
	}

	currentTime = currentTime.Add(5 * time.Minute)

	probesInWindow := tracker.ProbesInWindow()
	if probesInWindow != 0 {
		t.Errorf("ProbesInWindow() = %d, want 0 (all expired)", probesInWindow)
	}

	if tracker.CanAutoUnfence() {
		t.Error("CanAutoUnfence() = true, want false (probes expired)")
	}
}

func TestUnfenceTracker_CircularBuffer(t *testing.T) {
	tracker := NewUnfenceTracker(WithHistorySize(5))

	for i := 0; i < 10; i++ {
		tracker.RecordProbe(i, 3)
	}

	if tracker.TotalProbes() != 5 {
		t.Errorf("TotalProbes() = %d, want 5 (buffer should wrap)", tracker.TotalProbes())
	}
}

func TestUnfenceTracker_Reset(t *testing.T) {
	tracker := NewUnfenceTracker()

	for i := 0; i < 5; i++ {
		tracker.RecordProbe(0, 3)
	}

	tracker.Reset()

	if tracker.TotalProbes() != 0 {
		t.Errorf("TotalProbes() = %d after reset, want 0", tracker.TotalProbes())
	}
}

func TestUnfenceTracker_ProbesInWindow(t *testing.T) {
	now := time.Now()
	currentTime := now
	tracker := NewUnfenceTracker(
		WithWindowDuration(1*time.Minute),
		WithClock(func() time.Time { return currentTime }),
	)

	for i := 0; i < 3; i++ {
		tracker.RecordProbe(0, 3)
		currentTime = currentTime.Add(10 * time.Second)
	}

	currentTime = currentTime.Add(30 * time.Second)

	for i := 0; i < 2; i++ {
		tracker.RecordProbe(0, 3)
		currentTime = currentTime.Add(10 * time.Second)
	}

	probesInWindow := tracker.ProbesInWindow()
	if probesInWindow != 2 {
		t.Errorf("ProbesInWindow() = %d, want 2 (first 3 should be expired)", probesInWindow)
	}
}

func TestUnfenceTracker_Options(t *testing.T) {
	t.Run("WithHistorySize", func(t *testing.T) {
		tracker := NewUnfenceTracker(WithHistorySize(100))
		for i := 0; i < 100; i++ {
			tracker.RecordProbe(0, 3)
		}
		if tracker.TotalProbes() != 100 {
			t.Errorf("TotalProbes() = %d, want 100", tracker.TotalProbes())
		}
		tracker.RecordProbe(0, 3)
		if tracker.TotalProbes() != 100 {
			t.Errorf("TotalProbes() after overflow = %d, want 100", tracker.TotalProbes())
		}
	})

	t.Run("WithWindowDuration", func(t *testing.T) {
		now := time.Now()
		currentTime := now
		tracker := NewUnfenceTracker(
			WithWindowDuration(30*time.Second),
			WithClock(func() time.Time { return currentTime }),
		)

		tracker.RecordProbe(0, 3)
		currentTime = currentTime.Add(35 * time.Second)

		if tracker.ProbesInWindow() != 0 {
			t.Error("probe should be outside 30s window")
		}
	})

	t.Run("WithQuorumThreshold", func(t *testing.T) {
		now := time.Now()
		currentTime := now
		tracker := NewUnfenceTracker(
			WithQuorumThreshold(3),
			WithClock(func() time.Time { return currentTime }),
		)

		for i := 0; i < 3; i++ {
			tracker.RecordProbe(0, 3)
			currentTime = currentTime.Add(10 * time.Second)
		}

		if !tracker.CanAutoUnfence() {
			t.Error("CanAutoUnfence() = false, want true with threshold of 3")
		}
	})

	t.Run("invalid options ignored", func(t *testing.T) {
		tracker := NewUnfenceTracker(
			WithHistorySize(0),
			WithWindowDuration(0),
			WithQuorumThreshold(0),
			WithClock(nil),
		)

		if tracker.TotalProbes() != 0 {
			t.Error("tracker should still work with invalid options")
		}
	})
}
