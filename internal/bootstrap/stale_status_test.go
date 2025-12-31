package bootstrap

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestStaleStatus_String(t *testing.T) {
	tests := []struct {
		status   StaleStatus
		expected string
	}{
		{StaleStatusUnknown, "unknown"},
		{StaleStatusSafe, "safe"},
		{StaleStatusStale, "stale"},
		{StaleStatusQuestionable, "questionable"},
		{StaleStatusSplitBrain, "split_brain"},
		{StaleStatusSafeToUnfence, "safe_to_unfence"},
		{StaleStatus(99), "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			if got := tc.status.String(); got != tc.expected {
				t.Errorf("String() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestStaleStatus_RequiresAction(t *testing.T) {
	tests := []struct {
		status   StaleStatus
		expected bool
	}{
		{StaleStatusUnknown, false},
		{StaleStatusSafe, false},
		{StaleStatusStale, true},
		{StaleStatusQuestionable, false},
		{StaleStatusSplitBrain, true},
		{StaleStatusSafeToUnfence, true},
	}

	for _, tc := range tests {
		t.Run(tc.status.String(), func(t *testing.T) {
			if got := tc.status.RequiresAction(); got != tc.expected {
				t.Errorf("RequiresAction() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestStaleStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		status   StaleStatus
		expected bool
	}{
		{StaleStatusUnknown, false},
		{StaleStatusSafe, false},
		{StaleStatusStale, true},
		{StaleStatusQuestionable, false},
		{StaleStatusSplitBrain, true},
		{StaleStatusSafeToUnfence, false},
	}

	for _, tc := range tests {
		t.Run(tc.status.String(), func(t *testing.T) {
			if got := tc.status.IsTerminal(); got != tc.expected {
				t.Errorf("IsTerminal() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestStaleCheckDetails_CountReachable(t *testing.T) {
	details := &StaleCheckDetails{
		PeerResults: []PeerProbeResult{
			{ServerID: "node-1", Reachable: true},
			{ServerID: "node-2", Reachable: false},
			{ServerID: "node-3", Reachable: true},
			{ServerID: "node-4", Reachable: false},
		},
	}

	if got := details.CountReachable(); got != 2 {
		t.Errorf("CountReachable() = %d, want 2", got)
	}
}

func TestStaleCheckDetails_HasSplitBrain(t *testing.T) {
	t.Run("no split brain", func(t *testing.T) {
		details := &StaleCheckDetails{
			LocalClusterUUID: "cluster-1",
			PeerResults: []PeerProbeResult{
				{ServerID: "node-1", Reachable: true, ClusterUUID: "cluster-1"},
				{ServerID: "node-2", Reachable: true, ClusterUUID: "cluster-1"},
			},
		}

		hasSplit, peer := details.HasSplitBrain()
		if hasSplit {
			t.Errorf("HasSplitBrain() = true, want false")
		}
		if peer != nil {
			t.Errorf("peer = %v, want nil", peer)
		}
	})

	t.Run("split brain detected", func(t *testing.T) {
		details := &StaleCheckDetails{
			LocalClusterUUID: "cluster-1",
			PeerResults: []PeerProbeResult{
				{ServerID: "node-1", Reachable: true, ClusterUUID: "cluster-1"},
				{ServerID: "node-2", Reachable: true, ClusterUUID: "cluster-2"},
			},
		}

		hasSplit, peer := details.HasSplitBrain()
		if !hasSplit {
			t.Errorf("HasSplitBrain() = false, want true")
		}
		if peer == nil || peer.ServerID != "node-2" {
			t.Errorf("peer = %v, want node-2", peer)
		}
	})

	t.Run("unreachable peer with different UUID", func(t *testing.T) {
		details := &StaleCheckDetails{
			LocalClusterUUID: "cluster-1",
			PeerResults: []PeerProbeResult{
				{ServerID: "node-1", Reachable: false, ClusterUUID: "cluster-2"},
			},
		}

		hasSplit, _ := details.HasSplitBrain()
		if hasSplit {
			t.Errorf("HasSplitBrain() = true, want false (unreachable peers don't count)")
		}
	})
}

func TestStaleCheckDetails_HasHigherEpoch(t *testing.T) {
	t.Run("no higher epoch", func(t *testing.T) {
		details := &StaleCheckDetails{
			LocalFencingEpoch: 5,
			PeerResults: []PeerProbeResult{
				{ServerID: "node-1", Reachable: true, FencingEpoch: 3},
				{ServerID: "node-2", Reachable: true, FencingEpoch: 5},
			},
		}

		hasHigher, peer := details.HasHigherEpoch()
		if hasHigher {
			t.Errorf("HasHigherEpoch() = true, want false")
		}
		if peer != nil {
			t.Errorf("peer = %v, want nil", peer)
		}
	})

	t.Run("higher epoch detected", func(t *testing.T) {
		details := &StaleCheckDetails{
			LocalFencingEpoch: 5,
			PeerResults: []PeerProbeResult{
				{ServerID: "node-1", Reachable: true, FencingEpoch: 3},
				{ServerID: "node-2", Reachable: true, FencingEpoch: 7},
			},
		}

		hasHigher, peer := details.HasHigherEpoch()
		if !hasHigher {
			t.Errorf("HasHigherEpoch() = false, want true")
		}
		if peer == nil || peer.ServerID != "node-2" {
			t.Errorf("peer = %v, want node-2", peer)
		}
	})

	t.Run("unreachable peer with higher epoch", func(t *testing.T) {
		details := &StaleCheckDetails{
			LocalFencingEpoch: 5,
			PeerResults: []PeerProbeResult{
				{ServerID: "node-1", Reachable: false, FencingEpoch: 10},
			},
		}

		hasHigher, _ := details.HasHigherEpoch()
		if hasHigher {
			t.Errorf("HasHigherEpoch() = true, want false (unreachable peers don't count)")
		}
	})
}

func TestPeerProbeResult(t *testing.T) {
	result := PeerProbeResult{
		ServerID:     raft.ServerID("node-1"),
		Address:      raft.ServerAddress("127.0.0.1:8300"),
		Reachable:    true,
		ClusterUUID:  "cluster-123",
		FencingEpoch: 42,
		ProbeTime:    time.Now(),
	}

	if result.ServerID != "node-1" {
		t.Errorf("ServerID = %s, want node-1", result.ServerID)
	}
	if !result.Reachable {
		t.Errorf("Reachable = false, want true")
	}
	if result.FencingEpoch != 42 {
		t.Errorf("FencingEpoch = %d, want 42", result.FencingEpoch)
	}
}
