package bootstrap

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestNodeState_String(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{StateUninitialized, "uninitialized"},
		{StateBootstrapping, "bootstrapping"},
		{StateJoining, "joining"},
		{StateReady, "ready"},
		{StateRecovering, "recovering"},
		{StateDegraded, "degraded"},
		{StateFenced, "fenced"},
		{StateAwaitingWipe, "awaiting_wipe"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.state.String(); got != tt.expected {
				t.Errorf("NodeState.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestNodeState_IsTerminal(t *testing.T) {
	tests := []struct {
		state    NodeState
		terminal bool
	}{
		{StateUninitialized, false},
		{StateBootstrapping, false},
		{StateJoining, false},
		{StateReady, false},
		{StateRecovering, false},
		{StateDegraded, false},
		{StateFenced, true},
		{StateAwaitingWipe, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			if got := tt.state.IsTerminal(); got != tt.terminal {
				t.Errorf("NodeState.IsTerminal() = %v, want %v", got, tt.terminal)
			}
		})
	}
}

func TestNodeState_CanTransitionTo(t *testing.T) {
	validTransitions := map[NodeState][]NodeState{
		StateUninitialized: {StateBootstrapping, StateJoining},
		StateBootstrapping: {StateReady, StateFenced},
		StateJoining:       {StateReady, StateFenced},
		StateReady:         {StateRecovering, StateDegraded, StateFenced},
		StateRecovering:    {StateReady, StateFenced},
		StateDegraded:      {StateReady, StateRecovering, StateFenced},
		StateFenced:        {StateAwaitingWipe},
		StateAwaitingWipe:  {StateUninitialized},
	}

	allStates := []NodeState{
		StateUninitialized, StateBootstrapping, StateJoining,
		StateReady, StateRecovering, StateDegraded,
		StateFenced, StateAwaitingWipe,
	}

	for from, validTargets := range validTransitions {
		validSet := make(map[NodeState]bool)
		for _, target := range validTargets {
			validSet[target] = true
		}

		for _, to := range allStates {
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				expected := validSet[to]
				got := from.CanTransitionTo(to)
				if got != expected {
					t.Errorf("CanTransitionTo(%s -> %s) = %v, want %v", from, to, got, expected)
				}
			})
		}
	}
}

func TestClusterMeta_ComputeChecksum(t *testing.T) {
	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster-uuid",
		FencingToken:  1,
		FencingEpoch:  1,
		ServerID:      raft.ServerID("flow-0"),
		ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
		Ordinal:       0,
		State:         StateReady,
	}

	checksum1 := meta.ComputeChecksum()
	checksum2 := meta.ComputeChecksum()

	if checksum1 != checksum2 {
		t.Errorf("ComputeChecksum() not deterministic: %08x != %08x", checksum1, checksum2)
	}

	meta.FencingEpoch = 2
	checksum3 := meta.ComputeChecksum()

	if checksum1 == checksum3 {
		t.Error("ComputeChecksum() should change when fields change")
	}
}

func TestClusterMeta_Validate(t *testing.T) {
	validMeta := func() *ClusterMeta {
		return &ClusterMeta{
			Version:       CurrentMetaVersion,
			ClusterUUID:   "test-cluster-uuid",
			ServerID:      raft.ServerID("flow-0"),
			ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
			Ordinal:       0,
			State:         StateReady,
		}
	}

	t.Run("valid meta", func(t *testing.T) {
		meta := validMeta()
		if err := meta.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("missing version", func(t *testing.T) {
		meta := validMeta()
		meta.Version = 0
		if err := meta.Validate(); err == nil {
			t.Error("Validate() should fail for missing version")
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		meta := validMeta()
		meta.Version = CurrentMetaVersion + 1
		if err := meta.Validate(); err == nil {
			t.Error("Validate() should fail for unsupported version")
		}
	})

	t.Run("missing cluster_uuid in ready state", func(t *testing.T) {
		meta := validMeta()
		meta.ClusterUUID = ""
		meta.State = StateReady
		if err := meta.Validate(); err == nil {
			t.Error("Validate() should fail for missing cluster_uuid in ready state")
		}
	})

	t.Run("empty cluster_uuid allowed in uninitialized state", func(t *testing.T) {
		meta := validMeta()
		meta.ClusterUUID = ""
		meta.State = StateUninitialized
		if err := meta.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil for empty cluster_uuid in uninitialized state", err)
		}
	})

	t.Run("missing server_id", func(t *testing.T) {
		meta := validMeta()
		meta.ServerID = ""
		if err := meta.Validate(); err == nil {
			t.Error("Validate() should fail for missing server_id")
		}
	})

	t.Run("missing server_address", func(t *testing.T) {
		meta := validMeta()
		meta.ServerAddress = ""
		if err := meta.Validate(); err == nil {
			t.Error("Validate() should fail for missing server_address")
		}
	})

	t.Run("negative ordinal", func(t *testing.T) {
		meta := validMeta()
		meta.Ordinal = -1
		if err := meta.Validate(); err == nil {
			t.Error("Validate() should fail for negative ordinal")
		}
	})

	t.Run("missing state", func(t *testing.T) {
		meta := validMeta()
		meta.State = ""
		if err := meta.Validate(); err == nil {
			t.Error("Validate() should fail for missing state")
		}
	})
}

func TestClusterMeta_VerifyChecksum(t *testing.T) {
	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster-uuid",
		FencingToken:  1,
		FencingEpoch:  1,
		ServerID:      raft.ServerID("flow-0"),
		ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
		Ordinal:       0,
		State:         StateReady,
		BootstrapTime: time.Now(),
	}

	meta.UpdateChecksum()

	if !meta.VerifyChecksum() {
		t.Error("VerifyChecksum() should return true after UpdateChecksum()")
	}

	meta.Checksum = meta.Checksum + 1

	if meta.VerifyChecksum() {
		t.Error("VerifyChecksum() should return false for invalid checksum")
	}
}

func TestVoterInfo_Validate(t *testing.T) {
	t.Run("valid voter", func(t *testing.T) {
		v := &VoterInfo{
			ServerID: raft.ServerID("flow-0"),
			Address:  raft.ServerAddress("flow-0.flow.svc:7946"),
			Ordinal:  0,
		}
		if err := v.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("missing server_id", func(t *testing.T) {
		v := &VoterInfo{
			Address: raft.ServerAddress("flow-0.flow.svc:7946"),
			Ordinal: 0,
		}
		if err := v.Validate(); err == nil {
			t.Error("Validate() should fail for missing server_id")
		}
	})

	t.Run("missing address", func(t *testing.T) {
		v := &VoterInfo{
			ServerID: raft.ServerID("flow-0"),
			Ordinal:  0,
		}
		if err := v.Validate(); err == nil {
			t.Error("Validate() should fail for missing address")
		}
	})

	t.Run("negative ordinal", func(t *testing.T) {
		v := &VoterInfo{
			ServerID: raft.ServerID("flow-0"),
			Address:  raft.ServerAddress("flow-0.flow.svc:7946"),
			Ordinal:  -1,
		}
		if err := v.Validate(); err == nil {
			t.Error("Validate() should fail for negative ordinal")
		}
	})
}
