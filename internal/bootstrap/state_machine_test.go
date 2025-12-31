package bootstrap

import (
	"context"
	"testing"
	"time"
)

func newTestStateMachine(t *testing.T, initialState NodeState) (*StateMachine, string) {
	t.Helper()

	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	if initialState != StateUninitialized {
		meta := &ClusterMeta{
			Version:       CurrentMetaVersion,
			ClusterUUID:   "test-cluster",
			ServerID:      "node-0",
			ServerAddress: "127.0.0.1:8300",
			State:         initialState,
			BootstrapTime: time.Now(),
		}
		meta.UpdateChecksum()
		if err := metaStore.SaveMeta(meta); err != nil {
			t.Fatalf("save initial meta: %v", err)
		}
	}

	sm, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	if err := sm.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	return sm, tempDir
}

func TestStateMachine_Initialize_NoExistingMeta(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	sm, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	if err := sm.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if state := sm.CurrentState(); state != StateUninitialized {
		t.Errorf("CurrentState() = %s, want %s", state, StateUninitialized)
	}
}

func TestStateMachine_Initialize_WithExistingMeta(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "existing-cluster",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	sm, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	if err := sm.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if state := sm.CurrentState(); state != StateReady {
		t.Errorf("CurrentState() = %s, want %s", state, StateReady)
	}

	currentMeta := sm.CurrentMeta()
	if currentMeta.ClusterUUID != "existing-cluster" {
		t.Errorf("ClusterUUID = %s, want existing-cluster", currentMeta.ClusterUUID)
	}
}

func TestStateMachine_TransitionTo_Valid(t *testing.T) {
	tests := []struct {
		name   string
		from   NodeState
		to     NodeState
		reason string
	}{
		{"bootstrapping to ready", StateBootstrapping, StateReady, "bootstrap complete"},
		{"ready to fenced", StateReady, StateFenced, "fencing triggered"},
		{"recovering to ready", StateRecovering, StateReady, "recovery complete"},
		{"fenced to awaiting wipe", StateFenced, StateAwaitingWipe, "wipe required"},
		{"ready to recovering", StateReady, StateRecovering, "recovery started"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sm, _ := newTestStateMachine(t, tc.from)

			if err := sm.TransitionTo(tc.to, tc.reason); err != nil {
				t.Fatalf("TransitionTo(%s) error = %v", tc.to, err)
			}

			if state := sm.CurrentState(); state != tc.to {
				t.Errorf("CurrentState() = %s, want %s", state, tc.to)
			}
		})
	}
}

func TestStateMachine_TransitionTo_Invalid(t *testing.T) {
	tests := []struct {
		name string
		from NodeState
		to   NodeState
	}{
		{"ready to bootstrapping", StateReady, StateBootstrapping},
		{"recovering to uninitialized", StateRecovering, StateUninitialized},
		{"fenced to recovering", StateFenced, StateRecovering},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sm, _ := newTestStateMachine(t, tc.from)

			err := sm.TransitionTo(tc.to, "invalid transition")
			if err == nil {
				t.Fatalf("TransitionTo(%s) should have failed", tc.to)
			}

			var transitionErr *StateTransitionError
			if !errorAs(err, &transitionErr) {
				t.Errorf("error type = %T, want *StateTransitionError", err)
			}
		})
	}
}

func TestStateMachine_TransitionTo_Persists(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "persist-test",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateBootstrapping,
		BootstrapTime: time.Now(),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	sm, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}
	if err := sm.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if err := sm.TransitionTo(StateReady, "bootstrap complete"); err != nil {
		t.Fatalf("TransitionTo: %v", err)
	}

	loadedMeta, err := metaStore.LoadMeta()
	if err != nil {
		t.Fatalf("LoadMeta: %v", err)
	}

	if loadedMeta.State != StateReady {
		t.Errorf("persisted State = %s, want %s", loadedMeta.State, StateReady)
	}
}

func TestStateMachine_OnTransition(t *testing.T) {
	sm, _ := newTestStateMachine(t, StateBootstrapping)

	var callbackInvoked bool
	var capturedFrom, capturedTo NodeState

	sm.OnTransition(func(from, to NodeState, meta *ClusterMeta) {
		callbackInvoked = true
		capturedFrom = from
		capturedTo = to
	})

	if err := sm.TransitionTo(StateReady, "test"); err != nil {
		t.Fatalf("TransitionTo: %v", err)
	}

	if !callbackInvoked {
		t.Error("callback was not invoked")
	}
	if capturedFrom != StateBootstrapping {
		t.Errorf("callback from = %s, want %s", capturedFrom, StateBootstrapping)
	}
	if capturedTo != StateReady {
		t.Errorf("callback to = %s, want %s", capturedTo, StateReady)
	}
}

func TestStateMachine_CurrentMeta_ReturnsCopy(t *testing.T) {
	sm, _ := newTestStateMachine(t, StateReady)

	meta1 := sm.CurrentMeta()
	meta2 := sm.CurrentMeta()

	if meta1 == meta2 {
		t.Error("CurrentMeta() should return copies, not the same pointer")
	}

	meta1.ClusterUUID = "modified"
	meta3 := sm.CurrentMeta()
	if meta3.ClusterUUID == "modified" {
		t.Error("modifying returned meta should not affect internal state")
	}
}

func TestStateMachine_HandleStaleNode_NilGuard(t *testing.T) {
	sm, _ := newTestStateMachine(t, StateReady)

	err := sm.HandleStaleNode(context.Background())
	if err != nil {
		t.Errorf("HandleStaleNode with nil guard should not error, got: %v", err)
	}
}

func TestStateMachine_HandleStaleNode_Safe(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "safe-cluster",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	fm := NewFencingManager(
		&BootstrapConfig{ExpectedNodes: 3},
		tokenStore,
		epochStore,
		secrets,
		nil,
	)
	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager: %v", err)
	}

	prober := NewMockPeerProber()

	staleGuard := NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:         &BootstrapConfig{ExpectedNodes: 3},
		MetaStore:      metaStore,
		FencingManager: fm,
		UnfenceTracker: NewUnfenceTracker(),
		Prober:         prober,
		RaftConfigFunc: func() ([]VoterInfo, error) { return voters, nil },
	})

	sm, err := NewStateMachine(StateMachineDeps{
		MetaStore:      metaStore,
		FencingManager: fm,
		StaleGuard:     staleGuard,
		UnfenceTracker: NewUnfenceTracker(),
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}
	if err := sm.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if err := sm.HandleStaleNode(context.Background()); err != nil {
		t.Fatalf("HandleStaleNode: %v", err)
	}

	if state := sm.CurrentState(); state != StateReady {
		t.Errorf("state = %s, want %s (should remain safe)", state, StateReady)
	}
}

func TestStateMachine_HandleStaleNode_SplitBrain(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}

	expectedPeers := []VoterInfo{
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	fm := NewFencingManager(
		&BootstrapConfig{ExpectedNodes: 3},
		tokenStore,
		epochStore,
		secrets,
		nil,
	)
	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager: %v", err)
	}

	prober := NewMockPeerProber()
	prober.SetReachable("127.0.0.1:8301", "node-1", "cluster-2", 1)

	staleGuard := NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:            &BootstrapConfig{ExpectedNodes: 3},
		MetaStore:         metaStore,
		FencingManager:    fm,
		UnfenceTracker:    NewUnfenceTracker(),
		Prober:            prober,
		RaftConfigFunc:    func() ([]VoterInfo, error) { return voters, nil },
		ExpectedPeersFunc: func() []VoterInfo { return expectedPeers },
	})

	sm, err := NewStateMachine(StateMachineDeps{
		MetaStore:      metaStore,
		FencingManager: fm,
		StaleGuard:     staleGuard,
		UnfenceTracker: NewUnfenceTracker(),
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}
	if err := sm.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if err := sm.HandleStaleNode(context.Background()); err != nil {
		t.Fatalf("HandleStaleNode: %v", err)
	}

	if state := sm.CurrentState(); state != StateFenced {
		t.Errorf("state = %s, want %s (split-brain should fence)", state, StateFenced)
	}
}

func TestStateMachine_HandleStaleNode_Stale(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}

	expectedPeers := []VoterInfo{
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	fm := NewFencingManager(
		&BootstrapConfig{ExpectedNodes: 3},
		tokenStore,
		epochStore,
		secrets,
		nil,
	)
	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager: %v", err)
	}

	prober := NewMockPeerProber()
	prober.SetReachable("127.0.0.1:8301", "node-1", "cluster-1", 10)

	staleGuard := NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:            &BootstrapConfig{ExpectedNodes: 3},
		MetaStore:         metaStore,
		FencingManager:    fm,
		UnfenceTracker:    NewUnfenceTracker(),
		Prober:            prober,
		RaftConfigFunc:    func() ([]VoterInfo, error) { return voters, nil },
		ExpectedPeersFunc: func() []VoterInfo { return expectedPeers },
	})

	sm, err := NewStateMachine(StateMachineDeps{
		MetaStore:      metaStore,
		FencingManager: fm,
		StaleGuard:     staleGuard,
		UnfenceTracker: NewUnfenceTracker(),
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}
	if err := sm.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if err := sm.HandleStaleNode(context.Background()); err != nil {
		t.Fatalf("HandleStaleNode: %v", err)
	}

	state := sm.CurrentState()
	if state != StateFenced && state != StateAwaitingWipe {
		t.Errorf("state = %s, want %s or %s (stale should transition)", state, StateFenced, StateAwaitingWipe)
	}
}

func TestStateMachine_SetMeta(t *testing.T) {
	sm, _ := newTestStateMachine(t, StateUninitialized)

	newMeta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "set-meta-test",
		ServerID:      "node-99",
		ServerAddress: "127.0.0.1:9999",
		State:         StateReady,
		BootstrapTime: time.Now(),
	}

	sm.SetMeta(newMeta)

	if state := sm.CurrentState(); state != StateReady {
		t.Errorf("CurrentState() = %s, want %s", state, StateReady)
	}

	currentMeta := sm.CurrentMeta()
	if currentMeta.ServerID != "node-99" {
		t.Errorf("ServerID = %s, want node-99", currentMeta.ServerID)
	}
}

func errorAs(err error, target interface{}) bool {
	if err == nil {
		return false
	}
	switch t := target.(type) {
	case **StateTransitionError:
		if e, ok := err.(*StateTransitionError); ok {
			*t = e
			return true
		}
	}
	return false
}
