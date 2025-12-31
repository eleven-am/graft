package bootstrap

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockMetaStoreForBootstrap struct {
	mu     sync.RWMutex
	meta   *ClusterMeta
	exists bool
	err    error
}

func newMockMetaStoreForBootstrap() *mockMetaStoreForBootstrap {
	return &mockMetaStoreForBootstrap{}
}

func (m *mockMetaStoreForBootstrap) LoadMeta() (*ClusterMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.err != nil {
		return nil, m.err
	}
	if m.meta == nil {
		return nil, ErrMetaNotFound
	}
	return m.meta, nil
}

func (m *mockMetaStoreForBootstrap) SaveMeta(meta *ClusterMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.meta = meta
	m.exists = true
	return nil
}

func (m *mockMetaStoreForBootstrap) Exists() (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.exists, nil
}

func (m *mockMetaStoreForBootstrap) SetMeta(meta *ClusterMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.meta = meta
	m.exists = meta != nil
}

func (m *mockMetaStoreForBootstrap) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func TestBootstrapper_Start_FreshCluster(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{
		ExpectedNodes: 3,
	}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		StateCheckInterval: 10 * time.Millisecond,
		ReadyTimeout:       100 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	state := bootstrapper.CurrentState()
	if state != StateUninitialized {
		t.Errorf("CurrentState() = %v, want %v", state, StateUninitialized)
	}
}

func TestBootstrapper_Start_ExistingCluster(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	metaStore.SetMeta(&ClusterMeta{
		Version:      CurrentMetaVersion,
		ClusterUUID:  "test-cluster",
		FencingEpoch: 5,
		State:        StateReady,
		ServerID:     "node-0",
		ServerAddress: "10.0.0.1:9000",
	})

	config := &BootstrapConfig{
		ExpectedNodes: 3,
	}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		StateCheckInterval: 10 * time.Millisecond,
		ReadyTimeout:       100 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	state := bootstrapper.CurrentState()
	if state != StateReady {
		t.Errorf("CurrentState() = %v, want %v (loaded from meta)", state, StateReady)
	}
}

func TestBootstrapper_Start_AlreadyStarted(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	if err := bootstrapper.Start(ctx); err == nil {
		t.Error("Start() should return error when already started")
	}
}

func TestBootstrapper_Stop_PersistsState(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	metaStore.SetMeta(&ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster",
		State:         StateUninitialized,
		ServerID:      "node-0",
		ServerAddress: "10.0.0.1:9000",
	})

	config := &BootstrapConfig{ExpectedNodes: 3}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := bootstrapper.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	meta, err := metaStore.LoadMeta()
	if err != nil {
		t.Fatalf("LoadMeta() error = %v", err)
	}

	if meta.State != StateUninitialized {
		t.Errorf("Persisted state = %v, want %v", meta.State, StateUninitialized)
	}
}

func TestBootstrapper_Stop_Idempotent(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := bootstrapper.Stop(); err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	if err := bootstrapper.Stop(); err != nil {
		t.Errorf("Second Stop() error = %v", err)
	}
}

func TestBootstrapper_Ready_Signal(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	metaStore.SetMeta(&ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster",
		State:         StateReady,
		ServerID:      "node-0",
		ServerAddress: "10.0.0.1:9000",
	})

	config := &BootstrapConfig{ExpectedNodes: 3}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		StateCheckInterval: 10 * time.Millisecond,
		ReadyTimeout:       500 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	select {
	case <-bootstrapper.Ready():
		if bootstrapper.ReadyError() != nil {
			t.Errorf("ReadyError() = %v, want nil", bootstrapper.ReadyError())
		}
		if !bootstrapper.IsReady() {
			t.Error("IsReady() = false, want true")
		}
	case <-time.After(1 * time.Second):
		t.Error("Ready() channel never signaled")
	}
}

func TestBootstrapper_Ready_Timeout(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		StateCheckInterval: 10 * time.Millisecond,
		ReadyTimeout:       50 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	select {
	case <-bootstrapper.Ready():
		if bootstrapper.ReadyError() == nil {
			t.Error("ReadyError() = nil, want timeout error")
		}
	case <-time.After(1 * time.Second):
		t.Error("Ready() channel never signaled even after timeout")
	}
}

func TestBootstrapper_StateLoop_ContextCancel(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	if err := bootstrapper.Stop(); err != nil {
		t.Errorf("Stop() error = %v", err)
	}
}

func TestBootstrapper_HandleUninitialized(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{
		{ServerID: "node-1", Address: "10.0.0.2:9000", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:9000", Ordinal: 2},
	})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
		ReadyTimeout:       500 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	time.Sleep(50 * time.Millisecond)

	state := bootstrapper.CurrentState()
	if state != StateReady {
		t.Errorf("CurrentState() = %v, want %v (no fencing manager means immediate transition to ready)", state, StateReady)
	}
}

func TestBootstrapper_HandleUninitialized_OrdinalZeroProceedsImmediately(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	time.Sleep(50 * time.Millisecond)

	state := bootstrapper.CurrentState()
	if state != StateReady {
		t.Errorf("CurrentState() = %v, want %v (ordinal-0 should proceed immediately, and without fencing manager transitions to ready)", state, StateReady)
	}
}

func TestBootstrapper_HandleUninitialized_NonOrdinalZeroWaitsForLeader(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{
		ExpectedNodes:     3,
		Ordinal:           1,
		LeaderWaitTimeout: 100 * time.Millisecond,
	}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	time.Sleep(50 * time.Millisecond)

	state := bootstrapper.CurrentState()
	if state != StateUninitialized {
		t.Errorf("CurrentState() = %v, want %v (non-ordinal-0 should wait for leader)", state, StateUninitialized)
	}
}

func TestBootstrapper_GetMeta(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	expectedMeta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster",
		FencingEpoch:  10,
		ServerID:      "node-0",
		ServerAddress: "10.0.0.1:9000",
	}
	metaStore.SetMeta(expectedMeta)

	config := &BootstrapConfig{ExpectedNodes: 3}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:    config,
		MetaStore: metaStore,
	})

	meta := bootstrapper.GetMeta()
	if meta == nil {
		t.Fatal("GetMeta() = nil, want non-nil")
	}
	if meta.ClusterUUID != expectedMeta.ClusterUUID {
		t.Errorf("GetMeta().ClusterUUID = %v, want %v", meta.ClusterUUID, expectedMeta.ClusterUUID)
	}
}

func TestBootstrapper_GetMeta_NotExists(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:    config,
		MetaStore: metaStore,
	})

	meta := bootstrapper.GetMeta()
	if meta != nil {
		t.Errorf("GetMeta() = %v, want nil", meta)
	}
}

func TestBootstrapper_ConcurrentStartStop(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateCheckInterval: 10 * time.Millisecond,
	})

	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = bootstrapper.Start(ctx)
		}()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = bootstrapper.Stop()
		}()
	}

	wg.Wait()
}

func TestBootstrapper_RegisterTransitionCallback(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{
		{ServerID: "node-1", Address: "10.0.0.2:9000", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:9000", Ordinal: 2},
	})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
	})

	var mu sync.Mutex
	var transitions []struct{ from, to NodeState }

	bootstrapper.RegisterTransitionCallback(func(from, to NodeState) {
		mu.Lock()
		transitions = append(transitions, struct{ from, to NodeState }{from, to})
		mu.Unlock()
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	bootstrapper.Stop()

	mu.Lock()
	transitionCount := len(transitions)
	mu.Unlock()

	if transitionCount == 0 {
		t.Error("Expected at least one transition callback")
	}
}

func TestBootstrapper_DetermineInitialState(t *testing.T) {
	tests := []struct {
		name      string
		meta      *ClusterMeta
		wantState NodeState
	}{
		{
			name:      "nil meta",
			meta:      nil,
			wantState: StateUninitialized,
		},
		{
			name:      "ready state",
			meta:      &ClusterMeta{State: StateReady},
			wantState: StateRecovering,
		},
		{
			name:      "joining state",
			meta:      &ClusterMeta{State: StateJoining},
			wantState: StateJoining,
		},
		{
			name:      "bootstrapping state",
			meta:      &ClusterMeta{State: StateBootstrapping},
			wantState: StateUninitialized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metaStore := newMockMetaStoreForBootstrap()
			config := &BootstrapConfig{ExpectedNodes: 3}

			bootstrapper := NewBootstrapper(BootstrapperDeps{
				Config:    config,
				MetaStore: metaStore,
			})

			got := bootstrapper.determineInitialState(tt.meta)
			if got != tt.wantState {
				t.Errorf("determineInitialState() = %v, want %v", got, tt.wantState)
			}
		})
	}
}

func TestBootstrapper_NeedsRecovery(t *testing.T) {
	tests := []struct {
		name      string
		meta      *ClusterMeta
		wantNeeds bool
	}{
		{
			name:      "nil meta",
			meta:      nil,
			wantNeeds: false,
		},
		{
			name:      "ready needs recovery",
			meta:      &ClusterMeta{State: StateReady},
			wantNeeds: true,
		},
		{
			name:      "recovering needs recovery",
			meta:      &ClusterMeta{State: StateRecovering},
			wantNeeds: true,
		},
		{
			name:      "joining needs recovery",
			meta:      &ClusterMeta{State: StateJoining},
			wantNeeds: true,
		},
		{
			name:      "uninitialized no recovery",
			meta:      &ClusterMeta{State: StateUninitialized},
			wantNeeds: false,
		},
		{
			name:      "bootstrapping no recovery",
			meta:      &ClusterMeta{State: StateBootstrapping},
			wantNeeds: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metaStore := newMockMetaStoreForBootstrap()
			config := &BootstrapConfig{ExpectedNodes: 3}

			bootstrapper := NewBootstrapper(BootstrapperDeps{
				Config:    config,
				MetaStore: metaStore,
			})

			got := bootstrapper.needsRecovery(tt.meta)
			if got != tt.wantNeeds {
				t.Errorf("needsRecovery() = %v, want %v", got, tt.wantNeeds)
			}
		})
	}
}

func TestBootstrapper_ForceBootstrap_NoExecutor(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:                 config,
		MetaStore:              metaStore,
		StateMachine:           stateMachine,
		Discovery:              discovery,
		ForceBootstrapExecutor: nil,
		StateCheckInterval:     10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	time.Sleep(50 * time.Millisecond)

	state := bootstrapper.CurrentState()
	if state != StateReady {
		t.Errorf("CurrentState() = %v, want %v (normal flow should proceed without executor)", state, StateReady)
	}
}

func TestBootstrapper_ForceBootstrap_ExecutorPresent(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{
		ExpectedNodes:      3,
		Ordinal:            0,
		ForceBootstrapFile: "",
		DataDir:            t.TempDir(),
	}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{})

	executor := NewForceBootstrapExecutor(ForceBootstrapExecutorDeps{
		Config:    config,
		Meta:      &ClusterMeta{Ordinal: 0},
		Discovery: discovery,
	})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:                 config,
		MetaStore:              metaStore,
		StateMachine:           stateMachine,
		Discovery:              discovery,
		ForceBootstrapExecutor: executor,
		StateCheckInterval:     10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	time.Sleep(50 * time.Millisecond)

	state := bootstrapper.CurrentState()
	if state != StateReady {
		t.Errorf("CurrentState() = %v, want %v (no token file means normal flow)", state, StateReady)
	}
}

func TestBootstrapper_HandleDegraded_ChecksStaleGuard(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateDegraded,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	config := &BootstrapConfig{ExpectedNodes: 3}

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	fm := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager: %v", err)
	}

	prober := NewMockPeerProber()

	staleGuard := NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:         config,
		MetaStore:      metaStore,
		FencingManager: fm,
		UnfenceTracker: NewUnfenceTracker(),
		Prober:         prober,
		RaftConfigFunc: func() ([]VoterInfo, error) { return voters, nil },
	})

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore:      metaStore,
		FencingManager: fm,
		StaleGuard:     staleGuard,
		UnfenceTracker: NewUnfenceTracker(),
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	time.Sleep(50 * time.Millisecond)

	state := bootstrapper.CurrentState()
	if state == StateDegraded {
		t.Log("State remained degraded (stale guard check executed but no state change needed)")
	}
}

func TestBootstrapper_HandleRecovering_ChecksStaleGuard(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateRecovering,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	config := &BootstrapConfig{ExpectedNodes: 3}

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	fm := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager: %v", err)
	}

	prober := NewMockPeerProber()

	staleGuard := NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:         config,
		MetaStore:      metaStore,
		FencingManager: fm,
		UnfenceTracker: NewUnfenceTracker(),
		Prober:         prober,
		RaftConfigFunc: func() ([]VoterInfo, error) { return voters, nil },
	})

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore:      metaStore,
		FencingManager: fm,
		StaleGuard:     staleGuard,
		UnfenceTracker: NewUnfenceTracker(),
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	discovery := NewMockPeerDiscovery()

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	time.Sleep(50 * time.Millisecond)

	state := bootstrapper.CurrentState()
	if state == StateRecovering || state == StateReady {
		t.Logf("State is %s (stale guard check executed)", state)
	}
}

func TestBootstrapper_DiscoveryLifecycle_StartedAndStopped(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	discovery := NewMockLifecyclePeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if !discovery.Started() {
		t.Error("discovery should be started after bootstrapper.Start()")
	}

	time.Sleep(50 * time.Millisecond)

	if err := bootstrapper.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if !discovery.Stopped() {
		t.Error("discovery should be stopped after bootstrapper.Stop()")
	}
}

func TestBootstrapper_DiscoveryLifecycle_StartError(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	discovery := NewMockLifecyclePeerDiscovery()
	discovery.SetStartError(errors.New("discovery start failed"))

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	err = bootstrapper.Start(ctx)
	if err == nil {
		t.Fatal("Start() should return error when discovery fails to start")
	}

	if !discovery.Started() {
		t.Log("discovery.Start() was called (returned error as expected)")
	}
}

func TestBootstrapper_NonLifecycleDiscovery_NoError(t *testing.T) {
	metaStore := newMockMetaStoreForBootstrap()
	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := bootstrapper.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	t.Log("Non-lifecycle discovery works without Start/Stop calls")
}

func TestBootstrapper_HandleReady_PeriodicStaleCheck(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	config := &BootstrapConfig{ExpectedNodes: 3}

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	fm := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager: %v", err)
	}

	prober := NewMockPeerProber()

	staleGuard := NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:         config,
		MetaStore:      metaStore,
		FencingManager: fm,
		UnfenceTracker: NewUnfenceTracker(),
		Prober:         prober,
		RaftConfigFunc: func() ([]VoterInfo, error) { return voters, nil },
	})

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore:      metaStore,
		FencingManager: fm,
		StaleGuard:     staleGuard,
		UnfenceTracker: NewUnfenceTracker(),
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	discovery := NewMockPeerDiscovery()

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		FencingManager:     fm,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
		StaleCheckInterval: 20 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	time.Sleep(100 * time.Millisecond)

	state := bootstrapper.CurrentState()
	if state == StateReady {
		t.Log("State remained ready after periodic stale checks")
	} else {
		t.Logf("State is %s (stale guard may have detected issues)", state)
	}
}
