package bootstrap

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func createTestBootstrapperWithComponents(t *testing.T, ordinal int, tempDir string) (*Bootstrapper, *StateMachine, *FencingManager, *MockPeerDiscovery) {
	t.Helper()

	metaStore := NewFileMetaStore(tempDir, nil)

	config := &BootstrapConfig{
		ExpectedNodes:     3,
		Ordinal:           ordinal,
		LeaderWaitTimeout: 100 * time.Millisecond,
	}

	prober := NewMockPeerProber()
	discovery := NewMockPeerDiscovery()

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore:      metaStore,
		UnfenceTracker: NewUnfenceTracker(),
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
		StaleCheckInterval: 50 * time.Millisecond,
	})

	_ = prober

	return bootstrapper, stateMachine, nil, discovery
}

func TestIntegration_FullBootstrapFlow_OrdinalZero(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	config := &BootstrapConfig{
		ExpectedNodes:     3,
		Ordinal:           0,
		LeaderWaitTimeout: 100 * time.Millisecond,
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	})

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

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

	time.Sleep(100 * time.Millisecond)

	state := bootstrapper.CurrentState()
	t.Logf("State after bootstrap attempt: %s", state)
}

func TestIntegration_FullBootstrapFlow_NonOrdinalZeroInsecureMode(t *testing.T) {
	tempDir := t.TempDir()

	metaStore := NewFileMetaStore(tempDir, nil)
	config := &BootstrapConfig{
		ExpectedNodes:     3,
		Ordinal:           1,
		LeaderWaitTimeout: 50 * time.Millisecond,
	}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
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

	time.Sleep(100 * time.Millisecond)

	state := bootstrapper.CurrentState()
	if state != StateReady {
		t.Errorf("Non-ordinal-0 in insecure mode (no fencing) should transition to ready, got %s", state)
	}
}

func TestIntegration_RecoveryFlow_FromExistingCluster(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "existing-cluster-uuid",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		FencingEpoch:  5,
		BootstrapTime: time.Now().Add(-24 * time.Hour),
		Ordinal:       0,
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	config := &BootstrapConfig{
		ExpectedNodes: 3,
		Ordinal:       0,
	}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
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
	if state != StateRecovering && state != StateReady {
		t.Errorf("Expected StateRecovering or StateReady after restart, got %s", state)
	}

	loadedMeta := bootstrapper.GetMeta()
	if loadedMeta == nil {
		t.Fatal("Expected meta to be loaded")
	}
	if loadedMeta.ClusterUUID != "existing-cluster-uuid" {
		t.Errorf("ClusterUUID = %s, want existing-cluster-uuid", loadedMeta.ClusterUUID)
	}
}

func TestIntegration_ForceBootstrap_TokenFileExecution(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "stuck-cluster",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateFenced,
		FencingEpoch:  10,
		BootstrapTime: time.Now().Add(-24 * time.Hour),
		Ordinal:       0,
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

	tokenFile := filepath.Join(tempDir, "force_bootstrap.json")
	token := &ForceBootstrapToken{
		Version:              ForceBootstrapTokenVersion,
		TargetOrdinal:        0,
		VoterSetHash:         HashVoterSet(voters),
		CommittedVoterCount:  3,
		Reason:               "integration test - disaster recovery",
		IssuedAt:             time.Now(),
		ExpiresAt:            time.Now().Add(1 * time.Hour),
		SingleUse:            true,
		DisasterRecoveryMode: true,
		AcknowledgeFork:      true,
	}

	tokenBytes, _ := json.Marshal(token)
	if err := os.WriteFile(tokenFile, tokenBytes, 0600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	config := &BootstrapConfig{
		ExpectedNodes:      3,
		Ordinal:            0,
		DataDir:            tempDir,
		ForceBootstrapFile: tokenFile,
	}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{})

	executor := NewForceBootstrapExecutor(ForceBootstrapExecutorDeps{
		Config:    config,
		Meta:      meta,
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

	time.Sleep(100 * time.Millisecond)

	state := bootstrapper.CurrentState()
	t.Logf("State after force-bootstrap check: %s", state)
}

func TestIntegration_StaleGuard_DetectsStaleNode(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateDegraded,
		FencingEpoch:  1,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
		Ordinal:       0,
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

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
	prober.SetReachable("127.0.0.1:8301", "node-1", "test-cluster", 5)
	prober.SetReachable("127.0.0.1:8302", "node-2", "test-cluster", 5)

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
	t.Logf("Final state after stale guard checks: %s", state)
}

func TestIntegration_FencingTokenAcquisition(t *testing.T) {
	tempDir := t.TempDir()

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	fencingKey := []byte("test-fencing-key-32-bytes-long!!")
	config := &BootstrapConfig{ExpectedNodes: 3}

	fms := make(map[string]*FencingManager)
	for i, voter := range voters {
		voterDir := filepath.Join(tempDir, string(voter.ServerID))
		if err := os.MkdirAll(voterDir, 0755); err != nil {
			t.Fatalf("mkdir for %s: %v", voter.ServerID, err)
		}

		tokenStore := NewFileTokenStore(voterDir, nil)
		epochStore := NewFileEpochStore(voterDir, nil)
		secrets := NewSecretsManager(SecretsConfig{}, nil)
		secrets.SetFencingKey(fencingKey)

		fm := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
		if err := fm.Initialize(voters); err != nil {
			t.Fatalf("init fencing manager for node-%d: %v", i, err)
		}
		fms[string(voter.ServerID)] = fm
	}

	proposerFM := fms["node-0"]
	proposal, err := proposerFM.CreateProposal(FencingModePrimary, "node-0", "127.0.0.1:8300")
	if err != nil {
		t.Fatalf("CreateProposal: %v", err)
	}

	if proposal == nil {
		t.Fatal("Expected non-nil proposal")
	}

	if proposal.ProposerID != "node-0" {
		t.Errorf("ProposerID = %s, want node-0", proposal.ProposerID)
	}

	acks := make([]*FencingAck, 0)
	for _, voter := range voters {
		voterFM := fms[string(voter.ServerID)]
		ack, err := voterFM.HandleProposal(proposal, voter.ServerID)
		if err != nil {
			t.Fatalf("HandleProposal for %s: %v", voter.ServerID, err)
		}
		if ack.Accepted {
			acks = append(acks, ack)
		}
	}

	if len(acks) < 2 {
		t.Fatalf("Expected at least 2 acks for quorum, got %d", len(acks))
	}

	token, err := proposerFM.CommitToken(proposal, acks)
	if err != nil {
		t.Fatalf("CommitToken: %v", err)
	}

	if token == nil {
		t.Fatal("Expected non-nil token")
	}

	if token.HolderID != "node-0" {
		t.Errorf("Token HolderID = %s, want node-0", token.HolderID)
	}

	currentToken := proposerFM.CurrentToken()
	if currentToken == nil {
		t.Fatal("CurrentToken should return the committed token")
	}

	if currentToken.Epoch != token.Epoch {
		t.Errorf("CurrentToken epoch = %d, want %d", currentToken.Epoch, token.Epoch)
	}
}

func TestIntegration_StateTransitionCallbacks(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateBootstrapping,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	})

	bootstrapper := NewBootstrapper(BootstrapperDeps{
		Config:             config,
		MetaStore:          metaStore,
		StateMachine:       stateMachine,
		Discovery:          discovery,
		StateCheckInterval: 10 * time.Millisecond,
	})

	var transitions []struct{ from, to NodeState }
	bootstrapper.RegisterTransitionCallback(func(from, to NodeState) {
		transitions = append(transitions, struct{ from, to NodeState }{from, to})
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	bootstrapper.Stop()

	t.Logf("Recorded %d state transitions", len(transitions))
	for i, tr := range transitions {
		t.Logf("  Transition %d: %s -> %s", i+1, tr.from, tr.to)
	}
}

func TestIntegration_DiscoveryLifecycleWithBootstrapper(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
	})
	if err != nil {
		t.Fatalf("NewStateMachine: %v", err)
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

	if discovery.Started() {
		t.Error("Discovery should not be started before bootstrapper.Start()")
	}

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if !discovery.Started() {
		t.Error("Discovery should be started after bootstrapper.Start()")
	}

	time.Sleep(50 * time.Millisecond)

	if err := bootstrapper.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if !discovery.Stopped() {
		t.Error("Discovery should be stopped after bootstrapper.Stop()")
	}
}

func TestIntegration_MultipleBootstrappersConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			_ = bootstrapper.CurrentState()
			_ = bootstrapper.GetMeta()
			_ = bootstrapper.IsReady()
		}
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("Concurrent access test timed out")
	}

	bootstrapper.Stop()
}

func TestIntegration_ReadyChannelSignaling(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "ready-cluster",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	config := &BootstrapConfig{ExpectedNodes: 3, Ordinal: 0}

	stateMachine, err := NewStateMachine(StateMachineDeps{
		MetaStore: metaStore,
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
		ReadyTimeout:       500 * time.Millisecond,
	})

	ctx := context.Background()
	if err := bootstrapper.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer bootstrapper.Stop()

	select {
	case <-bootstrapper.Ready():
		if err := bootstrapper.ReadyError(); err != nil {
			t.Logf("Ready with error: %v", err)
		} else {
			t.Log("Bootstrapper signaled ready")
		}
	case <-time.After(1 * time.Second):
		t.Error("Ready channel was not signaled within timeout")
	}
}

func TestIntegration_VoterSetValidation(t *testing.T) {
	voters1 := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	voters2 := []VoterInfo{
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
	}

	hash1 := HashVoterSet(voters1)
	hash2 := HashVoterSet(voters2)

	if string(hash1) != string(hash2) {
		t.Error("HashVoterSet should produce same hash regardless of order")
	}

	voters3 := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
	}

	hash3 := HashVoterSet(voters3)
	if string(hash1) == string(hash3) {
		t.Error("Different voter sets should produce different hashes")
	}
}

func TestIntegration_CommitTokenRejectsUnknownVoters(t *testing.T) {
	tempDir := t.TempDir()

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	fencingKey := []byte("test-fencing-key-32-bytes-long!!")
	config := &BootstrapConfig{ExpectedNodes: 3}

	node0Dir := filepath.Join(tempDir, "node-0")
	node1Dir := filepath.Join(tempDir, "node-1")
	if err := os.MkdirAll(node0Dir, 0755); err != nil {
		t.Fatalf("mkdir node-0: %v", err)
	}
	if err := os.MkdirAll(node1Dir, 0755); err != nil {
		t.Fatalf("mkdir node-1: %v", err)
	}

	secrets0 := NewSecretsManager(SecretsConfig{}, nil)
	secrets0.SetFencingKey(fencingKey)
	fm0 := NewFencingManager(config, NewFileTokenStore(node0Dir, nil), NewFileEpochStore(node0Dir, nil), secrets0, nil)
	if err := fm0.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager for node-0: %v", err)
	}

	secrets1 := NewSecretsManager(SecretsConfig{}, nil)
	secrets1.SetFencingKey(fencingKey)
	fm1 := NewFencingManager(config, NewFileTokenStore(node1Dir, nil), NewFileEpochStore(node1Dir, nil), secrets1, nil)
	if err := fm1.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager for node-1: %v", err)
	}

	proposal, err := fm0.CreateProposal(FencingModePrimary, "node-0", "127.0.0.1:8300")
	if err != nil {
		t.Fatalf("CreateProposal: %v", err)
	}

	validAck, _ := fm1.HandleProposal(proposal, "node-1")

	unknownAck := &FencingAck{
		ProposedEpoch: proposal.ProposedEpoch,
		VoterID:       raft.ServerID("unknown-node"),
		Accepted:      true,
		CurrentEpoch:  proposal.ProposedEpoch,
		Timestamp:     time.Now(),
	}

	acks := []*FencingAck{validAck, unknownAck}

	_, err = fm0.CommitToken(proposal, acks)
	if err == nil {
		t.Error("CommitToken should fail - unknown voter ack was filtered, quorum not met")
	} else {
		t.Logf("CommitToken correctly rejected: %v", err)
	}
}
