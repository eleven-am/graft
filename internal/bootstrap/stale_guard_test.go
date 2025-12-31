package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func newTestStaleGuard(t *testing.T, voters []VoterInfo, meta *ClusterMeta, prober *MockPeerProber) *StaleSingleNodeGuard {
	return newTestStaleGuardWithPeers(t, voters, nil, meta, prober)
}

func newTestStaleGuardWithPeers(t *testing.T, voters []VoterInfo, expectedPeers []VoterInfo, meta *ClusterMeta, prober *MockPeerProber) *StaleSingleNodeGuard {
	t.Helper()

	tempDir := t.TempDir()

	metaStore := NewFileMetaStore(tempDir, nil)
	if meta != nil {
		meta.UpdateChecksum()
		if err := metaStore.SaveMeta(meta); err != nil {
			t.Fatalf("save meta: %v", err)
		}
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

	unfenceTracker := NewUnfenceTracker()

	var expectedPeersFunc func() []VoterInfo
	if expectedPeers != nil {
		expectedPeersFunc = func() []VoterInfo { return expectedPeers }
	}

	return NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:            &BootstrapConfig{ExpectedNodes: 3},
		MetaStore:         metaStore,
		FencingManager:    fm,
		UnfenceTracker:    unfenceTracker,
		Prober:            prober,
		RaftConfigFunc:    func() ([]VoterInfo, error) { return voters, nil },
		ExpectedPeersFunc: expectedPeersFunc,
	})
}

func TestStaleGuard_Check_FirstBoot_NoMeta(t *testing.T) {
	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)

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
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}
	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager: %v", err)
	}

	prober := NewMockPeerProber()

	guard := NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:         &BootstrapConfig{ExpectedNodes: 3},
		MetaStore:      metaStore,
		FencingManager: fm,
		UnfenceTracker: NewUnfenceTracker(),
		Prober:         prober,
		RaftConfigFunc: func() ([]VoterInfo, error) { return voters, nil },
	})

	status, details, err := guard.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	if status != StaleStatusSafe {
		t.Errorf("status = %s, want %s (first boot should be safe)", status, StaleStatusSafe)
	}

	if details.Reason != "no cluster metadata (first boot)" {
		t.Errorf("reason = %s, want 'no cluster metadata (first boot)'", details.Reason)
	}
}

func TestStaleGuard_Check_NotSingleNode(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}

	prober := NewMockPeerProber()
	guard := newTestStaleGuard(t, voters, meta, prober)

	status, details, err := guard.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	if status != StaleStatusSafe {
		t.Errorf("status = %s, want %s", status, StaleStatusSafe)
	}

	if details.PersistedVoters != 3 {
		t.Errorf("PersistedVoters = %d, want 3", details.PersistedVoters)
	}
}

func TestStaleGuard_Check_SingleNodeExpected(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}

	prober := NewMockPeerProber()

	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}

	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	fm := NewFencingManager(
		&BootstrapConfig{ExpectedNodes: 1},
		tokenStore,
		epochStore,
		secrets,
		nil,
	)
	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("init fencing manager: %v", err)
	}

	guard := NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:         &BootstrapConfig{ExpectedNodes: 1},
		MetaStore:      metaStore,
		FencingManager: fm,
		UnfenceTracker: NewUnfenceTracker(),
		Prober:         prober,
		RaftConfigFunc: func() ([]VoterInfo, error) { return voters, nil },
	})

	status, _, err := guard.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	if status != StaleStatusSafe {
		t.Errorf("status = %s, want %s (single node expected)", status, StaleStatusSafe)
	}
}

func TestStaleGuard_Check_NoPeersReachable_WithinSafetyWindow(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-10 * time.Second),
	}

	prober := NewMockPeerProber()
	prober.SetUnreachable("127.0.0.1:8301", "node-1", context.DeadlineExceeded)
	prober.SetUnreachable("127.0.0.1:8302", "node-2", context.DeadlineExceeded)

	guard := newTestStaleGuard(t, voters, meta, prober)

	status, details, err := guard.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	if status != StaleStatusSafe {
		t.Errorf("status = %s, want %s (within safety window)", status, StaleStatusSafe)
	}

	if details.ReachablePeers != 0 {
		t.Errorf("ReachablePeers = %d, want 0", details.ReachablePeers)
	}
}

func TestStaleGuard_Check_NoPeersReachable_Questionable(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}

	prober := NewMockPeerProber()

	guard := newTestStaleGuard(t, voters, meta, prober)

	status, _, err := guard.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	if status != StaleStatusQuestionable {
		t.Errorf("status = %s, want %s (awaiting probes)", status, StaleStatusQuestionable)
	}
}

func TestStaleGuard_Check_SplitBrain(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}

	expectedPeers := []VoterInfo{
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}

	prober := NewMockPeerProber()
	prober.SetReachable("127.0.0.1:8301", "node-1", "cluster-2", 1)

	guard := newTestStaleGuardWithPeers(t, voters, expectedPeers, meta, prober)

	status, details, err := guard.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	if status != StaleStatusSplitBrain {
		t.Errorf("status = %s, want %s", status, StaleStatusSplitBrain)
	}

	if details.Reason == "" {
		t.Error("expected reason to be set for split-brain")
	}
}

func TestStaleGuard_Check_StaleEpoch(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}

	expectedPeers := []VoterInfo{
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}

	prober := NewMockPeerProber()
	prober.SetReachable("127.0.0.1:8301", "node-1", "cluster-1", 10)

	guard := newTestStaleGuardWithPeers(t, voters, expectedPeers, meta, prober)

	status, _, err := guard.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	if status != StaleStatusStale {
		t.Errorf("status = %s, want %s", status, StaleStatusStale)
	}
}

func TestStaleGuard_Check_QuorumReachable(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}

	expectedPeers := []VoterInfo{
		{ServerID: "node-1", Address: "127.0.0.1:8301", Ordinal: 1},
		{ServerID: "node-2", Address: "127.0.0.1:8302", Ordinal: 2},
	}

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}

	prober := NewMockPeerProber()
	prober.SetReachable("127.0.0.1:8301", "node-1", "cluster-1", 0)

	guard := newTestStaleGuardWithPeers(t, voters, expectedPeers, meta, prober)

	status, details, err := guard.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	if status != StaleStatusSafe {
		t.Errorf("status = %s, want %s (quorum reachable)", status, StaleStatusSafe)
	}

	if details.ReachablePeers != 1 {
		t.Errorf("ReachablePeers = %d, want 1", details.ReachablePeers)
	}
}

func TestStaleGuard_Check_CanUnfence(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "127.0.0.1:8300", Ordinal: 0},
	}

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "cluster-1",
		ServerID:      "node-0",
		ServerAddress: "127.0.0.1:8300",
		State:         StateReady,
		BootstrapTime: time.Now().Add(-1 * time.Hour),
	}

	tempDir := t.TempDir()
	metaStore := NewFileMetaStore(tempDir, nil)
	meta.UpdateChecksum()
	if err := metaStore.SaveMeta(meta); err != nil {
		t.Fatalf("save meta: %v", err)
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

	now := time.Now()
	currentTime := now
	unfenceTracker := NewUnfenceTracker(
		WithQuorumThreshold(10),
		WithWindowDuration(5*time.Minute),
		WithClock(func() time.Time { return currentTime }),
	)

	for i := 0; i < 12; i++ {
		unfenceTracker.RecordProbe(0, 3)
		currentTime = currentTime.Add(20 * time.Second)
	}

	prober := NewMockPeerProber()

	guard := NewStaleSingleNodeGuard(StaleGuardDeps{
		Config:         &BootstrapConfig{ExpectedNodes: 3},
		MetaStore:      metaStore,
		FencingManager: fm,
		UnfenceTracker: unfenceTracker,
		Prober:         prober,
		RaftConfigFunc: func() ([]VoterInfo, error) { return voters, nil },
		Clock:          func() time.Time { return currentTime },
	})

	status, _, err := guard.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	if status != StaleStatusSafeToUnfence {
		t.Errorf("status = %s, want %s", status, StaleStatusSafeToUnfence)
	}
}

func TestMockPeerProber(t *testing.T) {
	prober := NewMockPeerProber()

	addr := raft.ServerAddress("127.0.0.1:8300")
	prober.SetReachable(addr, "node-1", "cluster-1", 5)

	result, err := prober.ProbePeer(context.Background(), addr)
	if err != nil {
		t.Fatalf("ProbePeer() error = %v", err)
	}

	if !result.Reachable {
		t.Error("Reachable = false, want true")
	}
	if result.ClusterUUID != "cluster-1" {
		t.Errorf("ClusterUUID = %s, want cluster-1", result.ClusterUUID)
	}
	if result.FencingEpoch != 5 {
		t.Errorf("FencingEpoch = %d, want 5", result.FencingEpoch)
	}

	prober.Reset()
	result, _ = prober.ProbePeer(context.Background(), addr)
	if result.Reachable {
		t.Error("Reachable = true after reset, want false")
	}
}
