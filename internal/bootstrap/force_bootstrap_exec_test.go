package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestPreflight_Check_QuorumReachable_Blocked(t *testing.T) {
	transport := NewMockBootstrapTransport()
	membershipStore := NewMockMembershipStore()

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	transport.SetClusterMeta("10.0.0.1:7946", &ClusterMeta{ServerID: "node-0"})
	transport.SetClusterMeta("10.0.0.2:7946", &ClusterMeta{ServerID: "node-1"})

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Transport:       transport,
		MembershipStore: membershipStore,
	})

	token := &ForceBootstrapToken{
		VoterSetHash:        HashVoterSet(voters),
		CommittedVoterCount: 3,
		AcknowledgeFork:     true,
	}

	result, err := preflight.Check(context.Background(), token)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if result.Safe {
		t.Error("Expected Safe=false when quorum is reachable")
	}

	if result.BlockReason == "" {
		t.Error("Expected BlockReason to be set")
	}
}

func TestPreflight_Check_SomeVotersReachable_NoAcknowledgeFork(t *testing.T) {
	transport := NewMockBootstrapTransport()
	membershipStore := NewMockMembershipStore()

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	transport.SetClusterMeta("10.0.0.1:7946", &ClusterMeta{ServerID: "node-0"})

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Transport:       transport,
		MembershipStore: membershipStore,
	})

	token := &ForceBootstrapToken{
		VoterSetHash:        HashVoterSet(voters),
		CommittedVoterCount: 3,
		AcknowledgeFork:     false,
	}

	result, err := preflight.Check(context.Background(), token)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if result.Safe {
		t.Error("Expected Safe=false without AcknowledgeFork")
	}

	if result.BlockReason == "" {
		t.Error("Expected BlockReason to mention AcknowledgeFork")
	}
}

func TestPreflight_Check_SomeVotersReachable_WithAcknowledgeFork(t *testing.T) {
	transport := NewMockBootstrapTransport()
	membershipStore := NewMockMembershipStore()

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	transport.SetClusterMeta("10.0.0.1:7946", &ClusterMeta{ServerID: "node-0"})
	transport.SetFencingEpoch("10.0.0.1:7946", 5)

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Transport:       transport,
		MembershipStore: membershipStore,
	})

	token := &ForceBootstrapToken{
		VoterSetHash:        HashVoterSet(voters),
		CommittedVoterCount: 3,
		BoundFencingEpoch:   10,
		AcknowledgeFork:     true,
	}

	result, err := preflight.Check(context.Background(), token)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if !result.Safe {
		t.Errorf("Expected Safe=true with AcknowledgeFork, got BlockReason: %s", result.BlockReason)
	}

	if len(result.Warnings) == 0 {
		t.Error("Expected warnings about reachable peers")
	}
}

func TestPreflight_Check_VoterSetHashMismatch(t *testing.T) {
	transport := NewMockBootstrapTransport()
	membershipStore := NewMockMembershipStore()

	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Transport:       transport,
		MembershipStore: membershipStore,
	})

	token := &ForceBootstrapToken{
		VoterSetHash:        []byte("wrong-hash"),
		CommittedVoterCount: 3,
	}

	result, err := preflight.Check(context.Background(), token)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if result.Safe {
		t.Error("Expected Safe=false for voter set hash mismatch")
	}

	if result.BlockReason == "" {
		t.Error("Expected BlockReason about hash mismatch")
	}
}

func TestPreflight_Check_StaleEpoch(t *testing.T) {
	transport := NewMockBootstrapTransport()
	membershipStore := NewMockMembershipStore()

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	transport.SetClusterMeta("10.0.0.1:7946", &ClusterMeta{ServerID: "node-0"})
	transport.SetFencingEpoch("10.0.0.1:7946", 20)

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Transport:       transport,
		MembershipStore: membershipStore,
	})

	token := &ForceBootstrapToken{
		VoterSetHash:        HashVoterSet(voters),
		CommittedVoterCount: 3,
		BoundFencingEpoch:   10,
		AcknowledgeFork:     true,
	}

	result, err := preflight.Check(context.Background(), token)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if result.Safe {
		t.Error("Expected Safe=false when peer has higher epoch")
	}

	if result.BlockReason == "" {
		t.Error("Expected BlockReason about stale token")
	}
}

func TestPreflight_Check_Success_NoPeersReachable(t *testing.T) {
	transport := NewMockBootstrapTransport()
	membershipStore := NewMockMembershipStore()

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Transport:       transport,
		MembershipStore: membershipStore,
	})

	token := &ForceBootstrapToken{
		VoterSetHash:        HashVoterSet(voters),
		CommittedVoterCount: 3,
		BoundFencingEpoch:   10,
		AcknowledgeFork:     true,
	}

	result, err := preflight.Check(context.Background(), token)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if !result.Safe {
		t.Errorf("Expected Safe=true when no peers reachable, got BlockReason: %s", result.BlockReason)
	}

	if !result.QuorumAbsenceProven {
		t.Error("Expected QuorumAbsenceProven=true")
	}

	if result.ReachablePeers != 0 {
		t.Errorf("Expected ReachablePeers=0, got %d", result.ReachablePeers)
	}
}

func TestPreflight_DisasterRecovery_ExistingRaftLog(t *testing.T) {
	transport := NewMockBootstrapTransport()
	discovery := NewMockPeerDiscovery()

	discovery.SetAddress(0, "10.0.0.1:7946")
	discovery.SetAddress(1, "10.0.0.2:7946")
	discovery.SetAddress(2, "10.0.0.3:7946")

	transport.SetLastIndex("10.0.0.1:7946", 100)

	config := &BootstrapConfig{
		ExpectedNodes: 3,
	}

	expectedVoters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Transport: transport,
		Discovery: discovery,
		Config:    config,
	})

	token := &ForceBootstrapToken{
		VoterSetHash:         HashVoterSet(expectedVoters),
		DisasterRecoveryMode: true,
		AcknowledgeFork:      true,
	}

	result, err := preflight.Check(context.Background(), token)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if result.Safe {
		t.Error("Expected Safe=false when peer has existing Raft log")
	}

	if result.BlockReason == "" {
		t.Error("Expected BlockReason about existing Raft log")
	}
}

func TestPreflight_DisasterRecovery_IgnoreQuorumCheck(t *testing.T) {
	transport := NewMockBootstrapTransport()
	discovery := NewMockPeerDiscovery()

	discovery.SetAddress(0, "10.0.0.1:7946")
	discovery.SetAddress(1, "10.0.0.2:7946")
	discovery.SetAddress(2, "10.0.0.3:7946")

	transport.SetLastIndex("10.0.0.1:7946", 0)
	transport.SetLastIndex("10.0.0.2:7946", 0)

	config := &BootstrapConfig{
		ExpectedNodes: 3,
		ForceBootstrap: ForceBootstrapConfig{
			AllowDRQuorumOverride: true,
		},
	}

	expectedVoters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Transport: transport,
		Discovery: discovery,
		Config:    config,
	})

	token := &ForceBootstrapToken{
		VoterSetHash:             HashVoterSet(expectedVoters),
		DisasterRecoveryMode:     true,
		AcknowledgeFork:          true,
		IgnoreQuorumCheck:        true,
		ConfirmIgnoreQuorumCheck: IgnoreQuorumCheckConfirmationPhrase,
	}

	result, err := preflight.Check(context.Background(), token)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if !result.Safe {
		t.Errorf("Expected Safe=true with IgnoreQuorumCheck, got BlockReason: %s", result.BlockReason)
	}

	if len(result.Warnings) == 0 {
		t.Error("Expected critical warning about force bootstrap with quorum")
	}
}

func TestPreflight_DisasterRecovery_RequiresAllowDRQuorumOverride(t *testing.T) {
	transport := NewMockBootstrapTransport()
	discovery := NewMockPeerDiscovery()

	discovery.SetAddress(0, "10.0.0.1:7946")
	discovery.SetAddress(1, "10.0.0.2:7946")
	discovery.SetAddress(2, "10.0.0.3:7946")

	transport.SetLastIndex("10.0.0.1:7946", 0)
	transport.SetLastIndex("10.0.0.2:7946", 0)

	config := &BootstrapConfig{
		ExpectedNodes: 3,
		ForceBootstrap: ForceBootstrapConfig{
			AllowDRQuorumOverride: false,
		},
	}

	expectedVoters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	preflight := NewForceBootstrapPreflight(ForceBootstrapPreflightDeps{
		Transport: transport,
		Discovery: discovery,
		Config:    config,
	})

	token := &ForceBootstrapToken{
		VoterSetHash:             HashVoterSet(expectedVoters),
		DisasterRecoveryMode:     true,
		AcknowledgeFork:          true,
		IgnoreQuorumCheck:        true,
		ConfirmIgnoreQuorumCheck: IgnoreQuorumCheckConfirmationPhrase,
	}

	result, err := preflight.Check(context.Background(), token)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if result.Safe {
		t.Error("Expected Safe=false when AllowDRQuorumOverride is false")
	}

	if result.BlockReason != "DR quorum override requires AllowDRQuorumOverride=true in executor config" {
		t.Errorf("Unexpected BlockReason: %s", result.BlockReason)
	}
}

func TestExecutor_CheckForToken_NotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "executor-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &BootstrapConfig{
		DataDir:            tmpDir,
		ForceBootstrapFile: filepath.Join(tmpDir, "force-bootstrap.json"),
	}

	executor := NewForceBootstrapExecutor(ForceBootstrapExecutorDeps{
		Config: config,
		Meta:   &ClusterMeta{Ordinal: 0},
	})

	found, token, err := executor.CheckForToken(context.Background())
	if err != nil {
		t.Fatalf("CheckForToken failed: %v", err)
	}

	if found {
		t.Error("Expected found=false when file doesn't exist")
	}

	if token != nil {
		t.Error("Expected token to be nil")
	}
}

func TestExecutor_CheckForToken_Expired(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "executor-expired-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tokenFile := filepath.Join(tmpDir, "force-bootstrap.json")

	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	generator := NewForceBootstrapTokenGenerator("", nil, nil, secrets, nil)
	token, err := generator.Generate(
		context.Background(),
		"cluster-uuid",
		"",
		1,
		"test",
		"admin",
		-time.Hour,
		&ForceTokenOptions{
			VoterSetHash:    []byte("hash"),
			AcknowledgeFork: true,
		},
	)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	data, _ := json.Marshal(token)
	os.WriteFile(tokenFile, data, 0600)

	config := &BootstrapConfig{
		DataDir:            tmpDir,
		ForceBootstrapFile: tokenFile,
	}

	executor := NewForceBootstrapExecutor(ForceBootstrapExecutorDeps{
		Config:  config,
		Meta:    &ClusterMeta{Ordinal: 0},
		Secrets: secrets,
	})

	_, _, err = executor.CheckForToken(context.Background())
	if !errors.Is(err, ErrForceBootstrapExpired) {
		t.Errorf("Expected ErrForceBootstrapExpired, got %v", err)
	}
}

func TestExecutor_CheckForToken_InvalidSignature(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "executor-sig-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tokenFile := filepath.Join(tmpDir, "force-bootstrap.json")

	token := &ForceBootstrapToken{
		Version:         ForceBootstrapTokenVersion,
		Token:           "test-token",
		ClusterUUID:     "cluster-uuid",
		VoterSetHash:    []byte("hash"),
		IssuedAt:        time.Now(),
		ExpiresAt:       time.Now().Add(time.Hour),
		TargetOrdinal:   0,
		AcknowledgeFork: true,
		Signature:       "invalid-signature",
	}

	data, _ := json.Marshal(token)
	os.WriteFile(tokenFile, data, 0600)

	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	config := &BootstrapConfig{
		DataDir:            tmpDir,
		ForceBootstrapFile: tokenFile,
	}

	executor := NewForceBootstrapExecutor(ForceBootstrapExecutorDeps{
		Config:  config,
		Meta:    &ClusterMeta{Ordinal: 0},
		Secrets: secrets,
	})

	_, _, err = executor.CheckForToken(context.Background())
	if !errors.Is(err, ErrInvalidForceBootstrapSignature) {
		t.Errorf("Expected ErrInvalidForceBootstrapSignature, got %v", err)
	}
}

func TestExecutor_CheckForToken_WrongOrdinal(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "executor-ordinal-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tokenFile := filepath.Join(tmpDir, "force-bootstrap.json")

	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	generator := NewForceBootstrapTokenGenerator("", nil, nil, secrets, nil)
	token, err := generator.Generate(
		context.Background(),
		"cluster-uuid",
		"",
		1,
		"test",
		"admin",
		time.Hour,
		&ForceTokenOptions{
			VoterSetHash:    []byte("hash"),
			TargetOrdinal:   5,
			AcknowledgeFork: true,
		},
	)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	data, _ := json.Marshal(token)
	os.WriteFile(tokenFile, data, 0600)

	config := &BootstrapConfig{
		DataDir:            tmpDir,
		ForceBootstrapFile: tokenFile,
	}

	executor := NewForceBootstrapExecutor(ForceBootstrapExecutorDeps{
		Config:  config,
		Meta:    &ClusterMeta{Ordinal: 0},
		Secrets: secrets,
	})

	_, _, err = executor.CheckForToken(context.Background())
	if !errors.Is(err, ErrForceBootstrapTargetMismatch) {
		t.Errorf("Expected ErrForceBootstrapTargetMismatch, got %v", err)
	}
}

func TestExecutor_Execute_SingleUse_AlreadyUsed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "executor-used-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	fencing := NewFencingManager(nil, nil, nil, nil, nil)
	fencing.mu.Lock()
	fencing.localEpoch = 15
	fencing.mu.Unlock()

	config := &BootstrapConfig{
		DataDir: tmpDir,
	}

	executor := NewForceBootstrapExecutor(ForceBootstrapExecutorDeps{
		Config:  config,
		Meta:    &ClusterMeta{Ordinal: 0, ServerID: "node-0"},
		Fencing: fencing,
		Secrets: secrets,
	})

	token := &ForceBootstrapToken{
		Token:             "single-use-token",
		BoundFencingEpoch: 10,
		SingleUse:         true,
	}

	err = executor.Execute(context.Background(), token)

	var blockedErr *ForceBootstrapBlockedError
	if !errors.As(err, &blockedErr) {
		t.Fatalf("Expected ForceBootstrapBlockedError, got %v", err)
	}
}

func TestExecutor_Execute_SingleUse_Success(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "executor-success-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tokenFile := filepath.Join(tmpDir, "force-bootstrap.json")

	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	transport := NewMockBootstrapTransport()
	membershipStore := NewMockMembershipStore()

	voters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	fencing := NewFencingManager(nil, nil, nil, nil, nil)
	fencing.mu.Lock()
	fencing.localEpoch = 10
	fencing.mu.Unlock()

	config := &BootstrapConfig{
		DataDir:            tmpDir,
		ForceBootstrapFile: tokenFile,
	}

	executor := NewForceBootstrapExecutor(ForceBootstrapExecutorDeps{
		Config:          config,
		Meta:            &ClusterMeta{Ordinal: 0, ServerID: "node-0"},
		Transport:       transport,
		MembershipStore: membershipStore,
		Fencing:         fencing,
		Secrets:         secrets,
	})

	os.WriteFile(tokenFile, []byte("{}"), 0600)

	token := &ForceBootstrapToken{
		Token:             "valid-single-use-token",
		ClusterUUID:       "new-cluster",
		VoterSetHash:      HashVoterSet(voters),
		BoundFencingEpoch: 10,
		SingleUse:         true,
		AcknowledgeFork:   true,
	}

	err = executor.Execute(context.Background(), token)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if _, err := os.Stat(tokenFile); !os.IsNotExist(err) {
		t.Error("Expected force bootstrap file to be removed")
	}

	if fencing.CurrentEpoch() != 11 {
		t.Errorf("Expected fencing epoch to be 11, got %d", fencing.CurrentEpoch())
	}

	if token.UsedAt.IsZero() {
		t.Error("Expected UsedAt to be set")
	}

	if token.UsedBy != "node-0" {
		t.Errorf("Expected UsedBy to be 'node-0', got %s", token.UsedBy)
	}
}

func TestExtractVotersFromConfig(t *testing.T) {
	config := &raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Nonvoter},
			{ID: "node-3", Address: "10.0.0.4:7946", Suffrage: raft.Voter},
		},
	}

	voters := extractVotersFromConfig(config)

	if len(voters) != 3 {
		t.Errorf("Expected 3 voters, got %d", len(voters))
	}

	for _, v := range voters {
		if v.ServerID == "node-2" {
			t.Error("Non-voter should not be included")
		}
	}
}
