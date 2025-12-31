package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestTokenUsageTracker_CheckLocalUsage(t *testing.T) {
	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{})

	tokenHash := "abc123def456"
	used, usedBy, usedAt := tracker.CheckLocalUsage(tokenHash)

	if used {
		t.Error("Expected token to not be used initially")
	}
	if usedBy != "" {
		t.Error("Expected usedBy to be empty")
	}
	if !usedAt.IsZero() {
		t.Error("Expected usedAt to be zero")
	}

	tracker.mu.Lock()
	tracker.usages[tokenHash] = &TokenUsageRecord{
		TokenHash: tokenHash,
		UsedAt:    time.Now(),
		UsedBy:    "node-0",
	}
	tracker.mu.Unlock()

	used, usedBy, usedAt = tracker.CheckLocalUsage(tokenHash)

	if !used {
		t.Error("Expected token to be used")
	}
	if usedBy != "node-0" {
		t.Errorf("Expected usedBy 'node-0', got %s", usedBy)
	}
	if usedAt.IsZero() {
		t.Error("Expected usedAt to be set")
	}
}

func TestTokenUsageTracker_PersistUsage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "token-usage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{
		DataDir: tmpDir,
	})

	token := &ForceBootstrapToken{
		Token:  "test-token-12345",
		UsedAt: time.Now(),
		UsedBy: "node-1",
	}

	if err := tracker.PersistUsage(token); err != nil {
		t.Fatalf("PersistUsage failed: %v", err)
	}

	tokenHash := ComputeTokenHash(token.Token)
	usageFile := filepath.Join(tmpDir, "token_usage", tokenHash+".json")

	if _, err := os.Stat(usageFile); os.IsNotExist(err) {
		t.Error("Expected usage file to exist")
	}

	used, usedBy, _ := tracker.CheckLocalUsage(tokenHash)
	if !used {
		t.Error("Expected token to be in memory after persist")
	}
	if usedBy != "node-1" {
		t.Errorf("Expected usedBy 'node-1', got %s", usedBy)
	}
}

func TestTokenUsageTracker_LoadPersistedUsages(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "token-usage-load-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tracker1 := NewTokenUsageTracker(TokenUsageTrackerDeps{
		DataDir: tmpDir,
	})

	token := &ForceBootstrapToken{
		Token:  "persisted-token-xyz",
		UsedAt: time.Now(),
		UsedBy: "node-2",
	}

	if err := tracker1.PersistUsage(token); err != nil {
		t.Fatalf("PersistUsage failed: %v", err)
	}

	tracker2 := NewTokenUsageTracker(TokenUsageTrackerDeps{
		DataDir: tmpDir,
	})

	if err := tracker2.LoadPersistedUsages(); err != nil {
		t.Fatalf("LoadPersistedUsages failed: %v", err)
	}

	tokenHash := ComputeTokenHash(token.Token)
	used, usedBy, _ := tracker2.CheckLocalUsage(tokenHash)

	if !used {
		t.Error("Expected token to be loaded from disk")
	}
	if usedBy != "node-2" {
		t.Errorf("Expected usedBy 'node-2', got %s", usedBy)
	}
}

func TestTokenUsageTracker_BroadcastUsage(t *testing.T) {
	transport := NewMockBootstrapTransport()
	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-key-for-signing-32-bytes!!"))

	meta := &ClusterMeta{
		ServerID: "node-0",
		Ordinal:  0,
	}

	notifiedAddrs := make(map[string]bool)
	transport.SetNotifyUsageFunc(func(addr string, notification *TokenUsageNotification) error {
		notifiedAddrs[addr] = true
		return nil
	})

	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{
		Transport: transport,
		Meta:      meta,
		Secrets:   secrets,
	})

	token := &ForceBootstrapToken{
		Token:             "broadcast-token",
		BoundFencingEpoch: 10,
		UsedAt:            time.Now(),
		UsedBy:            "node-0",
	}

	peers := []PeerInfo{
		{ServerID: "node-1", Address: "10.0.0.2:7946"},
		{ServerID: "node-2", Address: "10.0.0.3:7946"},
	}

	err := tracker.BroadcastUsage(context.Background(), token, peers)
	if err != nil {
		t.Fatalf("BroadcastUsage failed: %v", err)
	}

	if !notifiedAddrs["10.0.0.2:7946"] {
		t.Error("Expected node-1 to be notified")
	}
	if !notifiedAddrs["10.0.0.3:7946"] {
		t.Error("Expected node-2 to be notified")
	}
}

func TestTokenUsageTracker_CheckClusterWideUsage_EpochAdvanced(t *testing.T) {
	fencing := NewFencingManager(nil, nil, nil, nil, nil)
	fencing.mu.Lock()
	fencing.localEpoch = 15
	fencing.mu.Unlock()

	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{
		Fencing: fencing,
	})

	token := &ForceBootstrapToken{
		Token:             "old-token",
		BoundFencingEpoch: 10,
	}

	used, usedBy, _, err := tracker.CheckClusterWideUsage(context.Background(), token)
	if err != nil {
		t.Fatalf("CheckClusterWideUsage failed: %v", err)
	}

	if !used {
		t.Error("Expected token to be marked as used due to epoch advance")
	}
	if usedBy != "epoch_advanced" {
		t.Errorf("Expected usedBy 'epoch_advanced', got %s", usedBy)
	}
}

func TestTokenUsageTracker_CheckClusterWideUsage_PeerReports(t *testing.T) {
	transport := NewMockBootstrapTransport()
	membershipStore := NewMockMembershipStore()

	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	usedTime := time.Now().Add(-time.Hour)
	transport.SetTokenUsage("10.0.0.2:7946", &TokenUsageResponse{
		Used:   true,
		UsedBy: "node-1",
		UsedAt: usedTime,
	})

	meta := &ClusterMeta{
		ServerID: "node-0",
	}

	config := &BootstrapConfig{
		ExpectedNodes: 3,
	}

	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{
		Transport:       transport,
		MembershipStore: membershipStore,
		Meta:            meta,
		Config:          config,
	})

	token := &ForceBootstrapToken{
		Token:             "shared-token",
		BoundFencingEpoch: 10,
	}

	used, usedBy, usedAt, err := tracker.CheckClusterWideUsage(context.Background(), token)
	if err != nil {
		t.Fatalf("CheckClusterWideUsage failed: %v", err)
	}

	if !used {
		t.Error("Expected token to be marked as used from peer report")
	}
	if usedBy != "node-1" {
		t.Errorf("Expected usedBy 'node-1', got %s", usedBy)
	}
	if !usedAt.Equal(usedTime) {
		t.Errorf("Expected usedAt to match peer report")
	}
}

func TestTokenUsageNotification_Signature(t *testing.T) {
	secrets := NewMockSecretsManager()
	key := []byte("notification-signing-key-32char!")
	secrets.SetFencingKey(key)

	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{
		Secrets: secrets,
	})

	notification := &TokenUsageNotification{
		Version:   TokenUsageNotificationVersion,
		TokenHash: "abc123",
		UsedAt:    time.Now(),
		UsedBy:    "node-0",
		NewEpoch:  11,
		Timestamp: time.Now(),
		SenderID:  "node-0",
	}

	notification.Signature = tracker.signUsageNotification(notification, key)

	if notification.Signature == "" {
		t.Error("Expected signature to be set")
	}

	if !tracker.verifyUsageNotification(notification, key) {
		t.Error("Expected signature to be valid")
	}

	notification.NewEpoch = 12
	if tracker.verifyUsageNotification(notification, key) {
		t.Error("Expected signature to be invalid after tampering")
	}
}

func TestTokenUsageTracker_HandleUsageNotification(t *testing.T) {
	secrets := NewMockSecretsManager()
	key := []byte("notification-signing-key-32char!")
	secrets.SetFencingKey(key)

	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{
		Secrets: secrets,
	})

	notification := &TokenUsageNotification{
		Version:   TokenUsageNotificationVersion,
		TokenHash: "notification-token-hash",
		UsedAt:    time.Now(),
		UsedBy:    "node-1",
		NewEpoch:  15,
		Timestamp: time.Now(),
		SenderID:  "node-1",
	}
	notification.Signature = tracker.signUsageNotification(notification, key)

	err := tracker.HandleUsageNotification(notification)
	if err != nil {
		t.Fatalf("HandleUsageNotification failed: %v", err)
	}

	used, usedBy, _ := tracker.CheckLocalUsage(notification.TokenHash)
	if !used {
		t.Error("Expected token to be recorded")
	}
	if usedBy != "node-1" {
		t.Errorf("Expected usedBy 'node-1', got %s", usedBy)
	}
}

func TestTokenUsageTracker_HandleUsageNotification_InvalidSignature(t *testing.T) {
	secrets := NewMockSecretsManager()
	key := []byte("notification-signing-key-32char!")
	secrets.SetFencingKey(key)

	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{
		Secrets: secrets,
	})

	notification := &TokenUsageNotification{
		Version:   TokenUsageNotificationVersion,
		TokenHash: "bad-sig-token-hash",
		UsedAt:    time.Now(),
		UsedBy:    "node-1",
		NewEpoch:  15,
		Timestamp: time.Now(),
		SenderID:  "node-1",
		Signature: "invalid-signature",
	}

	err := tracker.HandleUsageNotification(notification)
	if err == nil {
		t.Error("Expected error for invalid signature")
	}
}

func TestTokenUsageTracker_GetUsage(t *testing.T) {
	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{})

	resp := tracker.GetUsage("nonexistent-hash")
	if resp.Used {
		t.Error("Expected Used to be false for nonexistent token")
	}

	tracker.mu.Lock()
	tracker.usages["existing-hash"] = &TokenUsageRecord{
		TokenHash: "existing-hash",
		UsedAt:    time.Now(),
		UsedBy:    "node-0",
	}
	tracker.mu.Unlock()

	resp = tracker.GetUsage("existing-hash")
	if !resp.Used {
		t.Error("Expected Used to be true")
	}
	if resp.UsedBy != "node-0" {
		t.Errorf("Expected UsedBy 'node-0', got %s", resp.UsedBy)
	}
}

func TestTokenUsageTracker_NilDataDir(t *testing.T) {
	tracker := NewTokenUsageTracker(TokenUsageTrackerDeps{})

	token := &ForceBootstrapToken{
		Token:  "no-persist-token",
		UsedAt: time.Now(),
		UsedBy: "node-0",
	}

	err := tracker.PersistUsage(token)
	if err != nil {
		t.Errorf("PersistUsage with no DataDir should not error: %v", err)
	}

	tokenHash := ComputeTokenHash(token.Token)
	used, _, _ := tracker.CheckLocalUsage(tokenHash)
	if !used {
		t.Error("Expected in-memory tracking to work without DataDir")
	}
}

func newTestMeta() *ClusterMeta {
	return &ClusterMeta{
		ServerID:      raft.ServerID("node-0"),
		ServerAddress: raft.ServerAddress("10.0.0.1:7946"),
		Ordinal:       0,
	}
}
