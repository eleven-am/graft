package bootstrap

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func newTestFencingManager(t *testing.T) (*FencingManager, *SecretsManager) {
	t.Helper()

	tempDir := t.TempDir()
	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)

	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	config := DefaultBootstrapConfig()
	config.DataDir = tempDir

	fm := NewFencingManager(config, tokenStore, epochStore, secrets, nil)

	return fm, secrets
}

func TestFencingManager_Initialize(t *testing.T) {
	fm, _ := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	if fm.CurrentEpoch() != 0 {
		t.Errorf("CurrentEpoch() = %d, want 0", fm.CurrentEpoch())
	}

	if fm.HasToken() {
		t.Error("HasToken() = true, want false for fresh manager")
	}
}

func TestFencingManager_CreateProposal(t *testing.T) {
	fm, _ := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	proposal, err := fm.CreateProposal(FencingModePrimary, raft.ServerID("node-0"), raft.ServerAddress("node-0:7946"))
	if err != nil {
		t.Fatalf("CreateProposal() error = %v", err)
	}

	if proposal.ProposedEpoch != 1 {
		t.Errorf("ProposedEpoch = %d, want 1", proposal.ProposedEpoch)
	}
	if proposal.ProposerID != "node-0" {
		t.Errorf("ProposerID = %s, want node-0", proposal.ProposerID)
	}
	if proposal.Mode != FencingModePrimary {
		t.Errorf("Mode = %s, want %s", proposal.Mode, FencingModePrimary)
	}
	if len(proposal.Signature) == 0 {
		t.Error("Signature should be set")
	}
}

func TestFencingManager_HandleProposal_Accept(t *testing.T) {
	fm, secrets := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	proposal := &FencingProposal{
		ProposedEpoch:   1,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(voters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal, secrets.FencingKey())

	ack, err := fm.HandleProposal(proposal, raft.ServerID("node-1"))
	if err != nil {
		t.Fatalf("HandleProposal() error = %v", err)
	}

	if !ack.Accepted {
		t.Errorf("Accepted = false, want true. RejectReason: %s", ack.RejectReason)
	}
	if ack.ProposedEpoch != 1 {
		t.Errorf("ProposedEpoch = %d, want 1", ack.ProposedEpoch)
	}
	if ack.VoterID != "node-1" {
		t.Errorf("VoterID = %s, want node-1", ack.VoterID)
	}

	if fm.CurrentEpoch() != 1 {
		t.Errorf("CurrentEpoch() = %d, want 1 after accepting proposal", fm.CurrentEpoch())
	}
}

func TestFencingManager_HandleProposal_StaleEpoch(t *testing.T) {
	fm, secrets := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	proposal1 := &FencingProposal{
		ProposedEpoch:   5,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(voters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal1, secrets.FencingKey())

	_, err := fm.HandleProposal(proposal1, raft.ServerID("node-1"))
	if err != nil {
		t.Fatalf("HandleProposal() error = %v", err)
	}

	proposal2 := &FencingProposal{
		ProposedEpoch:   3,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(voters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal2, secrets.FencingKey())

	ack, err := fm.HandleProposal(proposal2, raft.ServerID("node-1"))
	if err != nil {
		t.Fatalf("HandleProposal() error = %v", err)
	}

	if ack.Accepted {
		t.Error("Accepted = true, want false for stale epoch")
	}
	if ack.RejectReason == "" {
		t.Error("RejectReason should be set for rejected proposal")
	}
}

func TestFencingManager_HandleProposal_VoterSetMismatch(t *testing.T) {
	fm, secrets := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	differentVoters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	proposal := &FencingProposal{
		ProposedEpoch:   1,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(differentVoters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal, secrets.FencingKey())

	ack, err := fm.HandleProposal(proposal, raft.ServerID("node-1"))
	if err != nil {
		t.Fatalf("HandleProposal() error = %v", err)
	}

	if ack.Accepted {
		t.Error("Accepted = true, want false for voter set mismatch")
	}
}

func TestFencingManager_HandleProposal_InvalidSignature(t *testing.T) {
	fm, _ := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	wrongKey := []byte("wrong-fencing-key-32-bytes-long!")
	proposal := &FencingProposal{
		ProposedEpoch:   1,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(voters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal, wrongKey)

	ack, err := fm.HandleProposal(proposal, raft.ServerID("node-1"))
	if err != nil {
		t.Fatalf("HandleProposal() error = %v", err)
	}

	if ack.Accepted {
		t.Error("Accepted = true, want false for invalid signature")
	}
}

func TestFencingManager_CommitToken_Quorum(t *testing.T) {
	fm, secrets := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	proposal := &FencingProposal{
		ProposedEpoch:   1,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(voters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal, secrets.FencingKey())

	ack1 := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-0"),
		Accepted:      true,
		CurrentEpoch:  1,
		Timestamp:     time.Now(),
	}
	SignAck(ack1, secrets.FencingKey())

	ack2 := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-1"),
		Accepted:      true,
		CurrentEpoch:  1,
		Timestamp:     time.Now(),
	}
	SignAck(ack2, secrets.FencingKey())

	token, err := fm.CommitToken(proposal, []*FencingAck{ack1, ack2})
	if err != nil {
		t.Fatalf("CommitToken() error = %v", err)
	}

	if token.Epoch != 1 {
		t.Errorf("Epoch = %d, want 1", token.Epoch)
	}
	if token.HolderID != "node-0" {
		t.Errorf("HolderID = %s, want node-0", token.HolderID)
	}

	if !fm.HasToken() {
		t.Error("HasToken() = false after CommitToken()")
	}
	if !fm.IsTokenHolder(raft.ServerID("node-0")) {
		t.Error("IsTokenHolder(node-0) = false, want true")
	}
}

func TestFencingManager_CommitToken_InsufficientQuorum(t *testing.T) {
	fm, secrets := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	proposal := &FencingProposal{
		ProposedEpoch:   1,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(voters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal, secrets.FencingKey())

	ack1 := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-0"),
		Accepted:      true,
		CurrentEpoch:  1,
		Timestamp:     time.Now(),
	}
	SignAck(ack1, secrets.FencingKey())

	_, err := fm.CommitToken(proposal, []*FencingAck{ack1})
	if err == nil {
		t.Error("CommitToken() should fail for insufficient quorum")
	}

	if _, ok := err.(*QuorumNotReachedError); !ok {
		t.Errorf("CommitToken() error type = %T, want *QuorumNotReachedError", err)
	}
}

func TestFencingManager_ValidateToken(t *testing.T) {
	fm, secrets := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	token := &FencingToken{
		Version:      TokenVersion,
		Epoch:        1,
		HolderID:     raft.ServerID("node-0"),
		VoterSetHash: ComputeVoterSetHash(voters),
		QuorumSize:   2,
		Mode:         FencingModePrimary,
		AcquiredAt:   time.Now(),
	}
	token.UpdateChecksum()
	SignToken(token, secrets.FencingKey())

	if err := fm.ValidateToken(token); err != nil {
		t.Errorf("ValidateToken() = %v, want nil", err)
	}
}

func TestFencingManager_ValidateToken_Expired(t *testing.T) {
	fm, secrets := newTestFencingManager(t)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	token := &FencingToken{
		Version:      TokenVersion,
		Epoch:        1,
		HolderID:     raft.ServerID("node-0"),
		VoterSetHash: ComputeVoterSetHash(voters),
		QuorumSize:   1,
		Mode:         FencingModePrimary,
		AcquiredAt:   time.Now().Add(-2 * time.Hour),
		ExpiresAt:    time.Now().Add(-time.Hour),
	}
	token.UpdateChecksum()
	SignToken(token, secrets.FencingKey())

	err := fm.ValidateToken(token)
	if err == nil {
		t.Error("ValidateToken() should fail for expired token")
	}
}

func TestFencingManager_NoFencingKey(t *testing.T) {
	tempDir := t.TempDir()
	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)

	secrets := NewSecretsManager(SecretsConfig{}, nil)

	config := DefaultBootstrapConfig()
	config.DataDir = tempDir

	fm := NewFencingManager(config, tokenStore, epochStore, secrets, nil)

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
	}

	if err := fm.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	_, err := fm.CreateProposal(FencingModePrimary, raft.ServerID("node-0"), raft.ServerAddress("node-0:7946"))
	if err == nil {
		t.Error("CreateProposal() should fail without fencing key")
	}
}

func TestFencingManager_EpochPersistence(t *testing.T) {
	tempDir := t.TempDir()
	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)

	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	config := DefaultBootstrapConfig()
	config.DataDir = tempDir

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	fm1 := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
	if err := fm1.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	proposal := &FencingProposal{
		ProposedEpoch:   1,
		ExpectedEpoch:   0,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(voters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal, secrets.FencingKey())

	ack, err := fm1.HandleProposal(proposal, raft.ServerID("node-1"))
	if err != nil {
		t.Fatalf("HandleProposal() error = %v", err)
	}
	if !ack.Accepted {
		t.Fatalf("Proposal should be accepted, got reject: %s", ack.RejectReason)
	}

	fm2 := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
	if err := fm2.Initialize(voters); err != nil {
		t.Fatalf("Initialize() second manager error = %v", err)
	}

	if fm2.CurrentEpoch() != 1 {
		t.Errorf("CurrentEpoch() after restart = %d, want 1", fm2.CurrentEpoch())
	}

	staleProposal := &FencingProposal{
		ProposedEpoch:   1,
		ExpectedEpoch:   0,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(voters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(staleProposal, secrets.FencingKey())

	ack2, err := fm2.HandleProposal(staleProposal, raft.ServerID("node-1"))
	if err != nil {
		t.Fatalf("HandleProposal() error = %v", err)
	}
	if ack2.Accepted {
		t.Error("Stale proposal should be rejected after epoch persistence")
	}
}

func TestFencingManager_QuorumSize(t *testing.T) {
	fm, _ := newTestFencingManager(t)

	tests := []struct {
		name       string
		voterCount int
		wantQuorum int
	}{
		{"1 voter", 1, 1},
		{"2 voters", 2, 2},
		{"3 voters", 3, 2},
		{"4 voters", 4, 3},
		{"5 voters", 5, 3},
		{"7 voters", 7, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voters := make([]VoterInfo, tt.voterCount)
			for i := 0; i < tt.voterCount; i++ {
				voters[i] = VoterInfo{
					ServerID: raft.ServerID(fmt.Sprintf("node-%d", i)),
					Address:  raft.ServerAddress(fmt.Sprintf("node-%d:7946", i)),
					Ordinal:  i,
				}
			}

			if err := fm.Initialize(voters); err != nil {
				t.Fatalf("Initialize() error = %v", err)
			}

			if got := fm.QuorumSize(); got != tt.wantQuorum {
				t.Errorf("QuorumSize() = %d, want %d", got, tt.wantQuorum)
			}
		})
	}
}

func TestFencingManager_TokenRejectedOnVoterSetChange(t *testing.T) {
	tempDir := t.TempDir()
	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)

	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	config := DefaultBootstrapConfig()
	config.DataDir = tempDir

	originalVoters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	fm1 := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
	if err := fm1.Initialize(originalVoters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	proposal := &FencingProposal{
		ProposedEpoch:   1,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(originalVoters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal, secrets.FencingKey())

	ack1 := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-0"),
		Accepted:      true,
		CurrentEpoch:  1,
		Timestamp:     time.Now(),
	}
	SignAck(ack1, secrets.FencingKey())

	ack2 := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-1"),
		Accepted:      true,
		CurrentEpoch:  1,
		Timestamp:     time.Now(),
	}
	SignAck(ack2, secrets.FencingKey())

	token, err := fm1.CommitToken(proposal, []*FencingAck{ack1, ack2})
	if err != nil {
		t.Fatalf("CommitToken() error = %v", err)
	}
	if !fm1.HasToken() {
		t.Fatal("HasToken() should be true after commit")
	}

	changedVoters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
		{ServerID: raft.ServerID("node-3"), Address: raft.ServerAddress("node-3:7946"), Ordinal: 3},
		{ServerID: raft.ServerID("node-4"), Address: raft.ServerAddress("node-4:7946"), Ordinal: 4},
	}

	fm2 := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
	if err := fm2.Initialize(changedVoters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	if fm2.HasToken() {
		t.Error("HasToken() should be false after voter set change")
	}

	if fm2.CurrentEpoch() != token.Epoch {
		t.Errorf("CurrentEpoch() = %d, want %d (epoch should be preserved even when token rejected)",
			fm2.CurrentEpoch(), token.Epoch)
	}
}

func TestFencingManager_TokenAcceptedOnSameVoterSet(t *testing.T) {
	tempDir := t.TempDir()
	tokenStore := NewFileTokenStore(tempDir, nil)
	epochStore := NewFileEpochStore(tempDir, nil)

	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	config := DefaultBootstrapConfig()
	config.DataDir = tempDir

	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	fm1 := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
	if err := fm1.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	proposal := &FencingProposal{
		ProposedEpoch:   1,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    ComputeVoterSetHash(voters),
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}
	SignProposal(proposal, secrets.FencingKey())

	ack1 := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-0"),
		Accepted:      true,
		CurrentEpoch:  1,
		Timestamp:     time.Now(),
	}
	SignAck(ack1, secrets.FencingKey())

	ack2 := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-1"),
		Accepted:      true,
		CurrentEpoch:  1,
		Timestamp:     time.Now(),
	}
	SignAck(ack2, secrets.FencingKey())

	_, err := fm1.CommitToken(proposal, []*FencingAck{ack1, ack2})
	if err != nil {
		t.Fatalf("CommitToken() error = %v", err)
	}

	fm2 := NewFencingManager(config, tokenStore, epochStore, secrets, nil)
	if err := fm2.Initialize(voters); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	if !fm2.HasToken() {
		t.Error("HasToken() should be true when voter set matches")
	}
	if !fm2.IsTokenHolder(raft.ServerID("node-0")) {
		t.Error("IsTokenHolder(node-0) should be true")
	}
}
