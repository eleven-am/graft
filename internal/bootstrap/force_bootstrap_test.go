package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestForceBootstrapToken_Signature_Valid(t *testing.T) {
	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	generator := NewForceBootstrapTokenGenerator("", nil, secrets, nil)

	voterSetHash := []byte("test-voter-set-hash-bytes")
	token, err := generator.Generate(
		context.Background(),
		"cluster-uuid-123",
		"",
		10,
		"test reason",
		"admin",
		time.Hour,
		&ForceTokenOptions{
			VoterSetHash:        voterSetHash,
			CommittedVoterCount: 3,
			TargetOrdinal:       0,
			SingleUse:           true,
			AcknowledgeFork:     true,
		},
	)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if token.Signature == "" {
		t.Error("Expected signature to be set")
	}

	if !generator.VerifyTokenSignature(token, secrets.FencingKey()) {
		t.Error("Expected signature to be valid")
	}
}

func TestForceBootstrapToken_Signature_Tampered(t *testing.T) {
	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	generator := NewForceBootstrapTokenGenerator("", nil, secrets, nil)

	voterSetHash := []byte("test-voter-set-hash-bytes")
	token, err := generator.Generate(
		context.Background(),
		"cluster-uuid-123",
		"",
		10,
		"test reason",
		"admin",
		time.Hour,
		&ForceTokenOptions{
			VoterSetHash:        voterSetHash,
			CommittedVoterCount: 3,
			TargetOrdinal:       0,
			SingleUse:           true,
			AcknowledgeFork:     true,
		},
	)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	token.Reason = "tampered reason"

	if generator.VerifyTokenSignature(token, secrets.FencingKey()) {
		t.Error("Expected signature to be invalid after tampering")
	}
}

func TestForceBootstrapTokenGenerator_Generate_Basic(t *testing.T) {
	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	generator := NewForceBootstrapTokenGenerator("", nil, secrets, nil)

	voterSetHash := []byte("test-voter-set-hash")
	token, err := generator.Generate(
		context.Background(),
		"cluster-123",
		"prev-cluster-456",
		5,
		"test generation",
		"operator@example.com",
		30*time.Minute,
		&ForceTokenOptions{
			VoterSetHash:        voterSetHash,
			CommittedVoterCount: 5,
			TargetOrdinal:       2,
			SingleUse:           true,
			AcknowledgeFork:     false,
		},
	)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if token.Version != ForceBootstrapTokenVersion {
		t.Errorf("Expected version %d, got %d", ForceBootstrapTokenVersion, token.Version)
	}

	if token.ClusterUUID != "cluster-123" {
		t.Errorf("Expected cluster UUID 'cluster-123', got %s", token.ClusterUUID)
	}

	if token.PreviousClusterUUID != "prev-cluster-456" {
		t.Errorf("Expected previous cluster UUID 'prev-cluster-456', got %s", token.PreviousClusterUUID)
	}

	if token.BoundFencingEpoch != 5 {
		t.Errorf("Expected bound fencing epoch 5, got %d", token.BoundFencingEpoch)
	}

	if token.TargetOrdinal != 2 {
		t.Errorf("Expected target ordinal 2, got %d", token.TargetOrdinal)
	}

	if !token.SingleUse {
		t.Error("Expected SingleUse to be true")
	}

	if token.Token == "" {
		t.Error("Expected Token to be set")
	}

	if len(token.Token) != 64 {
		t.Errorf("Expected Token to be 64 hex chars, got %d", len(token.Token))
	}

	if token.IssuedAt.IsZero() {
		t.Error("Expected IssuedAt to be set")
	}

	if token.ExpiresAt.IsZero() {
		t.Error("Expected ExpiresAt to be set")
	}

	expectedExpiry := token.IssuedAt.Add(30 * time.Minute)
	if token.ExpiresAt.Sub(expectedExpiry) > time.Second {
		t.Errorf("ExpiresAt mismatch: expected around %v, got %v", expectedExpiry, token.ExpiresAt)
	}
}

func TestForceBootstrapTokenGenerator_Generate_DisasterRecovery(t *testing.T) {
	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	generator := NewForceBootstrapTokenGenerator("", nil, secrets, nil)

	voterSetHash := []byte("expected-nodes-hash")
	token, err := generator.Generate(
		context.Background(),
		"new-cluster-uuid",
		"",
		0,
		"disaster recovery",
		"operator",
		time.Hour,
		&ForceTokenOptions{
			VoterSetHash:         voterSetHash,
			CommittedVoterCount:  3,
			DisasterRecoveryMode: true,
			AcknowledgeFork:      true,
		},
	)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if !token.DisasterRecoveryMode {
		t.Error("Expected DisasterRecoveryMode to be true")
	}

	if !token.AcknowledgeFork {
		t.Error("Expected AcknowledgeFork to be true")
	}
}

func TestForceBootstrapTokenGenerator_Generate_MissingVoterSetHash(t *testing.T) {
	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-key"))

	generator := NewForceBootstrapTokenGenerator("", nil, secrets, nil)

	_, err := generator.Generate(
		context.Background(),
		"cluster-uuid",
		"",
		1,
		"reason",
		"admin",
		time.Hour,
		&ForceTokenOptions{
			CommittedVoterCount: 3,
		},
	)

	if err != ErrMissingVoterSetHash {
		t.Errorf("Expected ErrMissingVoterSetHash, got %v", err)
	}
}

func TestForceBootstrapTokenGenerator_Generate_IgnoreQuorumCheck_NoConfirmation(t *testing.T) {
	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-key"))

	generator := NewForceBootstrapTokenGenerator("", nil, secrets, nil)

	_, err := generator.Generate(
		context.Background(),
		"cluster-uuid",
		"",
		1,
		"reason",
		"admin",
		time.Hour,
		&ForceTokenOptions{
			VoterSetHash:        []byte("hash"),
			CommittedVoterCount: 3,
			IgnoreQuorumCheck:   true,
		},
	)

	if err != ErrIgnoreQuorumNotConfirmed {
		t.Errorf("Expected ErrIgnoreQuorumNotConfirmed, got %v", err)
	}
}

func TestForceBootstrapTokenGenerator_Generate_IgnoreQuorumCheck_WithConfirmation(t *testing.T) {
	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))

	generator := NewForceBootstrapTokenGenerator("", nil, secrets, nil)

	token, err := generator.Generate(
		context.Background(),
		"cluster-uuid",
		"",
		1,
		"emergency",
		"admin",
		time.Hour,
		&ForceTokenOptions{
			VoterSetHash:             []byte("hash"),
			CommittedVoterCount:      3,
			IgnoreQuorumCheck:        true,
			ConfirmIgnoreQuorumCheck: IgnoreQuorumCheckConfirmationPhrase,
			DisasterRecoveryMode:     true,
			AcknowledgeFork:          true,
		},
	)

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if !token.IgnoreQuorumCheck {
		t.Error("Expected IgnoreQuorumCheck to be true")
	}

	if token.ConfirmIgnoreQuorumCheck != IgnoreQuorumCheckConfirmationPhrase {
		t.Errorf("Expected confirmation phrase to be set")
	}
}

func TestForceBootstrapToken_IsExpired(t *testing.T) {
	token := &ForceBootstrapToken{
		IssuedAt:  time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	}

	if !token.IsExpired() {
		t.Error("Expected token to be expired")
	}

	token.ExpiresAt = time.Now().Add(time.Hour)
	if token.IsExpired() {
		t.Error("Expected token to not be expired")
	}
}

func TestForceBootstrapToken_IsUsed(t *testing.T) {
	token := &ForceBootstrapToken{}

	if token.IsUsed() {
		t.Error("Expected token to not be used")
	}

	token.UsedAt = time.Now()
	if !token.IsUsed() {
		t.Error("Expected token to be used")
	}
}

func TestForceBootstrapToken_ValidateVersion(t *testing.T) {
	tests := []struct {
		name      string
		version   uint32
		expectErr bool
		errMsg    string
	}{
		{
			name:      "version 0 deprecated",
			version:   0,
			expectErr: true,
			errMsg:    "deprecated",
		},
		{
			name:      "current version valid",
			version:   ForceBootstrapTokenVersion,
			expectErr: false,
		},
		{
			name:      "future version unsupported",
			version:   ForceBootstrapTokenVersion + 1,
			expectErr: true,
			errMsg:    "unsupported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := &ForceBootstrapToken{Version: tt.version}
			err := token.ValidateVersion()

			if tt.expectErr && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestComputeTokenHash(t *testing.T) {
	hash1 := ComputeTokenHash("token-abc-123")
	hash2 := ComputeTokenHash("token-abc-123")
	hash3 := ComputeTokenHash("token-xyz-456")

	if hash1 != hash2 {
		t.Error("Expected same input to produce same hash")
	}

	if hash1 == hash3 {
		t.Error("Expected different input to produce different hash")
	}

	if len(hash1) != 64 {
		t.Errorf("Expected hash length 64, got %d", len(hash1))
	}
}

func TestVerifyForceBootstrapTokenSignature(t *testing.T) {
	secrets := NewMockSecretsManager()
	key := []byte("test-fencing-key-32-bytes-long!!")
	secrets.SetFencingKey(key)

	generator := NewForceBootstrapTokenGenerator("", nil, secrets, nil)

	token, err := generator.Generate(
		context.Background(),
		"cluster-uuid",
		"",
		1,
		"test",
		"admin",
		time.Hour,
		&ForceTokenOptions{
			VoterSetHash:        []byte("hash"),
			CommittedVoterCount: 3,
			AcknowledgeFork:     true,
		},
	)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if !VerifyForceBootstrapTokenSignature(token, key) {
		t.Error("Expected signature to be valid")
	}

	wrongKey := []byte("wrong-key-that-is-also-32-bytes!")
	if VerifyForceBootstrapTokenSignature(token, wrongKey) {
		t.Error("Expected signature to be invalid with wrong key")
	}
}

func TestForceBootstrapTokenGenerator_RequireDedicatedKey(t *testing.T) {
	secrets := NewMockSecretsManager()
	secrets.SetFencingKey([]byte("fencing-key-32-bytes-long-here!"))

	config := &ForceBootstrapConfig{
		RequireDedicatedKey: true,
	}

	generator := NewForceBootstrapTokenGenerator("", nil, secrets, config)

	_, err := generator.Generate(
		context.Background(),
		"cluster-uuid",
		"",
		1,
		"test",
		"admin",
		time.Hour,
		&ForceTokenOptions{
			VoterSetHash: []byte("hash"),
		},
	)

	if err == nil {
		t.Error("Expected error when RequireDedicatedKey is true and no dedicated key")
	}

	if !errors.Is(err, ErrDedicatedKeyRequired) {
		t.Errorf("Expected ErrDedicatedKeyRequired, got %v", err)
	}

	secrets.SetForceBootstrapKey([]byte("dedicated-force-key-32-bytes!!!"))

	token, err := generator.Generate(
		context.Background(),
		"cluster-uuid",
		"",
		1,
		"test",
		"admin",
		time.Hour,
		&ForceTokenOptions{
			VoterSetHash: []byte("hash"),
		},
	)

	if err != nil {
		t.Fatalf("Generate should succeed with dedicated key: %v", err)
	}

	if token.Signature == "" {
		t.Error("Expected signature to be set")
	}
}
