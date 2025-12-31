package bootstrap

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const (
	ForceBootstrapTokenVersion          = 3
	IgnoreQuorumCheckConfirmationPhrase = "I_ACCEPT_SPLIT_BRAIN_RISK"
)

type ForceBootstrapToken struct {
	Version                  uint32    `json:"version"`
	Token                    string    `json:"token"`
	ClusterUUID              string    `json:"cluster_uuid"`
	PreviousClusterUUID      string    `json:"previous_cluster_uuid,omitempty"`
	BoundFencingEpoch        uint64    `json:"bound_fencing_epoch"`
	VoterSetHash             []byte    `json:"voter_set_hash"`
	CommittedVoterCount      int       `json:"committed_voter_count"`
	Reason                   string    `json:"reason"`
	AcknowledgeFork          bool      `json:"acknowledge_fork"`
	IssuedAt                 time.Time `json:"issued_at"`
	ExpiresAt                time.Time `json:"expires_at"`
	IssuedBy                 string    `json:"issued_by"`
	TargetOrdinal            int       `json:"target_ordinal"`
	SingleUse                bool      `json:"single_use"`
	DisasterRecoveryMode     bool      `json:"disaster_recovery_mode"`
	IgnoreQuorumCheck        bool      `json:"ignore_quorum_check"`
	ConfirmIgnoreQuorumCheck string    `json:"confirm_ignore_quorum_check,omitempty"`
	UsedAt                   time.Time `json:"used_at,omitempty"`
	UsedBy                   string    `json:"used_by,omitempty"`
	Signature                string    `json:"signature"`
}

type ForceTokenOptions struct {
	ProbeCluster             bool
	AcknowledgeFork          bool
	DisasterRecoveryMode     bool
	IgnoreQuorumCheck        bool
	ConfirmIgnoreQuorumCheck string
	VoterSetHash             []byte
	CommittedVoterCount      int
	TargetOrdinal            int
	SingleUse                bool
}

type ForceBootstrapTokenGenerator struct {
	keyFile   string
	transport BootstrapTransport
	secrets   *SecretsManager
	config    *ForceBootstrapConfig
}

func NewForceBootstrapTokenGenerator(
	keyFile string,
	transport BootstrapTransport,
	secrets *SecretsManager,
	config *ForceBootstrapConfig,
) *ForceBootstrapTokenGenerator {
	return &ForceBootstrapTokenGenerator{
		keyFile:   keyFile,
		transport: transport,
		secrets:   secrets,
		config:    config,
	}
}

func (g *ForceBootstrapTokenGenerator) Generate(
	ctx context.Context,
	clusterUUID string,
	previousClusterUUID string,
	currentEpoch uint64,
	reason string,
	issuedBy string,
	ttl time.Duration,
	opts *ForceTokenOptions,
) (*ForceBootstrapToken, error) {
	if opts == nil {
		opts = &ForceTokenOptions{}
	}

	if len(opts.VoterSetHash) == 0 && !opts.DisasterRecoveryMode {
		return nil, ErrMissingVoterSetHash
	}

	if opts.IgnoreQuorumCheck && opts.ConfirmIgnoreQuorumCheck != IgnoreQuorumCheckConfirmationPhrase {
		return nil, ErrIgnoreQuorumNotConfirmed
	}

	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("failed to generate token: %w", err)
	}

	now := time.Now()
	token := &ForceBootstrapToken{
		Version:                  ForceBootstrapTokenVersion,
		Token:                    hex.EncodeToString(tokenBytes),
		ClusterUUID:              clusterUUID,
		PreviousClusterUUID:      previousClusterUUID,
		BoundFencingEpoch:        currentEpoch,
		VoterSetHash:             opts.VoterSetHash,
		CommittedVoterCount:      opts.CommittedVoterCount,
		Reason:                   reason,
		AcknowledgeFork:          opts.AcknowledgeFork,
		IssuedAt:                 now,
		ExpiresAt:                now.Add(ttl),
		IssuedBy:                 issuedBy,
		TargetOrdinal:            opts.TargetOrdinal,
		SingleUse:                opts.SingleUse,
		DisasterRecoveryMode:     opts.DisasterRecoveryMode,
		IgnoreQuorumCheck:        opts.IgnoreQuorumCheck,
		ConfirmIgnoreQuorumCheck: opts.ConfirmIgnoreQuorumCheck,
	}

	key, err := g.getSigningKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get signing key: %w", err)
	}

	token.Signature = g.signToken(token, key)
	return token, nil
}

func (g *ForceBootstrapTokenGenerator) getSigningKey() ([]byte, error) {
	if g.secrets != nil && g.secrets.HasForceBootstrapKey() {
		return g.secrets.ForceBootstrapKey(), nil
	}

	if g.config != nil && g.config.RequireDedicatedKey {
		return nil, ErrDedicatedKeyRequired
	}

	if g.secrets != nil && g.secrets.HasFencingKey() {
		return g.secrets.FencingKey(), nil
	}
	return nil, ErrNoFencingKey
}

func (g *ForceBootstrapTokenGenerator) signToken(token *ForceBootstrapToken, key []byte) string {
	data := g.tokenSignatureData(token)
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func (g *ForceBootstrapTokenGenerator) VerifyTokenSignature(token *ForceBootstrapToken, key []byte) bool {
	expectedSig := g.signToken(token, key)
	return hmac.Equal([]byte(token.Signature), []byte(expectedSig))
}

func (g *ForceBootstrapTokenGenerator) tokenSignatureData(token *ForceBootstrapToken) []byte {
	data := struct {
		Version                  uint32    `json:"version"`
		Token                    string    `json:"token"`
		ClusterUUID              string    `json:"cluster_uuid"`
		PreviousClusterUUID      string    `json:"previous_cluster_uuid,omitempty"`
		BoundFencingEpoch        uint64    `json:"bound_fencing_epoch"`
		VoterSetHash             []byte    `json:"voter_set_hash"`
		CommittedVoterCount      int       `json:"committed_voter_count"`
		Reason                   string    `json:"reason"`
		AcknowledgeFork          bool      `json:"acknowledge_fork"`
		IssuedAt                 time.Time `json:"issued_at"`
		ExpiresAt                time.Time `json:"expires_at"`
		IssuedBy                 string    `json:"issued_by"`
		TargetOrdinal            int       `json:"target_ordinal"`
		SingleUse                bool      `json:"single_use"`
		DisasterRecoveryMode     bool      `json:"disaster_recovery_mode"`
		IgnoreQuorumCheck        bool      `json:"ignore_quorum_check"`
		ConfirmIgnoreQuorumCheck string    `json:"confirm_ignore_quorum_check,omitempty"`
	}{
		Version:                  token.Version,
		Token:                    token.Token,
		ClusterUUID:              token.ClusterUUID,
		PreviousClusterUUID:      token.PreviousClusterUUID,
		BoundFencingEpoch:        token.BoundFencingEpoch,
		VoterSetHash:             token.VoterSetHash,
		CommittedVoterCount:      token.CommittedVoterCount,
		Reason:                   token.Reason,
		AcknowledgeFork:          token.AcknowledgeFork,
		IssuedAt:                 token.IssuedAt,
		ExpiresAt:                token.ExpiresAt,
		IssuedBy:                 token.IssuedBy,
		TargetOrdinal:            token.TargetOrdinal,
		SingleUse:                token.SingleUse,
		DisasterRecoveryMode:     token.DisasterRecoveryMode,
		IgnoreQuorumCheck:        token.IgnoreQuorumCheck,
		ConfirmIgnoreQuorumCheck: token.ConfirmIgnoreQuorumCheck,
	}

	jsonData, _ := json.Marshal(data)
	return jsonData
}

func (t *ForceBootstrapToken) IsExpired() bool {
	return time.Now().After(t.ExpiresAt)
}

func (t *ForceBootstrapToken) IsUsed() bool {
	return !t.UsedAt.IsZero()
}

func (t *ForceBootstrapToken) ValidateVersion() error {
	if t.Version == 0 {
		return ErrDeprecatedTokenVersion
	}
	if t.Version != ForceBootstrapTokenVersion {
		return fmt.Errorf("unsupported token version: %d (expected %d)", t.Version, ForceBootstrapTokenVersion)
	}
	return nil
}

func ComputeTokenHash(tokenStr string) string {
	hash := sha256.Sum256([]byte(tokenStr))
	return hex.EncodeToString(hash[:])
}

func VerifyForceBootstrapTokenSignature(token *ForceBootstrapToken, key []byte) bool {
	gen := &ForceBootstrapTokenGenerator{}
	return gen.VerifyTokenSignature(token, key)
}

func (g *ForceBootstrapTokenGenerator) Validate(ctx context.Context, tokenJSON string) (*ForceBootstrapToken, error) {
	var token ForceBootstrapToken
	if err := json.Unmarshal([]byte(tokenJSON), &token); err != nil {
		return nil, fmt.Errorf("invalid token format: %w", err)
	}

	if err := token.ValidateVersion(); err != nil {
		return nil, err
	}

	if token.IsExpired() {
		return nil, ErrTokenExpired
	}

	if token.SingleUse && token.IsUsed() {
		return nil, ErrForceBootstrapAlreadyUsed
	}

	key, err := g.getSigningKey()
	if err != nil {
		return nil, fmt.Errorf("cannot validate signature: %w", err)
	}

	if !g.VerifyTokenSignature(&token, key) {
		return nil, ErrInvalidForceBootstrapSignature
	}

	return &token, nil
}
