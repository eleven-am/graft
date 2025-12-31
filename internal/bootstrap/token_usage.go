package bootstrap

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TokenUsageNotificationVersion = 1

type TokenUsageNotification struct {
	Version   uint32    `json:"version"`
	TokenHash string    `json:"token_hash"`
	UsedAt    time.Time `json:"used_at"`
	UsedBy    string    `json:"used_by"`
	NewEpoch  uint64    `json:"new_epoch"`
	Timestamp time.Time `json:"timestamp"`
	SenderID  string    `json:"sender_id"`
	Signature string    `json:"signature"`
}

type TokenUsageRecord struct {
	TokenHash string    `json:"token_hash"`
	UsedAt    time.Time `json:"used_at"`
	UsedBy    string    `json:"used_by"`
}

type TokenUsageResponse struct {
	Used         bool                    `json:"used"`
	UsedBy       string                  `json:"used_by,omitempty"`
	UsedAt       time.Time               `json:"used_at,omitempty"`
	TokenHash    string                  `json:"token_hash,omitempty"`
	ResponderID  string                  `json:"responder_id,omitempty"`
	Timestamp    time.Time               `json:"timestamp,omitempty"`
	Signature    string                  `json:"signature,omitempty"`
	Notification *TokenUsageNotification `json:"notification,omitempty"`
}

type TokenUsageTrackerDeps struct {
	DataDir   string
	Fencing   *FencingManager
	Transport BootstrapTransport
	Discovery PeerDiscovery
	Meta      *ClusterMeta
	Config    *BootstrapConfig
	Secrets   *SecretsManager
	Logger    *slog.Logger
}

type TokenUsageTracker struct {
	dataDir   string
	fencing   *FencingManager
	transport BootstrapTransport
	discovery PeerDiscovery
	meta      *ClusterMeta
	config    *BootstrapConfig
	secrets   *SecretsManager
	logger    *slog.Logger

	mu     sync.RWMutex
	usages map[string]*TokenUsageRecord
}

func NewTokenUsageTracker(deps TokenUsageTrackerDeps) *TokenUsageTracker {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &TokenUsageTracker{
		dataDir:   deps.DataDir,
		fencing:   deps.Fencing,
		transport: deps.Transport,
		discovery: deps.Discovery,
		meta:      deps.Meta,
		config:    deps.Config,
		secrets:   deps.Secrets,
		logger:    logger,
		usages:    make(map[string]*TokenUsageRecord),
	}
}

func (t *TokenUsageTracker) CheckClusterWideUsage(
	ctx context.Context,
	token *ForceBootstrapToken,
) (bool, string, time.Time, error) {
	tokenHash := ComputeTokenHash(token.Token)

	used, usedBy, usedAt := t.CheckLocalUsage(tokenHash)
	if used {
		return true, usedBy, usedAt, nil
	}

	if t.fencing != nil {
		currentEpoch := t.fencing.CurrentEpoch()
		if currentEpoch > token.BoundFencingEpoch {
			return true, "epoch_advanced", time.Time{}, nil
		}
	}

	if t.transport == nil || t.discovery == nil || t.config == nil {
		return false, "", time.Time{}, nil
	}

	myOrdinal := 0
	if t.meta != nil {
		myOrdinal = t.meta.Ordinal
	}

	for ordinal := 0; ordinal < t.config.ExpectedNodes; ordinal++ {
		if ordinal == myOrdinal {
			continue
		}

		addr := t.discovery.AddressForOrdinal(ordinal)
		if addr == "" {
			continue
		}

		resp, err := t.queryPeerUsage(ctx, string(addr), tokenHash)
		if err != nil {
			t.logger.Debug("failed to query peer for token usage",
				"ordinal", ordinal,
				"error", err,
			)
			continue
		}

		if resp != nil && resp.Used {
			return true, resp.UsedBy, resp.UsedAt, nil
		}
	}

	return false, "", time.Time{}, nil
}

func (t *TokenUsageTracker) queryPeerUsage(
	ctx context.Context,
	addr string,
	tokenHash string,
) (*TokenUsageResponse, error) {
	if t.transport == nil {
		return nil, fmt.Errorf("transport not configured")
	}

	resp, err := t.transport.GetTokenUsage(ctx, addr, tokenHash)
	if err != nil {
		return nil, err
	}

	if resp != nil && t.secrets != nil && t.secrets.HasFencingKey() {
		if !t.verifyUsageResponse(resp, t.secrets.FencingKey()) {
			t.logger.Warn("received usage response with invalid signature",
				"addr", addr,
				"responder", resp.ResponderID,
				"token_hash_prefix", tokenHash[:min(16, len(tokenHash))]+"...",
			)
			return nil, fmt.Errorf("invalid usage response signature from %s", addr)
		}
	}

	return resp, nil
}

func (t *TokenUsageTracker) CheckLocalUsage(tokenHash string) (bool, string, time.Time) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	record, ok := t.usages[tokenHash]
	if !ok {
		return false, "", time.Time{}
	}

	return true, record.UsedBy, record.UsedAt
}

func (t *TokenUsageTracker) PersistUsage(token *ForceBootstrapToken) error {
	tokenHash := ComputeTokenHash(token.Token)

	record := &TokenUsageRecord{
		TokenHash: tokenHash,
		UsedAt:    token.UsedAt,
		UsedBy:    token.UsedBy,
	}

	t.mu.Lock()
	t.usages[tokenHash] = record
	t.mu.Unlock()

	if t.dataDir == "" {
		return nil
	}

	usageDir := filepath.Join(t.dataDir, "token_usage")
	if err := os.MkdirAll(usageDir, 0700); err != nil {
		return fmt.Errorf("create usage dir: %w", err)
	}

	usageFile := filepath.Join(usageDir, tokenHash+".json")
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal usage record: %w", err)
	}

	f, err := os.OpenFile(usageFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("open usage file: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write usage file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync usage file: %w", err)
	}

	t.logger.Info("persisted token usage",
		"token_hash", tokenHash[:16]+"...",
		"used_by", record.UsedBy,
	)

	return nil
}

func (t *TokenUsageTracker) BroadcastUsage(
	ctx context.Context,
	token *ForceBootstrapToken,
	peers []PeerInfo,
) error {
	if t.transport == nil {
		return fmt.Errorf("transport not configured")
	}

	tokenHash := ComputeTokenHash(token.Token)
	notification := &TokenUsageNotification{
		Version:   TokenUsageNotificationVersion,
		TokenHash: tokenHash,
		UsedAt:    token.UsedAt,
		UsedBy:    token.UsedBy,
		NewEpoch:  token.BoundFencingEpoch + 1,
		Timestamp: time.Now(),
		SenderID:  string(t.meta.ServerID),
	}

	if t.secrets != nil && t.secrets.HasFencingKey() {
		notification.Signature = t.signUsageNotification(notification, t.secrets.FencingKey())
	}

	var broadcastErrors []error
	for _, peer := range peers {
		if err := t.transport.NotifyTokenUsage(ctx, string(peer.Address), notification); err != nil {
			t.logger.Warn("failed to notify peer of token usage",
				"peer", peer.ServerID,
				"error", err,
			)
			broadcastErrors = append(broadcastErrors, err)
		} else {
			t.logger.Debug("notified peer of token usage",
				"peer", peer.ServerID,
				"token_hash", tokenHash[:16]+"...",
			)
		}
	}

	if len(broadcastErrors) == len(peers) && len(peers) > 0 {
		return fmt.Errorf("failed to notify any peers of token usage")
	}

	return nil
}

func (t *TokenUsageTracker) HandleUsageNotification(notification *TokenUsageNotification) error {
	if notification.Version != TokenUsageNotificationVersion {
		return fmt.Errorf("unsupported notification version: %d", notification.Version)
	}

	if t.secrets != nil && t.secrets.HasFencingKey() {
		if !t.verifyUsageNotification(notification, t.secrets.FencingKey()) {
			t.logger.Warn("received usage notification with invalid signature",
				"sender", notification.SenderID,
				"token_hash", notification.TokenHash[:16]+"...",
			)
			return fmt.Errorf("invalid notification signature")
		}
	}

	record := &TokenUsageRecord{
		TokenHash: notification.TokenHash,
		UsedAt:    notification.UsedAt,
		UsedBy:    notification.UsedBy,
	}

	t.mu.Lock()
	t.usages[notification.TokenHash] = record
	t.mu.Unlock()

	t.logger.Info("recorded token usage from notification",
		"token_hash", notification.TokenHash[:16]+"...",
		"used_by", notification.UsedBy,
		"new_epoch", notification.NewEpoch,
	)

	return nil
}

func (t *TokenUsageTracker) GetUsage(tokenHash string) *TokenUsageResponse {
	used, usedBy, usedAt := t.CheckLocalUsage(tokenHash)

	responderID := ""
	if t.meta != nil {
		responderID = string(t.meta.ServerID)
	}

	resp := &TokenUsageResponse{
		Used:        used,
		TokenHash:   tokenHash,
		ResponderID: responderID,
		Timestamp:   time.Now(),
	}

	if used {
		resp.UsedBy = usedBy
		resp.UsedAt = usedAt
	}

	if t.secrets != nil && t.secrets.HasFencingKey() {
		resp.Signature = t.signUsageResponse(resp, t.secrets.FencingKey())
	}

	return resp
}

func (t *TokenUsageTracker) LoadPersistedUsages() error {
	if t.dataDir == "" {
		return nil
	}

	usageDir := filepath.Join(t.dataDir, "token_usage")
	entries, err := os.ReadDir(usageDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read usage dir: %w", err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		usageFile := filepath.Join(usageDir, entry.Name())
		data, err := os.ReadFile(usageFile)
		if err != nil {
			t.logger.Warn("failed to read usage file",
				"file", entry.Name(),
				"error", err,
			)
			continue
		}

		var record TokenUsageRecord
		if err := json.Unmarshal(data, &record); err != nil {
			t.logger.Warn("failed to unmarshal usage file",
				"file", entry.Name(),
				"error", err,
			)
			continue
		}

		t.usages[record.TokenHash] = &record
	}

	t.logger.Debug("loaded persisted token usages", "count", len(t.usages))
	return nil
}

func (t *TokenUsageTracker) signUsageNotification(notification *TokenUsageNotification, key []byte) string {
	data := t.notificationSignatureData(notification)
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func (t *TokenUsageTracker) verifyUsageNotification(notification *TokenUsageNotification, key []byte) bool {
	expectedSig := t.signUsageNotification(notification, key)
	return hmac.Equal([]byte(notification.Signature), []byte(expectedSig))
}

func (t *TokenUsageTracker) notificationSignatureData(notification *TokenUsageNotification) []byte {
	data := struct {
		Version   uint32    `json:"version"`
		TokenHash string    `json:"token_hash"`
		UsedAt    time.Time `json:"used_at"`
		UsedBy    string    `json:"used_by"`
		NewEpoch  uint64    `json:"new_epoch"`
		Timestamp time.Time `json:"timestamp"`
		SenderID  string    `json:"sender_id"`
	}{
		Version:   notification.Version,
		TokenHash: notification.TokenHash,
		UsedAt:    notification.UsedAt,
		UsedBy:    notification.UsedBy,
		NewEpoch:  notification.NewEpoch,
		Timestamp: notification.Timestamp,
		SenderID:  notification.SenderID,
	}

	jsonData, _ := json.Marshal(data)
	return jsonData
}

func (t *TokenUsageTracker) signUsageResponse(resp *TokenUsageResponse, key []byte) string {
	return SignUsageResponse(resp, key)
}

func (t *TokenUsageTracker) verifyUsageResponse(resp *TokenUsageResponse, key []byte) bool {
	return VerifyUsageResponse(resp, key)
}

func SignUsageResponse(resp *TokenUsageResponse, key []byte) string {
	data := usageResponseSignatureData(resp)
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifyUsageResponse(resp *TokenUsageResponse, key []byte) bool {
	if resp.Signature == "" {
		return false
	}
	expectedSig := SignUsageResponse(resp, key)
	return hmac.Equal([]byte(resp.Signature), []byte(expectedSig))
}

func usageResponseSignatureData(resp *TokenUsageResponse) []byte {
	data := struct {
		Used        bool      `json:"used"`
		UsedBy      string    `json:"used_by"`
		UsedAt      time.Time `json:"used_at"`
		TokenHash   string    `json:"token_hash"`
		ResponderID string    `json:"responder_id"`
		Timestamp   time.Time `json:"timestamp"`
	}{
		Used:        resp.Used,
		UsedBy:      resp.UsedBy,
		UsedAt:      resp.UsedAt,
		TokenHash:   resp.TokenHash,
		ResponderID: resp.ResponderID,
		Timestamp:   resp.Timestamp,
	}

	jsonData, _ := json.Marshal(data)
	return jsonData
}
