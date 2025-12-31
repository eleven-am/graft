package bootstrap

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"sync"
)

type SecretsManager struct {
	config SecretsConfig
	logger *slog.Logger
	mu     sync.RWMutex

	fencingKey        []byte
	adminToken        []byte
	wipeKey           []byte
	forceBootstrapKey []byte
}

func NewSecretsManager(config SecretsConfig, logger *slog.Logger) *SecretsManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &SecretsManager{
		config: config,
		logger: logger,
	}
}

func (s *SecretsManager) LoadSecrets() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.config.FencingKeyFile != "" {
		key, err := s.loadKeyFile(s.config.FencingKeyFile)
		if err != nil {
			return fmt.Errorf("load fencing key: %w", err)
		}
		s.fencingKey = key
		s.logger.Debug("loaded fencing key", "path", s.config.FencingKeyFile)
	}

	if s.config.AdminTokenFile != "" {
		token, err := s.loadKeyFile(s.config.AdminTokenFile)
		if err != nil {
			return fmt.Errorf("load admin token: %w", err)
		}
		s.adminToken = token
		s.logger.Debug("loaded admin token", "path", s.config.AdminTokenFile)
	}

	if s.config.WipeKeyFile != "" {
		key, err := s.loadKeyFile(s.config.WipeKeyFile)
		if err != nil {
			return fmt.Errorf("load wipe key: %w", err)
		}
		s.wipeKey = key
		s.logger.Debug("loaded wipe key", "path", s.config.WipeKeyFile)
	}

	if s.config.ForceBootstrapKeyFile != "" {
		key, err := s.loadKeyFile(s.config.ForceBootstrapKeyFile)
		if err != nil {
			return fmt.Errorf("load force bootstrap key: %w", err)
		}
		s.forceBootstrapKey = key
		s.logger.Debug("loaded force bootstrap key", "path", s.config.ForceBootstrapKeyFile)
	}

	return nil
}

func (s *SecretsManager) loadKeyFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(data), nil
}

func (s *SecretsManager) FencingKey() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fencingKey
}

func (s *SecretsManager) AdminToken() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.adminToken
}

func (s *SecretsManager) WipeKey() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.wipeKey
}

func (s *SecretsManager) HasFencingKey() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.fencingKey) > 0
}

func (s *SecretsManager) HasAdminToken() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.adminToken) > 0
}

func (s *SecretsManager) HasWipeKey() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.wipeKey) > 0
}

func (s *SecretsManager) SetFencingKey(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fencingKey = key
}

func (s *SecretsManager) SetAdminToken(token []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adminToken = token
}

func (s *SecretsManager) SetWipeKey(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wipeKey = key
}

func (s *SecretsManager) ForceBootstrapKey() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.forceBootstrapKey
}

func (s *SecretsManager) HasForceBootstrapKey() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.forceBootstrapKey) > 0
}

func (s *SecretsManager) SetForceBootstrapKey(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forceBootstrapKey = key
}
