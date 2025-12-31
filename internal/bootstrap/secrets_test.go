package bootstrap

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSecretsManager_LoadSecrets(t *testing.T) {
	tempDir := t.TempDir()

	fencingKeyPath := filepath.Join(tempDir, "fencing.key")
	adminTokenPath := filepath.Join(tempDir, "admin.token")
	wipeKeyPath := filepath.Join(tempDir, "wipe.key")

	if err := os.WriteFile(fencingKeyPath, []byte("fencing-key-content\n"), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.WriteFile(adminTokenPath, []byte("admin-token-content  \n"), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.WriteFile(wipeKeyPath, []byte("wipe-key-content"), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	config := SecretsConfig{
		FencingKeyFile: fencingKeyPath,
		AdminTokenFile: adminTokenPath,
		WipeKeyFile:    wipeKeyPath,
	}

	sm := NewSecretsManager(config, nil)

	if err := sm.LoadSecrets(); err != nil {
		t.Fatalf("LoadSecrets() error = %v", err)
	}

	if string(sm.FencingKey()) != "fencing-key-content" {
		t.Errorf("FencingKey() = %q, want %q", string(sm.FencingKey()), "fencing-key-content")
	}
	if string(sm.AdminToken()) != "admin-token-content" {
		t.Errorf("AdminToken() = %q, want %q", string(sm.AdminToken()), "admin-token-content")
	}
	if string(sm.WipeKey()) != "wipe-key-content" {
		t.Errorf("WipeKey() = %q, want %q", string(sm.WipeKey()), "wipe-key-content")
	}

	if !sm.HasFencingKey() {
		t.Error("HasFencingKey() = false, want true")
	}
	if !sm.HasAdminToken() {
		t.Error("HasAdminToken() = false, want true")
	}
	if !sm.HasWipeKey() {
		t.Error("HasWipeKey() = false, want true")
	}
}

func TestSecretsManager_MissingFile(t *testing.T) {
	config := SecretsConfig{
		FencingKeyFile: "/nonexistent/path/fencing.key",
	}

	sm := NewSecretsManager(config, nil)

	if err := sm.LoadSecrets(); err == nil {
		t.Error("LoadSecrets() should fail for missing file")
	}
}

func TestSecretsManager_EmptyConfig(t *testing.T) {
	config := SecretsConfig{}

	sm := NewSecretsManager(config, nil)

	if err := sm.LoadSecrets(); err != nil {
		t.Errorf("LoadSecrets() error = %v, want nil for empty config", err)
	}

	if sm.HasFencingKey() {
		t.Error("HasFencingKey() = true, want false for empty config")
	}
	if sm.HasAdminToken() {
		t.Error("HasAdminToken() = true, want false for empty config")
	}
	if sm.HasWipeKey() {
		t.Error("HasWipeKey() = true, want false for empty config")
	}
}

func TestSecretsManager_PartialConfig(t *testing.T) {
	tempDir := t.TempDir()

	fencingKeyPath := filepath.Join(tempDir, "fencing.key")
	if err := os.WriteFile(fencingKeyPath, []byte("fencing-key"), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	config := SecretsConfig{
		FencingKeyFile: fencingKeyPath,
	}

	sm := NewSecretsManager(config, nil)

	if err := sm.LoadSecrets(); err != nil {
		t.Errorf("LoadSecrets() error = %v, want nil", err)
	}

	if !sm.HasFencingKey() {
		t.Error("HasFencingKey() = false, want true")
	}
	if sm.HasAdminToken() {
		t.Error("HasAdminToken() = true, want false")
	}
	if sm.HasWipeKey() {
		t.Error("HasWipeKey() = true, want false")
	}
}

func TestSecretsManager_SetKeys(t *testing.T) {
	config := SecretsConfig{}
	sm := NewSecretsManager(config, nil)

	if sm.HasFencingKey() {
		t.Error("HasFencingKey() = true before SetFencingKey()")
	}

	sm.SetFencingKey([]byte("new-fencing-key"))

	if !sm.HasFencingKey() {
		t.Error("HasFencingKey() = false after SetFencingKey()")
	}
	if string(sm.FencingKey()) != "new-fencing-key" {
		t.Errorf("FencingKey() = %q, want %q", string(sm.FencingKey()), "new-fencing-key")
	}

	sm.SetAdminToken([]byte("new-admin-token"))
	if string(sm.AdminToken()) != "new-admin-token" {
		t.Errorf("AdminToken() = %q, want %q", string(sm.AdminToken()), "new-admin-token")
	}

	sm.SetWipeKey([]byte("new-wipe-key"))
	if string(sm.WipeKey()) != "new-wipe-key" {
		t.Errorf("WipeKey() = %q, want %q", string(sm.WipeKey()), "new-wipe-key")
	}
}

func TestSecretsManager_WhitespaceTrimming(t *testing.T) {
	tempDir := t.TempDir()

	keyPath := filepath.Join(tempDir, "key")
	if err := os.WriteFile(keyPath, []byte("  key-with-whitespace  \n\n"), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	config := SecretsConfig{
		FencingKeyFile: keyPath,
	}

	sm := NewSecretsManager(config, nil)

	if err := sm.LoadSecrets(); err != nil {
		t.Fatalf("LoadSecrets() error = %v", err)
	}

	if string(sm.FencingKey()) != "key-with-whitespace" {
		t.Errorf("FencingKey() = %q, want %q (whitespace should be trimmed)", string(sm.FencingKey()), "key-with-whitespace")
	}
}
