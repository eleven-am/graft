package bootstrap

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestFencingToken_Validate(t *testing.T) {
	validToken := func() *FencingToken {
		return &FencingToken{
			Version:      TokenVersion,
			Epoch:        1,
			HolderID:     raft.ServerID("node-0"),
			VoterSetHash: [32]byte{1, 2, 3},
			QuorumSize:   2,
			Mode:         FencingModePrimary,
			AcquiredAt:   time.Now(),
		}
	}

	t.Run("valid token", func(t *testing.T) {
		tok := validToken()
		if err := tok.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("missing version", func(t *testing.T) {
		tok := validToken()
		tok.Version = 0
		if err := tok.Validate(); err == nil {
			t.Error("Validate() should fail for missing version")
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		tok := validToken()
		tok.Version = TokenVersion + 1
		if err := tok.Validate(); err == nil {
			t.Error("Validate() should fail for unsupported version")
		}
	})

	t.Run("missing epoch", func(t *testing.T) {
		tok := validToken()
		tok.Epoch = 0
		if err := tok.Validate(); err == nil {
			t.Error("Validate() should fail for missing epoch")
		}
	})

	t.Run("missing holder_id", func(t *testing.T) {
		tok := validToken()
		tok.HolderID = ""
		if err := tok.Validate(); err == nil {
			t.Error("Validate() should fail for missing holder_id")
		}
	})

	t.Run("invalid quorum_size", func(t *testing.T) {
		tok := validToken()
		tok.QuorumSize = 0
		if err := tok.Validate(); err == nil {
			t.Error("Validate() should fail for invalid quorum_size")
		}
	})

	t.Run("invalid mode", func(t *testing.T) {
		tok := validToken()
		tok.Mode = "invalid"
		if err := tok.Validate(); err == nil {
			t.Error("Validate() should fail for invalid mode")
		}
	})

	t.Run("missing acquired_at", func(t *testing.T) {
		tok := validToken()
		tok.AcquiredAt = time.Time{}
		if err := tok.Validate(); err == nil {
			t.Error("Validate() should fail for missing acquired_at")
		}
	})
}

func TestFencingToken_ComputeChecksum(t *testing.T) {
	tok := &FencingToken{
		Version:      TokenVersion,
		Epoch:        1,
		HolderID:     raft.ServerID("node-0"),
		VoterSetHash: [32]byte{1, 2, 3},
		QuorumSize:   2,
		Mode:         FencingModePrimary,
		AcquiredAt:   time.Unix(1000000000, 0),
	}

	checksum1 := tok.ComputeChecksum()
	checksum2 := tok.ComputeChecksum()

	if checksum1 != checksum2 {
		t.Errorf("ComputeChecksum() not deterministic: %08x != %08x", checksum1, checksum2)
	}

	tok.Epoch = 2
	checksum3 := tok.ComputeChecksum()

	if checksum1 == checksum3 {
		t.Error("ComputeChecksum() should change when fields change")
	}
}

func TestFencingToken_VerifyChecksum(t *testing.T) {
	tok := &FencingToken{
		Version:      TokenVersion,
		Epoch:        1,
		HolderID:     raft.ServerID("node-0"),
		VoterSetHash: [32]byte{1, 2, 3},
		QuorumSize:   2,
		Mode:         FencingModePrimary,
		AcquiredAt:   time.Unix(1000000000, 0),
	}

	tok.UpdateChecksum()

	if !tok.VerifyChecksum() {
		t.Error("VerifyChecksum() should return true after UpdateChecksum()")
	}

	tok.Checksum = tok.Checksum + 1

	if tok.VerifyChecksum() {
		t.Error("VerifyChecksum() should return false for invalid checksum")
	}
}

func TestFencingToken_IsExpired(t *testing.T) {
	t.Run("no expiry", func(t *testing.T) {
		tok := &FencingToken{
			AcquiredAt: time.Now(),
		}
		if tok.IsExpired() {
			t.Error("IsExpired() should return false for token without expiry")
		}
	})

	t.Run("not expired", func(t *testing.T) {
		tok := &FencingToken{
			AcquiredAt: time.Now(),
			ExpiresAt:  time.Now().Add(time.Hour),
		}
		if tok.IsExpired() {
			t.Error("IsExpired() should return false for token not yet expired")
		}
	})

	t.Run("expired", func(t *testing.T) {
		tok := &FencingToken{
			AcquiredAt: time.Now().Add(-2 * time.Hour),
			ExpiresAt:  time.Now().Add(-time.Hour),
		}
		if !tok.IsExpired() {
			t.Error("IsExpired() should return true for expired token")
		}
	})
}

func TestFencingToken_MarshalUnmarshal(t *testing.T) {
	original := &FencingToken{
		Version:      TokenVersion,
		Epoch:        42,
		HolderID:     raft.ServerID("node-0"),
		VoterSetHash: [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		QuorumSize:   3,
		Mode:         FencingModeFallback,
		AcquiredAt:   time.Unix(1000000000, 123456789),
		ExpiresAt:    time.Unix(2000000000, 987654321),
		Signature:    []byte("test-signature"),
	}
	original.UpdateChecksum()

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}

	if string(data[0:4]) != TokenMagicBytes {
		t.Errorf("MarshalBinary() magic bytes = %q, want %q", string(data[0:4]), TokenMagicBytes)
	}

	if data[4] != TokenVersion {
		t.Errorf("MarshalBinary() version = %d, want %d", data[4], TokenVersion)
	}

	var decoded FencingToken
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}

	if decoded.Version != original.Version {
		t.Errorf("Version = %d, want %d", decoded.Version, original.Version)
	}
	if decoded.Epoch != original.Epoch {
		t.Errorf("Epoch = %d, want %d", decoded.Epoch, original.Epoch)
	}
	if decoded.HolderID != original.HolderID {
		t.Errorf("HolderID = %s, want %s", decoded.HolderID, original.HolderID)
	}
	if decoded.VoterSetHash != original.VoterSetHash {
		t.Errorf("VoterSetHash mismatch")
	}
	if decoded.QuorumSize != original.QuorumSize {
		t.Errorf("QuorumSize = %d, want %d", decoded.QuorumSize, original.QuorumSize)
	}
	if decoded.Mode != original.Mode {
		t.Errorf("Mode = %s, want %s", decoded.Mode, original.Mode)
	}
	if !decoded.AcquiredAt.Equal(original.AcquiredAt) {
		t.Errorf("AcquiredAt = %v, want %v", decoded.AcquiredAt, original.AcquiredAt)
	}
	if !decoded.ExpiresAt.Equal(original.ExpiresAt) {
		t.Errorf("ExpiresAt = %v, want %v", decoded.ExpiresAt, original.ExpiresAt)
	}
	if decoded.Checksum != original.Checksum {
		t.Errorf("Checksum = %08x, want %08x", decoded.Checksum, original.Checksum)
	}
	if string(decoded.Signature) != string(original.Signature) {
		t.Errorf("Signature = %v, want %v", decoded.Signature, original.Signature)
	}
}

func TestFencingToken_UnmarshalBinary_Errors(t *testing.T) {
	t.Run("data too short", func(t *testing.T) {
		var tok FencingToken
		if err := tok.UnmarshalBinary([]byte{1, 2, 3}); err == nil {
			t.Error("UnmarshalBinary() should fail for data too short")
		}
	})

	t.Run("invalid magic bytes", func(t *testing.T) {
		var tok FencingToken
		data := []byte("XXXX\x01\x00\x00\x00\x10")
		if err := tok.UnmarshalBinary(data); err == nil {
			t.Error("UnmarshalBinary() should fail for invalid magic bytes")
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		var tok FencingToken
		data := []byte("FTKN\xFF\x00\x00\x00\x10")
		if err := tok.UnmarshalBinary(data); err == nil {
			t.Error("UnmarshalBinary() should fail for unsupported version")
		}
	})
}

func TestFileTokenStore_SaveAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFileTokenStore(tempDir, nil)

	original := &FencingToken{
		Version:      TokenVersion,
		Epoch:        42,
		HolderID:     raft.ServerID("node-0"),
		VoterSetHash: [32]byte{1, 2, 3},
		QuorumSize:   2,
		Mode:         FencingModePrimary,
		AcquiredAt:   time.Now(),
		Signature:    []byte("test-signature"),
	}

	if err := store.SaveToken(original); err != nil {
		t.Fatalf("SaveToken() error = %v", err)
	}

	exists, err := store.Exists()
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Error("Exists() = false after SaveToken()")
	}

	loaded, err := store.LoadToken()
	if err != nil {
		t.Fatalf("LoadToken() error = %v", err)
	}

	if loaded.Epoch != original.Epoch {
		t.Errorf("Epoch = %d, want %d", loaded.Epoch, original.Epoch)
	}
	if loaded.HolderID != original.HolderID {
		t.Errorf("HolderID = %s, want %s", loaded.HolderID, original.HolderID)
	}
}

func TestFileTokenStore_Exists(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFileTokenStore(tempDir, nil)

	exists, err := store.Exists()
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Error("Exists() = true for non-existent token")
	}
}

func TestFileTokenStore_LoadToken_NotFound(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFileTokenStore(tempDir, nil)

	_, err := store.LoadToken()
	if err == nil {
		t.Error("LoadToken() should fail for non-existent token")
	}
}

func TestFileTokenStore_AtomicWrite(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFileTokenStore(tempDir, nil)

	token := &FencingToken{
		Version:      TokenVersion,
		Epoch:        1,
		HolderID:     raft.ServerID("node-0"),
		VoterSetHash: [32]byte{1, 2, 3},
		QuorumSize:   2,
		Mode:         FencingModePrimary,
		AcquiredAt:   time.Now(),
	}

	if err := store.SaveToken(token); err != nil {
		t.Fatalf("SaveToken() error = %v", err)
	}

	tempFile := filepath.Join(tempDir, "fencing_token.bin.tmp")
	if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
		t.Error("temp file should not exist after successful write")
	}

	finalFile := filepath.Join(tempDir, "fencing_token.bin")
	if _, err := os.Stat(finalFile); os.IsNotExist(err) {
		t.Error("final file should exist after successful write")
	}
}

func TestFileEpochStore_SaveAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFileEpochStore(tempDir, nil)

	epoch, err := store.LoadHighestSeenEpoch()
	if err != nil {
		t.Fatalf("LoadHighestSeenEpoch() error = %v", err)
	}
	if epoch != 0 {
		t.Errorf("LoadHighestSeenEpoch() = %d, want 0 for non-existent file", epoch)
	}

	if err := store.SaveHighestSeenEpoch(42); err != nil {
		t.Fatalf("SaveHighestSeenEpoch() error = %v", err)
	}

	loaded, err := store.LoadHighestSeenEpoch()
	if err != nil {
		t.Fatalf("LoadHighestSeenEpoch() error = %v", err)
	}
	if loaded != 42 {
		t.Errorf("LoadHighestSeenEpoch() = %d, want 42", loaded)
	}

	if err := store.SaveHighestSeenEpoch(100); err != nil {
		t.Fatalf("SaveHighestSeenEpoch() error = %v", err)
	}

	loaded, err = store.LoadHighestSeenEpoch()
	if err != nil {
		t.Fatalf("LoadHighestSeenEpoch() error = %v", err)
	}
	if loaded != 100 {
		t.Errorf("LoadHighestSeenEpoch() = %d, want 100", loaded)
	}
}

func TestFileEpochStore_AtomicWrite(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFileEpochStore(tempDir, nil)

	if err := store.SaveHighestSeenEpoch(1); err != nil {
		t.Fatalf("SaveHighestSeenEpoch() error = %v", err)
	}

	tempFile := filepath.Join(tempDir, "epoch_state.bin.tmp")
	if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
		t.Error("temp file should not exist after successful write")
	}

	finalFile := filepath.Join(tempDir, "epoch_state.bin")
	if _, err := os.Stat(finalFile); os.IsNotExist(err) {
		t.Error("final file should exist after successful write")
	}
}
