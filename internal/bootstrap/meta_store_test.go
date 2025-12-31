package bootstrap

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestFileMetaStore_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileMetaStore(tmpDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster-uuid",
		FencingToken:  1,
		FencingEpoch:  1,
		ServerID:      raft.ServerID("flow-0"),
		ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
		Ordinal:       0,
		State:         StateReady,
		BootstrapTime: time.Now().Truncate(time.Millisecond),
	}

	if err := store.SaveMeta(meta); err != nil {
		t.Fatalf("SaveMeta() error = %v", err)
	}

	loaded, err := store.LoadMeta()
	if err != nil {
		t.Fatalf("LoadMeta() error = %v", err)
	}

	if loaded.ClusterUUID != meta.ClusterUUID {
		t.Errorf("ClusterUUID = %s, want %s", loaded.ClusterUUID, meta.ClusterUUID)
	}
	if loaded.ServerID != meta.ServerID {
		t.Errorf("ServerID = %s, want %s", loaded.ServerID, meta.ServerID)
	}
	if loaded.State != meta.State {
		t.Errorf("State = %s, want %s", loaded.State, meta.State)
	}
	if loaded.FencingEpoch != meta.FencingEpoch {
		t.Errorf("FencingEpoch = %d, want %d", loaded.FencingEpoch, meta.FencingEpoch)
	}
}

func TestFileMetaStore_Exists(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileMetaStore(tmpDir, nil)

	exists, err := store.Exists()
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Error("Exists() = true, want false for new store")
	}

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster-uuid",
		ServerID:      raft.ServerID("flow-0"),
		ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
		Ordinal:       0,
		State:         StateReady,
	}

	if err := store.SaveMeta(meta); err != nil {
		t.Fatalf("SaveMeta() error = %v", err)
	}

	exists, err = store.Exists()
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Error("Exists() = false, want true after save")
	}
}

func TestFileMetaStore_LoadMeta_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileMetaStore(tmpDir, nil)

	_, err := store.LoadMeta()
	if err == nil {
		t.Error("LoadMeta() should fail for missing file")
	}
	if err != ErrMetaNotFound {
		t.Errorf("LoadMeta() error = %v, want ErrMetaNotFound", err)
	}
}

func TestFileMetaStore_ChecksumValidation(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileMetaStore(tmpDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster-uuid",
		ServerID:      raft.ServerID("flow-0"),
		ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
		Ordinal:       0,
		State:         StateReady,
	}

	if err := store.SaveMeta(meta); err != nil {
		t.Fatalf("SaveMeta() error = %v", err)
	}

	path := filepath.Join(tmpDir, "cluster_meta.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	var loaded ClusterMeta
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	loaded.Checksum = loaded.Checksum + 1
	corrupted, err := json.MarshalIndent(loaded, "", "  ")
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if err := os.WriteFile(path, corrupted, 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err = store.LoadMeta()
	if err == nil {
		t.Error("LoadMeta() should fail for corrupted checksum")
	}

	var metaCorruptionErr *MetaCorruptionError
	if _, ok := err.(*MetaCorruptionError); !ok {
		t.Errorf("LoadMeta() error type = %T, want *MetaCorruptionError", err)
	} else {
		metaCorruptionErr = err.(*MetaCorruptionError)
		if metaCorruptionErr.Path != path {
			t.Errorf("MetaCorruptionError.Path = %s, want %s", metaCorruptionErr.Path, path)
		}
	}
}

func TestFileMetaStore_SaveMeta_Nil(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileMetaStore(tmpDir, nil)

	err := store.SaveMeta(nil)
	if err == nil {
		t.Error("SaveMeta(nil) should fail")
	}
}

func TestFileMetaStore_SaveMeta_InvalidMeta(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileMetaStore(tmpDir, nil)

	meta := &ClusterMeta{
		Version: CurrentMetaVersion,
	}

	err := store.SaveMeta(meta)
	if err == nil {
		t.Error("SaveMeta() should fail for invalid meta")
	}
}

func TestFileMetaStore_UnsupportedVersion(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileMetaStore(tmpDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster-uuid",
		ServerID:      raft.ServerID("flow-0"),
		ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
		Ordinal:       0,
		State:         StateReady,
	}

	if err := store.SaveMeta(meta); err != nil {
		t.Fatalf("SaveMeta() error = %v", err)
	}

	path := filepath.Join(tmpDir, "cluster_meta.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	var loaded ClusterMeta
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	loaded.Version = CurrentMetaVersion + 1
	loaded.Checksum = loaded.ComputeChecksum()
	modified, err := json.MarshalIndent(loaded, "", "  ")
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if err := os.WriteFile(path, modified, 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err = store.LoadMeta()
	if err == nil {
		t.Error("LoadMeta() should fail for unsupported version")
	}
}

func TestFileMetaStore_AtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileMetaStore(tmpDir, nil)

	meta1 := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster-uuid",
		FencingEpoch:  1,
		ServerID:      raft.ServerID("flow-0"),
		ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
		Ordinal:       0,
		State:         StateReady,
	}

	if err := store.SaveMeta(meta1); err != nil {
		t.Fatalf("SaveMeta() error = %v", err)
	}

	meta2 := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster-uuid",
		FencingEpoch:  2,
		ServerID:      raft.ServerID("flow-0"),
		ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
		Ordinal:       0,
		State:         StateReady,
	}

	if err := store.SaveMeta(meta2); err != nil {
		t.Fatalf("SaveMeta() error = %v", err)
	}

	tmpPath := filepath.Join(tmpDir, "cluster_meta.json.tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Temp file should not exist after successful save")
	}

	loaded, err := store.LoadMeta()
	if err != nil {
		t.Fatalf("LoadMeta() error = %v", err)
	}
	if loaded.FencingEpoch != 2 {
		t.Errorf("FencingEpoch = %d, want 2", loaded.FencingEpoch)
	}
}

func TestFileMetaStore_CreatesDataDir(t *testing.T) {
	tmpDir := t.TempDir()
	nestedDir := filepath.Join(tmpDir, "a", "b", "c")
	store := NewFileMetaStore(nestedDir, nil)

	meta := &ClusterMeta{
		Version:       CurrentMetaVersion,
		ClusterUUID:   "test-cluster-uuid",
		ServerID:      raft.ServerID("flow-0"),
		ServerAddress: raft.ServerAddress("flow-0.flow.svc:7946"),
		Ordinal:       0,
		State:         StateReady,
	}

	if err := store.SaveMeta(meta); err != nil {
		t.Fatalf("SaveMeta() error = %v", err)
	}

	if _, err := os.Stat(nestedDir); os.IsNotExist(err) {
		t.Error("SaveMeta() should create nested data directory")
	}
}
