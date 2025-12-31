package bootstrap

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

type MetaStore interface {
	LoadMeta() (*ClusterMeta, error)
	SaveMeta(meta *ClusterMeta) error
	Exists() (bool, error)
}

type FileMetaStore struct {
	dataDir string
	logger  *slog.Logger
	mu      sync.RWMutex
}

func NewFileMetaStore(dataDir string, logger *slog.Logger) *FileMetaStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &FileMetaStore{
		dataDir: dataDir,
		logger:  logger,
	}
}

func (s *FileMetaStore) path() string {
	return filepath.Join(s.dataDir, "cluster_meta.json")
}

func (s *FileMetaStore) tempPath() string {
	return filepath.Join(s.dataDir, "cluster_meta.json.tmp")
}

func (s *FileMetaStore) LoadMeta() (*ClusterMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.path()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrMetaNotFound
		}
		return nil, fmt.Errorf("read cluster meta: %w", err)
	}

	var meta ClusterMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal cluster meta: %w", err)
	}

	if !meta.VerifyChecksum() {
		return nil, &MetaCorruptionError{
			Path:             path,
			ExpectedChecksum: meta.ComputeChecksum(),
			ActualChecksum:   meta.Checksum,
			Details:          "checksum verification failed",
		}
	}

	if meta.Version > CurrentMetaVersion {
		return nil, fmt.Errorf("unsupported meta version %d (max supported: %d)", meta.Version, CurrentMetaVersion)
	}

	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("validate cluster meta: %w", err)
	}

	s.logger.Debug("loaded cluster meta",
		"cluster_uuid", meta.ClusterUUID,
		"server_id", meta.ServerID,
		"state", meta.State,
		"fencing_epoch", meta.FencingEpoch,
	)

	return &meta, nil
}

func (s *FileMetaStore) SaveMeta(meta *ClusterMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if meta == nil {
		return fmt.Errorf("meta is nil")
	}

	if err := meta.Validate(); err != nil {
		return fmt.Errorf("validate cluster meta: %w", err)
	}

	meta.UpdateChecksum()

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal cluster meta: %w", err)
	}

	if err := os.MkdirAll(s.dataDir, 0700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	tempPath := s.tempPath()
	finalPath := s.path()

	if err := os.WriteFile(tempPath, data, 0600); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	tempFile, err := os.OpenFile(tempPath, os.O_WRONLY, 0600)
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("open temp file for sync: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return fmt.Errorf("fsync temp file: %w", err)
	}
	tempFile.Close()

	if err := os.Rename(tempPath, finalPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("rename temp to final: %w", err)
	}

	dir, err := os.Open(s.dataDir)
	if err != nil {
		return fmt.Errorf("open data directory for sync: %w", err)
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return fmt.Errorf("fsync data directory: %w", err)
	}
	dir.Close()

	s.logger.Debug("saved cluster meta",
		"cluster_uuid", meta.ClusterUUID,
		"server_id", meta.ServerID,
		"state", meta.State,
		"fencing_epoch", meta.FencingEpoch,
		"checksum", fmt.Sprintf("%08x", meta.Checksum),
	)

	return nil
}

func (s *FileMetaStore) Exists() (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, err := os.Stat(s.path())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("stat cluster meta: %w", err)
}
