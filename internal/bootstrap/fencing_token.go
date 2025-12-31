package bootstrap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const (
	TokenMagicBytes = "FTKN"
	TokenVersion    = uint8(1)
)

type FencingToken struct {
	Version      uint8         `json:"version"`
	Epoch        uint64        `json:"epoch"`
	HolderID     raft.ServerID `json:"holder_id"`
	VoterSetHash [32]byte      `json:"voter_set_hash"`
	QuorumSize   int           `json:"quorum_size"`
	Mode         FencingMode   `json:"mode"`
	AcquiredAt   time.Time     `json:"acquired_at"`
	ExpiresAt    time.Time     `json:"expires_at,omitempty"`
	Checksum     uint32        `json:"checksum"`
	Signature    []byte        `json:"signature"`
}

func (t *FencingToken) ComputeChecksum() uint32 {
	h := crc32.NewIEEE()
	binary.Write(h, binary.BigEndian, t.Version)
	binary.Write(h, binary.BigEndian, t.Epoch)
	h.Write([]byte(t.HolderID))
	h.Write(t.VoterSetHash[:])
	binary.Write(h, binary.BigEndian, int32(t.QuorumSize))
	h.Write([]byte(t.Mode))
	binary.Write(h, binary.BigEndian, t.AcquiredAt.UnixNano())
	binary.Write(h, binary.BigEndian, t.ExpiresAt.UnixNano())
	return h.Sum32()
}

func (t *FencingToken) UpdateChecksum() {
	t.Checksum = t.ComputeChecksum()
}

func (t *FencingToken) VerifyChecksum() bool {
	return t.Checksum == t.ComputeChecksum()
}

func (t *FencingToken) Validate() error {
	if t.Version == 0 {
		return fmt.Errorf("version must be set")
	}
	if t.Version > TokenVersion {
		return fmt.Errorf("unsupported token version %d (max supported: %d)", t.Version, TokenVersion)
	}
	if t.Epoch == 0 {
		return fmt.Errorf("epoch must be greater than 0")
	}
	if t.HolderID == "" {
		return fmt.Errorf("holder_id is required")
	}
	if t.QuorumSize < 1 {
		return fmt.Errorf("quorum_size must be at least 1")
	}
	if !t.Mode.IsValid() {
		return fmt.Errorf("invalid fencing mode: %s", t.Mode)
	}
	if t.AcquiredAt.IsZero() {
		return fmt.Errorf("acquired_at is required")
	}
	return nil
}

func (t *FencingToken) IsExpired() bool {
	if t.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(t.ExpiresAt)
}

func (t *FencingToken) SigningPayload() []byte {
	buf := make([]byte, 0, 256)

	buf = append(buf, t.Version)

	epochBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(epochBytes, t.Epoch)
	buf = append(buf, epochBytes...)

	buf = append(buf, []byte(t.HolderID)...)
	buf = append(buf, t.VoterSetHash[:]...)

	quorumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(quorumBytes, uint32(t.QuorumSize))
	buf = append(buf, quorumBytes...)

	buf = append(buf, []byte(t.Mode)...)

	acquiredBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(acquiredBytes, uint64(t.AcquiredAt.UnixNano()))
	buf = append(buf, acquiredBytes...)

	expiresBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(expiresBytes, uint64(t.ExpiresAt.UnixNano()))
	buf = append(buf, expiresBytes...)

	checksumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBytes, t.Checksum)
	buf = append(buf, checksumBytes...)

	return buf
}

func (t *FencingToken) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)

	buf.WriteString(TokenMagicBytes)
	buf.WriteByte(t.Version)

	payload := new(bytes.Buffer)
	binary.Write(payload, binary.BigEndian, t.Epoch)

	holderIDBytes := []byte(t.HolderID)
	binary.Write(payload, binary.BigEndian, uint16(len(holderIDBytes)))
	payload.Write(holderIDBytes)

	payload.Write(t.VoterSetHash[:])

	binary.Write(payload, binary.BigEndian, int32(t.QuorumSize))

	modeBytes := []byte(t.Mode)
	payload.WriteByte(uint8(len(modeBytes)))
	payload.Write(modeBytes)

	binary.Write(payload, binary.BigEndian, t.AcquiredAt.UnixNano())
	binary.Write(payload, binary.BigEndian, t.ExpiresAt.UnixNano())
	binary.Write(payload, binary.BigEndian, t.Checksum)

	binary.Write(payload, binary.BigEndian, uint16(len(t.Signature)))
	payload.Write(t.Signature)

	binary.Write(buf, binary.BigEndian, uint32(payload.Len()))
	buf.Write(payload.Bytes())

	return buf.Bytes(), nil
}

func (t *FencingToken) UnmarshalBinary(data []byte) error {
	if len(data) < 9 {
		return fmt.Errorf("data too short for token header")
	}

	if string(data[0:4]) != TokenMagicBytes {
		return fmt.Errorf("invalid magic bytes: expected %q, got %q", TokenMagicBytes, string(data[0:4]))
	}

	t.Version = data[4]
	if t.Version > TokenVersion {
		return fmt.Errorf("unsupported token version %d (max supported: %d)", t.Version, TokenVersion)
	}

	payloadLen := binary.BigEndian.Uint32(data[5:9])
	if uint32(len(data)) < 9+payloadLen {
		return fmt.Errorf("data too short for payload: expected %d bytes, got %d", 9+payloadLen, len(data))
	}

	payload := bytes.NewReader(data[9 : 9+payloadLen])

	if err := binary.Read(payload, binary.BigEndian, &t.Epoch); err != nil {
		return fmt.Errorf("read epoch: %w", err)
	}

	var holderIDLen uint16
	if err := binary.Read(payload, binary.BigEndian, &holderIDLen); err != nil {
		return fmt.Errorf("read holder_id length: %w", err)
	}
	holderIDBytes := make([]byte, holderIDLen)
	if _, err := payload.Read(holderIDBytes); err != nil {
		return fmt.Errorf("read holder_id: %w", err)
	}
	t.HolderID = raft.ServerID(holderIDBytes)

	if _, err := payload.Read(t.VoterSetHash[:]); err != nil {
		return fmt.Errorf("read voter_set_hash: %w", err)
	}

	var quorumSize int32
	if err := binary.Read(payload, binary.BigEndian, &quorumSize); err != nil {
		return fmt.Errorf("read quorum_size: %w", err)
	}
	t.QuorumSize = int(quorumSize)

	var modeLen uint8
	if err := binary.Read(payload, binary.BigEndian, &modeLen); err != nil {
		return fmt.Errorf("read mode length: %w", err)
	}
	modeBytes := make([]byte, modeLen)
	if _, err := payload.Read(modeBytes); err != nil {
		return fmt.Errorf("read mode: %w", err)
	}
	t.Mode = FencingMode(modeBytes)

	var acquiredNano int64
	if err := binary.Read(payload, binary.BigEndian, &acquiredNano); err != nil {
		return fmt.Errorf("read acquired_at: %w", err)
	}
	t.AcquiredAt = time.Unix(0, acquiredNano)

	var expiresNano int64
	if err := binary.Read(payload, binary.BigEndian, &expiresNano); err != nil {
		return fmt.Errorf("read expires_at: %w", err)
	}
	t.ExpiresAt = time.Unix(0, expiresNano)

	if err := binary.Read(payload, binary.BigEndian, &t.Checksum); err != nil {
		return fmt.Errorf("read checksum: %w", err)
	}

	var sigLen uint16
	if err := binary.Read(payload, binary.BigEndian, &sigLen); err != nil {
		return fmt.Errorf("read signature length: %w", err)
	}
	t.Signature = make([]byte, sigLen)
	if _, err := payload.Read(t.Signature); err != nil {
		return fmt.Errorf("read signature: %w", err)
	}

	return nil
}

type TokenStore interface {
	LoadToken() (*FencingToken, error)
	SaveToken(token *FencingToken) error
	Exists() (bool, error)
}

type FileTokenStore struct {
	dataDir string
	logger  *slog.Logger
	mu      sync.RWMutex
}

func NewFileTokenStore(dataDir string, logger *slog.Logger) *FileTokenStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &FileTokenStore{
		dataDir: dataDir,
		logger:  logger,
	}
}

func (s *FileTokenStore) path() string {
	return filepath.Join(s.dataDir, "fencing_token.bin")
}

func (s *FileTokenStore) tempPath() string {
	return filepath.Join(s.dataDir, "fencing_token.bin.tmp")
}

func (s *FileTokenStore) LoadToken() (*FencingToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.path()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoToken
		}
		return nil, fmt.Errorf("read fencing token: %w", err)
	}

	var token FencingToken
	if err := token.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("unmarshal fencing token: %w", err)
	}

	if !token.VerifyChecksum() {
		return nil, &TokenCorruptionError{
			Path:             path,
			ExpectedChecksum: token.ComputeChecksum(),
			ActualChecksum:   token.Checksum,
		}
	}

	if err := token.Validate(); err != nil {
		return nil, fmt.Errorf("validate fencing token: %w", err)
	}

	s.logger.Debug("loaded fencing token",
		"epoch", token.Epoch,
		"holder_id", token.HolderID,
		"mode", token.Mode,
	)

	return &token, nil
}

func (s *FileTokenStore) SaveToken(token *FencingToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if token == nil {
		return fmt.Errorf("token is nil")
	}

	if err := token.Validate(); err != nil {
		return fmt.Errorf("validate fencing token: %w", err)
	}

	token.UpdateChecksum()

	data, err := token.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal fencing token: %w", err)
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

	s.logger.Debug("saved fencing token",
		"epoch", token.Epoch,
		"holder_id", token.HolderID,
		"mode", token.Mode,
		"checksum", fmt.Sprintf("%08x", token.Checksum),
	)

	return nil
}

func (s *FileTokenStore) Exists() (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, err := os.Stat(s.path())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("stat fencing token: %w", err)
}

type EpochStore interface {
	LoadHighestSeenEpoch() (uint64, error)
	SaveHighestSeenEpoch(epoch uint64) error
}

type FileEpochStore struct {
	dataDir string
	logger  *slog.Logger
	mu      sync.RWMutex
}

func NewFileEpochStore(dataDir string, logger *slog.Logger) *FileEpochStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &FileEpochStore{
		dataDir: dataDir,
		logger:  logger,
	}
}

func (s *FileEpochStore) path() string {
	return filepath.Join(s.dataDir, "epoch_state.bin")
}

func (s *FileEpochStore) tempPath() string {
	return filepath.Join(s.dataDir, "epoch_state.bin.tmp")
}

func (s *FileEpochStore) LoadHighestSeenEpoch() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.path()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read epoch state: %w", err)
	}

	if len(data) < 12 {
		return 0, fmt.Errorf("epoch state file too short")
	}

	if string(data[0:4]) != "EPST" {
		return 0, fmt.Errorf("invalid epoch state magic bytes")
	}

	epoch := binary.BigEndian.Uint64(data[4:12])

	s.logger.Debug("loaded highest seen epoch", "epoch", epoch)

	return epoch, nil
}

func (s *FileEpochStore) SaveHighestSeenEpoch(epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.dataDir, 0700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	data := make([]byte, 12)
	copy(data[0:4], "EPST")
	binary.BigEndian.PutUint64(data[4:12], epoch)

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

	s.logger.Debug("saved highest seen epoch", "epoch", epoch)

	return nil
}
