package storage

import (
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

const leaseKeyPrefix = "lease:"

// LeaseManager provides a storage-backed implementation of ports.LeaseManagerPort.
type LeaseManager struct {
	storage ports.StoragePort
	logger  *slog.Logger
}

// NewLeaseManager constructs a new LeaseManager instance.
func NewLeaseManager(storage ports.StoragePort, logger *slog.Logger) *LeaseManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &LeaseManager{
		storage: storage,
		logger:  logger.With("component", "lease-manager"),
	}
}

// Key returns the storage key for a lease scoped to the provided namespace and identifier.
func (m *LeaseManager) Key(namespace, id string) string {
	return leaseKeyPrefix + namespace + ":" + id
}

// TryAcquire attempts to obtain the lease for the provided key.
func (m *LeaseManager) TryAcquire(key, owner string, ttl time.Duration, metadata map[string]string) (*ports.LeaseRecord, bool, error) {
	record, version, exists, err := m.readRecord(key)
	if err != nil {
		return nil, false, err
	}

	now := time.Now().UTC()
	if exists && record.Owner != "" && record.Owner != owner && record.ExpiresAt.After(now) {
		return &record, false, nil
	}

	newRecord := ports.LeaseRecord{
		Key:        key,
		Owner:      owner,
		ExpiresAt:  now.Add(ttl),
		RenewedAt:  now,
		Metadata:   cloneMetadata(metadata),
		Generation: 1,
	}
	if exists {
		newRecord.Generation = record.Generation + 1
	}

	payload, err := json.Marshal(newRecord)
	if err != nil {
		return nil, false, err
	}

	newVersion := int64(1)
	if exists {
		newVersion = version + 1
	}

	if err := m.storage.Put(key, payload, newVersion); err != nil {
		if isVersionMismatch(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return &newRecord, true, nil
}

// Renew extends the lease expiration if the caller still holds it.
func (m *LeaseManager) Renew(key, owner string, ttl time.Duration) (*ports.LeaseRecord, error) {
	record, version, exists, err := m.readRecord(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, domain.ErrLeaseNotFound
	}
	if record.Owner != owner {
		return nil, domain.ErrLeaseOwnedByOther
	}

	now := time.Now().UTC()
	record.RenewedAt = now
	record.ExpiresAt = now.Add(ttl)

	payload, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}

	if err := m.storage.Put(key, payload, version+1); err != nil {
		if isVersionMismatch(err) {
			return nil, domain.ErrLeaseOwnedByOther
		}
		return nil, err
	}

	return &record, nil
}

// Release relinquishes the lease if owned by the caller.
func (m *LeaseManager) Release(key, owner string) error {
	record, _, exists, err := m.readRecord(key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if record.Owner != owner {
		return domain.ErrLeaseOwnedByOther
	}

	if err := m.storage.Delete(key); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// ForceRelease removes the lease unconditionally.
func (m *LeaseManager) ForceRelease(key string) error {
	if err := m.storage.Delete(key); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// Get fetches the current lease record.
func (m *LeaseManager) Get(key string) (*ports.LeaseRecord, bool, error) {
	record, _, exists, err := m.readRecord(key)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	return &record, true, nil
}

func (m *LeaseManager) readRecord(key string) (ports.LeaseRecord, int64, bool, error) {
	value, version, exists, err := m.storage.Get(key)
	if err != nil {
		return ports.LeaseRecord{}, 0, false, err
	}
	if !exists || len(value) == 0 {
		return ports.LeaseRecord{}, version, false, nil
	}

	var record ports.LeaseRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return ports.LeaseRecord{}, version, false, err
	}
	return record, version, true, nil
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(metadata))
	for k, v := range metadata {
		cloned[k] = v
	}
	return cloned
}

func isVersionMismatch(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "version mismatch")
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "not found")
}
