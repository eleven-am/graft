package ports

import "time"

// LeaseRecord describes the serialized representation for a distributed lease stored in the
// underlying storage engine. Generation increments every time ownership changes.
type LeaseRecord struct {
	Key        string            `json:"key"`
	Owner      string            `json:"owner"`
	Generation int64             `json:"generation"`
	ExpiresAt  time.Time         `json:"expires_at"`
	RenewedAt  time.Time         `json:"renewed_at"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// LeaseManagerPort defines the contract for creating and maintaining storage-backed leases.
type LeaseManagerPort interface {
	// Key builds the storage key for a lease scoped to namespace/id.
	Key(namespace, id string) string

	// TryAcquire attempts to take ownership of a lease. It returns the resulting lease record, a boolean
	// indicating whether the caller became the owner, and any error encountered. Returning false with a nil
	// error means another owner currently holds the lease.
	TryAcquire(key, owner string, ttl time.Duration, metadata map[string]string) (*LeaseRecord, bool, error)

	// Renew extends the lease expiration if the supplied owner still holds it. Implementations should
	// return ErrLeaseOwnedByOther when the lease is held by someone else.
	Renew(key, owner string, ttl time.Duration) (*LeaseRecord, error)

	// Release relinquishes the lease if it is owned by the provided owner.
	Release(key, owner string) error

	// ForceRelease removes the lease regardless of owner. Intended for leader-driven cleanup paths.
	ForceRelease(key string) error

	// Get fetches the current lease record.
	Get(key string) (*LeaseRecord, bool, error)
}
