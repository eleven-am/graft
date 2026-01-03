package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/require"
)

type memoryStorage struct {
	mu   sync.Mutex
	data map[string]memoryEntry
}

type memoryEntry struct {
	value   []byte
	version int64
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{data: make(map[string]memoryEntry)}
}

func (m *memoryStorage) Get(key string) ([]byte, int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.data[key]
	if !ok {
		return nil, 0, false, nil
	}
	valCopy := append([]byte(nil), entry.value...)
	return valCopy, entry.version, true, nil
}

func (m *memoryStorage) Put(key string, value []byte, version int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	valCopy := append([]byte(nil), value...)
	m.data[key] = memoryEntry{value: valCopy, version: version}
	return nil
}

func (m *memoryStorage) PutWithTTL(key string, value []byte, version int64, _ time.Duration) error {
	return m.Put(key, value, version)
}

func (m *memoryStorage) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memoryStorage) Exists(key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[key]
	return ok, nil
}

func (m *memoryStorage) GetMetadata(string) (*ports.KeyMetadata, error) { panic("not implemented") }
func (m *memoryStorage) BatchWrite([]ports.WriteOp) error               { panic("not implemented") }
func (m *memoryStorage) GetNext(string) (string, []byte, bool, error)   { panic("not implemented") }
func (m *memoryStorage) GetNextAfter(string, string) (string, []byte, bool, error) {
	panic("not implemented")
}
func (m *memoryStorage) CountPrefix(string) (int, error)       { panic("not implemented") }
func (m *memoryStorage) AtomicIncrement(string) (int64, error) { panic("not implemented") }
func (m *memoryStorage) ListByPrefix(string) ([]ports.KeyValueVersion, error) {
	panic("not implemented")
}
func (m *memoryStorage) DeleteByPrefix(string) (int, error)   { panic("not implemented") }
func (m *memoryStorage) GetVersion(string) (int64, error)     { panic("not implemented") }
func (m *memoryStorage) ExpireAt(string, time.Time) error     { panic("not implemented") }
func (m *memoryStorage) GetTTL(string) (time.Duration, error) { panic("not implemented") }
func (m *memoryStorage) CleanExpired() (int, error)           { panic("not implemented") }
func (m *memoryStorage) RunInTransaction(func(tx ports.Transaction) error) error {
	return errors.New("not implemented")
}
func (m *memoryStorage) SetRaftNode(ports.RaftNode)         {}
func (m *memoryStorage) SetEventManager(ports.EventManager) {}
func (m *memoryStorage) Close() error                       { return nil }

func TestLeaseManagerTryAcquire(t *testing.T) {
	storage := newMemoryStorage()
	manager := NewLeaseManager(storage, nil)

	key := manager.Key("test", "resource")
	record, acquired, err := manager.TryAcquire(key, "node-a", time.Minute, nil)
	require.NoError(t, err)
	require.True(t, acquired)
	require.Equal(t, "node-a", record.Owner)
}

func TestLeaseManagerAcquireRespectsExistingOwner(t *testing.T) {
	storage := newMemoryStorage()
	manager := NewLeaseManager(storage, nil)
	key := manager.Key("test", "resource")

	_, _, err := manager.TryAcquire(key, "node-a", time.Minute, nil)
	require.NoError(t, err)

	_, acquired, err := manager.TryAcquire(key, "node-b", time.Minute, nil)
	require.NoError(t, err)
	require.False(t, acquired)
}

func TestLeaseManagerAcquireAfterExpiry(t *testing.T) {
	storage := newMemoryStorage()
	manager := NewLeaseManager(storage, nil)
	key := manager.Key("test", "resource")

	_, _, err := manager.TryAcquire(key, "node-a", 10*time.Millisecond, nil)
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	record, acquired, err := manager.TryAcquire(key, "node-b", time.Minute, nil)
	require.NoError(t, err)
	require.True(t, acquired)
	require.Equal(t, "node-b", record.Owner)
}

func TestLeaseManagerRenewAndRelease(t *testing.T) {
	storage := newMemoryStorage()
	manager := NewLeaseManager(storage, nil)
	key := manager.Key("test", "resource")

	initial, acquired, err := manager.TryAcquire(key, "node-a", 20*time.Millisecond, nil)
	require.NoError(t, err)
	require.True(t, acquired)

	time.Sleep(10 * time.Millisecond)

	renewed, err := manager.Renew(key, "node-a", time.Minute)
	require.NoError(t, err)
	require.True(t, renewed.ExpiresAt.After(initial.ExpiresAt))

	err = manager.Release(key, "node-a")
	require.NoError(t, err)

	_, exists, err := manager.Get(key)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestLeaseManagerRenewWrongOwner(t *testing.T) {
	storage := newMemoryStorage()
	manager := NewLeaseManager(storage, nil)
	key := manager.Key("test", "resource")

	_, _, err := manager.TryAcquire(key, "node-a", time.Minute, nil)
	require.NoError(t, err)

	_, err = manager.Renew(key, "node-b", time.Minute)
	require.Error(t, err)
	require.True(t, domain.IsLeaseOwnedByOther(err))
}

func TestLeaseManagerReleaseWrongOwner(t *testing.T) {
	storage := newMemoryStorage()
	manager := NewLeaseManager(storage, nil)
	key := manager.Key("test", "resource")

	_, _, err := manager.TryAcquire(key, "node-a", time.Minute, nil)
	require.NoError(t, err)

	err = manager.Release(key, "node-b")
	require.Error(t, err)
	require.True(t, domain.IsLeaseOwnedByOther(err))
}

func TestLeaseManagerForceRelease(t *testing.T) {
	storage := newMemoryStorage()
	manager := NewLeaseManager(storage, nil)
	key := manager.Key("test", "resource")

	_, _, err := manager.TryAcquire(key, "node-a", time.Minute, nil)
	require.NoError(t, err)

	err = manager.ForceRelease(key)
	require.NoError(t, err)

	_, exists, err := manager.Get(key)
	require.NoError(t, err)
	require.False(t, exists)
}
