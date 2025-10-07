package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type AppStorage struct {
	raftNode ports.RaftNode
	db       *badger.DB
	logger   *slog.Logger
	events   ports.EventManager

	mu     sync.RWMutex
	closed bool
}

func NewAppStorage(raftNode ports.RaftNode, db *badger.DB, logger *slog.Logger) *AppStorage {
	return &AppStorage{
		raftNode: raftNode,
		db:       db,
		logger:   logger.With("component", "app-storage"),
	}
}

// SetEventManager injects the event manager as the single event hub
func (s *AppStorage) SetEventManager(ev ports.EventManager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = ev
}

// iterate to the first non-metadata key under prefix, optionally after a given key
func (s *AppStorage) firstNonMeta(txn *badger.Txn, prefix []byte, afterKey string) (key string, value []byte, exists bool, err error) {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	if afterKey != "" {
		it.Seek([]byte(afterKey))
		if !it.Valid() {
			return "", nil, false, nil
		}
		it.Next()
	} else {
		it.Rewind()
	}

	for ; it.Valid(); it.Next() {
		item := it.Item()
		keyBytes := item.Key()
		if s.isMetadataKey(keyBytes) {
			continue
		}
		key = string(keyBytes)
		value, err = item.ValueCopy(nil)
		if err != nil {
			return "", nil, false, err
		}
		return key, value, true, nil
	}
	return "", nil, false, nil
}

// helper to iterate all non-metadata keys for a prefix
func (s *AppStorage) forEachNonMeta(txn *badger.Txn, prefix []byte, prefetchValues bool, fn func(item *badger.Item) error) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = prefetchValues
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		if s.isMetadataKey(item.Key()) {
			continue
		}
		if err := fn(item); err != nil {
			return err
		}
	}
	return nil
}

// readTTL reads ttl:<key> from the metadata store and returns the expiration time if present
func readTTL(txn *badger.Txn, key string) (*time.Time, error) {
	ttlKey := fmt.Sprintf("ttl:%s", key)
	item, err := txn.Get([]byte(ttlKey))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	ttlBytes, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	var expireAt time.Time
	if err := json.Unmarshal(ttlBytes, &expireAt); err != nil {
		return nil, fmt.Errorf("decode ttl for key %s: %w", key, err)
	}
	return &expireAt, nil
}

func (s *AppStorage) Get(key string) (value []byte, version int64, exists bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		value, version, exists, err = readValueAndVersion(txn, key)
		return err
	})

	return value, version, exists, err
}

func (s *AppStorage) Put(key string, value []byte, version int64) error {
	return s.putInternal(key, value, version, 0)
}

func (s *AppStorage) PutWithTTL(key string, value []byte, version int64, ttl time.Duration) error {
	return s.putInternal(key, value, version, ttl)
}

func (s *AppStorage) putInternal(key string, value []byte, version int64, ttl time.Duration) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return &domain.StorageError{Type: domain.ErrClosed, Message: "storage is closed"}
	}
	s.mu.RUnlock()

	if s.raftNode == nil {
		return domain.ErrNotStarted
	}

	var cmd *domain.Command
	if ttl > 0 {
		cmd = domain.NewPutWithTTLCommand(key, value, version, ttl)
	} else {
		cmd = domain.NewPutCommand(key, value, version)
	}

	result, err := s.raftNode.Apply(*cmd, 5*time.Second)
	if err != nil {
		return err
	}

	if !result.Success {
		return errors.New(result.Error)
	}

	s.waitForVersionsAndBroadcast(result.Events)

	return nil
}

// putDirect removed to avoid non-Raft write paths

func (s *AppStorage) Delete(key string) error {
	if s.raftNode == nil {
		return domain.ErrNotStarted
	}

	cmd := domain.NewDeleteCommand(key)
	result, err := s.raftNode.Apply(*cmd, 5*time.Second)
	if err != nil {
		return err
	}

	if !result.Success {
		return errors.New(result.Error)
	}

	s.waitForVersionsAndBroadcast(result.Events)

	return nil
}

func (s *AppStorage) Exists(key string) (bool, error) {
	_, _, exists, err := s.Get(key)
	return exists, err
}

func (s *AppStorage) GetMetadata(key string) (*ports.KeyMetadata, error) {
	value, version, exists, err := s.Get(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, domain.NewKeyNotFoundError(key)
	}

	metadata := &ports.KeyMetadata{
		Key:     key,
		Version: version,
		Size:    int64(len(value)),
		Updated: time.Now(),
	}

	err = s.db.View(func(txn *badger.Txn) error {
		exp, e := readTTL(txn, key)
		if e != nil {
			return e
		}
		metadata.ExpireAt = exp
		return nil
	})

	return metadata, nil
}

func (s *AppStorage) BatchWrite(ops []ports.WriteOp) error {
	batchOps := make([]domain.BatchOp, 0, len(ops))
	for _, op := range ops {
		switch op.Type {
		case ports.OpPut:
			batchOps = append(batchOps, domain.BatchOp{
				Type:  domain.CommandPut,
				Key:   op.Key,
				Value: op.Value,
			})
		case ports.OpDelete:
			batchOps = append(batchOps, domain.BatchOp{
				Type: domain.CommandDelete,
				Key:  op.Key,
			})
		default:
			return domain.ErrInvalidInput
		}
	}

	if s.raftNode == nil {
		return domain.ErrNotStarted
	}

	cmd := domain.NewBatchCommand(batchOps)
	result, err := s.raftNode.Apply(*cmd, 5*time.Second)
	if err != nil {
		return err
	}

	if !result.Success {
		return errors.New(result.Error)
	}

	s.waitForVersionsAndBroadcast(result.Events)

	return nil
}

func (s *AppStorage) ListByPrefix(prefix string) ([]ports.KeyValueVersion, error) {
	var results []ports.KeyValueVersion

	err := s.db.View(func(txn *badger.Txn) error {
		return s.forEachNonMeta(txn, []byte(prefix), false, func(item *badger.Item) error {
			key := string(item.Key())
			value, version, exists, err := readValueAndVersion(txn, key)
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			results = append(results, ports.KeyValueVersion{Key: key, Value: value, Version: version})
			return nil
		})
	})

	return results, err
}

func (s *AppStorage) GetNext(prefix string) (key string, value []byte, exists bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		k, v, ok, e := s.firstNonMeta(txn, []byte(prefix), "")
		if e != nil {
			return e
		}
		key, value, exists = k, v, ok
		return nil
	})

	return key, value, exists, err
}

func (s *AppStorage) GetNextAfter(prefix string, afterKey string) (key string, value []byte, exists bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		k, v, ok, e := s.firstNonMeta(txn, []byte(prefix), afterKey)
		if e != nil {
			return e
		}
		key, value, exists = k, v, ok
		return nil
	})

	return key, value, exists, err
}

func (s *AppStorage) CountPrefix(prefix string) (count int, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		return s.forEachNonMeta(txn, []byte(prefix), false, func(item *badger.Item) error {
			count++
			return nil
		})
	})

	return count, err
}

func (s *AppStorage) AtomicIncrement(key string) (newValue int64, err error) {
	if s.raftNode == nil {
		return 0, domain.ErrNotStarted
	}

	command := *domain.NewAtomicIncrementCommand(key, 1)

	result, err := s.raftNode.Apply(command, 5*time.Second)
	if err != nil {
		return 0, domain.NewDiscoveryError("app-storage", "raft_apply_increment", err)
	}

	if result == nil {
		return 0, domain.NewDiscoveryError("app-storage", "nil_increment_result", nil)
	}

	if !result.Success {
		return 0, fmt.Errorf("atomic increment failed: %s", result.Error)
	}

	s.waitForVersionsAndBroadcast(result.Events)

	value, _, exists, err := s.Get(key)
	if err != nil {
		return 0, domain.NewDiscoveryError("app-storage", "get_counter_after_increment", err)
	}

	if !exists {
		return 0, domain.NewDiscoveryError("app-storage", "counter_missing_after_increment", nil)
	}

	var counterValue int64
	if err := json.Unmarshal(value, &counterValue); err != nil {
		return 0, domain.NewDiscoveryError("app-storage", "parse_counter_value", err)
	}

	return counterValue, nil
}

// legacy Subscribe/broadcast removed in favor of EventManager hub

func (s *AppStorage) DeleteByPrefix(prefix string) (deletedCount int, err error) {
	keys, err := s.ListByPrefix(prefix)
	if err != nil {
		return 0, err
	}

	ops := make([]ports.WriteOp, 0, len(keys))
	for _, kv := range keys {
		ops = append(ops, ports.WriteOp{
			Type: ports.OpDelete,
			Key:  kv.Key,
		})
		deletedCount++
	}

	if len(ops) > 0 {
		err = s.BatchWrite(ops)
	}

	return deletedCount, err
}

func (s *AppStorage) GetVersion(key string) (int64, error) {
	_, version, exists, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, domain.NewKeyNotFoundError(key)
	}
	return version, nil
}

// IncrementVersion deprecated and removed. Use Put with expected version or higher-level managers.

func (s *AppStorage) ExpireAt(key string, expireTime time.Time) error {

	if s.raftNode != nil {
		value, version, exists, err := s.Get(key)
		if err != nil {
			return err
		}
		if !exists {
			return domain.NewKeyNotFoundError(key)
		}
		ttl := time.Until(expireTime)
		if ttl < 0 {
			ttl = 0
		}
		return s.PutWithTTL(key, value, version, ttl)
	}

	ttlKey := fmt.Sprintf("ttl:%s", key)
	ttlBytes, err := json.Marshal(expireTime)
	if err != nil {
		return fmt.Errorf("encode ttl for key %s: %w", key, err)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(ttlKey), ttlBytes)
	})
}

func (s *AppStorage) GetTTL(key string) (time.Duration, error) {
	var expireAt *time.Time
	err := s.db.View(func(txn *badger.Txn) error {
		exp, e := readTTL(txn, key)
		if e != nil {
			return e
		}
		expireAt = exp
		return nil
	})

	if err != nil {
		return 0, err
	}

	if expireAt == nil || expireAt.IsZero() {
		return 0, nil
	}

	return time.Until(*expireAt), nil
}

func (s *AppStorage) CleanExpired() (cleanedCount int, err error) {
	now := time.Now()
	var keysToDelete []string

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("ttl:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			ttlBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			var expireAt time.Time
			if err := json.Unmarshal(ttlBytes, &expireAt); err != nil {
				return fmt.Errorf("decode ttl for key %s: %w", string(item.Key()), err)
			}
			if now.After(expireAt) {
				key := string(item.Key())[4:]
				keysToDelete = append(keysToDelete, key)
			}
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	for _, key := range keysToDelete {
		if err := s.Delete(key); err == nil {
			cleanedCount++
		}
	}

	return cleanedCount, nil
}

func (s *AppStorage) RunInTransaction(fn func(tx ports.Transaction) error) error {
	if s.raftNode != nil {
		return domain.NewDiscoveryError("storage", "run_in_transaction", errors.New("RunInTransaction is disabled when Raft is active"))
	}
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	tx := &transaction{
		txn:     txn,
		storage: s,
	}

	if err := fn(tx); err != nil {
		return err
	}

	return txn.Commit()
}

func (s *AppStorage) SetRaftNode(node ports.RaftNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raftNode = node
}

func (s *AppStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "storage already closed"}
	}

	s.closed = true

	return nil
}

type transaction struct {
	txn     *badger.Txn
	storage *AppStorage
}

func (t *transaction) Get(key string) (value []byte, version int64, exists bool, err error) {
	return readValueAndVersion(t.txn, key)
}

func readValueAndVersion(txn *badger.Txn, key string) (value []byte, version int64, exists bool, err error) {
	item, err := txn.Get([]byte(key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}

	exists = true
	err = item.Value(func(val []byte) error {
		value = append([]byte(nil), val...)
		return nil
	})
	if err != nil {
		return nil, 0, false, err
	}

	versionKey := fmt.Sprintf("v:%s", key)
	versionItem, err := txn.Get([]byte(versionKey))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			version = 0
		} else {
			return nil, 0, false, err
		}
	} else {
		err = versionItem.Value(func(val []byte) error {
			return json.Unmarshal(val, &version)
		})
		if err != nil {
			return nil, 0, false, err
		}
	}

	return value, version, exists, nil
}

func (t *transaction) Put(key string, value []byte, version int64) error {
	return t.PutWithTTL(key, value, version, 0)
}

func (t *transaction) PutWithTTL(key string, value []byte, version int64, ttl time.Duration) error {
	versionKey := fmt.Sprintf("v:%s", key)
	versionBytes, err := json.Marshal(version)
	if err != nil {
		return fmt.Errorf("encode version for key %s: %w", key, err)
	}

	if err := t.txn.Set([]byte(key), value); err != nil {
		return err
	}
	if err := t.txn.Set([]byte(versionKey), versionBytes); err != nil {
		return err
	}
	if ttl > 0 {
		ttlKey := fmt.Sprintf("ttl:%s", key)
		expireAt := time.Now().Add(ttl)
		ttlBytes, err := json.Marshal(expireAt)
		if err != nil {
			return fmt.Errorf("encode ttl for key %s: %w", key, err)
		}
		if err := t.txn.Set([]byte(ttlKey), ttlBytes); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) Delete(key string) error {
	versionKey := fmt.Sprintf("v:%s", key)
	ttlKey := fmt.Sprintf("ttl:%s", key)

	if err := t.txn.Delete([]byte(key)); err != nil {
		return err
	}
	if err := t.txn.Delete([]byte(versionKey)); err != nil {
		return err
	}
	if err := t.txn.Delete([]byte(ttlKey)); err != nil {
		return err
	}

	return nil
}

func (t *transaction) Exists(key string) (bool, error) {
	_, err := t.txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	return err == nil, err
}

func (t *transaction) GetMetadata(key string) (*ports.KeyMetadata, error) {
	value, version, exists, err := t.Get(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, domain.NewKeyNotFoundError(key)
	}

	metadata := &ports.KeyMetadata{
		Key:     key,
		Version: version,
		Size:    int64(len(value)),
		Updated: time.Now(),
	}

	exp, err := readTTL(t.txn, key)
	if err != nil {
		return nil, err
	}
	metadata.ExpireAt = exp

	return metadata, nil
}

func (s *AppStorage) isMetadataKey(keyBytes []byte) bool {
	if len(keyBytes) >= 2 && keyBytes[0] == 'v' && keyBytes[1] == ':' {
		return true
	}
	if len(keyBytes) >= 4 && keyBytes[0] == 't' && keyBytes[1] == 't' && keyBytes[2] == 'l' && keyBytes[3] == ':' {
		return true
	}
	return false
}

func (t *transaction) Commit() error {
	return t.txn.Commit()
}

func (t *transaction) Rollback() error {
	t.txn.Discard()
	return nil
}

func (s *AppStorage) waitForVersionsAndBroadcast(events []domain.Event) {
	if s.raftNode == nil || s.raftNode.IsLeader() {
		if s.events != nil {
			_ = s.events.PublishStorageEvents(events)
		}
		return
	}

	timeout := time.NewTimer(200 * time.Millisecond)
	defer timeout.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for _, event := range events {
		for {
			select {
			case <-timeout.C:
				if s.events != nil {
					_ = s.events.PublishStorageEvents(events)
				}
				return
			case <-ticker.C:
				currentVersion, err := s.GetVersion(event.Key)
				if err == nil && currentVersion >= event.Version {
					goto nextEvent
				}
			}
		}
	nextEvent:
	}

	if s.events != nil {
		_ = s.events.PublishStorageEvents(events)
	}
}
