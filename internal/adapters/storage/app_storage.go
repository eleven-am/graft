package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	mu     sync.RWMutex
	subs   map[string][]chan ports.StorageEvent
	closed bool
}

func NewAppStorage(raftNode ports.RaftNode, db *badger.DB, logger *slog.Logger) *AppStorage {
	return &AppStorage{
		raftNode: raftNode,
		db:       db,
		logger:   logger.With("component", "app-storage"),
		subs:     make(map[string][]chan ports.StorageEvent),
	}
}

func (s *AppStorage) Get(key string) (value []byte, version int64, exists bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				exists = false
				return nil
			}
			return err
		}

		exists = true
		value, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		versionKey := fmt.Sprintf("v:%s", key)
		vItem, err := txn.Get([]byte(versionKey))
		if err == nil {
			versionBytes, _ := vItem.ValueCopy(nil)
			json.Unmarshal(versionBytes, &version)
		}

		return nil
	})

	return value, version, exists, err
}

func (s *AppStorage) Put(key string, value []byte, version int64) error {
	if s.raftNode == nil {
		return domain.ErrNotStarted
	}

	cmd := domain.NewPutCommand(key, value, version)
	result, err := s.raftNode.Apply(*cmd, 5*time.Second)
	if err != nil {
		return err
	}

	if !result.Success {
		return errors.New(result.Error)
	}

	for _, event := range result.Events {
		s.broadcastEvent(event)
	}

	return nil
}

func (s *AppStorage) PutWithTTL(key string, value []byte, version int64, ttl time.Duration) error {
	if err := s.Put(key, value, version); err != nil {
		return err
	}

	return s.ExpireAt(key, time.Now().Add(ttl))
}

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

	for _, event := range result.Events {
		s.broadcastEvent(event)
	}

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

	ttlKey := fmt.Sprintf("ttl:%s", key)
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(ttlKey))
		if err == nil {
			ttlBytes, _ := item.ValueCopy(nil)
			var expireAt time.Time
			if json.Unmarshal(ttlBytes, &expireAt) == nil {
				metadata.ExpireAt = &expireAt
			}
		}
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

	for _, event := range result.Events {
		s.broadcastEvent(event)
	}

	return nil
}

func (s *AppStorage) ListByPrefix(prefix string) ([]ports.KeyValueVersion, error) {
	var results []ports.KeyValueVersion

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			if len(key) > 2 && key[:2] == "v:" {
				continue
			}
			if len(key) > 4 && key[:4] == "ttl:" {
				continue
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			var version int64
			versionKey := fmt.Sprintf("v:%s", key)
			vItem, err := txn.Get([]byte(versionKey))
			if err == nil {
				versionBytes, _ := vItem.ValueCopy(nil)
				json.Unmarshal(versionBytes, &version)
			}

			results = append(results, ports.KeyValueVersion{
				Key:     key,
				Value:   value,
				Version: version,
			})
		}

		return nil
	})

	return results, err
}

func (s *AppStorage) GetNext(prefix string) (key string, value []byte, exists bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keyBytes := item.Key()
			key = string(keyBytes)
			
			if s.isMetadataKey(keyBytes) {
				continue
			}
			
			exists = true
			value, err = item.ValueCopy(nil)
			return err
		}
		
		exists = false
		return nil
	})
	
	return key, value, exists, err
}

func (s *AppStorage) GetNextAfter(prefix string, afterKey string) (key string, value []byte, exists bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		
		it.Seek([]byte(afterKey))
		if !it.Valid() {
			exists = false
			return nil
		}
		
		it.Next()
		
		for it.Valid() {
			item := it.Item()
			keyBytes := item.Key()
			key = string(keyBytes)
			
			if s.isMetadataKey(keyBytes) {
				it.Next()
				continue
			}
			
			exists = true
			value, err = item.ValueCopy(nil)
			return err
		}
		
		exists = false
		return nil
	})
	
	return key, value, exists, err
}

func (s *AppStorage) CountPrefix(prefix string) (count int, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		opts.PrefetchValues = false
		opts.PrefetchSize = 1
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Rewind(); it.Valid(); it.Next() {
			keyBytes := it.Item().Key()
			
			if s.isMetadataKey(keyBytes) {
				continue
			}
			
			count++
		}
		
		return nil
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

func (s *AppStorage) IncrementVersion(key string) (newVersion int64, err error) {
	value, currentVersion, exists, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, domain.NewKeyNotFoundError(key)
	}

	newVersion = currentVersion + 1
	return newVersion, s.Put(key, value, newVersion)
}

func (s *AppStorage) ExpireAt(key string, expireTime time.Time) error {
	ttlKey := fmt.Sprintf("ttl:%s", key)
	ttlBytes, _ := json.Marshal(expireTime)

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(ttlKey), ttlBytes)
	})
}

func (s *AppStorage) GetTTL(key string) (time.Duration, error) {
	ttlKey := fmt.Sprintf("ttl:%s", key)
	var expireAt time.Time

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(ttlKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}

		ttlBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return json.Unmarshal(ttlBytes, &expireAt)
	})

	if err != nil {
		return 0, err
	}

	if expireAt.IsZero() {
		return 0, nil
	}

	return time.Until(expireAt), nil
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
				continue
			}

			var expireAt time.Time
			if json.Unmarshal(ttlBytes, &expireAt) == nil {
				if now.After(expireAt) {
					key := string(item.Key())[4:]
					keysToDelete = append(keysToDelete, key)
				}
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

func (s *AppStorage) CreateSnapshot() (io.ReadCloser, error) {
	return nil, domain.ErrNotFound
}

func (s *AppStorage) CreateCompressedSnapshot() (io.ReadCloser, error) {
	return nil, domain.ErrNotFound
}

func (s *AppStorage) RestoreSnapshot(snapshot io.Reader) error {
	return domain.ErrNotFound
}

func (s *AppStorage) RestoreCompressedSnapshot(snapshot io.Reader) error {
	return domain.ErrNotFound
}

func (s *AppStorage) Subscribe(prefix string) (<-chan ports.StorageEvent, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil, &domain.StorageError{Type: domain.ErrClosed, Message: "storage is closed"}
	}

	ch := make(chan ports.StorageEvent, 100)
	s.subs[prefix] = append(s.subs[prefix], ch)

	unsubscribe := func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		subs := s.subs[prefix]
		for i, sub := range subs {
			if sub == ch {
				s.subs[prefix] = append(subs[:i], subs[i+1:]...)
				close(ch)
				break
			}
		}
	}

	return ch, unsubscribe, nil
}

func (s *AppStorage) SubscribeToKey(key string) (<-chan ports.StorageEvent, func(), error) {
	return s.Subscribe(key)
}

func (s *AppStorage) broadcastEvent(event domain.Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return
	}

	storageEvent := ports.StorageEvent{
		Type:      domain.EventType(event.Type),
		Key:       event.Key,
		Version:   event.Version,
		NodeID:    event.NodeID,
		Timestamp: event.Timestamp,
		RequestID: event.RequestID,
	}

	for prefix, subs := range s.subs {
		if len(storageEvent.Key) >= len(prefix) && storageEvent.Key[:len(prefix)] == prefix {
			for _, ch := range subs {
				select {
				case ch <- storageEvent:
				default:
				}
			}
		}
	}
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

	for _, subs := range s.subs {
		for _, ch := range subs {
			close(ch)
		}
	}

	s.subs = make(map[string][]chan ports.StorageEvent)

	return nil
}

type transaction struct {
	txn     *badger.Txn
	storage *AppStorage
}

func (t *transaction) Get(key string) (value []byte, version int64, exists bool, err error) {
	item, err := t.txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}

	value, err = item.ValueCopy(nil)
	if err != nil {
		return nil, 0, false, err
	}

	versionKey := fmt.Sprintf("v:%s", key)
	vItem, err := t.txn.Get([]byte(versionKey))
	if err == nil {
		versionBytes, _ := vItem.ValueCopy(nil)
		json.Unmarshal(versionBytes, &version)
	}

	return value, version, true, nil
}

func (t *transaction) Put(key string, value []byte, version int64) error {
	versionKey := fmt.Sprintf("v:%s", key)
	versionBytes, _ := json.Marshal(version)

	if err := t.txn.Set([]byte(key), value); err != nil {
		return err
	}
	return t.txn.Set([]byte(versionKey), versionBytes)
}

func (t *transaction) PutWithTTL(key string, value []byte, version int64, ttl time.Duration) error {
	if err := t.Put(key, value, version); err != nil {
		return err
	}

	ttlKey := fmt.Sprintf("ttl:%s", key)
	expireAt := time.Now().Add(ttl)
	ttlBytes, _ := json.Marshal(expireAt)

	return t.txn.Set([]byte(ttlKey), ttlBytes)
}

func (t *transaction) Delete(key string) error {
	versionKey := fmt.Sprintf("v:%s", key)
	ttlKey := fmt.Sprintf("ttl:%s", key)

	t.txn.Delete([]byte(key))
	t.txn.Delete([]byte(versionKey))
	t.txn.Delete([]byte(ttlKey))

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

	ttlKey := fmt.Sprintf("ttl:%s", key)
	item, err := t.txn.Get([]byte(ttlKey))
	if err == nil {
		ttlBytes, _ := item.ValueCopy(nil)
		var expireAt time.Time
		if json.Unmarshal(ttlBytes, &expireAt) == nil {
			metadata.ExpireAt = &expireAt
		}
	}

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