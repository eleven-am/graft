package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/hashicorp/raft"
)

type FSM struct {
	db               *badger.DB
	mu               sync.RWMutex
	versions         map[string]int64
	nodeID           string
	logger           *slog.Logger
	clusterID        string
	clusterValidated bool
}

func NewFSM(db *badger.DB, nodeID, clusterID string, logger *slog.Logger) *FSM {
	if logger == nil {
		logger = slog.Default()
	}
	
	fsm := &FSM{
		db:        db,
		versions:  make(map[string]int64),
		nodeID:    nodeID,
		logger:    logger.With("component", "raft-fsm", "node_id", nodeID),
		clusterID: clusterID,
	}
	
	if err := fsm.validateCluster(); err != nil {
		fsm.logger.Error("cluster validation failed during FSM creation", "error", err)
	} else {
		fsm.clusterValidated = true
	}
	
	return fsm
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd domain.Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("failed to unmarshal command", "error", err)
		return &domain.CommandResult{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal command: %v", err),
		}
	}

	if !f.clusterValidated {
		if err := f.validateCluster(); err != nil {
			f.logger.Error("cluster validation failed", "error", err)
			return &domain.CommandResult{
				Success: false,
				Error:   fmt.Sprintf("cluster validation failed: %v", err),
			}
		}
		f.clusterValidated = true
	}

	switch cmd.Type {
	case domain.CommandPut:
		return f.applyPut(cmd)
	case domain.CommandDelete:
		return f.applyDelete(cmd)
	case domain.CommandCAS:
		return f.applyCAS(cmd)
	case domain.CommandBatch:
		return f.applyBatch(cmd)
	default:
		f.logger.Error("unknown command type", "command_type", cmd.Type)
		return &domain.CommandResult{
			Success: false,
			Error:   fmt.Sprintf("unknown command type: %v", cmd.Type),
		}
	}
}

func (f *FSM) applyPut(cmd domain.Command) *domain.CommandResult {
	if cmd.Key == "" {
		return &domain.CommandResult{
			Success: false,
			Error:   "key cannot be empty",
		}
	}
	
	f.mu.Lock()
	defer f.mu.Unlock()

	currentVersion := f.versions[cmd.Key]
	
	if cmd.Version > 0 && cmd.Version != currentVersion+1 {
		return &domain.CommandResult{
			Success:     false,
			Error:       fmt.Sprintf("version mismatch: expected %d, got %d", currentVersion+1, cmd.Version),
			PrevVersion: currentVersion,
		}
	}

	newVersion := currentVersion + 1
	
	err := f.db.Update(func(txn *badger.Txn) error {
		versionKey := fmt.Sprintf("v:%s", cmd.Key)
		versionBytes, _ := json.Marshal(newVersion)
		
		if err := txn.Set([]byte(cmd.Key), cmd.Value); err != nil {
			return err
		}
		return txn.Set([]byte(versionKey), versionBytes)
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	f.versions[cmd.Key] = newVersion

	return &domain.CommandResult{
		Success: true,
		Version: newVersion,
		Events: []domain.Event{
			{
				Type:      domain.EventPut,
				Key:       cmd.Key,
				Version:   newVersion,
				NodeID:    f.nodeID,
				Timestamp: time.Now(),
				RequestID: cmd.RequestID,
			},
		},
	}
}

func (f *FSM) applyDelete(cmd domain.Command) *domain.CommandResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	err := f.db.Update(func(txn *badger.Txn) error {
		versionKey := fmt.Sprintf("v:%s", cmd.Key)
		if err := txn.Delete([]byte(cmd.Key)); err != nil {
			return err
		}
		return txn.Delete([]byte(versionKey))
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	delete(f.versions, cmd.Key)

	return &domain.CommandResult{
		Success: true,
		Events: []domain.Event{
			{
				Type:      domain.EventDelete,
				Key:       cmd.Key,
				NodeID:    f.nodeID,
				Timestamp: time.Now(),
				RequestID: cmd.RequestID,
			},
		},
	}
}

func (f *FSM) applyCAS(cmd domain.Command) *domain.CommandResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	var currentValue []byte
	var currentVersion int64

	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(cmd.Key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				currentValue = nil
				return nil
			}
			return err
		}
		
		currentValue, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		versionKey := fmt.Sprintf("v:%s", cmd.Key)
		vItem, err := txn.Get([]byte(versionKey))
		if err == nil {
			versionBytes, _ := vItem.ValueCopy(nil)
			json.Unmarshal(versionBytes, &currentVersion)
		}
		return nil
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	if cmd.Version > 0 {
		if currentVersion != cmd.Version {
			f.logger.Info("CAS version mismatch", 
				"key", cmd.Key,
				"expected_version", cmd.Version,
				"current_version", currentVersion)
			return &domain.CommandResult{
				Success:     false,
				Error:       fmt.Sprintf("version mismatch: expected %d, got %d", cmd.Version, currentVersion),
				PrevVersion: currentVersion,
			}
		}
	} else if cmd.Expected != nil {
		if !bytes.Equal(currentValue, cmd.Expected) {
			return &domain.CommandResult{
				Success: false,
				Error:   "value mismatch",
			}
		}
	}

	newVersion := currentVersion + 1
	
	err = f.db.Update(func(txn *badger.Txn) error {
		versionKey := fmt.Sprintf("v:%s", cmd.Key)
		versionBytes, _ := json.Marshal(newVersion)
		
		if err := txn.Set([]byte(cmd.Key), cmd.Value); err != nil {
			return err
		}
		return txn.Set([]byte(versionKey), versionBytes)
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	f.versions[cmd.Key] = newVersion

	return &domain.CommandResult{
		Success:     true,
		Version:     newVersion,
		PrevVersion: currentVersion,
		Events: []domain.Event{
			{
				Type:      domain.EventCAS,
				Key:       cmd.Key,
				Version:   newVersion,
				NodeID:    f.nodeID,
				Timestamp: time.Now(),
				RequestID: cmd.RequestID,
			},
		},
	}
}

func (f *FSM) applyBatch(cmd domain.Command) *domain.CommandResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	results := make([]domain.Result, 0, len(cmd.Batch))
	events := make([]domain.Event, 0, len(cmd.Batch))

	err := f.db.Update(func(txn *badger.Txn) error {
		for _, op := range cmd.Batch {
			switch op.Type {
			case domain.CommandPut:
				newVersion := f.versions[op.Key] + 1
				versionKey := fmt.Sprintf("v:%s", op.Key)
				versionBytes, _ := json.Marshal(newVersion)
				
				if err := txn.Set([]byte(op.Key), op.Value); err != nil {
					return err
				}
				if err := txn.Set([]byte(versionKey), versionBytes); err != nil {
					return err
				}
				
				f.versions[op.Key] = newVersion
				results = append(results, domain.Result{
					Key:     op.Key,
					Success: true,
					Version: newVersion,
				})
				events = append(events, domain.Event{
					Type:      domain.EventPut,
					Key:       op.Key,
					Version:   newVersion,
					NodeID:    f.nodeID,
					Timestamp: time.Now(),
					RequestID: cmd.RequestID,
				})

			case domain.CommandDelete:
				versionKey := fmt.Sprintf("v:%s", op.Key)
				if err := txn.Delete([]byte(op.Key)); err != nil {
					return err
				}
				if err := txn.Delete([]byte(versionKey)); err != nil {
					return err
				}
				
				delete(f.versions, op.Key)
				results = append(results, domain.Result{
					Key:     op.Key,
					Success: true,
				})
				events = append(events, domain.Event{
					Type:      domain.EventDelete,
					Key:       op.Key,
					NodeID:    f.nodeID,
					Timestamp: time.Now(),
					RequestID: cmd.RequestID,
				})

			case domain.CommandCAS:
				item, err := txn.Get([]byte(op.Key))
				var currentValue []byte
				if err == nil {
					currentValue, _ = item.ValueCopy(nil)
				}
				
				if op.Expected != nil && !bytes.Equal(currentValue, op.Expected) {
					results = append(results, domain.Result{
						Key:     op.Key,
						Success: false,
						Error:   "value mismatch in batch",
					})
					continue
				}
				
				newVersion := f.versions[op.Key] + 1
				versionKey := fmt.Sprintf("v:%s", op.Key)
				versionBytes, _ := json.Marshal(newVersion)
				
				if err := txn.Set([]byte(op.Key), op.Value); err != nil {
					return err
				}
				if err := txn.Set([]byte(versionKey), versionBytes); err != nil {
					return err
				}
				
				f.versions[op.Key] = newVersion
				results = append(results, domain.Result{
					Key:     op.Key,
					Success: true,
					Version: newVersion,
				})
				events = append(events, domain.Event{
					Type:      domain.EventCAS,
					Key:       op.Key,
					Version:   newVersion,
					NodeID:    f.nodeID,
					Timestamp: time.Now(),
					RequestID: cmd.RequestID,
				})
			}
		}
		return nil
	})

	if err != nil {
		return &domain.CommandResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &domain.CommandResult{
		Success:      true,
		BatchResults: results,
		Events:       events,
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Info("starting FSM snapshot creation")
	f.mu.RLock()
	defer f.mu.RUnlock()

	snapshot := &fsmSnapshot{
		Versions: make(map[string]int64),
		Data:     make(map[string][]byte),
	}

	for k, v := range f.versions {
		snapshot.Versions[k] = v
	}

	err := f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			snapshot.Data[key] = value
		}
		return nil
	})

	if err != nil {
		f.logger.Error("failed to create FSM snapshot", "error", err)
	} else {
		f.logger.Info("FSM snapshot creation completed", "keys_count", len(snapshot.Data))
	}

	return snapshot, err
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	f.logger.Info("starting FSM restore from snapshot")
	defer rc.Close()

	var snapshot fsmSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		f.logger.Error("failed to decode snapshot", "error", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.versions = make(map[string]int64)
	if snapshot.Versions != nil {
		for k, v := range snapshot.Versions {
			f.versions[k] = v
		}
	}

	if err := f.db.DropAll(); err != nil {
		f.logger.Error("failed to drop all data during restore", "error", err)
		return err
	}

	err := f.db.Update(func(txn *badger.Txn) error {
		for key, value := range snapshot.Data {
			if err := txn.Set([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		f.logger.Error("failed to restore FSM data", "error", err)
	} else {
		f.logger.Info("FSM restore completed", "keys_restored", len(snapshot.Data), "versions_restored", len(snapshot.Versions))
	}

	return err
}

type fsmSnapshot struct {
	Versions map[string]int64  `json:"versions"`
	Data     map[string][]byte `json:"data"`
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(s)
		if err != nil {
			return err
		}

		if _, err := sink.Write(b); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (s *fsmSnapshot) Release() {}

func (f *FSM) validateCluster() error {
	const clusterKey = "cluster_id"
	
	return f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(clusterKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return f.db.Update(func(updateTxn *badger.Txn) error {
					f.logger.Info("initializing cluster ID in FSM", "cluster_id", f.clusterID)
					return updateTxn.Set([]byte(clusterKey), []byte(f.clusterID))
				})
			}
			return err
		}
		
		storedClusterID, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		
		if string(storedClusterID) != f.clusterID {
			return fmt.Errorf("cluster ID mismatch: expected %s, got %s", f.clusterID, string(storedClusterID))
		}
		
		return nil
	})
}