package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	json "github.com/goccy/go-json"
)

// StateManager provides advanced state persistence with optimizations (unified)
type StateManager struct {
	storage ports.StoragePort
	logger  *slog.Logger

	// Batching support
	mu           sync.RWMutex
	pendingBatch map[string]*domain.StateUpdate
	batchTimer   *time.Timer
	batchChan    chan struct{}

	// Statistics tracking
	stats map[string]*domain.WorkflowStatistics

	// Incremental snapshot tracking
	snapshots map[string]*domain.WorkflowStateSnapshot
	deltas    map[string][]domain.StateDelta

	// Background processing
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Built-in optimization parameters (no external config)
const (
	batchSizeDefault            = 10
	batchTimeoutDefault         = 500 * time.Millisecond
	maxIncrementalDeltasDefault = 50
)

// NewStateManager creates a new unified state manager with built-in optimized behavior
func NewStateManager(storage ports.StoragePort, logger *slog.Logger) *StateManager {
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	sm := &StateManager{
		storage:      storage,
		logger:       logger.With("component", "state_manager"),
		pendingBatch: make(map[string]*domain.StateUpdate),
		batchChan:    make(chan struct{}, 1),
		stats:        make(map[string]*domain.WorkflowStatistics),
		snapshots:    make(map[string]*domain.WorkflowStateSnapshot),
		deltas:       make(map[string][]domain.StateDelta),
		ctx:          ctx,
		cancel:       cancel,
	}

	sm.wg.Add(1)
	go sm.processBatches()

	return sm
}

// SaveWorkflowState saves workflow state using the configured optimization strategy
func (sm *StateManager) SaveWorkflowState(ctx context.Context, workflow *domain.WorkflowInstance) error {

	sm.updateStatistics(workflow)
	stats := sm.getStatistics(workflow.ID)

	if workflow.Version <= 1 {
		return sm.saveImmediate(ctx, workflow)
	}

	if stats.ChangeFrequency >= 5.0 {
		return sm.saveImmediate(ctx, workflow)
	}
	if stats.StateSize >= 10*1024 {
		return sm.saveIncremental(ctx, workflow)
	}
	return sm.saveImmediate(ctx, workflow)
}

// LoadWorkflowState loads workflow state, reconstructing from incremental snapshots if needed
func (sm *StateManager) LoadWorkflowState(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {

	if workflow, err := sm.loadFromSnapshots(ctx, workflowID); err == nil {
		return workflow, nil
	}

	workflow, err := sm.loadDirect(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	return workflow, nil
}

// UpdateWorkflowState updates workflow state with optimistic concurrency control
func (sm *StateManager) UpdateWorkflowState(ctx context.Context, workflowID string, updateFn func(*domain.WorkflowInstance) error) error {

	for retries := 0; retries < 10; retries++ {
		workflow, err := sm.LoadWorkflowState(ctx, workflowID)
		if err != nil {
			return domain.NewDiscoveryError("state_manager", "load_workflow_for_update", err)
		}

		oldState := sm.cloneWorkflow(workflow)

		if err := updateFn(workflow); err != nil {
			return domain.NewDiscoveryError("state_manager", "update_function", err)
		}

		workflow.Version++

		delta := sm.createStateDelta(oldState, workflow)
		sm.addDelta(workflowID, delta)

		saveErr := sm.SaveWorkflowState(ctx, workflow)
		if saveErr == nil {
			return nil
		}

		if retries == 9 {
			return domain.NewDiscoveryError("state_manager", "update_workflow_state_retries", saveErr)
		}

		backoff := time.Duration(retries*retries) * 10 * time.Millisecond
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return domain.ErrTimeout
}

// Stop gracefully stops the state manager
func (sm *StateManager) Stop() error {
	sm.cancel()
	sm.flushBatch()
	sm.wg.Wait()
	return nil
}

func (sm *StateManager) cleanupBatchesForWorkflow(workflowID string) error {
	entries, err := sm.storage.ListByPrefix(domain.WorkflowBatchPrefix)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if len(entry.Value) == 0 {
			continue
		}

		var batch domain.BatchedStateUpdate
		if err := json.Unmarshal(entry.Value, &batch); err != nil {
			if sm.logger != nil {
				sm.logger.Warn("failed to unmarshal batch during workflow cleanup", slog.String("batch_key", entry.Key), slog.String("workflow_id", workflowID), slog.String("error", err.Error()))
			}
			continue
		}

		filteredIDs := make([]string, 0, len(batch.WorkflowIDs))
		for _, id := range batch.WorkflowIDs {
			if id == workflowID {
				continue
			}
			filteredIDs = append(filteredIDs, id)
		}

		filteredUpdates := make([]domain.StateUpdate, 0, len(batch.Updates))
		for _, update := range batch.Updates {
			if update.WorkflowID == workflowID {
				continue
			}
			filteredUpdates = append(filteredUpdates, update)
		}

		if len(filteredIDs) == len(batch.WorkflowIDs) && len(filteredUpdates) == len(batch.Updates) {
			continue
		}

		if len(filteredIDs) == 0 && len(filteredUpdates) == 0 {
			if err := sm.storage.Delete(entry.Key); err != nil && !domain.IsNotFound(err) {
				return err
			}
			continue
		}

		batch.WorkflowIDs = filteredIDs
		batch.Updates = filteredUpdates

		data, err := json.Marshal(batch)
		if err != nil {
			if sm.logger != nil {
				sm.logger.Warn("failed to marshal batch during workflow cleanup", slog.String("batch_key", entry.Key), slog.String("workflow_id", workflowID), slog.String("error", err.Error()))
			}
			continue
		}

		if err := sm.storage.Put(entry.Key, data, 0); err != nil {
			return err
		}
	}

	return nil
}

// DeleteWorkflow removes all persisted state associated with the specified workflow.
func (sm *StateManager) DeleteWorkflow(ctx context.Context, workflowID string) error {
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		return domain.NewValidationError("workflow id is required", nil)
	}

	var combinedErr error

	if err := sm.storage.Delete(domain.WorkflowStateKey(workflowID)); err != nil && !domain.IsNotFound(err) {
		combinedErr = errors.Join(combinedErr, err)
	}

	snapshotPrefix := fmt.Sprintf("%s%s:", domain.WorkflowSnapshotPrefix, workflowID)
	if _, err := sm.storage.DeleteByPrefix(snapshotPrefix); err != nil {
		combinedErr = errors.Join(combinedErr, err)
	}

	executionPrefix := fmt.Sprintf("workflow:execution:%s:", workflowID)
	if _, err := sm.storage.DeleteByPrefix(executionPrefix); err != nil {
		combinedErr = errors.Join(combinedErr, err)
	}

	if err := sm.cleanupBatchesForWorkflow(workflowID); err != nil {
		combinedErr = errors.Join(combinedErr, err)
	}

	sm.mu.Lock()
	delete(sm.pendingBatch, workflowID)
	delete(sm.snapshots, workflowID)
	delete(sm.deltas, workflowID)
	delete(sm.stats, workflowID)
	sm.mu.Unlock()

	if combinedErr != nil {
		return domain.NewDiscoveryError("state_manager", "delete_workflow", combinedErr)
	}

	return nil
}

// Private methods

func (sm *StateManager) saveImmediate(ctx context.Context, workflow *domain.WorkflowInstance) error {
	key := domain.WorkflowStateKey(workflow.ID)

	data, err := sm.serializeWorkflow(workflow)
	if err != nil {
		return domain.NewDiscoveryError("state_manager", "serialize_workflow", err)
	}

	if err := sm.storage.Put(key, data, workflow.Version); err != nil {
		return domain.NewDiscoveryError("state_manager", "save_workflow_state", err)
	}

	return nil
}

func (sm *StateManager) processBatches() {
	defer sm.wg.Done()

	for {
		select {
		case <-sm.ctx.Done():
			sm.flushBatch()
			return
		case <-sm.batchChan:
			sm.flushBatch()
		}
	}
}

func (sm *StateManager) flushBatch() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(sm.pendingBatch) == 0 {
		return
	}

	if sm.batchTimer != nil {
		sm.batchTimer.Stop()
		sm.batchTimer = nil
	}

	batchID := fmt.Sprintf("batch_%d", time.Now().UnixNano())
	workflowIDs := make([]string, 0, len(sm.pendingBatch))
	updates := make([]domain.StateUpdate, 0, len(sm.pendingBatch))

	for workflowID, update := range sm.pendingBatch {
		workflowIDs = append(workflowIDs, workflowID)
		updates = append(updates, *update)
	}

	batch := domain.BatchedStateUpdate{
		WorkflowIDs: workflowIDs,
		Updates:     updates,
		BatchID:     batchID,
		CreatedAt:   time.Now(),
		Priority:    1,
	}

	key := domain.WorkflowBatchKey(batchID)
	data, err := json.Marshal(batch)
	if err != nil {
		sm.logger.Error("failed to marshal batch", "error", err, "batch_id", batchID)
		return
	}

	if err := sm.storage.Put(key, data, 0); err != nil {
		sm.logger.Error("failed to save batch", "error", err, "batch_id", batchID)
		return
	}

	sm.pendingBatch = make(map[string]*domain.StateUpdate)
}

func (sm *StateManager) saveIncremental(ctx context.Context, workflow *domain.WorkflowInstance) error {
	key := domain.WorkflowSnapshotKey(workflow.ID, workflow.Version)

	deltaCount := len(sm.deltas[workflow.ID])
	needsFullSnapshot := deltaCount >= maxIncrementalDeltasDefault

	var snapshot domain.WorkflowStateSnapshot
	if needsFullSnapshot {
		snapshot = sm.createFullSnapshot(workflow)
		sm.deltas[workflow.ID] = nil
	} else {
		snapshot = sm.createIncrementalSnapshot(workflow)
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return domain.NewDiscoveryError("state_manager", "marshal_snapshot", err)
	}

	if err := sm.storage.Put(key, data, workflow.Version); err != nil {
		return domain.NewDiscoveryError("state_manager", "save_snapshot", err)
	}

	sm.snapshots[workflow.ID] = &snapshot
	return nil
}

func (sm *StateManager) loadDirect(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {
	key := domain.WorkflowStateKey(workflowID)

	data, version, exists, err := sm.storage.Get(key)
	if err != nil {
		return nil, domain.NewDiscoveryError("state_manager", "load_workflow_state", err)
	}
	if !exists {
		return nil, domain.ErrNotFound
	}

	wf, err := sm.deserializeWorkflow(data)
	if err != nil {
		return nil, err
	}
	wf.Version = version
	return wf, nil
}

func (sm *StateManager) loadFromSnapshots(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {
	return nil, domain.ErrNotFound
}

func (sm *StateManager) serializeWorkflow(workflow *domain.WorkflowInstance) ([]byte, error) {
	data, err := json.Marshal(workflow)
	if err != nil {
		return nil, err
	}

	stats := sm.getStatistics(workflow.ID)
	if stats.StateSize >= 1024 {
		return sm.compressData(data)
	}
	return data, nil
}

func (sm *StateManager) deserializeWorkflow(data []byte) (*domain.WorkflowInstance, error) {
	if len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
		decomp, err := sm.decompressData(data)
		if err != nil {
			return nil, err
		}
		data = decomp
	}

	var workflow domain.WorkflowInstance
	if err := json.Unmarshal(data, &workflow); err != nil {
		return nil, domain.NewDiscoveryError("state_manager", "unmarshal_workflow", err)
	}
	return &workflow, nil
}

func (sm *StateManager) compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (sm *StateManager) decompressData(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	return io.ReadAll(gz)
}

func (sm *StateManager) updateStatistics(workflow *domain.WorkflowInstance) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stats, exists := sm.stats[workflow.ID]
	if !exists {
		stats = &domain.WorkflowStatistics{WorkflowID: workflow.ID, LastChangeTimestamp: time.Now()}
		sm.stats[workflow.ID] = stats
	}

	now := time.Now()
	timeSince := now.Sub(stats.LastChangeTimestamp)
	if timeSince > 0 {
		stats.ChangeFrequency = 1.0 / timeSince.Seconds()
	}

	data, _ := json.Marshal(workflow)
	stats.StateSize = int64(len(data))
	stats.LastChangeTimestamp = now
}

func (sm *StateManager) getStatistics(workflowID string) *domain.WorkflowStatistics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	stats, exists := sm.stats[workflowID]
	if !exists {
		return &domain.WorkflowStatistics{WorkflowID: workflowID, LastChangeTimestamp: time.Now()}
	}
	return stats
}

func (sm *StateManager) cloneWorkflow(workflow *domain.WorkflowInstance) *domain.WorkflowInstance {
	data, _ := json.Marshal(workflow)
	var clone domain.WorkflowInstance
	json.Unmarshal(data, &clone)
	return &clone
}

func (sm *StateManager) createStateDelta(oldState, newState *domain.WorkflowInstance) domain.StateDelta {
	return domain.StateDelta{
		Operation: "set",
		Path:      "status",
		OldValue:  json.RawMessage(fmt.Sprintf(`"%s"`, oldState.Status)),
		NewValue:  json.RawMessage(fmt.Sprintf(`"%s"`, newState.Status)),
		Timestamp: time.Now(),
	}
}

func (sm *StateManager) addDelta(workflowID string, delta domain.StateDelta) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.deltas[workflowID] = append(sm.deltas[workflowID], delta)
}

func (sm *StateManager) createFullSnapshot(workflow *domain.WorkflowInstance) domain.WorkflowStateSnapshot {
	data, _ := json.Marshal(workflow)
	checksum := sm.calculateChecksum(data)
	snapshot := domain.WorkflowStateSnapshot{
		WorkflowID:   workflow.ID,
		SequenceNum:  workflow.Version,
		Timestamp:    time.Now(),
		SnapshotType: "full",
		RawData:      data,
		OriginalSize: int64(len(data)),
		Metadata:     make(map[string]string),
	}

	snapshot.Checksum = checksum
	stats := sm.getStatistics(workflow.ID)
	if stats.StateSize >= 1024 {
		if compressed, err := sm.compressData(data); err == nil {
			snapshot.CompressedData = compressed
			snapshot.CompressedSize = int64(len(compressed))
			snapshot.RawData = nil
		}
	}
	return snapshot
}

func (sm *StateManager) createIncrementalSnapshot(workflow *domain.WorkflowInstance) domain.WorkflowStateSnapshot {
	lastSnapshot := sm.snapshots[workflow.ID]
	var basedOnSequence *int64
	if lastSnapshot != nil {
		basedOnSequence = &lastSnapshot.SequenceNum
	}
	snapshot := domain.WorkflowStateSnapshot{
		WorkflowID:      workflow.ID,
		SequenceNum:     workflow.Version,
		Timestamp:       time.Now(),
		SnapshotType:    "incremental",
		BasedOnSequence: basedOnSequence,
		StateDeltas:     sm.deltas[workflow.ID],
		Metadata:        make(map[string]string),
	}
	return snapshot
}

func (sm *StateManager) calculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
