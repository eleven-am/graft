package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	json "github.com/goccy/go-json"
)

// OptimizedStateManager provides advanced state persistence with optimizations
type OptimizedStateManager struct {
	storage ports.StoragePort
	logger  *slog.Logger
	config  domain.StateOptimizationConfig

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

// NewOptimizedStateManager creates a new optimized state manager
func NewOptimizedStateManager(storage ports.StoragePort, config domain.StateOptimizationConfig, logger *slog.Logger) *OptimizedStateManager {
	ctx, cancel := context.WithCancel(context.Background())

	sm := &OptimizedStateManager{
		storage:      storage,
		logger:       logger.With("component", "optimized_state_manager"),
		config:       config,
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
func (sm *OptimizedStateManager) SaveWorkflowState(ctx context.Context, workflow *domain.WorkflowInstance) error {
	sm.updateStatistics(workflow)
	stats := sm.getStatistics(workflow.ID)

	if workflow.Version <= 1 {
		return sm.saveImmediate(ctx, workflow)
	}

	switch {
	case sm.config.ShouldUseBatching(stats):
		return sm.addToBatch(ctx, workflow)
	case sm.config.ShouldUseIncremental(stats):
		return sm.saveIncremental(ctx, workflow)
	default:
		return sm.saveImmediate(ctx, workflow)
	}
}

// LoadWorkflowState loads workflow state, reconstructing from incremental snapshots if needed
func (sm *OptimizedStateManager) LoadWorkflowState(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {

	if workflow, err := sm.loadFromSnapshots(ctx, workflowID); err == nil {
		return workflow, nil
	}

	return sm.loadDirect(ctx, workflowID)
}

// UpdateWorkflowState updates workflow state with optimistic concurrency control
func (sm *OptimizedStateManager) UpdateWorkflowState(ctx context.Context, workflowID string, updateFn func(*domain.WorkflowInstance) error) error {
	for retries := 0; retries < 10; retries++ {
		workflow, err := sm.LoadWorkflowState(ctx, workflowID)
		if err != nil {
			return domain.NewDiscoveryError("optimized_state_manager", "load_workflow_for_update", err)
		}

		oldState := sm.cloneWorkflow(workflow)

		if err := updateFn(workflow); err != nil {
			return domain.NewDiscoveryError("optimized_state_manager", "update_function", err)
		}

		workflow.Version++

		if sm.config.IncrementalSnapshots {
			delta := sm.createStateDelta(oldState, workflow)
			sm.addDelta(workflowID, delta)
		}

		if err := sm.SaveWorkflowState(ctx, workflow); err == nil {
			return nil
		}

		if retries == 9 {
			return domain.NewDiscoveryError("optimized_state_manager", "update_workflow_state_retries", err)
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

// Stop gracefully stops the optimized state manager
func (sm *OptimizedStateManager) Stop() error {
	sm.cancel()

	sm.flushBatch()

	sm.wg.Wait()
	return nil
}

// Private methods

func (sm *OptimizedStateManager) saveImmediate(ctx context.Context, workflow *domain.WorkflowInstance) error {
	key := fmt.Sprintf("workflow:state:%s", workflow.ID)

	data, err := sm.serializeWorkflow(workflow)
	if err != nil {
		return domain.NewDiscoveryError("optimized_state_manager", "serialize_workflow", err)
	}

	if err := sm.storage.Put(key, data, workflow.Version); err != nil {
		return domain.NewDiscoveryError("optimized_state_manager", "save_workflow_state", err)
	}

	return nil
}

func (sm *OptimizedStateManager) addToBatch(ctx context.Context, workflow *domain.WorkflowInstance) error {

	data, err := sm.serializeWorkflow(workflow)
	if err != nil {
		return domain.NewDiscoveryError("optimized_state_manager", "serialize_workflow_for_batch", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	update := &domain.StateUpdate{
		WorkflowID:   workflow.ID,
		UpdateType:   "state_changed",
		StateChanges: []domain.StateDelta{},
		Timestamp:    time.Now(),
		Metadata: map[string]string{
			"serialized_data": string(data),
			"version":         fmt.Sprintf("%d", workflow.Version),
		},
	}

	sm.pendingBatch[workflow.ID] = update

	if sm.batchTimer == nil && len(sm.pendingBatch) > 0 {
		sm.batchTimer = time.AfterFunc(sm.config.BatchTimeout, func() {
			select {
			case sm.batchChan <- struct{}{}:
			default:

			}
		})
	}

	if len(sm.pendingBatch) >= sm.config.BatchSize {
		select {
		case sm.batchChan <- struct{}{}:
		default:

		}
	}

	return nil
}

func (sm *OptimizedStateManager) processBatches() {
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

func (sm *OptimizedStateManager) flushBatch() {
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

	key := fmt.Sprintf("workflow:batch:%s", batchID)
	data, err := json.Marshal(batch)
	if err != nil {
		sm.logger.Error("failed to marshal batch", "error", err, "batch_id", batchID)
		return
	}

	if err := sm.storage.Put(key, data, 0); err != nil {
		sm.logger.Error("failed to save batch", "error", err, "batch_id", batchID)
		return
	}

	sm.logger.Debug("flushed state batch", "batch_id", batchID, "update_count", len(updates))

	sm.pendingBatch = make(map[string]*domain.StateUpdate)
}

func (sm *OptimizedStateManager) saveIncremental(ctx context.Context, workflow *domain.WorkflowInstance) error {
	key := fmt.Sprintf("workflow:snapshot:%s:%d", workflow.ID, workflow.Version)

	deltaCount := len(sm.deltas[workflow.ID])
	needsFullSnapshot := deltaCount >= sm.config.MaxIncrementalDeltas

	var snapshot domain.WorkflowStateSnapshot
	if needsFullSnapshot {
		snapshot = sm.createFullSnapshot(workflow)

		sm.deltas[workflow.ID] = nil
	} else {
		snapshot = sm.createIncrementalSnapshot(workflow)
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return domain.NewDiscoveryError("optimized_state_manager", "marshal_snapshot", err)
	}

	if err := sm.storage.Put(key, data, workflow.Version); err != nil {
		return domain.NewDiscoveryError("optimized_state_manager", "save_snapshot", err)
	}

	sm.snapshots[workflow.ID] = &snapshot
	return nil
}

func (sm *OptimizedStateManager) loadDirect(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {
	key := fmt.Sprintf("workflow:state:%s", workflowID)

	data, _, exists, err := sm.storage.Get(key)
	if err != nil {
		return nil, domain.NewDiscoveryError("optimized_state_manager", "load_workflow_state", err)
	}
	if !exists {
		return nil, domain.ErrNotFound
	}

	return sm.deserializeWorkflow(data)
}

func (sm *OptimizedStateManager) loadFromSnapshots(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {

	return nil, domain.ErrNotFound
}

func (sm *OptimizedStateManager) serializeWorkflow(workflow *domain.WorkflowInstance) ([]byte, error) {
	data, err := json.Marshal(workflow)
	if err != nil {
		return nil, err
	}

	stats := sm.getStatistics(workflow.ID)
	if sm.config.ShouldCompress(stats.StateSize) {
		return sm.compressData(data)
	}

	return data, nil
}

func (sm *OptimizedStateManager) deserializeWorkflow(data []byte) (*domain.WorkflowInstance, error) {

	if len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
		decompressed, err := sm.decompressData(data)
		if err != nil {
			return nil, err
		}
		data = decompressed
	}

	var workflow domain.WorkflowInstance
	if err := json.Unmarshal(data, &workflow); err != nil {
		return nil, domain.NewDiscoveryError("optimized_state_manager", "unmarshal_workflow", err)
	}

	return &workflow, nil
}

func (sm *OptimizedStateManager) compressData(data []byte) ([]byte, error) {
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

func (sm *OptimizedStateManager) decompressData(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	return io.ReadAll(gz)
}

func (sm *OptimizedStateManager) updateStatistics(workflow *domain.WorkflowInstance) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stats, exists := sm.stats[workflow.ID]
	if !exists {
		stats = &domain.WorkflowStatistics{
			WorkflowID:          workflow.ID,
			LastChangeTimestamp: time.Now(),
		}
		sm.stats[workflow.ID] = stats
	}

	now := time.Now()
	timeSinceLastChange := now.Sub(stats.LastChangeTimestamp)
	if timeSinceLastChange > 0 {
		stats.ChangeFrequency = 1.0 / timeSinceLastChange.Seconds()
	}

	data, _ := json.Marshal(workflow)
	stats.StateSize = int64(len(data))
	stats.LastChangeTimestamp = now
}

func (sm *OptimizedStateManager) getStatistics(workflowID string) *domain.WorkflowStatistics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats, exists := sm.stats[workflowID]
	if !exists {
		return &domain.WorkflowStatistics{
			WorkflowID:          workflowID,
			LastChangeTimestamp: time.Now(),
		}
	}

	return stats
}

func (sm *OptimizedStateManager) cloneWorkflow(workflow *domain.WorkflowInstance) *domain.WorkflowInstance {
	data, _ := json.Marshal(workflow)
	var clone domain.WorkflowInstance
	json.Unmarshal(data, &clone)
	return &clone
}

func (sm *OptimizedStateManager) createStateDelta(oldState, newState *domain.WorkflowInstance) domain.StateDelta {

	return domain.StateDelta{
		Operation: "set",
		Path:      "status",
		OldValue:  json.RawMessage(fmt.Sprintf(`"%s"`, oldState.Status)),
		NewValue:  json.RawMessage(fmt.Sprintf(`"%s"`, newState.Status)),
		Timestamp: time.Now(),
	}
}

func (sm *OptimizedStateManager) addDelta(workflowID string, delta domain.StateDelta) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.deltas[workflowID] = append(sm.deltas[workflowID], delta)
}

func (sm *OptimizedStateManager) createFullSnapshot(workflow *domain.WorkflowInstance) domain.WorkflowStateSnapshot {
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

	if sm.config.EnableChecksums {
		snapshot.Checksum = checksum
	}

	stats := sm.getStatistics(workflow.ID)
	if sm.config.ShouldCompress(stats.StateSize) {
		if compressed, err := sm.compressData(data); err == nil {
			snapshot.CompressedData = compressed
			snapshot.CompressedSize = int64(len(compressed))
			snapshot.RawData = nil
		}
	}

	return snapshot
}

func (sm *OptimizedStateManager) createIncrementalSnapshot(workflow *domain.WorkflowInstance) domain.WorkflowStateSnapshot {
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

func (sm *OptimizedStateManager) calculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
