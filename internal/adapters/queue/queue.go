package queue

import (
	"context"
	"fmt"
	json "github.com/eleven-am/graft/internal/xjson"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/google/uuid"
)

type Queue struct {
	name            string
	storage         ports.StoragePort
	eventManager    ports.EventManager
	logger          *slog.Logger
	mu              sync.RWMutex
	closed          bool
	workflowIndex   sync.Map
	claimedIndex    sync.Map
	claimToWorkflow sync.Map
}

func NewQueue(name string, storage ports.StoragePort, eventManager ports.EventManager, logger *slog.Logger) *Queue {
	if logger == nil {
		logger = slog.Default()
	}
	q := &Queue{
		name:         name,
		storage:      storage,
		eventManager: eventManager,
		logger:       logger,
	}

	return q
}

func (q *Queue) newQueueError(message string, cause error, opts ...domain.ErrorOption) *domain.DomainError {
	merged := []domain.ErrorOption{domain.WithComponent("queue.Queue"), domain.WithContextDetail("queue_name", q.name)}
	if len(opts) > 0 {
		merged = append(merged, opts...)
	}
	return domain.NewResourceError(message, cause, merged...)
}

func (q *Queue) Enqueue(item []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(item) > 1<<20 {
		return &domain.StorageError{Type: domain.ErrStorageFull, Message: "queue item too large"}
	}
	if q.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	sequence, err := q.getNextSequence()
	if err != nil {
		return err
	}

	queueItem := domain.NewQueueItem(item, sequence)
	itemBytes, err := queueItem.ToBytes()
	if err != nil {
		return err
	}

	key := domain.QueueReadyKey(q.name, sequence)
	if err := q.storage.Put(key, itemBytes, 0); err != nil {
		return err
	}

	q.updateWorkflowIndex(item, sequence, true)

	if q.eventManager != nil {
		event := domain.Event{Type: domain.EventPut, Key: key, Timestamp: time.Now()}
		_ = q.eventManager.Broadcast(event)
	}

	return nil
}

func (q *Queue) Peek() (item []byte, exists bool, err error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return nil, false, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	prefix := fmt.Sprintf("queue:%s:ready:", q.name)
	_, value, exists, err := q.storage.GetNext(prefix)
	if err != nil {
		return nil, false, err
	}

	if !exists {
		return nil, false, nil
	}

	queueItem, err := domain.QueueItemFromBytes(value)
	if err != nil {
		return nil, false, err
	}

	return queueItem.Data, true, nil
}

func (q *Queue) Claim() (item []byte, claimID string, exists bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, "", false, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	prefix := fmt.Sprintf("queue:%s:ready:", q.name)
	now := time.Now()

	currentKey, value, itemExists, err := q.storage.GetNext(prefix)
	if err != nil {
		return nil, "", false, err
	}

	for itemExists {
		queueItem, err := domain.QueueItemFromBytes(value)
		if err != nil {
			currentKey, value, itemExists, err = q.storage.GetNextAfter(prefix, currentKey)
			if err != nil {
				return nil, "", false, err
			}
			continue
		}

		var workItem struct {
			ProcessAfter time.Time `json:"process_after"`
		}
		dataStr := string(queueItem.Data)
		if strings.Contains(dataStr, "\"process_after\"") {
			if err := json.Unmarshal(queueItem.Data, &workItem); err != nil {
				return nil, "", false, q.newQueueError(
					"failed to decode process_after metadata",
					err,
					domain.WithContextDetail("sequence", strconv.FormatInt(queueItem.Sequence, 10)),
					domain.WithContextDetail("storage_key", currentKey),
					domain.WithContextDetail("operation", "claim"),
				)
			}
		}

		if workItem.ProcessAfter.IsZero() || workItem.ProcessAfter.Before(now) || workItem.ProcessAfter.Equal(now) {
			claimID = uuid.New().String()
			claimedItem := domain.NewClaimedItem(queueItem.Data, claimID, queueItem.Sequence)
			claimedBytes, err := claimedItem.ToBytes()
			if err != nil {
				return nil, "", false, err
			}

			ops := []ports.WriteOp{
				{
					Type: ports.OpDelete,
					Key:  currentKey,
				},
				{
					Type:  ports.OpPut,
					Key:   domain.QueueClaimedKey(q.name, claimID),
					Value: claimedBytes,
				},
			}

			err = q.storage.BatchWrite(ops)
			if err != nil {
				return nil, "", false, err
			}

			q.updateWorkflowIndex(queueItem.Data, queueItem.Sequence, false)
			q.updateClaimedIndex(claimID, queueItem.Data, true)

			return queueItem.Data, claimID, true, nil
		}

		currentKey, value, itemExists, err = q.storage.GetNextAfter(prefix, currentKey)
		if err != nil {
			return nil, "", false, err
		}
	}

	return nil, "", false, nil
}

func (q *Queue) Complete(claimID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	key := domain.QueueClaimedKey(q.name, claimID)
	q.removeClaimedIndexByClaimID(claimID)
	return q.storage.Delete(key)
}

func (q *Queue) Release(claimID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	claimedKey := domain.QueueClaimedKey(q.name, claimID)
	value, _, exists, err := q.storage.Get(claimedKey)
	if err != nil {
		return err
	}
	if !exists {
		return &domain.StorageError{Type: domain.ErrKeyNotFound, Key: claimedKey, Message: "claimed item not found"}
	}

	claimedItem, err := domain.ClaimedItemFromBytes(value)
	if err != nil {
		return err
	}

	type workItemMeta struct {
		ProcessAfter time.Time `json:"process_after"`
		Deferrals    int       `json:"deferrals,omitempty"`
	}

	var meta workItemMeta
	updatedData := claimedItem.Data
	if err := json.Unmarshal(claimedItem.Data, &meta); err == nil {
		meta.Deferrals++
		const deferralLimit = 10
		if meta.Deferrals >= deferralLimit {
			seq, seqErr := q.getNextDeadLetterSequence()
			if seqErr != nil {
				return seqErr
			}
			dlqItem := domain.NewDeadLetterQueueItem(claimedItem.Data, fmt.Sprintf("exceeded deferrals (%d)", deferralLimit), meta.Deferrals, seq)
			dlqBytes, encErr := dlqItem.ToBytes()
			if encErr != nil {
				return encErr
			}
			dlqKey := domain.QueueDeadLetterKey(q.name, dlqItem.ID)

			ops := []ports.WriteOp{
				{Type: ports.OpPut, Key: dlqKey, Value: dlqBytes},
				{Type: ports.OpDelete, Key: claimedKey},
			}
			if err := q.storage.BatchWrite(ops); err != nil {
				return err
			}
			q.updateWorkflowIndex(claimedItem.Data, claimedItem.Sequence, false)
			return nil
		}

		delay := 50*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
		meta.ProcessAfter = time.Now().Add(delay)
		if newBytes, encErr := json.Marshal(meta); encErr == nil {
			updatedData = newBytes
		}
	}

	queueItem := domain.NewQueueItem(updatedData, claimedItem.Sequence)
	itemBytes, err := queueItem.ToBytes()
	if err != nil {
		return err
	}

	readyKey := domain.QueueReadyKey(q.name, claimedItem.Sequence)
	ops := []ports.WriteOp{
		{Type: ports.OpPut, Key: readyKey, Value: itemBytes},
		{Type: ports.OpDelete, Key: claimedKey},
	}
	if err := q.storage.BatchWrite(ops); err != nil {
		return err
	}

	q.updateWorkflowIndex(updatedData, claimedItem.Sequence, true)
	q.removeClaimedIndexByClaimID(claimID)
	return nil
}

func (q *Queue) WaitForItem(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{}, 1)

	go func() {
		defer close(ch)

		prefix := fmt.Sprintf("queue:%s:ready:", q.name)

		type channelSubscriber interface {
			SubscribeToChannel(prefix string) (<-chan domain.Event, func(), error)
		}

		if sub, ok := q.eventManager.(channelSubscriber); ok {
			eventsCh, unsubscribe, err := sub.SubscribeToChannel(prefix)
			if err == nil && eventsCh != nil && unsubscribe != nil {
				defer unsubscribe()
				for {
					select {
					case <-ctx.Done():
						return
					case _, ok := <-eventsCh:
						if !ok {
							return
						}
						select {
						case ch <- struct{}{}:
						default:
						}
					}
				}
			}
		}

		<-ctx.Done()
	}()

	return ch
}

func (q *Queue) Size() (int, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return 0, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	readyPrefix := fmt.Sprintf("queue:%s:ready:", q.name)
	readyCount, err := q.storage.CountPrefix(readyPrefix)
	if err != nil {
		return 0, err
	}

	blockedPrefix := fmt.Sprintf("queue:%s:blocked:", q.name)
	blockedCount, err := q.storage.CountPrefix(blockedPrefix)
	if err != nil {
		return 0, err
	}

	return readyCount + blockedCount, nil
}

func (q *Queue) HasItemsWithPrefix(dataPrefix string) (bool, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return false, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	if strings.HasPrefix(dataPrefix, `"workflow_id":"`) {
		workflowID := q.extractWorkflowIDFromPrefix(dataPrefix)
		if workflowID != "" {
			hasItems := q.hasWorkflowItems(workflowID)
			return hasItems, nil
		}
	}

	readyPrefix := fmt.Sprintf("queue:%s:ready:", q.name)
	items, err := q.storage.ListByPrefix(readyPrefix)
	if err != nil {
		return false, err
	}

	for _, item := range items {
		queueItem, err := domain.QueueItemFromBytes(item.Value)
		if err != nil {
			continue
		}

		if len(queueItem.Data) > 0 {
			dataStr := string(queueItem.Data)
			if len(dataStr) >= len(dataPrefix) &&
				dataStr[:len(dataPrefix)] == dataPrefix ||
				strings.Contains(dataStr, dataPrefix) {
				return true, nil
			}
		}
	}

	blockedPrefix := fmt.Sprintf("queue:%s:blocked:", q.name)
	blockedItems, err := q.storage.ListByPrefix(blockedPrefix)
	if err != nil {
		return false, err
	}

	for _, item := range blockedItems {
		blockedItem, err := domain.BlockedItemFromBytes(item.Value)
		if err != nil {
			continue
		}

		if len(blockedItem.Data) > 0 {
			dataStr := string(blockedItem.Data)
			if len(dataStr) >= len(dataPrefix) &&
				dataStr[:len(dataPrefix)] == dataPrefix ||
				strings.Contains(dataStr, dataPrefix) {
				return true, nil
			}
		}
	}

	return false, nil
}

func (q *Queue) HasClaimedItemsWithPrefix(dataPrefix string) (bool, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return false, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	if strings.HasPrefix(dataPrefix, `"workflow_id":"`) {
		workflowID := q.extractWorkflowIDFromPrefix(dataPrefix)
		if workflowID != "" {
			if _, exists := q.claimedIndex.Load(workflowID); exists {
				return true, nil
			}

		}
	}

	prefix := fmt.Sprintf("queue:%s:claimed:", q.name)
	items, err := q.storage.ListByPrefix(prefix)
	if err != nil {
		return false, err
	}

	for _, item := range items {
		claimedItem, err := domain.ClaimedItemFromBytes(item.Value)
		if err != nil {
			continue
		}
		if len(claimedItem.Data) > 0 {
			dataStr := string(claimedItem.Data)
			if strings.Contains(dataStr, dataPrefix) {
				return true, nil
			}
		}
	}

	return false, nil
}

func (q *Queue) GetClaimedItemsWithPrefix(dataPrefix string) ([]ports.ClaimedItem, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return nil, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	var results []ports.ClaimedItem
	if strings.HasPrefix(dataPrefix, `"workflow_id":"`) {
		workflowID := q.extractWorkflowIDFromPrefix(dataPrefix)
		if workflowID != "" {
			if ids, ok := q.claimedIndex.Load(workflowID); ok {
				idMap := ids.(*sync.Map)
				idMap.Range(func(k, _ interface{}) bool {
					claimID, _ := k.(string)
					key := domain.QueueClaimedKey(q.name, claimID)
					if value, _, exists, err := q.storage.Get(key); err == nil && exists {
						if ci, e := domain.ClaimedItemFromBytes(value); e == nil {
							results = append(results, ports.ClaimedItem{Data: ci.Data, ClaimID: ci.ClaimID, ClaimedAt: ci.ClaimedAt, Sequence: ci.Sequence})
						}
					}
					return true
				})
				return results, nil
			}

		}
	}

	prefix := fmt.Sprintf("queue:%s:claimed:", q.name)
	items, err := q.storage.ListByPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		claimedItem, err := domain.ClaimedItemFromBytes(item.Value)
		if err != nil {
			continue
		}
		if len(claimedItem.Data) > 0 && strings.Contains(string(claimedItem.Data), dataPrefix) {
			results = append(results, ports.ClaimedItem{Data: claimedItem.Data, ClaimID: claimedItem.ClaimID, ClaimedAt: claimedItem.ClaimedAt, Sequence: claimedItem.Sequence})
		}
	}

	return results, nil
}

func (q *Queue) GetItemsWithPrefix(dataPrefix string) ([][]byte, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return nil, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	if strings.HasPrefix(dataPrefix, `"workflow_id":"`) {
		workflowID := q.extractWorkflowIDFromPrefix(dataPrefix)
		if workflowID != "" {
			sequences := q.getWorkflowSequences(workflowID)
			if len(sequences) == 0 {
				return [][]byte{}, nil
			}

			var matchingItems [][]byte
			for _, seq := range sequences {
				readyKey := domain.QueueReadyKey(q.name, seq)
				value, _, exists, err := q.storage.Get(readyKey)
				if err != nil {
					return nil, err
				}
				if exists {
					queueItem, err := domain.QueueItemFromBytes(value)
					if err != nil {
						continue
					}

					matchingItems = append(matchingItems, queueItem.Data)
					continue
				}

				blockedKey := domain.QueueBlockedKey(q.name, seq)
				blockedValue, _, blockedExists, err := q.storage.Get(blockedKey)
				if err != nil {
					return nil, err
				}
				if !blockedExists {
					continue
				}

				blockedItem, err := domain.BlockedItemFromBytes(blockedValue)
				if err != nil {
					continue
				}

				matchingItems = append(matchingItems, blockedItem.Data)
			}
			return matchingItems, nil
		}
	}

	var matchingItems [][]byte
	readyPrefix := fmt.Sprintf("queue:%s:ready:", q.name)
	items, err := q.storage.ListByPrefix(readyPrefix)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		queueItem, err := domain.QueueItemFromBytes(item.Value)
		if err != nil {
			continue
		}

		if len(queueItem.Data) > 0 {
			dataStr := string(queueItem.Data)
			if len(dataStr) >= len(dataPrefix) &&
				dataStr[:len(dataPrefix)] == dataPrefix ||
				strings.Contains(dataStr, dataPrefix) {
				matchingItems = append(matchingItems, queueItem.Data)
			}
		}
	}

	blockedPrefix := fmt.Sprintf("queue:%s:blocked:", q.name)
	blockedItems, err := q.storage.ListByPrefix(blockedPrefix)
	if err != nil {
		return nil, err
	}

	for _, item := range blockedItems {
		blockedItem, err := domain.BlockedItemFromBytes(item.Value)
		if err != nil {
			continue
		}

		if len(blockedItem.Data) > 0 {
			dataStr := string(blockedItem.Data)
			if len(dataStr) >= len(dataPrefix) &&
				dataStr[:len(dataPrefix)] == dataPrefix ||
				strings.Contains(dataStr, dataPrefix) {
				matchingItems = append(matchingItems, blockedItem.Data)
			}
		}
	}

	return matchingItems, nil
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	return nil
}

func (q *Queue) getNextSequence() (int64, error) {
	key := domain.QueueSequenceKey(q.name)
	return q.storage.AtomicIncrement(key)
}

func (q *Queue) SendToDeadLetter(item []byte, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	sequence, err := q.getNextDeadLetterSequence()
	if err != nil {
		return err
	}

	dlqItem := domain.NewDeadLetterQueueItem(item, reason, 0, sequence)
	itemBytes, err := dlqItem.ToBytes()
	if err != nil {
		return err
	}

	key := domain.QueueDeadLetterKey(q.name, dlqItem.ID)
	return q.storage.Put(key, itemBytes, 0)
}

func (q *Queue) GetDeadLetterItems(limit int) ([]ports.DeadLetterItem, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return nil, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	prefix := fmt.Sprintf("queue:%s:deadletter:", q.name)
	items, err := q.storage.ListByPrefix(prefix)
	if err != nil {
		return nil, err
	}

	var dlqItems []ports.DeadLetterItem
	for i, item := range items {
		if limit > 0 && i >= limit {
			break
		}

		dlqItem, err := domain.DeadLetterQueueItemFromBytes(item.Value)
		if err != nil {
			continue
		}

		dlqItems = append(dlqItems, ports.DeadLetterItem{
			ID:         dlqItem.ID,
			Item:       dlqItem.Data,
			Reason:     dlqItem.Reason,
			Timestamp:  dlqItem.Timestamp,
			RetryCount: dlqItem.RetryCount,
		})
	}

	return dlqItems, nil
}

func (q *Queue) GetDeadLetterSize() (int, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return 0, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	prefix := fmt.Sprintf("queue:%s:deadletter:", q.name)
	return q.storage.CountPrefix(prefix)
}

func (q *Queue) RetryFromDeadLetter(itemID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	dlqKey := domain.QueueDeadLetterKey(q.name, itemID)
	value, _, exists, err := q.storage.Get(dlqKey)
	if err != nil {
		return err
	}

	if !exists {
		return &domain.StorageError{Type: domain.ErrKeyNotFound, Key: dlqKey, Message: "dead letter item not found"}
	}

	dlqItem, err := domain.DeadLetterQueueItemFromBytes(value)
	if err != nil {
		return err
	}

	if err := q.Enqueue(dlqItem.Data); err != nil {
		return err
	}

	return q.storage.Delete(dlqKey)
}

func (q *Queue) getNextDeadLetterSequence() (int64, error) {
	key := domain.QueueDeadLetterSequenceKey(q.name)
	return q.storage.AtomicIncrement(key)
}

func (q *Queue) updateWorkflowIndex(itemData []byte, sequence int64, add bool) {
	workflowID := q.extractWorkflowID(itemData)
	if workflowID == "" {
		return
	}

	if add {
		sequenceMapInterface, _ := q.workflowIndex.LoadOrStore(workflowID, &sync.Map{})
		sequenceMap := sequenceMapInterface.(*sync.Map)
		sequenceMap.Store(sequence, struct{}{})
	} else {
		if sequenceMapInterface, exists := q.workflowIndex.Load(workflowID); exists {
			sequenceMap := sequenceMapInterface.(*sync.Map)
			sequenceMap.Delete(sequence)

			var hasItems bool
			sequenceMap.Range(func(_, _ interface{}) bool {
				hasItems = true
				return false
			})

			if !hasItems {
				q.workflowIndex.Delete(workflowID)
			}
		}
	}
}

func (q *Queue) extractWorkflowID(itemData []byte) string {
	itemStr := string(itemData)

	workflowIDStart := strings.Index(itemStr, `"workflow_id":"`)
	if workflowIDStart == -1 {
		return ""
	}

	valueStart := workflowIDStart + len(`"workflow_id":"`)

	valueEnd := strings.Index(itemStr[valueStart:], `"`)
	if valueEnd == -1 {
		return ""
	}

	return itemStr[valueStart : valueStart+valueEnd]
}

func (q *Queue) extractWorkflowIDFromPrefix(dataPrefix string) string {
	if !strings.HasPrefix(dataPrefix, `"workflow_id":"`) {
		return ""
	}

	valueStart := len(`"workflow_id":"`)
	if len(dataPrefix) <= valueStart {
		return ""
	}

	remaining := dataPrefix[valueStart:]
	valueEnd := strings.Index(remaining, `"`)
	if valueEnd == -1 {
		return remaining
	}

	return remaining[:valueEnd]
}

func (q *Queue) hasWorkflowItems(workflowID string) bool {
	_, exists := q.workflowIndex.Load(workflowID)
	return exists
}

func (q *Queue) getWorkflowSequences(workflowID string) []int64 {
	sequenceMapInterface, exists := q.workflowIndex.Load(workflowID)
	if !exists {
		return nil
	}

	sequenceMap := sequenceMapInterface.(*sync.Map)
	var sequences []int64

	sequenceMap.Range(func(key, _ interface{}) bool {
		if seq, ok := key.(int64); ok {
			sequences = append(sequences, seq)
		}
		return true
	})

	return sequences
}

func (q *Queue) rebuildWorkflowIndex() {
	readyPrefix := fmt.Sprintf("queue:%s:ready:", q.name)
	blockedPrefix := fmt.Sprintf("queue:%s:blocked:", q.name)

	currentKey, value, exists, err := q.storage.GetNext(readyPrefix)
	if err == nil && exists {
		for exists {
			queueItem, decodeErr := domain.QueueItemFromBytes(value)
			if decodeErr == nil {
				q.updateWorkflowIndex(queueItem.Data, queueItem.Sequence, true)
			}

			currentKey, value, exists, err = q.storage.GetNextAfter(readyPrefix, currentKey)
			if err != nil {
				break
			}
		}
	}

	blockedKey, blockedValue, blockedExists, blockedErr := q.storage.GetNext(blockedPrefix)
	if blockedErr == nil && blockedExists {
		for blockedExists {
			blockedItem, decodeErr := domain.BlockedItemFromBytes(blockedValue)
			if decodeErr == nil {
				q.updateWorkflowIndex(blockedItem.Data, blockedItem.Sequence, true)
			}

			blockedKey, blockedValue, blockedExists, blockedErr = q.storage.GetNextAfter(blockedPrefix, blockedKey)
			if blockedErr != nil {
				break
			}
		}
	}
}

func (q *Queue) rebuildClaimedIndex() {
	prefix := fmt.Sprintf("queue:%s:claimed:", q.name)
	currentKey, value, exists, err := q.storage.GetNext(prefix)
	if err != nil || !exists {
		return
	}
	for exists {
		if claimedItem, e := domain.ClaimedItemFromBytes(value); e == nil {
			q.updateClaimedIndex(claimedItem.ClaimID, claimedItem.Data, true)
		}
		currentKey, value, exists, err = q.storage.GetNextAfter(prefix, currentKey)
		if err != nil {
			break
		}
	}
}

func (q *Queue) updateClaimedIndex(claimID string, itemData []byte, add bool) {
	workflowID := q.extractWorkflowID(itemData)
	if workflowID == "" {
		return
	}
	if add {
		mInterface, _ := q.claimedIndex.LoadOrStore(workflowID, &sync.Map{})
		m := mInterface.(*sync.Map)
		m.Store(claimID, struct{}{})
		q.claimToWorkflow.Store(claimID, workflowID)
	} else {
		q.removeClaimedIndexByClaimID(claimID)
	}
}

func (q *Queue) removeClaimedIndexByClaimID(claimID string) {
	if wf, ok := q.claimToWorkflow.Load(claimID); ok {
		workflowID := wf.(string)
		if mInterface, ok := q.claimedIndex.Load(workflowID); ok {
			m := mInterface.(*sync.Map)
			m.Delete(claimID)
			empty := true
			m.Range(func(_, _ interface{}) bool { empty = false; return false })
			if empty {
				q.claimedIndex.Delete(workflowID)
			}
		}
		q.claimToWorkflow.Delete(claimID)
	}
}

func (q *Queue) Start(ctx context.Context) error {
	q.rebuildWorkflowIndex()
	q.rebuildClaimedIndex()
	return nil
}

// Two-tier queue methods

func (q *Queue) EnqueueBlocked(item []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(item) > 1<<20 {
		return &domain.StorageError{Type: domain.ErrStorageFull, Message: "queue item too large"}
	}
	if q.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	sequence, err := q.getNextSequence()
	if err != nil {
		return err
	}

	blockedItem := domain.NewBlockedItem(item, sequence)
	itemBytes, err := blockedItem.ToBytes()
	if err != nil {
		return err
	}

	key := domain.QueueBlockedKey(q.name, sequence)
	if err := q.storage.Put(key, itemBytes, 0); err != nil {
		return err
	}

	q.updateWorkflowIndex(item, sequence, true)

	return nil
}

func (q *Queue) EnqueueReady(item []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(item) > 1<<20 {
		return &domain.StorageError{Type: domain.ErrStorageFull, Message: "queue item too large"}
	}
	if q.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	sequence, err := q.getNextSequence()
	if err != nil {
		return err
	}

	queueItem := domain.NewQueueItem(item, sequence)
	itemBytes, err := queueItem.ToBytes()
	if err != nil {
		return err
	}

	key := domain.QueueReadyKey(q.name, sequence)
	if err := q.storage.Put(key, itemBytes, 0); err != nil {
		return err
	}

	q.updateWorkflowIndex(item, sequence, true)

	if q.eventManager != nil {
		event := domain.Event{Type: domain.EventPut, Key: key, Timestamp: time.Now()}
		_ = q.eventManager.Broadcast(event)
	}

	return nil
}

func (q *Queue) GetBlockedForWorkflow(workflowID string) ([]*domain.BlockedItem, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return nil, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	var blockedItems []*domain.BlockedItem
	prefix := fmt.Sprintf("queue:%s:blocked:", q.name)
	items, err := q.storage.ListByPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		blockedItem, err := domain.BlockedItemFromBytes(item.Value)
		if err != nil {
			continue
		}

		itemWorkflowID := q.extractWorkflowID(blockedItem.Data)
		if itemWorkflowID == workflowID {
			blockedItems = append(blockedItems, blockedItem)
		}
	}

	return blockedItems, nil
}

func (q *Queue) GetReadyForWorkflow(workflowID string) ([][]byte, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return nil, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	var readyItems [][]byte
	prefix := fmt.Sprintf("queue:%s:ready:", q.name)
	items, err := q.storage.ListByPrefix(prefix)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		queueItem, err := domain.QueueItemFromBytes(item.Value)
		if err != nil {
			continue
		}

		itemWorkflowID := q.extractWorkflowID(queueItem.Data)
		if itemWorkflowID == workflowID {
			readyItems = append(readyItems, queueItem.Data)
		}
	}

	return readyItems, nil
}

func (q *Queue) PromoteBlockedToReady(item *domain.BlockedItem) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	item.UpdateCheckCount()

	queueItem := domain.NewQueueItem(item.Data, item.Sequence)
	readyBytes, err := queueItem.ToBytes()
	if err != nil {
		return err
	}

	blockedKey := domain.QueueBlockedKey(q.name, item.Sequence)
	readyKey := domain.QueueReadyKey(q.name, item.Sequence)

	ops := []ports.WriteOp{
		{Type: ports.OpDelete, Key: blockedKey},
		{Type: ports.OpPut, Key: readyKey, Value: readyBytes},
	}

	if err := q.storage.BatchWrite(ops); err != nil {
		return err
	}

	q.updateWorkflowIndex(item.Data, item.Sequence, true)

	if q.eventManager != nil {
		event := domain.Event{Type: domain.EventPut, Key: readyKey, Timestamp: time.Now()}
		_ = q.eventManager.Broadcast(event)
	}

	return nil
}

func (q *Queue) MoveReadyToBlocked(item []byte, sequence int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	blockedItem := domain.NewBlockedItem(item, sequence)
	blockedBytes, err := blockedItem.ToBytes()
	if err != nil {
		return err
	}

	readyKey := domain.QueueReadyKey(q.name, sequence)
	blockedKey := domain.QueueBlockedKey(q.name, sequence)

	ops := []ports.WriteOp{
		{Type: ports.OpDelete, Key: readyKey},
		{Type: ports.OpPut, Key: blockedKey, Value: blockedBytes},
	}

	if err := q.storage.BatchWrite(ops); err != nil {
		return err
	}

	return nil
}

func (q *Queue) HasBlockedItemsWithPrefix(dataPrefix string) (bool, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return false, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	prefix := fmt.Sprintf("queue:%s:blocked:", q.name)
	items, err := q.storage.ListByPrefix(prefix)
	if err != nil {
		return false, err
	}

	for _, item := range items {
		blockedItem, err := domain.BlockedItemFromBytes(item.Value)
		if err != nil {
			continue
		}

		if len(blockedItem.Data) > 0 {
			dataStr := string(blockedItem.Data)
			if strings.Contains(dataStr, dataPrefix) {
				return true, nil
			}
		}
	}

	return false, nil
}

func (q *Queue) HasReadyItemsWithPrefix(dataPrefix string) (bool, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return false, &domain.StorageError{Type: domain.ErrClosed, Message: "queue is closed"}
	}

	prefix := fmt.Sprintf("queue:%s:ready:", q.name)
	items, err := q.storage.ListByPrefix(prefix)
	if err != nil {
		return false, err
	}

	for _, item := range items {
		queueItem, err := domain.QueueItemFromBytes(item.Value)
		if err != nil {
			continue
		}

		if len(queueItem.Data) > 0 {
			dataStr := string(queueItem.Data)
			if strings.Contains(dataStr, dataPrefix) {
				return true, nil
			}
		}
	}

	return false, nil
}
