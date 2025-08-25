package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type BadgerEventStore struct {
	db     *badger.DB
	logger *slog.Logger
}

func NewBadgerEventStore(db *badger.DB, logger *slog.Logger) *BadgerEventStore {
	if logger == nil {
		logger = slog.Default()
	}

	return &BadgerEventStore{
		db:     db,
		logger: logger.With("component", "event-store"),
	}
}

func (bes *BadgerEventStore) StoreEvent(ctx context.Context, event *ports.StateChangeEvent) error {
	if event == nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "event cannot be nil",
		}
	}

	if event.WorkflowID == "" || event.EventID == "" {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "workflow ID and event ID are required",
		}
	}

	// Serialize event to JSON
	eventData, err := json.Marshal(event)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to marshal event: %v", err),
			Details: map[string]interface{}{
				"event_id":    event.EventID,
				"workflow_id": event.WorkflowID,
			},
		}
	}

	// Create composite key: workflow_id:sequence_number:event_id
	key := fmt.Sprintf("event:%s:%010d:%s",
		event.WorkflowID,
		event.SequenceNumber,
		event.EventID)

	// Store with TTL (events expire after 24 hours by default)
	ttl := 24 * time.Hour

	err = bes.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), eventData).WithTTL(ttl)
		return txn.SetEntry(entry)
	})

	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to store event: %v", err),
			Details: map[string]interface{}{
				"event_id":    event.EventID,
				"workflow_id": event.WorkflowID,
				"key":         key,
			},
		}
	}

	// Also store latest sequence number for the workflow
	seqKey := fmt.Sprintf("seq:%s", event.WorkflowID)
	seqData := []byte(fmt.Sprintf("%d", event.SequenceNumber))

	err = bes.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(seqKey), seqData).WithTTL(ttl)
		return txn.SetEntry(entry)
	})

	if err != nil {
		bes.logger.Warn("failed to store sequence number",
			"error", err,
			"workflow_id", event.WorkflowID,
			"sequence", event.SequenceNumber)
	}

	bes.logger.Debug("stored event",
		"event_id", event.EventID,
		"workflow_id", event.WorkflowID,
		"sequence", event.SequenceNumber,
		"key", key)

	return nil
}

func (bes *BadgerEventStore) GetEventsFromSequence(ctx context.Context, workflowID string, fromSequence int64) ([]*ports.StateChangeEvent, error) {
	if workflowID == "" {
		return nil, domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "workflow ID is required",
		}
	}

	var events []*ports.StateChangeEvent

	err := bes.db.View(func(txn *badger.Txn) error {
		// Create iterator with prefix for this workflow
		prefix := fmt.Sprintf("event:%s:", workflowID)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to the starting sequence number
		startKey := fmt.Sprintf("event:%s:%010d:", workflowID, fromSequence)

		for it.Seek([]byte(startKey)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var event ports.StateChangeEvent
				if err := json.Unmarshal(val, &event); err != nil {
					bes.logger.Warn("failed to unmarshal event",
						"error", err,
						"key", string(item.Key()))
					return nil // Skip malformed events
				}

				// Only include events with sequence >= fromSequence
				if event.SequenceNumber >= fromSequence {
					events = append(events, &event)
				}

				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to retrieve events: %v", err),
			Details: map[string]interface{}{
				"workflow_id":   workflowID,
				"from_sequence": fromSequence,
			},
		}
	}

	bes.logger.Debug("retrieved events",
		"workflow_id", workflowID,
		"from_sequence", fromSequence,
		"count", len(events))

	return events, nil
}

func (bes *BadgerEventStore) GetLatestSequence(ctx context.Context, workflowID string) (int64, error) {
	if workflowID == "" {
		return 0, domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "workflow ID is required",
		}
	}

	var sequence int64

	err := bes.db.View(func(txn *badger.Txn) error {
		seqKey := fmt.Sprintf("seq:%s", workflowID)
		item, err := txn.Get([]byte(seqKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				sequence = 0
				return nil
			}
			return err
		}

		return item.Value(func(val []byte) error {
			var err error
			sequence, err = strconv.ParseInt(string(val), 10, 64)
			return err
		})
	})

	if err != nil {
		return 0, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to get latest sequence: %v", err),
			Details: map[string]interface{}{
				"workflow_id": workflowID,
			},
		}
	}

	return sequence, nil
}

func (bes *BadgerEventStore) CleanupOldEvents(ctx context.Context, before time.Time) error {
	var deletedCount int

	err := bes.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys for deletion
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte("event:")
		var keysToDelete [][]byte

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			// Check if item has expired or is older than 'before' time
			if item.ExpiresAt() > 0 && time.Unix(int64(item.ExpiresAt()), 0).Before(before) {
				keysToDelete = append(keysToDelete, item.KeyCopy(nil))
			}
		}

		// Delete the keys
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				bes.logger.Warn("failed to delete event",
					"error", err,
					"key", string(key))
			} else {
				deletedCount++
			}
		}

		return nil
	})

	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to cleanup old events: %v", err),
			Details: map[string]interface{}{
				"before": before,
			},
		}
	}

	if deletedCount > 0 {
		bes.logger.Info("cleaned up old events",
			"count", deletedCount,
			"before", before)
	}

	return nil
}

// Additional utility methods

func (bes *BadgerEventStore) GetEventsByWorkflow(ctx context.Context, workflowID string, limit int) ([]*ports.StateChangeEvent, error) {
	if workflowID == "" {
		return nil, domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "workflow ID is required",
		}
	}

	var events []*ports.StateChangeEvent

	err := bes.db.View(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("event:%s:", workflowID)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)) && (limit == 0 || count < limit); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var event ports.StateChangeEvent
				if err := json.Unmarshal(val, &event); err != nil {
					bes.logger.Warn("failed to unmarshal event",
						"error", err,
						"key", string(item.Key()))
					return nil // Skip malformed events
				}

				events = append(events, &event)
				count++

				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to get events by workflow: %v", err),
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"limit":       limit,
			},
		}
	}

	return events, nil
}

func (bes *BadgerEventStore) GetEventByID(ctx context.Context, workflowID, eventID string) (*ports.StateChangeEvent, error) {
	if workflowID == "" || eventID == "" {
		return nil, domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "workflow ID and event ID are required",
		}
	}

	var event *ports.StateChangeEvent

	err := bes.db.View(func(txn *badger.Txn) error {
		// We need to iterate to find the event since we don't know the sequence number
		prefix := fmt.Sprintf("event:%s:", workflowID)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := string(item.Key())

			// Check if this key contains our event ID
			if len(key) < len(eventID) || key[len(key)-len(eventID):] != eventID {
				continue
			}

			return item.Value(func(val []byte) error {
				var e ports.StateChangeEvent
				if err := json.Unmarshal(val, &e); err != nil {
					return err
				}

				if e.EventID == eventID {
					event = &e
					return nil
				}

				return nil
			})
		}

		return nil
	})

	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to get event by ID: %v", err),
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"event_id":    eventID,
			},
		}
	}

	if event == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeNotFound,
			Message: "event not found",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"event_id":    eventID,
			},
		}
	}

	return event, nil
}
