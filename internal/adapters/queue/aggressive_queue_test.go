package queue

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

// Aggressive edge case and stress tests

func TestQueueAdapter_ConcurrentEnqueueDequeue(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()
	numWorkers := 10
	itemsPerWorker := 100
	totalItems := numWorkers * itemsPerWorker

	var wg sync.WaitGroup
	enqueueErrors := make(chan error, totalItems)
	dequeueErrors := make(chan error, totalItems)
	dequeuedItems := make(chan *ports.QueueItem, totalItems)

	// Start enqueuers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				item := ports.QueueItem{
					ID:         fmt.Sprintf("item-%d-%d", workerID, j),
					WorkflowID: fmt.Sprintf("workflow-%d", workerID),
					NodeName:   fmt.Sprintf("node-%d", j),
					Priority:   rand.Intn(10),
				}
				if err := queue.Enqueue(ctx, item); err != nil {
					enqueueErrors <- err
				}
			}
		}(i)
	}

	// Start dequeuers (slightly delayed to let some items accumulate)
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				item, err := queue.Dequeue(ctx, ports.WithClaim(fmt.Sprintf("worker-%d", workerID), 1*time.Minute))
				if err != nil {
					if err == domain.ErrNotFound {
						// Expected when queue is empty
						continue
					}
					dequeueErrors <- err
				} else if item != nil {
					dequeuedItems <- item
				}
			}
		}(i)
	}

	wg.Wait()
	close(enqueueErrors)
	close(dequeueErrors)
	close(dequeuedItems)

	// Check for errors
	for err := range enqueueErrors {
		t.Errorf("Enqueue error: %v", err)
	}
	for err := range dequeueErrors {
		t.Errorf("Dequeue error: %v", err)
	}

	// Count dequeued items
	dequeuedCount := 0
	for range dequeuedItems {
		dequeuedCount++
	}

	t.Logf("Enqueued: %d items, Dequeued: %d items", totalItems, dequeuedCount)
}

func TestQueueAdapter_ExtremelyLargeItems(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	// Create an item with massive content
	largeContent := strings.Repeat("x", 1024*1024) // 1MB string
	item := ports.QueueItem{
		ID:         "large-item",
		WorkflowID: "large-workflow",
		NodeName:   "large-node",
		Priority:   5,
		Config:     largeContent,
	}

	err := queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Failed to enqueue large item: %v", err)
	}

	dequeued, err := queue.Dequeue(ctx, ports.WithClaim("test-node", 1*time.Minute))
	if err != nil {
		t.Fatalf("Failed to dequeue large item: %v", err)
	}

	if dequeued == nil {
		t.Fatal("Expected dequeued item, got nil")
	}

	if dequeued.ID != item.ID {
		t.Errorf("Expected item ID %s, got %s", item.ID, dequeued.ID)
	}
}

func TestQueueAdapter_UnicodeAndSpecialCharacters(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	testCases := []struct {
		name string
		item ports.QueueItem
	}{
		{
			name: "unicode_emojis",
			item: ports.QueueItem{
				ID:         "🚀🔥💻🌟",
				WorkflowID: "workflow-🦄",
				NodeName:   "node-🎯",
				Priority:   1,
			},
		},
		{
			name: "special_chars",
			item: ports.QueueItem{
				ID:         "item@#$%^&*()!",
				WorkflowID: "workflow~`[]{}|\\:;\"'<>?,./",
				NodeName:   "node-_+=",
				Priority:   2,
			},
		},
		{
			name: "unicode_text",
			item: ports.QueueItem{
				ID:         "こんにちは世界",
				WorkflowID: "مرحبا بالعالم",
				NodeName:   "Здравствуй мир",
				Priority:   3,
			},
		},
		{
			name: "empty_strings",
			item: ports.QueueItem{
				ID:         "empty-test",
				WorkflowID: "",
				NodeName:   "",
				Priority:   0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := queue.Enqueue(ctx, tc.item)
			if err != nil {
				t.Fatalf("Failed to enqueue item with %s: %v", tc.name, err)
			}

			dequeued, err := queue.Dequeue(ctx, ports.WithClaim("test-node", 1*time.Minute))
			if err != nil {
				t.Fatalf("Failed to dequeue item with %s: %v", tc.name, err)
			}

			if dequeued == nil {
				t.Fatalf("Expected dequeued item for %s, got nil", tc.name)
			}

			if dequeued.ID != tc.item.ID {
				t.Errorf("Expected item ID %s, got %s for %s", tc.item.ID, dequeued.ID, tc.name)
			}
		})
	}
}

func TestQueueAdapter_ClaimExpiration(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	item := ports.QueueItem{
		ID:         "expire-test",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err := queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Dequeue with very short claim duration
	dequeued, err := queue.Dequeue(ctx, ports.WithClaim("test-node", 1*time.Millisecond))
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if dequeued == nil {
		t.Fatal("Expected dequeued item, got nil")
	}

	// Wait for claim to expire
	time.Sleep(10 * time.Millisecond)

	// Process expired claims
	err = queue.ProcessExpiredClaims(ctx)
	if err != nil {
		t.Fatalf("ProcessExpiredClaims failed: %v", err)
	}

	// Item should be back in ready queue
	items, err := queue.GetItems(ctx)
	if err != nil {
		t.Fatalf("GetItems failed: %v", err)
	}

	if len(items) != 1 {
		t.Errorf("Expected 1 item after claim expiration, got %d", len(items))
	}
}

func TestQueueAdapter_InvalidParameters(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	testCases := []struct {
		name        string
		item        ports.QueueItem
		expectError bool
	}{
		{
			name: "empty_id",
			item: ports.QueueItem{
				ID:         "",
				WorkflowID: "workflow-1",
				NodeName:   "node-1",
				Priority:   1,
			},
			expectError: true,
		},
		{
			name: "negative_priority",
			item: ports.QueueItem{
				ID:         "neg-priority",
				WorkflowID: "workflow-1",
				NodeName:   "node-1",
				Priority:   -100,
			},
			expectError: false, // Negative priority should be allowed
		},
		{
			name: "extreme_priority",
			item: ports.QueueItem{
				ID:         "extreme-priority",
				WorkflowID: "workflow-1",
				NodeName:   "node-1",
				Priority:   2147483647, // Max int32
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := queue.Enqueue(ctx, tc.item)
			if tc.expectError && err == nil {
				t.Errorf("Expected error for %s, but got none", tc.name)
			}
			if !tc.expectError && err != nil {
				t.Errorf("Unexpected error for %s: %v", tc.name, err)
			}
		})
	}
}

func TestQueueAdapter_DequeueOptionsValidation(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	// Add an item first
	item := ports.QueueItem{
		ID:         "test-item",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err := queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	testCases := []struct {
		name        string
		options     []ports.DequeueOption
		expectError bool
	}{
		{
			name:        "empty_node_id",
			options:     []ports.DequeueOption{ports.WithClaim("", 1*time.Minute)},
			expectError: true,
		},
		{
			name:        "zero_claim_duration",
			options:     []ports.DequeueOption{ports.WithClaim("test-node", 0)},
			expectError: true,
		},
		{
			name:        "negative_claim_duration",
			options:     []ports.DequeueOption{ports.WithClaim("test-node", -1*time.Minute)},
			expectError: true,
		},
		{
			name:        "excessive_claim_duration",
			options:     []ports.DequeueOption{ports.WithClaim("test-node", 2*time.Hour)},
			expectError: true,
		},
		{
			name:        "valid_options",
			options:     []ports.DequeueOption{ports.WithClaim("test-node", 30*time.Minute)},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := queue.Dequeue(ctx, tc.options...)
			if tc.expectError && err == nil {
				t.Errorf("Expected error for %s, but got none", tc.name)
			}
			if !tc.expectError && err != nil {
				t.Errorf("Unexpected error for %s: %v", tc.name, err)
			}
		})
	}
}

func TestQueueAdapter_BatchOperationsEdgeCases(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	testCases := []struct {
		name         string
		items        []ports.QueueItem
		expectError  bool
		errorMessage string
	}{
		{
			name:        "empty_batch",
			items:       []ports.QueueItem{},
			expectError: false,
		},
		{
			name: "duplicate_ids_in_batch",
			items: []ports.QueueItem{
				{ID: "duplicate", WorkflowID: "w1", NodeName: "n1", Priority: 1},
				{ID: "duplicate", WorkflowID: "w2", NodeName: "n2", Priority: 2},
			},
			expectError:  true,
			errorMessage: "duplicate ID in batch",
		},
		{
			name: "oversized_batch",
			items: func() []ports.QueueItem {
				items := make([]ports.QueueItem, 150) // Over the limit
				for i := range items {
					items[i] = ports.QueueItem{
						ID:         fmt.Sprintf("item-%d", i),
						WorkflowID: "workflow-1",
						NodeName:   "node-1",
						Priority:   1,
					}
				}
				return items
			}(),
			expectError:  true,
			errorMessage: "capacity limit",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := queue.EnqueueBatch(ctx, tc.items)
			if tc.expectError && err == nil {
				t.Errorf("Expected error for %s, but got none", tc.name)
			}
			if !tc.expectError && err != nil {
				t.Errorf("Unexpected error for %s: %v", tc.name, err)
			}
			if tc.expectError && err != nil && tc.errorMessage != "" {
				if !strings.Contains(err.Error(), tc.errorMessage) {
					t.Errorf("Expected error to contain '%s', got '%s'", tc.errorMessage, err.Error())
				}
			}
		})
	}
}

func TestQueueAdapter_PriorityOrdering(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	// Enqueue items with different priorities
	priorities := []int{1, 5, 3, 8, 2, 10, 4}
	for i, priority := range priorities {
		item := ports.QueueItem{
			ID:         fmt.Sprintf("item-%d", i),
			WorkflowID: "workflow-1",
			NodeName:   "node-1",
			Priority:   priority,
			EnqueuedAt: time.Now(),
		}
		err := queue.Enqueue(ctx, item)
		if err != nil {
			t.Fatalf("Enqueue failed for priority %d: %v", priority, err)
		}
	}

	// Dequeue items and verify they come out in priority order (highest first)
	var dequeuedPriorities []int
	for i := 0; i < len(priorities); i++ {
		item, err := queue.Dequeue(ctx, ports.WithClaim("test-node", 1*time.Minute))
		if err != nil {
			t.Fatalf("Dequeue failed: %v", err)
		}
		if item == nil {
			t.Fatal("Expected item, got nil")
		}
		dequeuedPriorities = append(dequeuedPriorities, item.Priority)
	}

	// Check that priorities are in descending order
	for i := 1; i < len(dequeuedPriorities); i++ {
		if dequeuedPriorities[i] > dequeuedPriorities[i-1] {
			t.Errorf("Priority ordering violated: %v", dequeuedPriorities)
			break
		}
	}

	t.Logf("Enqueued priorities: %v", priorities)
	t.Logf("Dequeued priorities: %v", dequeuedPriorities)
}

func TestQueueAdapter_WorkClaimValidation(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	// Test invalid claim operations
	err := queue.VerifyWorkClaim(ctx, "non-existent-item", "test-node")
	if err == nil {
		t.Error("Expected error for non-existent work claim")
	}

	err = queue.ReleaseWorkClaim(ctx, "non-existent-item", "test-node")
	if err == nil {
		t.Error("Expected error for releasing non-existent work claim")
	}

	// Test valid claim workflow
	item := ports.QueueItem{
		ID:         "claim-test",
		WorkflowID: "workflow-1",
		NodeName:   "node-1",
		Priority:   1,
		EnqueuedAt: time.Now(),
	}

	err = queue.Enqueue(ctx, item)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	dequeued, err := queue.Dequeue(ctx, ports.WithClaim("test-node", 1*time.Minute))
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Verify claim exists
	err = queue.VerifyWorkClaim(ctx, dequeued.ID, "test-node")
	if err != nil {
		t.Errorf("VerifyWorkClaim failed: %v", err)
	}

	// Test claim with wrong node ID
	err = queue.VerifyWorkClaim(ctx, dequeued.ID, "wrong-node")
	if err == nil {
		t.Error("Expected error for wrong node ID")
	}

	// Release claim
	err = queue.ReleaseWorkClaim(ctx, dequeued.ID, "test-node")
	if err != nil {
		t.Errorf("ReleaseWorkClaim failed: %v", err)
	}

	// Verify claim no longer exists
	err = queue.VerifyWorkClaim(ctx, dequeued.ID, "test-node")
	if err == nil {
		t.Error("Expected error for released claim")
	}
}

func TestQueueAdapter_CorruptedStorage(t *testing.T) {
	storage := NewMockStorage()
	queue := NewAdapter(storage, "test-node", ports.QueueTypeReady, slog.Default())

	ctx := context.Background()

	// Manually corrupt storage data
	corruptedData := []byte("invalid-json-data")
	key := readyPrefix + "corrupted"
	err := storage.Put(ctx, key, corruptedData)
	if err != nil {
		t.Fatalf("Failed to put corrupted data: %v", err)
	}

	// GetItems should handle corrupted data gracefully
	items, err := queue.GetItems(ctx)
	if err != nil {
		t.Errorf("GetItems failed with corrupted data: %v", err)
	}

	// Should return empty list or skip corrupted items
	t.Logf("Retrieved %d items with corrupted data in storage", len(items))
}
