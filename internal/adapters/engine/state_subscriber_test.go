package engine

import (
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/ports/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestStateSubscriptionManager_Subscribe(t *testing.T) {
	logger := slog.Default()

	// Create a minimal engine setup for testing subscription manager
	engine := &Engine{
		logger: logger,
	}
	engine.stateManager = NewStateManager(engine)
	engine.storage = createMockStorageAdapter()
	engine.stateSubscriber = NewStateSubscriptionManager(engine, "test-node-id", logger)

	workflowID := "test-workflow-123"

	// Subscribe to workflow state changes
	statusChan, unsubscribe, err := engine.stateSubscriber.Subscribe(workflowID)
	require.NoError(t, err)
	require.NotNil(t, statusChan)
	require.NotNil(t, unsubscribe)
	defer unsubscribe()

	// For non-existent workflow, should not receive initial state
	select {
	case <-statusChan:
		t.Fatal("Should not receive state for non-existent workflow")
	case <-time.After(100 * time.Millisecond):
		// Expected - no state should be sent for non-existent workflow
	}

	// Verify subscription was registered
	count := engine.stateSubscriber.GetSubscriberCount(workflowID)
	assert.Equal(t, 1, count)
}

func TestStateSubscriptionManager_Unsubscribe(t *testing.T) {
	logger := slog.Default()

	// Create a minimal engine setup for testing subscription manager
	engine := &Engine{
		logger: logger,
	}
	engine.stateManager = NewStateManager(engine)
	engine.storage = createMockStorageAdapter()
	engine.stateSubscriber = NewStateSubscriptionManager(engine, "test-node-id", logger)

	workflowID := "test-workflow-unsub"

	// Subscribe
	statusChan, unsubscribe, err := engine.stateSubscriber.Subscribe(workflowID)
	require.NoError(t, err)
	defer func() {
		// Cleanup in case test fails before unsubscribe
		select {
		case <-statusChan:
		default:
		}
	}()

	// Verify subscriber exists
	assert.Equal(t, 1, engine.stateSubscriber.GetSubscriberCount(workflowID))

	// Unsubscribe
	unsubscribe()

	// Verify subscriber was removed
	assert.Equal(t, 0, engine.stateSubscriber.GetSubscriberCount(workflowID))

	// Channel should be closed
	select {
	case _, ok := <-statusChan:
		assert.False(t, ok, "Channel should be closed")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Channel should have been closed immediately")
	}
}

func TestStateSubscriptionManager_MultipleSubscribers(t *testing.T) {
	logger := slog.Default()

	// Create a minimal engine setup for testing subscription manager
	engine := &Engine{
		logger: logger,
	}
	engine.stateManager = NewStateManager(engine)
	engine.storage = createMockStorageAdapter()
	engine.stateSubscriber = NewStateSubscriptionManager(engine, "test-node-id", logger)

	workflowID := "test-workflow-multi"

	// Create multiple subscribers
	var unsubscribeFuncs []func()

	for i := 0; i < 3; i++ {
		statusChan, unsubscribe, err := engine.stateSubscriber.Subscribe(workflowID)
		require.NoError(t, err)
		require.NotNil(t, statusChan)
		unsubscribeFuncs = append(unsubscribeFuncs, unsubscribe)
	}

	defer func() {
		for _, unsubscribe := range unsubscribeFuncs {
			unsubscribe()
		}
	}()

	// Verify all subscribers are registered
	count := engine.stateSubscriber.GetSubscriberCount(workflowID)
	assert.Equal(t, 3, count)
}

func TestStateSubscriptionManager_StateChangeNotification(t *testing.T) {
	logger := slog.Default()

	// Create a minimal engine setup for testing subscription manager
	engine := &Engine{
		logger: logger,
	}
	engine.stateManager = NewStateManager(engine)

	// Create mocks for all dependencies
	mockStorage := &mocks.MockStoragePort{}
	mockPendingQueue := &mocks.MockQueuePort{}
	mockReadyQueue := &mocks.MockQueuePort{}
	mockNodeRegistry := &mocks.MockNodeRegistryPort{}

	// Set up workflow data response
	workflowData := `{"id":"test-workflow-change","status":"running","version":1,"started_at":"2024-01-01T00:00:00Z","current_state":{"test":"updated"},"executed_nodes":[],"metadata":{}}`
	mockStorage.On("Get", mock.Anything, "workflow:state:test-workflow-change").Return([]byte(workflowData), nil).Once()
	mockStorage.On("Get", mock.Anything, mock.AnythingOfType("string")).Return(nil, nil).Maybe()
	mockStorage.On("Put", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil).Maybe()
	mockStorage.On("List", mock.Anything, mock.AnythingOfType("string")).Return([]ports.KeyValue{}, nil).Maybe()
	mockStorage.On("Delete", mock.Anything, mock.AnythingOfType("string")).Return(nil).Maybe()

	// Set up queue mocks
	mockPendingQueue.On("GetItems", mock.Anything).Return([]ports.QueueItem{}, nil).Maybe()
	mockReadyQueue.On("GetItems", mock.Anything).Return([]ports.QueueItem{}, nil).Maybe()

	// Set up node registry mocks
	mockNodeRegistry.On("RegisterNode", mock.Anything).Return(nil).Maybe()
	mockNodeRegistry.On("GetNode", mock.AnythingOfType("string")).Return(nil, nil).Maybe()
	mockNodeRegistry.On("ListNodes").Return([]string{}).Maybe()

	// Assign all dependencies
	engine.storage = mockStorage
	engine.pendingQueue = mockPendingQueue
	engine.readyQueue = mockReadyQueue
	engine.nodeRegistry = mockNodeRegistry
	engine.nodeID = "test-node"

	// Initialize data collector
	engine.initializeDataCollector()

	// Create subscription manager
	engine.stateSubscriber = NewStateSubscriptionManager(engine, "test-node-id", logger)

	workflowID := "test-workflow-change"

	// Subscribe
	statusChan, unsubscribe, err := engine.stateSubscriber.Subscribe(workflowID)
	require.NoError(t, err)
	defer unsubscribe()

	// Trigger a state change event directly on the subscription manager
	event := StateChangeEvent{
		WorkflowID: workflowID,
		ChangedBy:  "test-node",
		NewState:   map[string]interface{}{"test": "updated"},
		Timestamp:  time.Now(),
		NodeName:   "test-node",
		EventType:  EventTypeNodeCompleted,
	}

	// Send the event through the state subscriber directly
	engine.stateSubscriber.OnStateChange(event)

	// Should receive updated state
	select {
	case updatedStatus := <-statusChan:
		assert.Equal(t, workflowID, updatedStatus.WorkflowID)
		assert.Equal(t, ports.WorkflowStateRunning, updatedStatus.Status)
	case <-time.After(1 * time.Second):
		t.Fatal("Should have received state change notification")
	}
}

func TestStateSubscriptionManager_TotalSubscriberCount(t *testing.T) {
	logger := slog.Default()

	// Create a minimal engine setup for testing subscription manager
	engine := &Engine{
		logger: logger,
	}
	engine.stateManager = NewStateManager(engine)
	engine.storage = createMockStorageAdapter()
	engine.stateSubscriber = NewStateSubscriptionManager(engine, "test-node-id", logger)

	// Create workflows and subscribers
	workflows := []string{"workflow-1", "workflow-2", "workflow-3"}
	var unsubscribers []func()

	for _, workflowID := range workflows {
		// Add 2 subscribers per workflow
		for i := 0; i < 2; i++ {
			statusChan, unsubscribe, err := engine.stateSubscriber.Subscribe(workflowID)
			require.NoError(t, err)
			require.NotNil(t, statusChan)
			unsubscribers = append(unsubscribers, unsubscribe)
		}
	}

	defer func() {
		for _, unsubscribe := range unsubscribers {
			unsubscribe()
		}
	}()

	// Should have 6 total subscribers (3 workflows × 2 subscribers each)
	totalCount := engine.stateSubscriber.GetTotalSubscriberCount()
	assert.Equal(t, 6, totalCount)

	// Check individual workflow counts
	for _, workflowID := range workflows {
		count := engine.stateSubscriber.GetSubscriberCount(workflowID)
		assert.Equal(t, 2, count, "Workflow %s should have 2 subscribers", workflowID)
	}
}

// Helper functions

func createMockStorageAdapter() ports.StoragePort {
	mockStorage := &mocks.MockStoragePort{}
	// Allow general storage operations
	mockStorage.On("Get", mock.Anything, mock.AnythingOfType("string")).Return(nil, nil).Maybe()
	mockStorage.On("Put", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil).Maybe()
	mockStorage.On("List", mock.Anything, mock.AnythingOfType("string")).Return([]ports.KeyValue{}, nil).Maybe()
	mockStorage.On("Delete", mock.Anything, mock.AnythingOfType("string")).Return(nil).Maybe()
	return mockStorage
}
