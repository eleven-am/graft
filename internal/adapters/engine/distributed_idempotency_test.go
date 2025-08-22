package engine

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/eleven-am/graft/internal/testutil/workflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type DistributedIdempotencyTestNode struct{}

func (n *DistributedIdempotencyTestNode) GetName() string {
	return "distributed-idempotency-test-node"
}

func (n *DistributedIdempotencyTestNode) Execute(ctx context.Context, state interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
	time.Sleep(10 * time.Millisecond)

	result := map[string]interface{}{
		"processed": true,
		"timestamp": time.Now().Unix(),
	}

	duplicateKey := "duplicate-distributed-key"
	uniqueKey := "unique-distributed-key"

	nextNodes := []ports.NextNode{
		{
			NodeName:       "consumer-node",
			Config:         map[string]interface{}{"source": "first"},
			IdempotencyKey: &duplicateKey,
		},
		{
			NodeName:       "consumer-node",
			Config:         map[string]interface{}{"source": "second"},
			IdempotencyKey: &duplicateKey,
		},
		{
			NodeName:       "consumer-node",
			Config:         map[string]interface{}{"source": "unique"},
			IdempotencyKey: &uniqueKey,
		},
	}

	return result, nextNodes, nil
}

func (n *DistributedIdempotencyTestNode) CanStart(ctx context.Context, state interface{}, config interface{}) bool {
	return true
}

func TestDistributedIdempotency(t *testing.T) {
	engine := NewEngine(Config{}, slog.Default())
	mockComponents := workflow.SetupMockComponents(t)

	engine.SetNodeRegistry(mockComponents.NodeRegistry)
	engine.SetResourceManager(mockComponents.ResourceManager)
	engine.SetStorage(mockComponents.Storage)
	engine.SetQueue(mockComponents.Queue)

	consumerNode := workflow.CreateSuccessfulTestNode("consumer-node")
	testNode := &DistributedIdempotencyTestNode{}

	mockComponents.NodeRegistry.EXPECT().GetNode("distributed-idempotency-test-node").Return(testNode, nil)
	mockComponents.NodeRegistry.EXPECT().GetNode("consumer-node").Return(consumerNode, nil).Maybe()

	workflowInstance := &WorkflowInstance{
		ID:           "distributed-test",
		Status:       ports.WorkflowStateRunning,
		CurrentState: map[string]interface{}{"input": "test data"},
		StartedAt:    time.Now(),
		Metadata:     map[string]string{"test": "distributed-idempotency"},
	}

	engine.activeWorkflows["distributed-test"] = workflowInstance

	executor := NewNodeExecutor(engine)

	testItem := &ports.QueueItem{
		ID:         "test-item-1",
		WorkflowID: "distributed-test",
		NodeName:   "distributed-idempotency-test-node",
		Config:     map[string]interface{}{},
		EnqueuedAt: time.Now(),
	}

	var capturedKeys []string
	var enqueuedItems []ports.QueueItem
	var claimedKeys = make(map[string]bool)

	mockComponents.Storage.On("Get", mock.Anything, mock.MatchedBy(func(key string) bool {
		return key == "workflow:idempotency:distributed-test:duplicate-distributed-key" ||
			key == "workflow:idempotency:distributed-test:unique-distributed-key"
	})).Return(func(ctx context.Context, key string) ([]byte, error) {
		capturedKeys = append(capturedKeys, key)
		if claimedKeys[key] {
			return []byte(`{"claimed":true}`), nil
		}
		return nil, domain.NewNotFoundError("key", "not found")
	})

	mockComponents.Storage.On("Put", mock.Anything, mock.MatchedBy(func(key string) bool {
		return key == "workflow:idempotency:distributed-test:duplicate-distributed-key" ||
			key == "workflow:idempotency:distributed-test:unique-distributed-key"
	}), mock.AnythingOfType("[]uint8")).Return(func(ctx context.Context, key string, value []byte) error {
		claimedKeys[key] = true
		return nil
	})

	mockComponents.Storage.On("Put", mock.Anything, mock.MatchedBy(func(key string) bool {
		return key == "workflow:state:distributed-test"
	}), mock.AnythingOfType("[]uint8")).Return(nil)

	mockComponents.Queue.On("EnqueueReady", mock.Anything, mock.AnythingOfType("ports.QueueItem")).Run(func(args mock.Arguments) {
		item := args.Get(1).(ports.QueueItem)
		enqueuedItems = append(enqueuedItems, item)
	}).Return(nil).Maybe()

	mockComponents.ResourceManager.EXPECT().CanExecuteNode("distributed-idempotency-test-node").Return(true).Maybe()
	mockComponents.ResourceManager.EXPECT().AcquireNode("distributed-idempotency-test-node").Return(nil).Maybe()
	mockComponents.ResourceManager.EXPECT().ReleaseNode("distributed-idempotency-test-node").Return(nil).Maybe()

	err := executor.ExecuteNode(context.Background(), testItem)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(enqueuedItems), "Should only enqueue 2 items (duplicate key prevents third)")

	nodeNames := make([]string, len(enqueuedItems))
	for i, item := range enqueuedItems {
		nodeNames[i] = item.NodeName
	}
	assert.Equal(t, []string{"consumer-node", "consumer-node"}, nodeNames)

	assert.Equal(t, 3, len(capturedKeys), "Should attempt to claim 3 idempotency keys (duplicate tried twice)")
}

func TestDistributedIdempotencyCleanup(t *testing.T) {
	mockComponents := workflow.SetupMockComponents(t)

	orchestrator := &CleanupOrchestrator{
		storage: mockComponents.Storage,
		logger:  slog.Default(),
	}

	mockComponents.Storage.On("List", mock.Anything, "workflow:idempotency:test-workflow:").Return([]ports.KeyValue{
		{Key: "workflow:idempotency:test-workflow:key1", Value: []byte(`{"key":"value1"}`)},
		{Key: "workflow:idempotency:test-workflow:key2", Value: []byte(`{"key":"value2"}`)},
	}, nil)

	mockComponents.Storage.On("Batch", mock.Anything, mock.MatchedBy(func(ops []ports.Operation) bool {
		return len(ops) == 2 &&
			ops[0].Type == ports.OpDelete &&
			ops[0].Key == "workflow:idempotency:test-workflow:key1" &&
			ops[1].Type == ports.OpDelete &&
			ops[1].Key == "workflow:idempotency:test-workflow:key2"
	})).Return(nil)

	err := orchestrator.cleanupIdempotencyKeys(context.Background(), "test-workflow")
	assert.NoError(t, err)
}
