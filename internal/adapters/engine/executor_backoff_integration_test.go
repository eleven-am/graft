package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestExecutor_RequeueNodeWithBackoff_IntegrationTests(t *testing.T) {
	tests := []struct {
		name             string
		retryCount       int
		expectedMinDelay time.Duration
		expectedMaxDelay time.Duration
	}{
		{
			name:             "retry count 0",
			retryCount:       0,
			expectedMinDelay: 500 * time.Millisecond,
			expectedMaxDelay: 1500 * time.Millisecond,
		},
		{
			name:             "retry count 1",
			retryCount:       1,
			expectedMinDelay: 1 * time.Second,
			expectedMaxDelay: 3 * time.Second,
		},
		{
			name:             "retry count 10",
			retryCount:       10,
			expectedMinDelay: 2*time.Minute + 30*time.Second,
			expectedMaxDelay: 7*time.Minute + 30*time.Second,
		},
		{
			name:             "retry count 21 (overflow protection)",
			retryCount:       21,
			expectedMinDelay: 2*time.Minute + 30*time.Second,
			expectedMaxDelay: 7*time.Minute + 30*time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			queueMock := &mocks.MockQueuePort{}
			nodeRegistryMock := &mocks.MockNodeRegistryPort{}
			storageMock := &mocks.MockStoragePort{}
			eventManagerMock := &mocks.MockEventManager{}
			loadBalancerMock := &mocks.MockLoadBalancer{}

			config := domain.EngineConfig{RetryAttempts: 25}
			metrics := domain.NewExecutionMetrics()
			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

			executor := NewExecutor(config, "test-node", nodeRegistryMock, nil, queueMock, storageMock, eventManagerMock, loadBalancerMock, logger, metrics)

			workflowID := "test-workflow"
			nodeName := "test-node"
			configData := json.RawMessage(`{"test": "config"}`)

			queueMock.On("Enqueue", mock.MatchedBy(func(itemBytes []byte) bool {
				var workItem WorkItem
				if err := json.Unmarshal(itemBytes, &workItem); err != nil {
					return false
				}

				if workItem.WorkflowID != workflowID {
					return false
				}
				if workItem.NodeName != nodeName {
					return false
				}
				expectedRetryCount := tt.retryCount + 1
				if tt.retryCount > 20 {
					expectedRetryCount = 21
				}
				if workItem.RetryCount != expectedRetryCount {
					return false
				}

				now := time.Now()
				actualDelay := workItem.ProcessAfter.Sub(now)

				if actualDelay < tt.expectedMinDelay || actualDelay > tt.expectedMaxDelay {
					t.Logf("Delay %v not in range [%v, %v]", actualDelay, tt.expectedMinDelay, tt.expectedMaxDelay)
					return false
				}

				return true
			})).Return(nil).Once()

			err := executor.requeueNodeWithBackoff(ctx, workflowID, nodeName, configData, tt.retryCount)

			assert.NoError(t, err)
			queueMock.AssertExpectations(t)
		})
	}
}

func TestExecutor_RequeueNodeWithBackoff_DelayClampingAndJitter(t *testing.T) {
	ctx := context.Background()

	queueMock := &mocks.MockQueuePort{}
	stateManagerMock := &mocks.MockStateManagerPort{}
	nodeRegistryMock := &mocks.MockNodeRegistryPort{}
	storageMock := &mocks.MockStoragePort{}
	eventManagerMock := &mocks.MockEventManager{}
	loadBalancerMock := &mocks.MockLoadBalancer{}

	config := domain.EngineConfig{RetryAttempts: 25}
	metrics := domain.NewExecutionMetrics()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	executor := NewExecutor(config, "test-node", nodeRegistryMock, stateManagerMock, queueMock, storageMock, eventManagerMock, loadBalancerMock, logger, metrics)

	workflowID := "test-workflow"
	nodeName := "test-node"
	configData := json.RawMessage(`{"test": "config"}`)
	retryCount := 3

	var delays []time.Duration
	queueMock.On("Enqueue", mock.Anything).Run(func(args mock.Arguments) {
		itemBytes := args.Get(0).([]byte)
		var workItem WorkItem
		json.Unmarshal(itemBytes, &workItem)
		actualDelay := workItem.ProcessAfter.Sub(time.Now())
		delays = append(delays, actualDelay)
	}).Return(nil).Times(10)

	for i := 0; i < 10; i++ {
		err := executor.requeueNodeWithBackoff(ctx, workflowID, nodeName, configData, retryCount)
		assert.NoError(t, err)
	}

	baseDelay := 1 * time.Second * time.Duration(1<<uint(retryCount))
	minExpected := time.Duration(float64(baseDelay) * 0.5)
	maxExpected := time.Duration(float64(baseDelay) * 1.5)

	for _, delay := range delays {
		assert.True(t, delay >= minExpected && delay <= maxExpected,
			"Delay %v not in jitter range [%v, %v]", delay, minExpected, maxExpected)
		assert.True(t, delay <= 5*time.Minute,
			"Delay %v should be clamped to max 5 minutes", delay)
	}

	uniqueDelays := make(map[time.Duration]bool)
	for _, delay := range delays {
		uniqueDelays[delay.Round(time.Millisecond)] = true
	}
	assert.True(t, len(uniqueDelays) > 1, "Jitter should produce different delays")

	queueMock.AssertExpectations(t)
}

func TestExecutor_RequeueNodeWithBackoff_MaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()

	queueMock := &mocks.MockQueuePort{}
	stateManagerMock := &mocks.MockStateManagerPort{}
	nodeRegistryMock := &mocks.MockNodeRegistryPort{}
	storageMock := &mocks.MockStoragePort{}
	eventManagerMock := &mocks.MockEventManager{}
	loadBalancerMock := &mocks.MockLoadBalancer{}

	config := domain.EngineConfig{RetryAttempts: 5}
	metrics := domain.NewExecutionMetrics()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	executor := NewExecutor(config, "test-node", nodeRegistryMock, stateManagerMock, queueMock, storageMock, eventManagerMock, loadBalancerMock, logger, metrics)

	workflowID := "test-workflow"
	nodeName := "test-node"
	configData := json.RawMessage(`{"test": "config"}`)
	retryCount := 6

	queueMock.On("SendToDeadLetter", mock.MatchedBy(func(itemBytes []byte) bool {
		var workItem WorkItem
		if err := json.Unmarshal(itemBytes, &workItem); err != nil {
			return false
		}
		return workItem.WorkflowID == workflowID &&
			workItem.NodeName == nodeName &&
			workItem.RetryCount == retryCount
	}), mock.AnythingOfType("string")).Return(nil).Once()

	err := executor.requeueNodeWithBackoff(ctx, workflowID, nodeName, configData, retryCount)

	assert.NoError(t, err)
	queueMock.AssertExpectations(t)
}
