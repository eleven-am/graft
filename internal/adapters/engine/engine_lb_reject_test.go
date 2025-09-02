package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/stretchr/testify/assert"
)

func TestEngine_ProcessNextItem_LoadBalancerReject(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	nodeRegistryMock := &mocks.MockNodeRegistryPort{}
	queueMock := &mocks.MockQueuePort{}
	storageMock := &mocks.MockStoragePort{}
	eventManagerMock := &mocks.MockEventManager{}
	loadBalancerMock := &mocks.MockLoadBalancer{}

	config := domain.EngineConfig{}
	engine := NewEngine(config, "test-node", nodeRegistryMock, queueMock, storageMock, eventManagerMock, loadBalancerMock, logger)
	engine.ctx = ctx

	workItem := WorkItem{
		WorkflowID: "test-workflow",
		NodeName:   "test-node",
	}
	workItemBytes, _ := json.Marshal(workItem)
	claimID := "test-claim-123"

	queueMock.On("Claim").Return(workItemBytes, claimID, true, nil).Once()

	loadBalancerMock.On("ShouldExecuteNode", "test-node", "test-workflow", "test-node").
		Return(false, nil).Once()

	queueMock.On("Release", claimID).Return(nil).Once()

	processed, err := engine.processNextItem()

	assert.NoError(t, err)
	assert.True(t, processed)

	queueMock.AssertExpectations(t)
	loadBalancerMock.AssertExpectations(t)
}

func TestEngine_ProcessNextItem_LoadBalancerRejectWithReleaseError(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	nodeRegistryMock := &mocks.MockNodeRegistryPort{}
	queueMock := &mocks.MockQueuePort{}
	storageMock := &mocks.MockStoragePort{}
	eventManagerMock := &mocks.MockEventManager{}
	loadBalancerMock := &mocks.MockLoadBalancer{}

	config := domain.EngineConfig{}
	engine := NewEngine(config, "test-node", nodeRegistryMock, queueMock, storageMock, eventManagerMock, loadBalancerMock, logger)
	engine.ctx = ctx

	workItem := WorkItem{
		WorkflowID: "test-workflow",
		NodeName:   "test-node",
	}
	workItemBytes, _ := json.Marshal(workItem)
	claimID := "test-claim-456"

	queueMock.On("Claim").Return(workItemBytes, claimID, true, nil).Once()

	loadBalancerMock.On("ShouldExecuteNode", "test-node", "test-workflow", "test-node").
		Return(false, nil).Once()

	queueMock.On("Release", claimID).Return(assert.AnError).Once()

	processed, err := engine.processNextItem()

	assert.NoError(t, err)
	assert.True(t, processed)

	queueMock.AssertExpectations(t)
	loadBalancerMock.AssertExpectations(t)
}

func TestEngine_ProcessNextItem_LoadBalancerError(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	nodeRegistryMock := &mocks.MockNodeRegistryPort{}
	queueMock := &mocks.MockQueuePort{}
	storageMock := &mocks.MockStoragePort{}
	eventManagerMock := &mocks.MockEventManager{}
	loadBalancerMock := &mocks.MockLoadBalancer{}

	config := domain.EngineConfig{}
	engine := NewEngine(config, "test-node", nodeRegistryMock, queueMock, storageMock, eventManagerMock, loadBalancerMock, logger)
	engine.ctx = ctx

	workItem := WorkItem{
		WorkflowID: "test-workflow",
		NodeName:   "test-node",
	}
	workItemBytes, _ := json.Marshal(workItem)
	claimID := "test-claim-789"

	queueMock.On("Claim").Return(workItemBytes, claimID, true, nil).Once()

	loadBalancerMock.On("ShouldExecuteNode", "test-node", "test-workflow", "test-node").
		Return(false, assert.AnError).Once()

	queueMock.On("Release", claimID).Return(nil).Once()

	processed, err := engine.processNextItem()

	assert.Error(t, err)
	assert.True(t, processed)

	queueMock.AssertExpectations(t)
	loadBalancerMock.AssertExpectations(t)
}
