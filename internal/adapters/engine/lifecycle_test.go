package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockNode struct {
	mock.Mock
}

func (m *MockNode) Execute(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
	args := m.Called(ctx, globalState, config)
	return args.Get(0), args.Get(1).([]ports.NextNode), args.Error(2)
}

func (m *MockNode) GetName() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockNode) CanStart(ctx context.Context, globalState interface{}, config interface{}) bool {
	args := m.Called(ctx, globalState, config)
	return args.Bool(0)
}

func (m *MockNode) GetInputSchema() map[string]interface{} {
	args := m.Called()
	return args.Get(0).(map[string]interface{})
}

func (m *MockNode) GetOutputSchema() map[string]interface{} {
	args := m.Called()
	return args.Get(0).(map[string]interface{})
}

func TestRecoverableExecutor_ExecuteWithRecovery_Success(t *testing.T) {
	logger := slog.Default()
	metricsTracker := NewMetricsTracker()
	executor := NewRecoverableExecutor(logger, metricsTracker)
	
	mockNode := new(MockNode)
	mockNode.On("Execute", mock.Anything, mock.Anything, mock.Anything).Return(
		map[string]interface{}{"result": "success"},
		[]ports.NextNode{},
		nil,
	)
	
	item := &ports.QueueItem{
		WorkflowID: "test-workflow",
		NodeName:   "test-node",
		Config:     map[string]interface{}{},
	}
	
	result, nextNodes, err := executor.ExecuteWithRecovery(
		context.Background(),
		mockNode,
		map[string]interface{}{},
		map[string]interface{}{},
		item,
	)
	
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, nextNodes)
	
	resultMap, ok := result.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "success", resultMap["result"])
	
	mockNode.AssertExpectations(t)
}

func TestRecoverableExecutor_ExecuteWithRecovery_NodeError(t *testing.T) {
	logger := slog.Default()
	metricsTracker := NewMetricsTracker()
	executor := NewRecoverableExecutor(logger, metricsTracker)
	
	mockNode := new(MockNode)
	expectedError := errors.New("node execution failed")
	mockNode.On("Execute", mock.Anything, mock.Anything, mock.Anything).Return(
		nil,
		[]ports.NextNode{},
		expectedError,
	)
	
	item := &ports.QueueItem{
		WorkflowID: "test-workflow",
		NodeName:   "test-node",
		Config:     map[string]interface{}{},
	}
	
	result, nextNodes, err := executor.ExecuteWithRecovery(
		context.Background(),
		mockNode,
		map[string]interface{}{},
		map[string]interface{}{},
		item,
	)
	
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	assert.Nil(t, result)
	assert.Empty(t, nextNodes)
	
	mockNode.AssertExpectations(t)
}

type PanicNode struct{}

func (p *PanicNode) Execute(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
	panic("test panic message")
}

func (p *PanicNode) GetName() string {
	return "panic-node"
}

func (p *PanicNode) CanStart(ctx context.Context, globalState interface{}, config interface{}) bool {
	return true
}

func (p *PanicNode) GetInputSchema() map[string]interface{} {
	return map[string]interface{}{}
}

func (p *PanicNode) GetOutputSchema() map[string]interface{} {
	return map[string]interface{}{}
}

func TestRecoverableExecutor_ExecuteWithRecovery_Panic(t *testing.T) {
	logger := slog.Default()
	metricsTracker := NewMetricsTracker()
	executor := NewRecoverableExecutor(logger, metricsTracker)
	
	panicNode := &PanicNode{}
	
	item := &ports.QueueItem{
		WorkflowID: "test-workflow",
		NodeName:   "panic-node",
		Config:     map[string]interface{}{},
	}
	
	result, nextNodes, err := executor.ExecuteWithRecovery(
		context.Background(),
		panicNode,
		map[string]interface{}{},
		map[string]interface{}{},
		item,
	)
	
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, nextNodes)
	
	panicErr, ok := err.(*domain.WorkflowPanicError)
	assert.True(t, ok)
	assert.Equal(t, "test-workflow", panicErr.WorkflowID)
	assert.Equal(t, "panic-node", panicErr.NodeID)
	assert.Equal(t, "test panic message", panicErr.PanicValue)
	assert.NotEmpty(t, panicErr.StackTrace)
}

func TestLifecycleManager_TriggerCompletion(t *testing.T) {
	logger := slog.Default()
	config := HandlerConfig{
		Timeout:    1 * time.Second,
		MaxRetries: 1,
	}
	metricsTracker := NewMetricsTracker()
	manager := NewLifecycleManager(logger, config, metricsTracker)
	
	var handlerCalled atomic.Bool
	completionHandler := func(workflowID string, finalState interface{}) {
		handlerCalled.Store(true)
		assert.Equal(t, "test-workflow", workflowID)
		assert.Equal(t, map[string]interface{}{"final": "state"}, finalState)
	}
	
	manager.RegisterHandlers([]ports.CompletionHandler{completionHandler}, nil)
	
	err := manager.TriggerCompletion("test-workflow", map[string]interface{}{"final": "state"})
	assert.NoError(t, err)
	
	time.Sleep(100 * time.Millisecond)
	assert.True(t, handlerCalled.Load())
}

func TestLifecycleManager_TriggerError(t *testing.T) {
	logger := slog.Default()
	config := HandlerConfig{
		Timeout:    1 * time.Second,
		MaxRetries: 1,
	}
	metricsTracker := NewMetricsTracker()
	manager := NewLifecycleManager(logger, config, metricsTracker)
	
	var handlerCalled atomic.Bool
	expectedError := errors.New("test error")
	errorHandler := func(workflowID string, currentState interface{}, err error) {
		handlerCalled.Store(true)
		assert.Equal(t, "test-workflow", workflowID)
		assert.Equal(t, map[string]interface{}{"current": "state"}, currentState)
		assert.Equal(t, expectedError, err)
	}
	
	manager.RegisterHandlers(nil, []ports.ErrorHandler{errorHandler})
	
	err := manager.TriggerError("test-workflow", map[string]interface{}{"current": "state"}, expectedError)
	assert.NoError(t, err)
	
	time.Sleep(100 * time.Millisecond)
	assert.True(t, handlerCalled.Load())
}

func TestLifecycleManager_TriggerError_WithPanicError(t *testing.T) {
	logger := slog.Default()
	config := HandlerConfig{
		Timeout:    1 * time.Second,
		MaxRetries: 1,
	}
	metricsTracker := NewMetricsTracker()
	manager := NewLifecycleManager(logger, config, metricsTracker)
	
	var handlerCalled atomic.Bool
	panicErr := domain.NewPanicError("test-workflow", "test-node", "panic message")
	errorHandler := func(workflowID string, currentState interface{}, err error) {
		handlerCalled.Store(true)
		assert.Equal(t, "test-workflow", workflowID)
		assert.Equal(t, panicErr, err)
	}
	
	manager.RegisterHandlers(nil, []ports.ErrorHandler{errorHandler})
	
	err := manager.TriggerError("test-workflow", map[string]interface{}{"current": "state"}, panicErr)
	assert.NoError(t, err)
	
	time.Sleep(100 * time.Millisecond)
	assert.True(t, handlerCalled.Load())
}

type PanicHandler struct {
	called atomic.Bool
}

func (p *PanicHandler) CompletionHandler(workflowID string, finalState interface{}) {
	p.called.Store(true)
	panic("handler panic")
}

func TestLifecycleManager_HandlerPanicRecovery(t *testing.T) {
	logger := slog.Default()
	config := HandlerConfig{
		Timeout:    1 * time.Second,
		MaxRetries: 1,
	}
	metricsTracker := NewMetricsTracker()
	manager := NewLifecycleManager(logger, config, metricsTracker)
	
	panicHandler := &PanicHandler{}
	
	manager.RegisterHandlers([]ports.CompletionHandler{panicHandler.CompletionHandler}, nil)
	
	err := manager.TriggerCompletion("test-workflow", map[string]interface{}{"final": "state"})
	assert.NoError(t, err)
	
	time.Sleep(200 * time.Millisecond)
	assert.True(t, panicHandler.called.Load())
}

type SlowHandler struct {
	called atomic.Bool
}

func (s *SlowHandler) CompletionHandler(workflowID string, finalState interface{}) {
	s.called.Store(true)
	time.Sleep(2 * time.Second)
}

func TestLifecycleManager_HandlerTimeout(t *testing.T) {
	logger := slog.Default()
	config := HandlerConfig{
		Timeout:    100 * time.Millisecond,
		MaxRetries: 1,
	}
	metricsTracker := NewMetricsTracker()
	manager := NewLifecycleManager(logger, config, metricsTracker)
	
	slowHandler := &SlowHandler{}
	
	manager.RegisterHandlers([]ports.CompletionHandler{slowHandler.CompletionHandler}, nil)
	
	err := manager.TriggerCompletion("test-workflow", map[string]interface{}{"final": "state"})
	assert.NoError(t, err)
	
	time.Sleep(300 * time.Millisecond)
	assert.True(t, slowHandler.called.Load())
}

func TestNewPanicError(t *testing.T) {
	panicErr := domain.NewPanicError("workflow-123", "node-456", "test panic value")
	
	assert.Equal(t, "workflow-123", panicErr.WorkflowID)
	assert.Equal(t, "node-456", panicErr.NodeID)
	assert.Equal(t, "test panic value", panicErr.PanicValue)
	assert.NotEmpty(t, panicErr.StackTrace)
	assert.NotEmpty(t, panicErr.RecoveredAt)
	assert.WithinDuration(t, time.Now(), panicErr.Timestamp, time.Second)
}

func TestPanicError_Error(t *testing.T) {
	panicErr := domain.NewPanicError("workflow-123", "node-456", "test panic")
	
	expected := "node execution panicked: node-456"
	assert.Equal(t, expected, panicErr.Error())
}

func TestLifecycleManager_MultipleHandlers(t *testing.T) {
	logger := slog.Default()
	config := HandlerConfig{
		Timeout:    1 * time.Second,
		MaxRetries: 1,
	}
	metricsTracker := NewMetricsTracker()
	manager := NewLifecycleManager(logger, config, metricsTracker)
	
	var handler1Called atomic.Bool
	var handler2Called atomic.Bool
	
	handler1 := func(workflowID string, finalState interface{}) {
		handler1Called.Store(true)
	}
	
	handler2 := func(workflowID string, finalState interface{}) {
		handler2Called.Store(true)
	}
	
	manager.RegisterHandlers([]ports.CompletionHandler{handler1, handler2}, nil)
	
	err := manager.TriggerCompletion("test-workflow", map[string]interface{}{"final": "state"})
	assert.NoError(t, err)
	
	time.Sleep(100 * time.Millisecond)
	assert.True(t, handler1Called.Load())
	assert.True(t, handler2Called.Load())
}

func TestRecoverableExecutor_MarkNodeFailed(t *testing.T) {
	logger := slog.Default()
	metricsTracker := NewMetricsTracker()
	executor := NewRecoverableExecutor(logger, metricsTracker)
	
	item := &ports.QueueItem{
		WorkflowID: "test-workflow",
		NodeName:   "failed-node",
		Config:     map[string]interface{}{"key": "value"},
	}
	
	executedNode := executor.MarkNodeFailed(context.Background(), item, "panic occurred")
	
	assert.Equal(t, "failed-node", executedNode.NodeName)
	assert.Equal(t, ports.NodeExecutionStatusPanicFailed, executedNode.Status)
	assert.Equal(t, "panic occurred", *executedNode.Error)
	assert.Equal(t, map[string]interface{}{"key": "value"}, executedNode.Config)
	assert.Nil(t, executedNode.Results)
}