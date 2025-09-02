package transport

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type mockEngine struct {
	lastTrigger domain.WorkflowTrigger
	shouldError bool
}

func (m *mockEngine) Start(ctx context.Context) error {
	return nil
}

func (m *mockEngine) Stop() error {
	return nil
}

func (m *mockEngine) ProcessTrigger(trigger domain.WorkflowTrigger) error {
	m.lastTrigger = trigger
	if m.shouldError {
		return domain.NewNotFoundError("workflow", trigger.WorkflowID)
	}
	return nil
}

func (m *mockEngine) GetWorkflowStatus(workflowID string) (*domain.WorkflowStatus, error) {
	return nil, nil
}

func (m *mockEngine) PauseWorkflow(ctx context.Context, workflowID string) error {
	return nil
}

func (m *mockEngine) ResumeWorkflow(ctx context.Context, workflowID string) error {
	return nil
}

func (m *mockEngine) StopWorkflow(ctx context.Context, workflowID string) error {
	return nil
}

func (m *mockEngine) GetMetrics() domain.ExecutionMetrics {
	return domain.ExecutionMetrics{}
}

func TestGRPCTransport_BasicLifecycle(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	transport := NewGRPCTransport(logger, domain.TransportConfig{})

	ctx := context.Background()
	err := transport.Start(ctx, "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Stop()

	mockEng := &mockEngine{}
	transport.RegisterEngine(mockEng)

	time.Sleep(100 * time.Millisecond)
}

func TestGRPCTransport_RegisterEngine(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	transport := NewGRPCTransport(logger, domain.TransportConfig{})
	mockEng := &mockEngine{}

	transport.RegisterEngine(mockEng)

	if transport.engine != mockEng {
		t.Error("Engine not properly registered")
	}
}
