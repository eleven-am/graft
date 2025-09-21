package transport

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto"
	"github.com/stretchr/testify/mock"
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

func TestApplyCommand_Leader(t *testing.T) {
	mockRaft := mocks.NewMockRaftNode(t)
	mockRaft.On("IsLeader").Return(true)
	mockRaft.On("Apply", mock.Anything, mock.Anything).Return(&domain.CommandResult{
		Success: true,
		Version: 42,
		Events: []domain.Event{
			{
				Type:      domain.EventPut,
				Key:       "test-key",
				Version:   42,
				NodeID:    "node1",
				Timestamp: time.Now(),
				RequestID: "req-123",
			},
		},
	}, nil)

	transport := &GRPCTransport{
		raft: mockRaft,
		cfg:  domain.TransportConfig{},
	}

	cmd := domain.NewPutCommand("test-key", []byte("test-value"), 42)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	req := &pb.ApplyRequest{
		Command: cmdBytes,
	}

	resp, err := transport.ApplyCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("ApplyCommand failed: %v", err)
	}

	if !resp.Success {
		t.Errorf("Expected success=true, got %v", resp.Success)
	}

	if resp.Version != 42 {
		t.Errorf("Expected version=42, got %d", resp.Version)
	}

	if len(resp.Events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(resp.Events))
	}

	if resp.Events[0].Key != "test-key" {
		t.Errorf("Expected event key 'test-key', got '%s'", resp.Events[0].Key)
	}
}

func TestApplyCommand_NonLeader(t *testing.T) {
	mockRaft := mocks.NewMockRaftNode(t)
	mockRaft.On("IsLeader").Return(false)
	mockRaft.On("LeaderAddr").Return("127.0.0.1:8001")
	mockRaft.On("GetClusterInfo").Return(ports.ClusterInfo{
		Leader: &ports.RaftNodeInfo{
			ID: "leader-node",
		},
	})

	transport := &GRPCTransport{
		raft: mockRaft,
		cfg:  domain.TransportConfig{},
	}

	cmd := domain.NewPutCommand("test-key", []byte("test-value"), 42)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	req := &pb.ApplyRequest{
		Command: cmdBytes,
	}

	resp, err := transport.ApplyCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("ApplyCommand failed: %v", err)
	}

	if resp.Success {
		t.Errorf("Expected success=false for non-leader, got %v", resp.Success)
	}

	if resp.LeaderAddr != "127.0.0.1:8001" {
		t.Errorf("Expected leader address '127.0.0.1:8001', got '%s'", resp.LeaderAddr)
	}

	if resp.LeaderId != "leader-node" {
		t.Errorf("Expected leader ID 'leader-node', got '%s'", resp.LeaderId)
	}
}

func TestApplyCommand_NoRaft(t *testing.T) {
	transport := &GRPCTransport{
		raft: nil,
		cfg:  domain.TransportConfig{},
	}

	req := &pb.ApplyRequest{
		Command: []byte("invalid"),
	}

	resp, err := transport.ApplyCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("ApplyCommand failed: %v", err)
	}

	if resp.Success {
		t.Errorf("Expected success=false when raft not available, got %v", resp.Success)
	}

	if resp.Error != "raft not available" {
		t.Errorf("Expected error 'raft not available', got '%s'", resp.Error)
	}
}
