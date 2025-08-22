package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

func TestGRPCServer_Start(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := server.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	defer server.Stop()

	err = server.Start(ctx)
	if err == nil {
		t.Error("Expected error when starting already started server")
	}

	if _, ok := err.(domain.Error); !ok {
		t.Error("Expected domain.Error for duplicate start")
	}
}

func TestGRPCServer_Stop(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)

	err := server.Stop()
	if err != nil {
		t.Error("Expected no error when stopping non-started server")
	}

	ctx := context.Background()
	err = server.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	err = server.Stop()
	if err != nil {
		t.Errorf("Failed to stop server: %v", err)
	}

	err = server.Stop()
	if err != nil {
		t.Error("Expected no error when stopping already stopped server")
	}
}

func TestGRPCServer_JoinCluster(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)

	tests := []struct {
		name          string
		handler       ClusterHandler
		request       *pb.JoinClusterRequest
		expectSuccess bool
		expectError   bool
	}{
		{
			name:    "no handler",
			handler: nil,
			request: &pb.JoinClusterRequest{
				Node: &pb.NodeInfo{
					Id:      "node-1",
					Address: "127.0.0.1",
					Port:    8001,
				},
			},
			expectSuccess: false,
			expectError:   true,
		},
		{
			name: "successful join",
			handler: &mockClusterHandler{
				joinFunc: func(ctx context.Context, req *ports.JoinRequest) (*ports.JoinResponse, error) {
					return &ports.JoinResponse{
						Success: true,
						Leader:  "127.0.0.1:8000",
					}, nil
				},
			},
			request: &pb.JoinClusterRequest{
				Node: &pb.NodeInfo{
					Id:      "node-1",
					Address: "127.0.0.1",
					Port:    8001,
				},
			},
			expectSuccess: true,
			expectError:   false,
		},
		{
			name: "handler error",
			handler: &mockClusterHandler{
				joinFunc: func(ctx context.Context, req *ports.JoinRequest) (*ports.JoinResponse, error) {
					return nil, fmt.Errorf("join failed")
				},
			},
			request: &pb.JoinClusterRequest{
				Node: &pb.NodeInfo{
					Id:      "node-1",
					Address: "127.0.0.1",
					Port:    8001,
				},
			},
			expectSuccess: false,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server.SetHandlers(tt.handler, nil, nil)

			resp, err := server.JoinCluster(context.Background(), tt.request)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if resp.Status.Success != tt.expectSuccess {
				t.Errorf("Expected success=%v, got %v", tt.expectSuccess, resp.Status.Success)
			}
		})
	}
}

func TestGRPCServer_LeaveCluster(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)

	tests := []struct {
		name          string
		handler       ClusterHandler
		request       *pb.LeaveClusterRequest
		expectSuccess bool
		expectError   bool
	}{
		{
			name:    "no handler",
			handler: nil,
			request: &pb.LeaveClusterRequest{
				NodeId: "node-1",
			},
			expectSuccess: false,
			expectError:   true,
		},
		{
			name: "successful leave",
			handler: &mockClusterHandler{
				leaveFunc: func(ctx context.Context, req *ports.LeaveRequest) (*ports.LeaveResponse, error) {
					return &ports.LeaveResponse{
						Success: true,
					}, nil
				},
			},
			request: &pb.LeaveClusterRequest{
				NodeId: "node-1",
			},
			expectSuccess: true,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server.SetHandlers(tt.handler, nil, nil)

			resp, err := server.LeaveCluster(context.Background(), tt.request)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if resp.Status.Success != tt.expectSuccess {
				t.Errorf("Expected success=%v, got %v", tt.expectSuccess, resp.Status.Success)
			}
		})
	}
}

func TestGRPCServer_RaftService_AppendEntries(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
		MaxMsgSize:  1024 * 1024,
	}
	
	server := NewGRPCServer(logger, config)
	
	ctx := context.Background()
	_, err := server.AppendEntries(ctx, &pb.AppendEntriesRequest{})
	if err == nil {
		t.Error("Expected error when no Raft handler is set")
	}
	
	raftHandler := &mockRaftHandler{
		appendEntriesFunc: func(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
			return &pb.AppendEntriesResponse{Term: 1, Success: true}, nil
		},
	}
	server.SetHandlers(nil, raftHandler, nil)
	
	resp, err := server.AppendEntries(ctx, &pb.AppendEntriesRequest{Term: 1})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Term != 1 || !resp.Success {
		t.Error("Expected successful response")
	}
}

func TestGRPCServer_RaftService_RequestVote(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
		MaxMsgSize:  1024 * 1024,
	}
	
	server := NewGRPCServer(logger, config)
	
	ctx := context.Background()
	_, err := server.RequestVote(ctx, &pb.RequestVoteRequest{})
	if err == nil {
		t.Error("Expected error when no Raft handler is set")
	}
	
	raftHandler := &mockRaftHandler{
		requestVoteFunc: func(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
			return &pb.RequestVoteResponse{Term: 1, VoteGranted: true}, nil
		},
	}
	server.SetHandlers(nil, raftHandler, nil)
	
	resp, err := server.RequestVote(ctx, &pb.RequestVoteRequest{Term: 1})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Term != 1 || !resp.VoteGranted {
		t.Error("Expected vote to be granted")
	}
}

func TestGRPCServer_RaftService_InstallSnapshot(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
		MaxMsgSize:  1024 * 1024,
	}
	
	server := NewGRPCServer(logger, config)
	
	ctx := context.Background()
	_, err := server.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{})
	if err == nil {
		t.Error("Expected error when no Raft handler is set")
	}
	
	raftHandler := &mockRaftHandler{
		installSnapshotFunc: func(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
			return &pb.InstallSnapshotResponse{Term: 1, Success: true}, nil
		},
	}
	server.SetHandlers(nil, raftHandler, nil)
	
	resp, err := server.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{Term: 1})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Term != 1 || !resp.Success {
		t.Error("Expected successful response")
	}
}

func TestGRPCServer_WorkflowService_ProposeStateUpdate(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
		MaxMsgSize:  1024 * 1024,
	}
	
	server := NewGRPCServer(logger, config)
	
	ctx := context.Background()
	_, err := server.ProposeStateUpdate(ctx, &pb.ProposeStateUpdateRequest{})
	if err == nil {
		t.Error("Expected error when no Workflow handler is set")
	}
	
	workflowHandler := &mockWorkflowHandler{
		proposeFunc: func(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error) {
			return &pb.ProposeStateUpdateResponse{Status: &pb.Status{Success: true}}, nil
		},
	}
	server.SetHandlers(nil, nil, workflowHandler)
	
	resp, err := server.ProposeStateUpdate(ctx, &pb.ProposeStateUpdateRequest{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !resp.Status.Success {
		t.Error("Expected successful response")
	}
}

func TestGRPCServer_WorkflowService_GetWorkflowState(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
		MaxMsgSize:  1024 * 1024,
	}
	
	server := NewGRPCServer(logger, config)
	
	ctx := context.Background()
	_, err := server.GetWorkflowState(ctx, &pb.GetWorkflowStateRequest{})
	if err == nil {
		t.Error("Expected error when no Workflow handler is set")
	}
	
	workflowHandler := &mockWorkflowHandler{
		getStateFunc: func(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error) {
			return &pb.GetWorkflowStateResponse{Status: &pb.Status{Success: true}}, nil
		},
	}
	server.SetHandlers(nil, nil, workflowHandler)
	
	resp, err := server.GetWorkflowState(ctx, &pb.GetWorkflowStateRequest{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !resp.Status.Success {
		t.Error("Expected successful response")
	}
}

func TestGRPCServer_WorkflowService_SubmitTrigger(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
		MaxMsgSize:  1024 * 1024,
	}
	
	server := NewGRPCServer(logger, config)
	
	ctx := context.Background()
	_, err := server.SubmitTrigger(ctx, &pb.SubmitTriggerRequest{})
	if err == nil {
		t.Error("Expected error when no Workflow handler is set")
	}
	
	workflowHandler := &mockWorkflowHandler{
		submitTriggerFunc: func(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error) {
			return &pb.SubmitTriggerResponse{Status: &pb.Status{Success: true}}, nil
		},
	}
	server.SetHandlers(nil, nil, workflowHandler)
	
	resp, err := server.SubmitTrigger(ctx, &pb.SubmitTriggerRequest{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !resp.Status.Success {
		t.Error("Expected successful response")
	}
}

func TestGRPCServer_GetLeader(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)

	tests := []struct {
		name          string
		handler       ClusterHandler
		expectSuccess bool
		expectError   bool
		expectLeader  bool
	}{
		{
			name:          "no handler",
			handler:       nil,
			expectSuccess: false,
			expectError:   true,
			expectLeader:  false,
		},
		{
			name: "successful get leader",
			handler: &mockClusterHandler{
				getLeaderFunc: func(ctx context.Context) (*ports.LeaderInfo, error) {
					return &ports.LeaderInfo{
						NodeID:  "leader-1",
						Address: "127.0.0.1:8000",
					}, nil
				},
			},
			expectSuccess: true,
			expectError:   false,
			expectLeader:  true,
		},
		{
			name: "no leader found",
			handler: &mockClusterHandler{
				getLeaderFunc: func(ctx context.Context) (*ports.LeaderInfo, error) {
					return nil, domain.NewNotFoundError("leader", "")
				},
			},
			expectSuccess: false,
			expectError:   false,
			expectLeader:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server.SetHandlers(tt.handler, nil, nil)

			resp, err := server.GetLeader(context.Background(), &pb.Empty{})

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if resp.Status.Success != tt.expectSuccess {
				t.Errorf("Expected success=%v, got %v", tt.expectSuccess, resp.Status.Success)
			}

			if tt.expectLeader && resp.Leader == nil {
				t.Error("Expected leader info but got nil")
			}

			if !tt.expectLeader && resp.Leader != nil {
				t.Error("Expected no leader info but got one")
			}
		})
	}
}