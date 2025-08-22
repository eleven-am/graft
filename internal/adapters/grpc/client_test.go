package grpc

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

type testClusterService struct {
	pb.UnimplementedClusterServiceServer
	joinFunc     func(context.Context, *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error)
	leaveFunc    func(context.Context, *pb.LeaveClusterRequest) (*pb.LeaveClusterResponse, error)
	getLeaderFunc func(context.Context, *pb.Empty) (*pb.GetLeaderResponse, error)
}

type testRaftService struct {
	pb.UnimplementedRaftServiceServer
	appendEntriesFunc   func(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	requestVoteFunc     func(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	installSnapshotFunc func(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
}

func (s *testRaftService) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if s.appendEntriesFunc != nil {
		return s.appendEntriesFunc(ctx, req)
	}
	return &pb.AppendEntriesResponse{Term: 1, Success: true}, nil
}

func (s *testRaftService) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if s.requestVoteFunc != nil {
		return s.requestVoteFunc(ctx, req)
	}
	return &pb.RequestVoteResponse{Term: 1, VoteGranted: true}, nil
}

func (s *testRaftService) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if s.installSnapshotFunc != nil {
		return s.installSnapshotFunc(ctx, req)
	}
	return &pb.InstallSnapshotResponse{Term: 1, Success: true}, nil
}

type testWorkflowService struct {
	pb.UnimplementedWorkflowServiceServer
	proposeFunc       func(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error)
	getStateFunc      func(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error)
	submitTriggerFunc func(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error)
}

func (s *testWorkflowService) ProposeStateUpdate(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error) {
	if s.proposeFunc != nil {
		return s.proposeFunc(ctx, req)
	}
	return &pb.ProposeStateUpdateResponse{Status: &pb.Status{Success: true}}, nil
}

func (s *testWorkflowService) GetWorkflowState(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error) {
	if s.getStateFunc != nil {
		return s.getStateFunc(ctx, req)
	}
	return &pb.GetWorkflowStateResponse{Status: &pb.Status{Success: true}}, nil
}

func (s *testWorkflowService) SubmitTrigger(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error) {
	if s.submitTriggerFunc != nil {
		return s.submitTriggerFunc(ctx, req)
	}
	return &pb.SubmitTriggerResponse{Status: &pb.Status{Success: true}}, nil
}

func (s *testClusterService) JoinCluster(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	if s.joinFunc != nil {
		return s.joinFunc(ctx, req)
	}
	return &pb.JoinClusterResponse{
		Status: &pb.Status{Success: true},
	}, nil
}

func (s *testClusterService) LeaveCluster(ctx context.Context, req *pb.LeaveClusterRequest) (*pb.LeaveClusterResponse, error) {
	if s.leaveFunc != nil {
		return s.leaveFunc(ctx, req)
	}
	return &pb.LeaveClusterResponse{
		Status: &pb.Status{Success: true},
	}, nil
}

func (s *testClusterService) GetLeader(ctx context.Context, req *pb.Empty) (*pb.GetLeaderResponse, error) {
	if s.getLeaderFunc != nil {
		return s.getLeaderFunc(ctx, req)
	}
	return &pb.GetLeaderResponse{
		Status: &pb.Status{Success: true},
		Leader: &pb.NodeInfo{
			Id:      "leader-1",
			Address: "127.0.0.1:8000",
		},
	}, nil
}

func startTestServer(t *testing.T, service *testClusterService) (string, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterClusterServiceServer(server, service)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	cleanup := func() {
		server.GracefulStop()
		listener.Close()
	}

	return listener.Addr().String(), cleanup
}

func startTestRaftServer(t *testing.T, service *testRaftService) (string, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterRaftServiceServer(server, service)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	return listener.Addr().String(), func() {
		server.GracefulStop()
	}
}

func startTestWorkflowServer(t *testing.T, service *testWorkflowService) (string, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterWorkflowServiceServer(server, service)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	return listener.Addr().String(), func() {
		server.GracefulStop()
	}
}

func TestGRPCClient_JoinCluster(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		ConnectTimeout: 2 * time.Second,
		RequestTimeout: 2 * time.Second,
	}

	client := NewGRPCClient(logger, config)
	defer client.Close()

	tests := []struct {
		name          string
		service       *testClusterService
		request       *ports.JoinRequest
		expectSuccess bool
		expectError   bool
	}{
		{
			name: "successful join",
			service: &testClusterService{
				joinFunc: func(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
					return &pb.JoinClusterResponse{
						Status: &pb.Status{Success: true},
						Leader: &pb.NodeInfo{
							Id:      "leader-1",
							Address: "127.0.0.1:8000",
						},
					}, nil
				},
			},
			request: &ports.JoinRequest{
				NodeID:  "node-1",
				Address: "127.0.0.1:8001",
			},
			expectSuccess: true,
			expectError:   false,
		},
		{
			name: "failed join",
			service: &testClusterService{
				joinFunc: func(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
					return &pb.JoinClusterResponse{
						Status: &pb.Status{
							Success: false,
							Error:   "cluster full",
						},
					}, nil
				},
			},
			request: &ports.JoinRequest{
				NodeID:  "node-1",
				Address: "127.0.0.1:8001",
			},
			expectSuccess: false,
			expectError:   false,
		},
		{
			name: "server error",
			service: &testClusterService{
				joinFunc: func(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
					return nil, status.Error(codes.Internal, "internal error")
				},
			},
			request: &ports.JoinRequest{
				NodeID:  "node-1",
				Address: "127.0.0.1:8001",
			},
			expectSuccess: false,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, cleanup := startTestServer(t, tt.service)
			defer cleanup()

			time.Sleep(100 * time.Millisecond)

			resp, err := client.JoinCluster(context.Background(), addr, tt.request)

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

			if resp.Success != tt.expectSuccess {
				t.Errorf("Expected success=%v, got %v", tt.expectSuccess, resp.Success)
			}
		})
	}
}

func TestGRPCClient_LeaveCluster(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		ConnectTimeout: 2 * time.Second,
		RequestTimeout: 2 * time.Second,
	}

	client := NewGRPCClient(logger, config)
	defer client.Close()

	tests := []struct {
		name          string
		service       *testClusterService
		request       *ports.LeaveRequest
		expectSuccess bool
		expectError   bool
	}{
		{
			name: "successful leave",
			service: &testClusterService{
				leaveFunc: func(ctx context.Context, req *pb.LeaveClusterRequest) (*pb.LeaveClusterResponse, error) {
					return &pb.LeaveClusterResponse{
						Status: &pb.Status{Success: true},
					}, nil
				},
			},
			request: &ports.LeaveRequest{
				NodeID: "node-1",
			},
			expectSuccess: true,
			expectError:   false,
		},
		{
			name: "failed leave",
			service: &testClusterService{
				leaveFunc: func(ctx context.Context, req *pb.LeaveClusterRequest) (*pb.LeaveClusterResponse, error) {
					return &pb.LeaveClusterResponse{
						Status: &pb.Status{
							Success: false,
							Error:   "node not found",
						},
					}, nil
				},
			},
			request: &ports.LeaveRequest{
				NodeID: "node-1",
			},
			expectSuccess: false,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, cleanup := startTestServer(t, tt.service)
			defer cleanup()

			time.Sleep(100 * time.Millisecond)

			resp, err := client.LeaveCluster(context.Background(), addr, tt.request)

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

			if resp.Success != tt.expectSuccess {
				t.Errorf("Expected success=%v, got %v", tt.expectSuccess, resp.Success)
			}
		})
	}
}

func TestGRPCClient_GetLeader(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		ConnectTimeout: 2 * time.Second,
		RequestTimeout: 2 * time.Second,
	}

	client := NewGRPCClient(logger, config)
	defer client.Close()

	tests := []struct {
		name         string
		service      *testClusterService
		expectLeader bool
		expectError  bool
	}{
		{
			name: "leader found",
			service: &testClusterService{
				getLeaderFunc: func(ctx context.Context, req *pb.Empty) (*pb.GetLeaderResponse, error) {
					return &pb.GetLeaderResponse{
						Status: &pb.Status{Success: true},
						Leader: &pb.NodeInfo{
							Id:      "leader-1",
							Address: "127.0.0.1:8000",
						},
					}, nil
				},
			},
			expectLeader: true,
			expectError:  false,
		},
		{
			name: "no leader",
			service: &testClusterService{
				getLeaderFunc: func(ctx context.Context, req *pb.Empty) (*pb.GetLeaderResponse, error) {
					return &pb.GetLeaderResponse{
						Status: &pb.Status{
							Success: false,
							Error:   "no leader elected",
						},
					}, nil
				},
			},
			expectLeader: false,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, cleanup := startTestServer(t, tt.service)
			defer cleanup()

			time.Sleep(100 * time.Millisecond)

			leader, err := client.GetLeader(context.Background(), addr)

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

			if tt.expectLeader && leader == nil {
				t.Error("Expected leader but got nil")
			}

			if !tt.expectLeader && leader != nil {
				t.Error("Expected no leader but got one")
			}
		})
	}
}

func TestGRPCClient_CircuitBreaker(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		ConnectTimeout: 1 * time.Second,
		RequestTimeout: 1 * time.Second,
	}

	client := NewGRPCClient(logger, config)
	defer client.Close()

	invalidAddr := "127.0.0.1:99999"

	for i := 0; i < 6; i++ {
		_, err := client.GetLeader(context.Background(), invalidAddr)
		if err == nil {
			t.Errorf("Expected error on attempt %d", i+1)
		}
	}

	_, err := client.GetLeader(context.Background(), invalidAddr)
	if err == nil {
		t.Error("Expected circuit breaker to be open")
	}
}

func TestGRPCClient_ConnectionPooling(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		ConnectTimeout: 2 * time.Second,
		RequestTimeout: 2 * time.Second,
	}

	client := NewGRPCClient(logger, config)
	defer client.Close()

	service := &testClusterService{}
	addr, cleanup := startTestServer(t, service)
	defer cleanup()

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 5; i++ {
		_, err := client.GetLeader(context.Background(), addr)
		if err != nil {
			t.Errorf("Request %d failed: %v", i+1, err)
		}
	}

	client.mu.RLock()
	numConns := len(client.connections)
	client.mu.RUnlock()

	if numConns != 1 {
		t.Errorf("Expected 1 connection in pool, got %d", numConns)
	}
}

func TestGRPCClient_RaftMethods(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		PoolSize:       10,
		RequestTimeout: 5 * time.Second,
		MaxRetries:     3,
	}
	client := NewGRPCClient(logger, config)
	defer client.Close()

	t.Run("AppendEntries", func(t *testing.T) {
		service := &testRaftService{
			appendEntriesFunc: func(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
				if req.Term != 5 {
					t.Errorf("Expected term 5, got %d", req.Term)
				}
				return &pb.AppendEntriesResponse{Term: 5, Success: true}, nil
			},
		}

		addr, cleanup := startTestRaftServer(t, service)
		defer cleanup()

		req := &pb.AppendEntriesRequest{
			Term:         5,
			LeaderId:     "leader-1",
			PrevLogIndex: 10,
			PrevLogTerm:  4,
		}

		resp, err := client.AppendEntries(context.Background(), addr, req)
		if err != nil {
			t.Errorf("AppendEntries failed: %v", err)
		}
		if resp == nil || resp.Term != 5 || !resp.Success {
			t.Error("Unexpected AppendEntries response")
		}
	})

	t.Run("RequestVote", func(t *testing.T) {
		service := &testRaftService{
			requestVoteFunc: func(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
				if req.Term != 6 {
					t.Errorf("Expected term 6, got %d", req.Term)
				}
				return &pb.RequestVoteResponse{Term: 6, VoteGranted: true}, nil
			},
		}

		addr, cleanup := startTestRaftServer(t, service)
		defer cleanup()

		req := &pb.RequestVoteRequest{
			Term:         6,
			CandidateId:  "candidate-1",
			LastLogIndex: 15,
			LastLogTerm:  5,
		}

		resp, err := client.RequestVote(context.Background(), addr, req)
		if err != nil {
			t.Errorf("RequestVote failed: %v", err)
		}
		if resp == nil || resp.Term != 6 || !resp.VoteGranted {
			t.Error("Unexpected RequestVote response")
		}
	})

	t.Run("InstallSnapshot", func(t *testing.T) {
		service := &testRaftService{
			installSnapshotFunc: func(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
				if req.Term != 7 {
					t.Errorf("Expected term 7, got %d", req.Term)
				}
				return &pb.InstallSnapshotResponse{Term: 7, Success: true}, nil
			},
		}

		addr, cleanup := startTestRaftServer(t, service)
		defer cleanup()

		req := &pb.InstallSnapshotRequest{
			Term:              7,
			LeaderId:          "leader-1",
			LastIncludedIndex: 20,
			LastIncludedTerm:  6,
			Data:              []byte("snapshot data"),
		}

		resp, err := client.InstallSnapshot(context.Background(), addr, req)
		if err != nil {
			t.Errorf("InstallSnapshot failed: %v", err)
		}
		if resp == nil || resp.Term != 7 || !resp.Success {
			t.Error("Unexpected InstallSnapshot response")
		}
	})
}

func TestGRPCClient_WorkflowMethods(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		PoolSize:       10,
		RequestTimeout: 5 * time.Second,
		MaxRetries:     3,
	}
	client := NewGRPCClient(logger, config)
	defer client.Close()

	t.Run("ProposeStateUpdate", func(t *testing.T) {
		service := &testWorkflowService{
			proposeFunc: func(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error) {
				if req.WorkflowId != "wf-123" {
					t.Errorf("Expected workflow ID wf-123, got %s", req.WorkflowId)
				}
				return &pb.ProposeStateUpdateResponse{
					Status:   &pb.Status{Success: true},
					LogIndex: 42,
				}, nil
			},
		}

		addr, cleanup := startTestWorkflowServer(t, service)
		defer cleanup()

		req := &pb.ProposeStateUpdateRequest{
			WorkflowId: "wf-123",
			State:      &pb.WorkflowState{WorkflowId: "wf-123", Status: "running"},
		}

		resp, err := client.ProposeStateUpdate(context.Background(), addr, req)
		if err != nil {
			t.Errorf("ProposeStateUpdate failed: %v", err)
		}
		if resp == nil || !resp.Status.Success || resp.LogIndex != 42 {
			t.Error("Unexpected ProposeStateUpdate response")
		}
	})

	t.Run("GetWorkflowState", func(t *testing.T) {
		service := &testWorkflowService{
			getStateFunc: func(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error) {
				if req.WorkflowId != "wf-456" {
					t.Errorf("Expected workflow ID wf-456, got %s", req.WorkflowId)
				}
				return &pb.GetWorkflowStateResponse{
					Status: &pb.Status{Success: true},
					State:  &pb.WorkflowState{WorkflowId: "wf-456", Status: "completed"},
				}, nil
			},
		}

		addr, cleanup := startTestWorkflowServer(t, service)
		defer cleanup()

		req := &pb.GetWorkflowStateRequest{
			WorkflowId: "wf-456",
		}

		resp, err := client.GetWorkflowState(context.Background(), addr, req)
		if err != nil {
			t.Errorf("GetWorkflowState failed: %v", err)
		}
		if resp == nil || !resp.Status.Success || resp.State.Status != "completed" {
			t.Error("Unexpected GetWorkflowState response")
		}
	})

	t.Run("SubmitTrigger", func(t *testing.T) {
		service := &testWorkflowService{
			submitTriggerFunc: func(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error) {
				if req.WorkflowId != "wf-789" {
					t.Errorf("Expected workflow ID wf-789, got %s", req.WorkflowId)
				}
				return &pb.SubmitTriggerResponse{
					Status:      &pb.Status{Success: true},
					ExecutionId: "exec-123",
				}, nil
			},
		}

		addr, cleanup := startTestWorkflowServer(t, service)
		defer cleanup()

		req := &pb.SubmitTriggerRequest{
			WorkflowId:  "wf-789",
			TriggerType: "manual",
			Payload:     []byte("trigger event"),
		}

		resp, err := client.SubmitTrigger(context.Background(), addr, req)
		if err != nil {
			t.Errorf("SubmitTrigger failed: %v", err)
		}
		if resp == nil || !resp.Status.Success || resp.ExecutionId != "exec-123" {
			t.Error("Unexpected SubmitTrigger response")
		}
	})
}

func TestGRPCClient_TLSConnection(t *testing.T) {
	logger := slog.Default()
	
	config := &ClientConfig{
		PoolSize:       10,
		RequestTimeout: 5 * time.Second,
		MaxRetries:     3,
		TLS:            nil,
	}
	client := NewGRPCClient(logger, config)
	
	service := &testClusterService{}
	addr, cleanup := startTestServer(t, service)
	defer cleanup()
	
	time.Sleep(100 * time.Millisecond)
	
	_, err := client.GetLeader(context.Background(), addr)
	if err != nil {
		t.Errorf("Failed to connect without TLS: %v", err)
	}
	
	client.Close()
	
	config.TLS = &ports.TLSConfig{
		Enabled:  true,
		CertFile: "nonexistent.crt",
	}
	client = NewGRPCClient(logger, config)
	
	_, _ = client.GetLeader(context.Background(), addr)
	
	client.Close()
}

func TestGRPCClient_ErrorHandling(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		PoolSize:       10,
		RequestTimeout: 1 * time.Second,
		MaxRetries:     1,
	}
	client := NewGRPCClient(logger, config)
	defer client.Close()

	unreachableAddr := "127.0.0.1:59999"
	
	_, err := client.JoinCluster(context.Background(), unreachableAddr, &ports.JoinRequest{})
	if err == nil {
		t.Error("Expected error for unreachable address")
	}
	
	_, err = client.AppendEntries(context.Background(), unreachableAddr, &pb.AppendEntriesRequest{})
	if err == nil {
		t.Error("Expected error for unreachable address")
	}
	
	_, err = client.ProposeStateUpdate(context.Background(), unreachableAddr, &pb.ProposeStateUpdateRequest{})
	if err == nil {
		t.Error("Expected error for unreachable address")
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	
	service := &testClusterService{
		joinFunc: func(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
			time.Sleep(100 * time.Millisecond)
			return &pb.JoinClusterResponse{Status: &pb.Status{Success: true}}, nil
		},
	}
	
	addr, cleanup := startTestServer(t, service)
	defer cleanup()
	
	_, err = client.JoinCluster(ctx, addr, &ports.JoinRequest{})
	if err == nil {
		t.Error("Expected timeout error")
	}
}

func TestGRPCClient_ConnectionManagement(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		PoolSize:       2,
		RequestTimeout: 5 * time.Second,
		MaxRetries:     3,
	}
	client := NewGRPCClient(logger, config)
	defer client.Close()

	service := &testClusterService{}
	addr1, cleanup1 := startTestServer(t, service)
	defer cleanup1()
	addr2, cleanup2 := startTestServer(t, service)
	defer cleanup2()
	addr3, cleanup3 := startTestServer(t, service)
	defer cleanup3()

	time.Sleep(100 * time.Millisecond)

	_, err := client.GetLeader(context.Background(), addr1)
	if err != nil {
		t.Errorf("Failed to connect to server 1: %v", err)
	}

	_, err = client.GetLeader(context.Background(), addr2)
	if err != nil {
		t.Errorf("Failed to connect to server 2: %v", err)
	}

	_, err = client.GetLeader(context.Background(), addr3)
	if err != nil {
		t.Errorf("Failed to connect to server 3: %v", err)
	}

	_, err = client.GetLeader(context.Background(), addr1)
	if err != nil {
		t.Errorf("Failed to reuse connection to server 1: %v", err)
	}

	client.mu.RLock()
	numConns := len(client.connections)
	client.mu.RUnlock()

	if numConns > 3 {
		t.Errorf("Expected max 3 connections, got %d", numConns)
	}
}