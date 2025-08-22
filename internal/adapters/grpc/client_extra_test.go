package grpc

import (
	"context"
	"testing"
	"time"
	"log/slog"

	"github.com/stretchr/testify/assert"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

func TestGRPCClient_LoadTLSCredentials(t *testing.T) {
	logger := slog.Default()
	
	config := &ClientConfig{
		TLS: nil,
	}
	client := NewGRPCClient(logger, config)
	
	creds, err := client.loadTLSCredentials()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TLS credentials requested but not configured")
	
	config.TLS = &ports.TLSConfig{
		Enabled: false,
	}
	client = NewGRPCClient(logger, config)
	
	creds, err = client.loadTLSCredentials()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TLS credentials requested but not configured")
	
	config.TLS = &ports.TLSConfig{
		Enabled: true,
		CAFile:  "",
	}
	client = NewGRPCClient(logger, config)
	
	creds, err = client.loadTLSCredentials()
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "failed to load client TLS credentials")
	
	config.TLS = &ports.TLSConfig{
		Enabled: true,
		CAFile:  "nonexistent.ca",
	}
	client = NewGRPCClient(logger, config)
	
	creds, err = client.loadTLSCredentials()
	assert.Error(t, err)
	assert.Nil(t, creds)
}

func TestGRPCClient_CircuitBreaker_Recovery(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{
		MaxRetries:   3,
		RetryBackoff: 10 * time.Millisecond,
	}
	
	client := NewGRPCClient(logger, config)
	address := "test-address"
	
	client.circuitBreaker[address] = &CircuitBreaker{
		failureCount:    0,
		state:           CircuitClosed,
		threshold:       5,
		timeout:         100 * time.Millisecond,
	}
	
	err := client.checkCircuitBreaker(address)
	assert.NoError(t, err)
	
	client.circuitBreaker[address].failureCount = 3
	client.circuitBreaker[address].lastFailureTime = time.Now()
	err = client.checkCircuitBreaker(address)
	assert.NoError(t, err)
	
	client.circuitBreaker[address].state = CircuitOpen
	client.circuitBreaker[address].lastFailureTime = time.Now()
	err = client.checkCircuitBreaker(address)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "circuit breaker open")
	
	client.circuitBreaker[address].lastFailureTime = time.Now().Add(-200 * time.Millisecond)
	err = client.checkCircuitBreaker(address)
	assert.NoError(t, err)
	assert.Equal(t, CircuitHalfOpen, client.circuitBreaker[address].state)
	
	client.recordSuccess(address)
	assert.Equal(t, CircuitClosed, client.circuitBreaker[address].state)
	assert.Equal(t, 0, client.circuitBreaker[address].failureCount)
	
	for i := 0; i < 6; i++ {
		client.recordFailure(address)
	}
	assert.Equal(t, CircuitOpen, client.circuitBreaker[address].state)
}

func TestGRPCClient_RecordSuccessFailure(t *testing.T) {
	logger := slog.Default()
	config := &ClientConfig{}
	
	client := NewGRPCClient(logger, config)
	address := "test-address"
	
	client.recordSuccess(address)
	
	client.circuitBreaker[address] = &CircuitBreaker{
		failureCount: 3,
		state:        CircuitHalfOpen,
		threshold:    5,
		timeout:      100 * time.Millisecond,
	}
	
	client.recordSuccess(address)
	assert.Equal(t, 0, client.circuitBreaker[address].failureCount)
	assert.Equal(t, CircuitClosed, client.circuitBreaker[address].state)
	
	for i := 0; i < 4; i++ {
		client.recordFailure(address)
		assert.Equal(t, i+1, client.circuitBreaker[address].failureCount)
	}
	assert.Equal(t, CircuitClosed, client.circuitBreaker[address].state)
	
	client.recordFailure(address)
	assert.Equal(t, CircuitOpen, client.circuitBreaker[address].state)
}

func TestGRPCClient_LeaveCluster_FullCoverage(t *testing.T) {
	testService := &testClusterService{}
	addr, cleanup := startTestServer(t, testService)
	defer cleanup()
	
	logger := slog.Default()
	config := &ClientConfig{
		ConnectTimeout: 1 * time.Second,
		RequestTimeout: 1 * time.Second,
	}
	
	client := NewGRPCClient(logger, config)
	defer client.Close()
	
	ctx := context.Background()
	
	testService.leaveFunc = func(ctx context.Context, req *pb.LeaveClusterRequest) (*pb.LeaveClusterResponse, error) {
		return &pb.LeaveClusterResponse{
			Status: &pb.Status{
				Success: true,
			},
		}, nil
	}
	
	req := &ports.LeaveRequest{
		NodeID: "test-node",
	}
	
	resp, err := client.LeaveCluster(ctx, addr, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	
	testService.leaveFunc = func(ctx context.Context, req *pb.LeaveClusterRequest) (*pb.LeaveClusterResponse, error) {
		return &pb.LeaveClusterResponse{
			Status: &pb.Status{
				Success: false,
				Error:   "leave failed",
			},
		}, nil
	}
	
	resp, err = client.LeaveCluster(ctx, addr, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.Success)
	assert.Equal(t, "leave failed", resp.Error)
}

func TestGRPCClient_RaftMethods_FullCoverage(t *testing.T) {
	
	logger := slog.Default()
	config := &ClientConfig{
		ConnectTimeout: 1 * time.Second,
		RequestTimeout: 1 * time.Second,
	}
	
	client := NewGRPCClient(logger, config)
	defer client.Close()
	
	ctx := context.Background()
	
	raftService := &testRaftService{
		requestVoteFunc: func(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
			return &pb.RequestVoteResponse{
				Term:        2,
				VoteGranted: true,
			}, nil
		},
	}
	raftAddr, raftCleanup := startTestRaftServer(t, raftService)
	defer raftCleanup()
	
	voteReq := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "candidate-1",
		LastLogIndex: 10,
		LastLogTerm:  1,
	}
	
	voteResp, err := client.RequestVote(ctx, raftAddr, voteReq)
	assert.NoError(t, err)
	assert.NotNil(t, voteResp)
	assert.Equal(t, int64(2), voteResp.Term)
	assert.True(t, voteResp.VoteGranted)
	
	raftService.installSnapshotFunc = func(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
		return &pb.InstallSnapshotResponse{
			Term:    3,
			Success: true,
		}, nil
	}
	
	snapReq := &pb.InstallSnapshotRequest{
		Term:              2,
		LeaderId:          "leader-1",
		LastIncludedIndex: 20,
		LastIncludedTerm:  2,
		Data:              []byte("snapshot-data"),
	}
	
	snapResp, err := client.InstallSnapshot(ctx, raftAddr, snapReq)
	assert.NoError(t, err)
	assert.NotNil(t, snapResp)
	assert.Equal(t, int64(3), snapResp.Term)
	assert.True(t, snapResp.Success)
}

func TestGRPCClient_WorkflowMethods_FullCoverage(t *testing.T) {
	workflowService := &testWorkflowService{
		getStateFunc: func(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error) {
			return &pb.GetWorkflowStateResponse{
				Status: &pb.Status{
					Success: true,
				},
				State: &pb.WorkflowState{
					WorkflowId: "workflow-1",
					Status:     "running",
					Nodes: map[string]*pb.NodeExecution{
						"step1": {
							Id:       "node-1",
							NodeName: "step1",
							Status:   "completed",
						},
					},
				},
			}, nil
		},
		submitTriggerFunc: func(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error) {
			return &pb.SubmitTriggerResponse{
				Status: &pb.Status{
					Success: true,
				},
				ExecutionId: "execution-123",
			}, nil
		},
	}
	
	workflowAddr, workflowCleanup := startTestWorkflowServer(t, workflowService)
	defer workflowCleanup()
	
	logger := slog.Default()
	config := &ClientConfig{
		ConnectTimeout: 1 * time.Second,
		RequestTimeout: 1 * time.Second,
	}
	
	client := NewGRPCClient(logger, config)
	defer client.Close()
	
	ctx := context.Background()
	
	stateReq := &pb.GetWorkflowStateRequest{
		WorkflowId: "workflow-1",
	}
	
	stateResp, err := client.GetWorkflowState(ctx, workflowAddr, stateReq)
	assert.NoError(t, err)
	assert.NotNil(t, stateResp)
	assert.True(t, stateResp.Status.Success)
	assert.Equal(t, "workflow-1", stateResp.State.WorkflowId)
	assert.Equal(t, "running", stateResp.State.Status)
	
	triggerReq := &pb.SubmitTriggerRequest{
		WorkflowId:  "workflow-1",
		TriggerType: "manual",
		Payload:     []byte("trigger-data"),
	}
	
	triggerResp, err := client.SubmitTrigger(ctx, workflowAddr, triggerReq)
	assert.NoError(t, err)
	assert.NotNil(t, triggerResp)
	assert.True(t, triggerResp.Status.Success)
	assert.Equal(t, "execution-123", triggerResp.ExecutionId)
}