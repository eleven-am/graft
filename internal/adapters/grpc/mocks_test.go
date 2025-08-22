package grpc

import (
	"context"
	"testing"
	"fmt"

	"github.com/stretchr/testify/assert"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

func TestMockClusterHandler_DefaultBehavior(t *testing.T) {
	mock := &mockClusterHandler{}
	ctx := context.Background()
	
	joinResp, err := mock.JoinCluster(ctx, &ports.JoinRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, joinResp)
	assert.True(t, joinResp.Success)
	
	leaveResp, err := mock.LeaveCluster(ctx, &ports.LeaveRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, leaveResp)
	assert.True(t, leaveResp.Success)
	
	leaderInfo, err := mock.GetLeader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, leaderInfo)
	assert.Equal(t, "leader-1", leaderInfo.NodeID)
	assert.Equal(t, "127.0.0.1:8001", leaderInfo.Address)
}

func TestMockClusterHandler_CustomBehavior(t *testing.T) {
	ctx := context.Background()
	
	mock := &mockClusterHandler{
		joinFunc: func(ctx context.Context, req *ports.JoinRequest) (*ports.JoinResponse, error) {
			return nil, fmt.Errorf("custom join error")
		},
	}
	
	joinResp, err := mock.JoinCluster(ctx, &ports.JoinRequest{})
	assert.Error(t, err)
	assert.Nil(t, joinResp)
	assert.Equal(t, "custom join error", err.Error())
	
	mock = &mockClusterHandler{
		leaveFunc: func(ctx context.Context, req *ports.LeaveRequest) (*ports.LeaveResponse, error) {
			return nil, fmt.Errorf("custom leave error")
		},
	}
	
	leaveResp, err := mock.LeaveCluster(ctx, &ports.LeaveRequest{})
	assert.Error(t, err)
	assert.Nil(t, leaveResp)
	assert.Equal(t, "custom leave error", err.Error())
	
	mock = &mockClusterHandler{
		getLeaderFunc: func(ctx context.Context) (*ports.LeaderInfo, error) {
			return nil, fmt.Errorf("custom leader error")
		},
	}
	
	leaderInfo, err := mock.GetLeader(ctx)
	assert.Error(t, err)
	assert.Nil(t, leaderInfo)
	assert.Equal(t, "custom leader error", err.Error())
}

func TestMockRaftHandler_DefaultBehavior(t *testing.T) {
	mock := &mockRaftHandler{}
	ctx := context.Background()
	
	appendResp, err := mock.AppendEntries(ctx, &pb.AppendEntriesRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, appendResp)
	assert.Equal(t, int64(1), appendResp.Term)
	assert.True(t, appendResp.Success)
	
	voteResp, err := mock.RequestVote(ctx, &pb.RequestVoteRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, voteResp)
	assert.Equal(t, int64(1), voteResp.Term)
	assert.True(t, voteResp.VoteGranted)
	
	snapResp, err := mock.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, snapResp)
	assert.Equal(t, int64(1), snapResp.Term)
	assert.True(t, snapResp.Success)
}

func TestMockRaftHandler_CustomBehavior(t *testing.T) {
	ctx := context.Background()
	
	mock := &mockRaftHandler{
		appendEntriesFunc: func(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
			return nil, fmt.Errorf("append error")
		},
	}
	
	appendResp, err := mock.AppendEntries(ctx, &pb.AppendEntriesRequest{})
	assert.Error(t, err)
	assert.Nil(t, appendResp)
	
	mock = &mockRaftHandler{
		requestVoteFunc: func(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
			return nil, fmt.Errorf("vote error")
		},
	}
	
	voteResp, err := mock.RequestVote(ctx, &pb.RequestVoteRequest{})
	assert.Error(t, err)
	assert.Nil(t, voteResp)
	
	mock = &mockRaftHandler{
		installSnapshotFunc: func(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
			return nil, fmt.Errorf("snapshot error")
		},
	}
	
	snapResp, err := mock.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{})
	assert.Error(t, err)
	assert.Nil(t, snapResp)
}

func TestMockWorkflowHandler_DefaultBehavior(t *testing.T) {
	mock := &mockWorkflowHandler{}
	ctx := context.Background()
	
	proposeResp, err := mock.ProposeStateUpdate(ctx, &pb.ProposeStateUpdateRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, proposeResp)
	assert.NotNil(t, proposeResp.Status)
	assert.True(t, proposeResp.Status.Success)
	
	stateResp, err := mock.GetWorkflowState(ctx, &pb.GetWorkflowStateRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, stateResp)
	assert.NotNil(t, stateResp.Status)
	assert.True(t, stateResp.Status.Success)
	
	triggerResp, err := mock.SubmitTrigger(ctx, &pb.SubmitTriggerRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, triggerResp)
	assert.NotNil(t, triggerResp.Status)
	assert.True(t, triggerResp.Status.Success)
}

func TestMockWorkflowHandler_CustomBehavior(t *testing.T) {
	ctx := context.Background()
	
	mock := &mockWorkflowHandler{
		proposeFunc: func(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error) {
			return nil, fmt.Errorf("propose error")
		},
	}
	
	proposeResp, err := mock.ProposeStateUpdate(ctx, &pb.ProposeStateUpdateRequest{})
	assert.Error(t, err)
	assert.Nil(t, proposeResp)
	
	mock = &mockWorkflowHandler{
		getStateFunc: func(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error) {
			return nil, fmt.Errorf("state error")
		},
	}
	
	stateResp, err := mock.GetWorkflowState(ctx, &pb.GetWorkflowStateRequest{})
	assert.Error(t, err)
	assert.Nil(t, stateResp)
	
	mock = &mockWorkflowHandler{
		submitTriggerFunc: func(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error) {
			return nil, fmt.Errorf("trigger error")
		},
	}
	
	triggerResp, err := mock.SubmitTrigger(ctx, &pb.SubmitTriggerRequest{})
	assert.Error(t, err)
	assert.Nil(t, triggerResp)
}