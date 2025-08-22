package grpc

import (
	"context"
	
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

type mockClusterHandler struct {
	joinFunc      func(ctx context.Context, req *ports.JoinRequest) (*ports.JoinResponse, error)
	leaveFunc     func(ctx context.Context, req *ports.LeaveRequest) (*ports.LeaveResponse, error)
	getLeaderFunc func(ctx context.Context) (*ports.LeaderInfo, error)
}

func (m *mockClusterHandler) JoinCluster(ctx context.Context, req *ports.JoinRequest) (*ports.JoinResponse, error) {
	if m.joinFunc != nil {
		return m.joinFunc(ctx, req)
	}
	return &ports.JoinResponse{Success: true}, nil
}

func (m *mockClusterHandler) LeaveCluster(ctx context.Context, req *ports.LeaveRequest) (*ports.LeaveResponse, error) {
	if m.leaveFunc != nil {
		return m.leaveFunc(ctx, req)
	}
	return &ports.LeaveResponse{Success: true}, nil
}

func (m *mockClusterHandler) GetLeader(ctx context.Context) (*ports.LeaderInfo, error) {
	if m.getLeaderFunc != nil {
		return m.getLeaderFunc(ctx)
	}
	return &ports.LeaderInfo{NodeID: "leader-1", Address: "127.0.0.1:8001"}, nil
}

type mockRaftHandler struct {
	appendEntriesFunc   func(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	requestVoteFunc     func(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	installSnapshotFunc func(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
}

func (m *mockRaftHandler) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if m.appendEntriesFunc != nil {
		return m.appendEntriesFunc(ctx, req)
	}
	return &pb.AppendEntriesResponse{Term: 1, Success: true}, nil
}

func (m *mockRaftHandler) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if m.requestVoteFunc != nil {
		return m.requestVoteFunc(ctx, req)
	}
	return &pb.RequestVoteResponse{Term: 1, VoteGranted: true}, nil
}

func (m *mockRaftHandler) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if m.installSnapshotFunc != nil {
		return m.installSnapshotFunc(ctx, req)
	}
	return &pb.InstallSnapshotResponse{Term: 1, Success: true}, nil
}

type mockWorkflowHandler struct {
	proposeFunc       func(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error)
	getStateFunc      func(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error)
	submitTriggerFunc func(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error)
}

func (m *mockWorkflowHandler) ProposeStateUpdate(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error) {
	if m.proposeFunc != nil {
		return m.proposeFunc(ctx, req)
	}
	return &pb.ProposeStateUpdateResponse{
		Status: &pb.Status{Success: true},
	}, nil
}

func (m *mockWorkflowHandler) GetWorkflowState(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error) {
	if m.getStateFunc != nil {
		return m.getStateFunc(ctx, req)
	}
	return &pb.GetWorkflowStateResponse{
		Status: &pb.Status{Success: true},
	}, nil
}

func (m *mockWorkflowHandler) SubmitTrigger(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error) {
	if m.submitTriggerFunc != nil {
		return m.submitTriggerFunc(ctx, req)
	}
	return &pb.SubmitTriggerResponse{
		Status: &pb.Status{Success: true},
	}, nil
}