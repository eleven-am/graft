package grpc

import (
	"context"

	pb "github.com/eleven-am/graft/internal/proto/gen"
	"github.com/eleven-am/graft/internal/ports"
)

type ClusterHandler interface {
	JoinCluster(ctx context.Context, req *ports.JoinRequest) (*ports.JoinResponse, error)
	LeaveCluster(ctx context.Context, req *ports.LeaveRequest) (*ports.LeaveResponse, error)
	GetLeader(ctx context.Context) (*ports.LeaderInfo, error)
}

type RaftHandler interface {
	AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
}

type WorkflowHandler interface {
	ProposeStateUpdate(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error)
	GetWorkflowState(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error)
	SubmitTrigger(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error)
}

type StorageHandler interface {
	ReadFromNode(ctx context.Context, req *pb.ReadFromNodeRequest) (*pb.ReadFromNodeResponse, error)
	WriteToNode(ctx context.Context, req *pb.WriteToNodeRequest) (*pb.WriteToNodeResponse, error)
	DeleteFromNode(ctx context.Context, req *pb.DeleteFromNodeRequest) (*pb.DeleteFromNodeResponse, error)
	BatchOperation(ctx context.Context, req *pb.BatchOperationRequest) (*pb.BatchOperationResponse, error)
}

type TransportClient interface {
	JoinCluster(ctx context.Context, address string, req *ports.JoinRequest) (*ports.JoinResponse, error)
	LeaveCluster(ctx context.Context, address string, req *ports.LeaveRequest) (*ports.LeaveResponse, error)
	GetLeader(ctx context.Context, address string) (*ports.LeaderInfo, error)
	AppendEntries(ctx context.Context, address string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	RequestVote(ctx context.Context, address string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	InstallSnapshot(ctx context.Context, address string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
	ProposeStateUpdate(ctx context.Context, address string, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error)
	GetWorkflowState(ctx context.Context, address string, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error)
	SubmitTrigger(ctx context.Context, address string, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error)
	ReadFromNode(ctx context.Context, address string, req *pb.ReadFromNodeRequest) (*pb.ReadFromNodeResponse, error)
	Close() error
}

type TransportServer interface {
	Start(ctx context.Context) error
	Stop() error
	GetAddress() string
	SetHandlers(cluster ClusterHandler, raft RaftHandler, workflow WorkflowHandler)
	SetStorageHandler(storage StorageHandler)
}