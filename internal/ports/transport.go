package ports

import (
	"context"

	"github.com/eleven-am/graft/internal/domain"
	"google.golang.org/grpc"
)

type TransportPort interface {
	Start(ctx context.Context, address string, port int) error
	Stop() error

	RegisterEngine(engine EnginePort)
	RegisterRaft(raft RaftNode)
	RegisterLoadSink(sink LoadSink)
	RegisterService(registrar func(*grpc.Server))

	SendTrigger(ctx context.Context, nodeAddr string, trigger domain.WorkflowTrigger) error
	SendJoinRequest(ctx context.Context, nodeAddr string, request *JoinRequest) (*JoinResponse, error)
	SendApplyCommand(ctx context.Context, nodeAddr string, cmd *domain.Command) (*domain.CommandResult, string, error)
	SendPublishLoad(ctx context.Context, nodeAddr string, update LoadUpdate) error

	GetLeaderInfo(ctx context.Context, peerAddr string) (leaderID, leaderAddr string, err error)
	RequestAddVoter(ctx context.Context, leaderAddr, nodeID, nodeAddr string) error
}

type JoinRequest struct {
	NodeID   string
	Address  string
	Port     int
	Metadata map[string]string
}

type JoinResponse struct {
	Accepted bool
	NodeID   string
	Message  string
}

type WorkflowRequest struct {
	WorkflowID string
	NodeName   string
	Config     []byte
}

type WorkflowResponse struct {
	Success bool
	Message string
}

type TriggerRequest struct {
	Trigger *domain.WorkflowTrigger
}

type TriggerResponse struct {
	Success bool
	Message string
}

type LoadUpdate struct {
	NodeID          string
	ActiveWorkflows int
	TotalWeight     float64
	RecentLatencyMs float64
	RecentErrorRate float64
	// Pressure is a normalized local load indicator (cpu+mem in [0,2]).
	// Transport currently sends this via the protobuf LoadUpdate.capacity field.
	Pressure  float64
	Timestamp int64
}
