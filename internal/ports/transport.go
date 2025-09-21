package ports

import (
	"context"

	"github.com/eleven-am/graft/internal/domain"
)

type TransportPort interface {
	Start(ctx context.Context, address string, port int) error
	Stop() error

	RegisterEngine(engine EnginePort)
	RegisterRaft(raft RaftNode)
	RegisterLoadSink(sink LoadSink)

	SendTrigger(ctx context.Context, nodeAddr string, trigger domain.WorkflowTrigger) error
	SendJoinRequest(ctx context.Context, nodeAddr string, request *JoinRequest) (*JoinResponse, error)
	SendApplyCommand(ctx context.Context, nodeAddr string, cmd *domain.Command) (*domain.CommandResult, string, error)
	SendPublishLoad(ctx context.Context, nodeAddr string, update LoadUpdate) error
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
	Capacity        float64
	Timestamp       int64
}
