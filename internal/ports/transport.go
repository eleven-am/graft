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
	
	SendTrigger(ctx context.Context, nodeAddr string, trigger domain.WorkflowTrigger) error
	SendJoinRequest(ctx context.Context, nodeAddr string, request *JoinRequest) (*JoinResponse, error)
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