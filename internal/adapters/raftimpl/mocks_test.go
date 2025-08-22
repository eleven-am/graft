package raftimpl

import (
	"context"
	"fmt"

	"github.com/eleven-am/graft/internal/ports"
)

type MockTransportPort struct {
	ForwardToLeaderFunc func(ctx context.Context, req ports.ForwardRequest) (*ports.ForwardResponse, error)
	ForwardToNodeFunc   func(ctx context.Context, nodeID string, req ports.ForwardRequest) (*ports.ForwardResponse, error)
}

func (m *MockTransportPort) Start(ctx context.Context, config ports.TransportConfig) error {
	return nil
}

func (m *MockTransportPort) Stop() error {
	return nil
}

func (m *MockTransportPort) JoinCluster(ctx context.Context, req ports.JoinRequest) (*ports.JoinResponse, error) {
	return &ports.JoinResponse{Success: true}, nil
}

func (m *MockTransportPort) LeaveCluster(ctx context.Context, req ports.LeaveRequest) (*ports.LeaveResponse, error) {
	return &ports.LeaveResponse{Success: true}, nil
}

func (m *MockTransportPort) GetLeader(ctx context.Context) (*ports.LeaderInfo, error) {
	return &ports.LeaderInfo{
		NodeID:  "leader-node",
		Address: "127.0.0.1:8300",
	}, nil
}

func (m *MockTransportPort) ForwardToLeader(ctx context.Context, req ports.ForwardRequest) (*ports.ForwardResponse, error) {
	if m.ForwardToLeaderFunc != nil {
		return m.ForwardToLeaderFunc(ctx, req)
	}
	return &ports.ForwardResponse{Success: true}, nil
}

func (m *MockTransportPort) ForwardToNode(ctx context.Context, nodeID string, req ports.ForwardRequest) (*ports.ForwardResponse, error) {
	if m.ForwardToNodeFunc != nil {
		return m.ForwardToNodeFunc(ctx, nodeID, req)
	}
	return &ports.ForwardResponse{Success: true}, nil
}

func NewMockTransportPort() *MockTransportPort {
	return &MockTransportPort{}
}

func NewMockTransportPortWithError() *MockTransportPort {
	return &MockTransportPort{
		ForwardToLeaderFunc: func(ctx context.Context, req ports.ForwardRequest) (*ports.ForwardResponse, error) {
			return nil, fmt.Errorf("mock transport error")
		},
	}
}

func NewMockTransportPortWithFailure() *MockTransportPort {
	return &MockTransportPort{
		ForwardToLeaderFunc: func(ctx context.Context, req ports.ForwardRequest) (*ports.ForwardResponse, error) {
			return &ports.ForwardResponse{
				Success: false,
				Error:   "mock failure response",
			}, nil
		},
	}
}