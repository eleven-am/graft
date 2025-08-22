package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

type GRPCTransport struct {
	logger       *slog.Logger
	server       TransportServer
	client       TransportClient
	clientConfig *ClientConfig
	mu           sync.RWMutex
	started      bool
	leaderInfo   *ports.LeaderInfo
	peerTracking map[string]*ports.PeerInfo
	handlers     *TransportHandlers
}

type TransportConfig struct {
	ServerConfig *ServerConfig
	ClientConfig *ClientConfig
	NodeID       string
}


type TransportHandlers struct {
	ClusterHandler  ClusterHandler
	RaftHandler     RaftHandler
	WorkflowHandler WorkflowHandler
}

func NewGRPCTransport(logger *slog.Logger) *GRPCTransport {
	return &GRPCTransport{
		logger:       logger.With("component", "transport", "adapter", "grpc"),
		peerTracking: make(map[string]*ports.PeerInfo),
	}
}

func (t *GRPCTransport) Start(ctx context.Context, config ports.TransportConfig) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.started {
		return domain.NewConflictError("transport", "already started")
	}

	serverConfig := &ServerConfig{
		BindAddress: config.BindAddress,
		BindPort:    config.BindPort,
		TLS:         config.TLS,
		MaxMsgSize:  10 * 1024 * 1024,
	}

	clientConfig := &ClientConfig{
		MaxMsgSize:       10 * 1024 * 1024,
		TLS:              config.TLS,
		ConnectTimeout:   5 * time.Second,
		RequestTimeout:   10 * time.Second,
		MaxRetries:       3,
		RetryBackoff:     100 * time.Millisecond,
		PoolSize:         10,
		KeepAliveTime:    30 * time.Second,
		KeepAliveTimeout: 10 * time.Second,
	}

	t.server = NewGRPCServer(t.logger, serverConfig)
	t.client = NewGRPCClient(t.logger, clientConfig)
	t.clientConfig = clientConfig

	if t.handlers != nil {
		t.server.SetHandlers(
			t.handlers.ClusterHandler,
			t.handlers.RaftHandler,
			t.handlers.WorkflowHandler,
		)
	}

	if err := t.server.Start(ctx); err != nil {
		t.logger.Error("failed to start server", "error", err)
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to start transport server",
			Details: map[string]interface{}{
				"address": config.BindAddress,
				"port":    config.BindPort,
				"error":   err.Error(),
			},
		}
	}

	t.started = true
	t.logger.Info("transport started", "address", config.BindAddress, "port", config.BindPort)

	go t.healthCheckLoop(ctx)

	return nil
}

func (t *GRPCTransport) Stop() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.started {
		return nil
	}

	t.logger.Info("stopping transport")

	if t.server != nil {
		if err := t.server.Stop(); err != nil {
			t.logger.Error("failed to stop server", "error", err)
		}
	}

	if t.client != nil {
		if err := t.client.Close(); err != nil {
			t.logger.Error("failed to close client", "error", err)
		}
	}

	t.started = false
	t.logger.Info("transport stopped")

	return nil
}

func (t *GRPCTransport) SetHandlers(handlers *TransportHandlers) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.handlers = handlers
	if t.server != nil {
		t.server.SetHandlers(
			handlers.ClusterHandler,
			handlers.RaftHandler,
			handlers.WorkflowHandler,
		)
	}
}

func (t *GRPCTransport) JoinCluster(ctx context.Context, req ports.JoinRequest) (*ports.JoinResponse, error) {
	t.mu.RLock()
	leader := t.leaderInfo
	t.mu.RUnlock()

	if leader == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "no leader available",
		}
	}

	resp, err := t.client.JoinCluster(ctx, leader.Address, &req)
	if err != nil {
		t.logger.Error("failed to join cluster", "leader", leader.Address, "error", err)
		return nil, err
	}

	if resp.Success {
		t.trackPeer(&ports.PeerInfo{
			NodeID:    req.NodeID,
			Address:   req.Address,
			LastSeen:  time.Now(),
			IsHealthy: true,
		})
	}

	return resp, nil
}

func (t *GRPCTransport) LeaveCluster(ctx context.Context, req ports.LeaveRequest) (*ports.LeaveResponse, error) {
	t.mu.RLock()
	leader := t.leaderInfo
	t.mu.RUnlock()

	if leader == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "no leader available",
		}
	}

	resp, err := t.client.LeaveCluster(ctx, leader.Address, &req)
	if err != nil {
		t.logger.Error("failed to leave cluster", "leader", leader.Address, "error", err)
		return nil, err
	}

	if resp.Success {
		t.removePeer(req.NodeID)
	}

	return resp, nil
}

func (t *GRPCTransport) GetLeader(ctx context.Context) (*ports.LeaderInfo, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.leaderInfo == nil {
		return nil, domain.NewNotFoundError("leader", "")
	}

	return &ports.LeaderInfo{
		NodeID:  t.leaderInfo.NodeID,
		Address: t.leaderInfo.Address,
	}, nil
}

func (t *GRPCTransport) ForwardToLeader(ctx context.Context, req ports.ForwardRequest) (*ports.ForwardResponse, error) {
	t.mu.RLock()
	leader := t.leaderInfo
	t.mu.RUnlock()

	if leader == nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: "no leader available",
		}
	}

	switch req.Type {
	case "workflow_state_update":
		pbReq := &pb.ProposeStateUpdateRequest{}
		if err := json.Unmarshal(req.Payload, pbReq); err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: fmt.Sprintf("failed to unmarshal workflow state update payload: %v", err),
			}
		}
		
		resp, err := t.client.ProposeStateUpdate(ctx, leader.Address, pbReq)
		if err != nil {
			return nil, err
		}

		resultData, err := json.Marshal(resp)
		if err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: fmt.Sprintf("failed to marshal workflow state update response: %v", err),
			}
		}

		return &ports.ForwardResponse{
			Success: resp.Status.Success,
			Result:  resultData,
		}, nil

	case "workflow_trigger":
		pbReq := &pb.SubmitTriggerRequest{}
		if err := json.Unmarshal(req.Payload, pbReq); err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: fmt.Sprintf("failed to unmarshal workflow trigger payload: %v", err),
			}
		}
		
		resp, err := t.client.SubmitTrigger(ctx, leader.Address, pbReq)
		if err != nil {
			return nil, err
		}

		resultData, err := json.Marshal(resp)
		if err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: fmt.Sprintf("failed to marshal workflow trigger response: %v", err),
			}
		}

		return &ports.ForwardResponse{
			Success: resp.Status.Success,
			Result:  resultData,
		}, nil

	default:
		return nil, domain.NewValidationError("request_type", fmt.Sprintf("unknown request type: %s", req.Type))
	}
}

func (t *GRPCTransport) ForwardToNode(ctx context.Context, nodeID string, req ports.ForwardRequest) (*ports.ForwardResponse, error) {
	t.mu.RLock()
	peerInfo, exists := t.peerTracking[nodeID]
	t.mu.RUnlock()

	if !exists {
		return nil, domain.Error{
			Type:    domain.ErrorTypeNotFound,
			Message: fmt.Sprintf("node %s not found in peer tracking", nodeID),
		}
	}

	switch req.Type {
	case "READ":
		pbReq := &pb.ReadFromNodeRequest{
			Key: string(req.Payload),
		}
		
		resp, err := t.client.ReadFromNode(ctx, peerInfo.Address, pbReq)
		if err != nil {
			return nil, err
		}

		return &ports.ForwardResponse{
			Success: resp.Status.Success,
			Result:  resp.Value,
			Error:   resp.Status.Error,
		}, nil

	default:
		return nil, domain.NewValidationError("request_type", fmt.Sprintf("unknown request type for node forwarding: %s", req.Type))
	}
}

func (t *GRPCTransport) UpdateLeader(nodeID string, address string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.leaderInfo = &ports.LeaderInfo{
		NodeID:  nodeID,
		Address: address,
	}

	for id, peer := range t.peerTracking {
		peer.IsLeader = (id == nodeID)
	}

	t.logger.Info("leader updated", "nodeID", nodeID, "address", address)
}

func (t *GRPCTransport) trackPeer(info *ports.PeerInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.peerTracking[info.NodeID] = info
	t.logger.Debug("peer tracked", "nodeID", info.NodeID, "address", info.Address)
}

func (t *GRPCTransport) removePeer(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.peerTracking, nodeID)
	t.logger.Debug("peer removed", "nodeID", nodeID)
}

func (t *GRPCTransport) GetPeers() map[string]ports.PeerInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	peers := make(map[string]ports.PeerInfo)
	for k, v := range t.peerTracking {
		peers[k] = ports.PeerInfo{
			NodeID:    v.NodeID,
			Address:   v.Address,
			LastSeen:  v.LastSeen,
			IsHealthy: v.IsHealthy,
			IsLeader:  v.IsLeader,
		}
	}

	return peers
}

func (t *GRPCTransport) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.performHealthChecks(ctx)
		}
	}
}

func (t *GRPCTransport) performHealthChecks(ctx context.Context) {
	t.mu.RLock()
	peers := make(map[string]*ports.PeerInfo)
	for k, v := range t.peerTracking {
		peers[k] = v
	}
	t.mu.RUnlock()

	for _, peer := range peers {
		timeout := 5 * time.Second
		if t.clientConfig != nil && t.clientConfig.RequestTimeout > 0 {
			timeout = t.clientConfig.RequestTimeout
		}
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err := t.client.GetLeader(checkCtx, peer.Address)
		cancel()

		t.mu.Lock()
		if p, exists := t.peerTracking[peer.NodeID]; exists {
			if err != nil {
				p.IsHealthy = false
				t.logger.Warn("peer health check failed", "nodeID", peer.NodeID, "error", err)
			} else {
				p.IsHealthy = true
				p.LastSeen = time.Now()
			}
		}
		t.mu.Unlock()
	}
}

func (t *GRPCTransport) SendRaftMessage(ctx context.Context, nodeID string, message interface{}) error {
	t.mu.RLock()
	peer, exists := t.peerTracking[nodeID]
	t.mu.RUnlock()

	if !exists {
		return domain.NewNotFoundError("peer", nodeID)
	}

	if !peer.IsHealthy {
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: fmt.Sprintf("peer %s is not healthy", nodeID),
		}
	}

	switch msg := message.(type) {
	case *pb.AppendEntriesRequest:
		_, err := t.client.AppendEntries(ctx, peer.Address, msg)
		return err

	case *pb.RequestVoteRequest:
		_, err := t.client.RequestVote(ctx, peer.Address, msg)
		return err

	case *pb.InstallSnapshotRequest:
		_, err := t.client.InstallSnapshot(ctx, peer.Address, msg)
		return err

	default:
		return domain.NewValidationError("message_type", fmt.Sprintf("unknown message type: %T", message))
	}
}