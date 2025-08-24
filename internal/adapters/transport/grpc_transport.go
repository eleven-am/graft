package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	pb.UnimplementedGraftTransportServer
	mu            sync.RWMutex
	logger        *slog.Logger
	server        *grpc.Server
	listener      net.Listener
	handler       ports.MessageHandler
	raft          ports.RaftPort
	leaderNodeID  string
	leaderAddress string
	leaderClient  pb.GraftTransportClient
	leaderConn    *grpc.ClientConn
	nodeAddress   string
	serverStarted chan struct{}
}

func NewGRPCTransport(logger *slog.Logger) *GRPCTransport {
	if logger == nil {
		logger = slog.Default()
	}

	return &GRPCTransport{
		logger:        logger.With("component", "transport", "adapter", "grpc"),
		serverStarted: make(chan struct{}),
	}
}

func (g *GRPCTransport) SetRaft(raft ports.RaftPort) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.raft = raft
}

func (g *GRPCTransport) Start(ctx context.Context, address string, port int) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.server != nil {
		return domain.NewTransportError("grpc", "start", domain.ErrAlreadyStarted)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	listenAddr := fmt.Sprintf("%s:%d", address, port)
	g.logger.Info("starting gRPC transport", "address", listenAddr)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return domain.NewTransportError("grpc", "listen", err)
	}

	g.listener = listener
	g.nodeAddress = listener.Addr().String()
	g.server = grpc.NewServer()

	pb.RegisterGraftTransportServer(g.server, g)

	go func() {
		close(g.serverStarted)
		if err := g.server.Serve(g.listener); err != nil && g.server != nil {
			g.logger.Error("grpc server failed", "error", err)
		}
	}()

	select {
	case <-ctx.Done():
		g.server.GracefulStop()
		return ctx.Err()
	case <-g.serverStarted:
	}

	g.logger.Info("gRPC transport started successfully", "address", g.nodeAddress)
	return nil
}

func (g *GRPCTransport) Stop() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.server == nil {
		return domain.NewTransportError("grpc", "stop", domain.ErrNotStarted)
	}

	g.logger.Info("stopping gRPC transport")

	if g.leaderConn != nil {
		g.leaderConn.Close()
		g.leaderConn = nil
		g.leaderClient = nil
	}

	g.server.GracefulStop()
	g.server = nil

	if g.listener != nil {
		g.listener.Close()
		g.listener = nil
	}

	g.leaderNodeID = ""
	g.leaderAddress = ""
	g.serverStarted = make(chan struct{})

	g.logger.Info("gRPC transport stopped")
	return nil
}

func (g *GRPCTransport) SendToLeader(ctx context.Context, message ports.Message) (*ports.Response, error) {
	g.mu.RLock()
	client := g.leaderClient
	g.mu.RUnlock()

	if client == nil {
		return nil, domain.NewLeaderError("send", domain.ErrConnection)
	}

	req := &pb.MessageRequest{
		Type:    g.convertMessageType(message.Type),
		From:    message.From,
		Payload: message.Payload,
	}

	resp, err := client.SendMessage(ctx, req)
	if err != nil {
		return nil, domain.NewLeaderError("send", err)
	}

	return &ports.Response{
		Success: resp.Success,
		Data:    resp.Data,
		Error:   resp.Error,
	}, nil
}

func (g *GRPCTransport) SetMessageHandler(handler ports.MessageHandler) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.handler = handler
}

func (g *GRPCTransport) UpdateLeader(nodeID string, address string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.leaderNodeID == nodeID && g.leaderAddress == address {
		return
	}

	g.logger.Info("updating leader", "old_leader", g.leaderNodeID, "new_leader", nodeID, "address", address)

	if g.leaderConn != nil {
		g.leaderConn.Close()
		g.leaderConn = nil
		g.leaderClient = nil
	}

	g.leaderNodeID = nodeID
	g.leaderAddress = address

	if address != "" {
		g.connectToLeader(address)
	}
}

func (g *GRPCTransport) GetAddress() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.nodeAddress
}

func (g *GRPCTransport) RequestJoinFromPeer(ctx context.Context, peerAddress string, nodeInfo ports.NodeInfo) error {
	conn, err := grpc.DialContext(ctx, peerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return domain.NewConnectionError(peerAddress, err)
	}
	defer conn.Close()

	client := pb.NewGraftTransportClient(conn)

	req := &pb.JoinRequest{
		NodeId:      nodeInfo.ID,
		RaftAddress: nodeInfo.Address,
		RaftPort:    int32(nodeInfo.Port),
	}

	resp, err := client.RequestJoin(ctx, req)
	if err != nil {
		return domain.NewTransportError("grpc", "request_join", err)
	}

	if !resp.Success {
		if resp.LeaderAddress != "" && resp.LeaderId != "" {
			return g.RequestJoinFromPeer(ctx, resp.LeaderAddress, nodeInfo)
		}
		return domain.NewTransportError("grpc", "request_join", fmt.Errorf(resp.Error))
	}

	g.logger.Info("successfully joined cluster", "peer", peerAddress, "node_id", nodeInfo.ID)
	return nil
}

func (g *GRPCTransport) connectToLeader(address string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		connErr := domain.NewConnectionError(address, err)
		g.logger.Error("failed to connect to leader", "address", address, "error", connErr)
		return
	}

	g.leaderConn = conn
	g.leaderClient = pb.NewGraftTransportClient(conn)

	g.logger.Info("connected to leader", "address", address)
}

func (g *GRPCTransport) SendMessage(ctx context.Context, req *pb.MessageRequest) (*pb.MessageResponse, error) {
	g.mu.RLock()
	handler := g.handler
	g.mu.RUnlock()

	if handler == nil {
		return &pb.MessageResponse{
			Success: false,
			Error:   "no message handler configured",
		}, nil
	}

	message := ports.Message{
		Type:    g.convertFromProtoMessageType(req.Type),
		From:    req.From,
		Payload: req.Payload,
	}

	resp, err := handler.HandleMessage(ctx, message)
	if err != nil {
		return &pb.MessageResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.MessageResponse{
		Success: resp.Success,
		Data:    resp.Data,
		Error:   resp.Error,
	}, nil
}

func (g *GRPCTransport) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return &pb.HealthCheckResponse{
		Healthy: true,
		NodeId:  req.NodeId,
	}, nil
}

func (g *GRPCTransport) GetLeaderAddress(ctx context.Context, req *pb.LeaderAddressRequest) (*pb.LeaderAddressResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	g.mu.RLock()
	raft := g.raft
	g.mu.RUnlock()

	if raft == nil {
		return &pb.LeaderAddressResponse{
			RaftAddress: "",
			LeaderId:    "",
			IsLeader:    false,
		}, nil
	}

	leaderID, leaderAddr := raft.GetLeader()
	isLeader := raft.IsLeader()

	return &pb.LeaderAddressResponse{
		RaftAddress: leaderAddr,
		LeaderId:    leaderID,
		IsLeader:    isLeader,
	}, nil
}

func (g *GRPCTransport) RequestJoin(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	g.mu.RLock()
	raft := g.raft
	g.mu.RUnlock()

	if raft == nil {
		return &pb.JoinResponse{
			Success: false,
			Error:   "raft not initialized",
		}, nil
	}

	if !raft.IsLeader() {
		leaderID, leaderAddr := raft.GetLeader()
		return &pb.JoinResponse{
			Success:       false,
			Error:         "not leader",
			LeaderId:      leaderID,
			LeaderAddress: leaderAddr,
		}, nil
	}

	nodeInfo := ports.Peer{
		ID:      req.NodeId,
		Address: req.RaftAddress,
		Port:    int(req.RaftPort),
	}

	err := raft.Join([]ports.Peer{nodeInfo})
	if err != nil {
		return &pb.JoinResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	g.logger.Info("node joined cluster", "node_id", req.NodeId, "address", req.RaftAddress)

	return &pb.JoinResponse{
		Success: true,
	}, nil
}

func (g *GRPCTransport) convertMessageType(msgType ports.MessageType) pb.MessageType {
	switch msgType {
	case ports.StorageRead:
		return pb.MessageType_STORAGE_READ
	case ports.StorageWrite:
		return pb.MessageType_STORAGE_WRITE
	case ports.StorageDelete:
		return pb.MessageType_STORAGE_DELETE
	case ports.HealthCheck:
		return pb.MessageType_HEALTH_CHECK
	default:
		return pb.MessageType_HEALTH_CHECK
	}
}

func (g *GRPCTransport) convertFromProtoMessageType(msgType pb.MessageType) ports.MessageType {
	switch msgType {
	case pb.MessageType_STORAGE_READ:
		return ports.StorageRead
	case pb.MessageType_STORAGE_WRITE:
		return ports.StorageWrite
	case pb.MessageType_STORAGE_DELETE:
		return ports.StorageDelete
	case pb.MessageType_HEALTH_CHECK:
		return ports.HealthCheck
	default:
		return ports.HealthCheck
	}
}
