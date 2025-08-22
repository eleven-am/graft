package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

type GRPCServer struct {
	pb.UnimplementedClusterServiceServer
	pb.UnimplementedRaftServiceServer
	pb.UnimplementedWorkflowServiceServer
	pb.UnimplementedStorageServiceServer

	logger       *slog.Logger
	server       *grpc.Server
	listener     net.Listener
	config       *ServerConfig
	mu           sync.RWMutex
	started      bool
	shutdownChan chan struct{}

	clusterHandler  ClusterHandler
	raftHandler     RaftHandler
	workflowHandler WorkflowHandler
	storageHandler  StorageHandler
}

type ServerConfig struct {
	BindAddress string
	BindPort    int
	TLS         *ports.TLSConfig
	MaxMsgSize  int
}

func NewGRPCServer(logger *slog.Logger, config *ServerConfig) *GRPCServer {
	return &GRPCServer{
		logger:       logger.With("component", "grpc-server"),
		config:       config,
		shutdownChan: make(chan struct{}),
	}
}

func (s *GRPCServer) SetHandlers(cluster ClusterHandler, raft RaftHandler, workflow WorkflowHandler) {
	s.clusterHandler = cluster
	s.raftHandler = raft
	s.workflowHandler = workflow
}

func (s *GRPCServer) SetStorageHandler(storage StorageHandler) {
	s.storageHandler = storage
}

func (s *GRPCServer) GetAddress() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return fmt.Sprintf("%s:%d", s.config.BindAddress, s.config.BindPort)
}

func (s *GRPCServer) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return domain.NewConflictError("server", "already started")
	}

	addr := fmt.Sprintf("%s:%d", s.config.BindAddress, s.config.BindPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.logger.Error("failed to listen", "error", err, "address", addr)
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to start gRPC server listener",
			Details: map[string]interface{}{
				"address": addr,
				"error":   err.Error(),
			},
		}
	}
	s.listener = listener

	var serverOpts []grpc.ServerOption

	serverOpts = append(serverOpts,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			s.loggingInterceptor,
			grpc_prometheus.UnaryServerInterceptor,
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			s.streamLoggingInterceptor,
			grpc_prometheus.StreamServerInterceptor,
		)),
	)

	if s.config.MaxMsgSize > 0 {
		serverOpts = append(serverOpts,
			grpc.MaxRecvMsgSize(s.config.MaxMsgSize),
			grpc.MaxSendMsgSize(s.config.MaxMsgSize),
		)
	}

	if s.config.TLS != nil && s.config.TLS.Enabled {
		creds, err := s.loadTLSCredentials()
		if err != nil {
			s.logger.Error("failed to load TLS credentials", "error", err)
			return domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to load TLS credentials for gRPC server",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	s.server = grpc.NewServer(serverOpts...)

	pb.RegisterClusterServiceServer(s.server, s)
	pb.RegisterRaftServiceServer(s.server, s)
	pb.RegisterWorkflowServiceServer(s.server, s)
	pb.RegisterStorageServiceServer(s.server, s)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s.server, healthServer)

	reflection.Register(s.server)

	grpc_prometheus.Register(s.server)

	s.started = true

	go func() {
		s.logger.Info("gRPC server starting", "address", addr)
		if err := s.server.Serve(s.listener); err != nil && err != grpc.ErrServerStopped {
			s.logger.Error("gRPC server failed", "error", err)
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
			s.Stop()
		case <-s.shutdownChan:
			return
		}
	}()

	s.logger.Info("gRPC server started successfully", "address", addr)
	return nil
}

func (s *GRPCServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	close(s.shutdownChan)

	s.logger.Info("stopping gRPC server")
	s.server.GracefulStop()
	s.started = false
	s.logger.Info("gRPC server stopped")

	return nil
}

func (s *GRPCServer) loadTLSCredentials() (credentials.TransportCredentials, error) {
	if s.config.TLS == nil || !s.config.TLS.Enabled {
		return nil, domain.NewConfigurationError("tls", "TLS credentials requested but not configured", "enable TLS configuration in server config")
	}

	creds, err := credentials.NewServerTLSFromFile(s.config.TLS.CertFile, s.config.TLS.KeyFile)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to load server TLS credentials from files",
			Details: map[string]interface{}{
				"cert_file": s.config.TLS.CertFile,
				"key_file":  s.config.TLS.KeyFile,
				"error":     err.Error(),
			},
		}
	}

	return creds, nil
}

func (s *GRPCServer) loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	s.logger.Debug("handling request", "method", info.FullMethod)
	resp, err := handler(ctx, req)
	if err != nil {
		s.logger.Error("request failed", "method", info.FullMethod, "error", err)
	}
	return resp, err
}

func (s *GRPCServer) streamLoggingInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	s.logger.Debug("handling stream", "method", info.FullMethod)
	err := handler(srv, ss)
	if err != nil {
		s.logger.Error("stream failed", "method", info.FullMethod, "error", err)
	}
	return err
}

func (s *GRPCServer) JoinCluster(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	if s.clusterHandler == nil {
		return nil, status.Error(codes.Unimplemented, "cluster handler not configured")
	}

	joinReq := &ports.JoinRequest{
		NodeID:   req.Node.Id,
		Address:  fmt.Sprintf("%s:%d", req.Node.Address, req.Node.Port),
		Metadata: req.Node.Metadata,
	}

	resp, err := s.clusterHandler.JoinCluster(ctx, joinReq)
	if err != nil {
		return &pb.JoinClusterResponse{
			Status: &pb.Status{
				Success: false,
				Error:   err.Error(),
			},
		}, nil
	}

	var leader *pb.NodeInfo
	if resp.Leader != "" {
		host, portStr, err := net.SplitHostPort(resp.Leader)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "invalid leader address format: %s (error: %v)", resp.Leader, err)
		}

		port, err := strconv.ParseInt(portStr, 10, 32)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "invalid leader port: %s", portStr)
		}

		leader = &pb.NodeInfo{
			Id:       resp.Leader,
			Address:  host,
			Port:     int32(port),
			Metadata: make(map[string]string),
		}
	}

	return &pb.JoinClusterResponse{
		Status: &pb.Status{
			Success: resp.Success,
			Error:   resp.Error,
		},
		Leader: leader,
	}, nil
}

func (s *GRPCServer) LeaveCluster(ctx context.Context, req *pb.LeaveClusterRequest) (*pb.LeaveClusterResponse, error) {
	if s.clusterHandler == nil {
		return nil, status.Error(codes.Unimplemented, "cluster handler not configured")
	}

	leaveReq := &ports.LeaveRequest{
		NodeID: req.NodeId,
	}

	resp, err := s.clusterHandler.LeaveCluster(ctx, leaveReq)
	if err != nil {
		return &pb.LeaveClusterResponse{
			Status: &pb.Status{
				Success: false,
				Error:   err.Error(),
			},
		}, nil
	}

	return &pb.LeaveClusterResponse{
		Status: &pb.Status{
			Success: resp.Success,
			Error:   resp.Error,
		},
	}, nil
}

func (s *GRPCServer) GetLeader(ctx context.Context, req *pb.Empty) (*pb.GetLeaderResponse, error) {
	if s.clusterHandler == nil {
		return nil, status.Error(codes.Unimplemented, "cluster handler not configured")
	}

	leader, err := s.clusterHandler.GetLeader(ctx)
	if err != nil {
		return &pb.GetLeaderResponse{
			Status: &pb.Status{
				Success: false,
				Error:   err.Error(),
			},
		}, nil
	}

	return &pb.GetLeaderResponse{
		Status: &pb.Status{
			Success: true,
		},
		Leader: &pb.NodeInfo{
			Id:      leader.NodeID,
			Address: leader.Address,
		},
	}, nil
}

func (s *GRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if s.raftHandler == nil {
		return nil, status.Error(codes.Unimplemented, "raft handler not configured")
	}
	return s.raftHandler.AppendEntries(ctx, req)
}

func (s *GRPCServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if s.raftHandler == nil {
		return nil, status.Error(codes.Unimplemented, "raft handler not configured")
	}
	return s.raftHandler.RequestVote(ctx, req)
}

func (s *GRPCServer) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if s.raftHandler == nil {
		return nil, status.Error(codes.Unimplemented, "raft handler not configured")
	}
	return s.raftHandler.InstallSnapshot(ctx, req)
}

func (s *GRPCServer) ProposeStateUpdate(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error) {
	if s.workflowHandler == nil {
		return nil, status.Error(codes.Unimplemented, "workflow handler not configured")
	}
	return s.workflowHandler.ProposeStateUpdate(ctx, req)
}

func (s *GRPCServer) GetWorkflowState(ctx context.Context, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error) {
	if s.workflowHandler == nil {
		return nil, status.Error(codes.Unimplemented, "workflow handler not configured")
	}
	return s.workflowHandler.GetWorkflowState(ctx, req)
}

func (s *GRPCServer) SubmitTrigger(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error) {
	if s.workflowHandler == nil {
		return nil, status.Error(codes.Unimplemented, "workflow handler not configured")
	}
	return s.workflowHandler.SubmitTrigger(ctx, req)
}

func (s *GRPCServer) ReadFromNode(ctx context.Context, req *pb.ReadFromNodeRequest) (*pb.ReadFromNodeResponse, error) {
	if s.storageHandler == nil {
		return nil, status.Error(codes.Unimplemented, "storage handler not configured")
	}
	return s.storageHandler.ReadFromNode(ctx, req)
}

func (s *GRPCServer) WriteToNode(ctx context.Context, req *pb.WriteToNodeRequest) (*pb.WriteToNodeResponse, error) {
	if s.storageHandler == nil {
		return nil, status.Error(codes.Unimplemented, "storage handler not configured")
	}
	return s.storageHandler.WriteToNode(ctx, req)
}

func (s *GRPCServer) DeleteFromNode(ctx context.Context, req *pb.DeleteFromNodeRequest) (*pb.DeleteFromNodeResponse, error) {
	if s.storageHandler == nil {
		return nil, status.Error(codes.Unimplemented, "storage handler not configured")
	}
	return s.storageHandler.DeleteFromNode(ctx, req)
}

func (s *GRPCServer) BatchOperation(ctx context.Context, req *pb.BatchOperationRequest) (*pb.BatchOperationResponse, error) {
	if s.storageHandler == nil {
		return nil, status.Error(codes.Unimplemented, "storage handler not configured")
	}
	return s.storageHandler.BatchOperation(ctx, req)
}