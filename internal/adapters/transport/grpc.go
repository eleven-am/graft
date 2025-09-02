package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	pb.UnimplementedGraftNodeServer
	logger *slog.Logger
	server *grpc.Server
	engine ports.EnginePort
	raft   ports.RaftNode

	address string
	port    int

	useTLS   bool
	certFile string
	keyFile  string
	caFile   string
}

func NewGRPCTransport(logger *slog.Logger) *GRPCTransport {
	if logger == nil {
		logger = slog.Default()
	}

	return &GRPCTransport{
		logger: logger.With("component", "transport", "adapter", "grpc"),
	}
}

func (t *GRPCTransport) ConfigureTransport(cfg domain.TransportConfig) {
	t.useTLS = cfg.EnableTLS
	t.certFile = cfg.TLSCertFile
	t.keyFile = cfg.TLSKeyFile
	t.caFile = cfg.TLSCAFile
}

func (t *GRPCTransport) Start(ctx context.Context, bindAddr string, port int) error {
	host, _, err := net.SplitHostPort(bindAddr)
	if err != nil {
		host = bindAddr
	}

	t.address = host
	t.port = port

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	var opts []grpc.ServerOption
	if t.useTLS {
		cert, err := tls.LoadX509KeyPair(t.certFile, t.keyFile)
		if err != nil {
			return fmt.Errorf("failed to load TLS keypair: %w", err)
		}
		tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
		if t.caFile != "" {
			caBytes, err := os.ReadFile(t.caFile)
			if err == nil {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(caBytes) {
					tlsCfg.ClientCAs = pool
					tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
				}
			}
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	t.server = grpc.NewServer(opts...)
	pb.RegisterGraftNodeServer(t.server, t)

	t.logger.Info("starting gRPC transport", "address", addr)

	go func() {
		if err := t.server.Serve(listener); err != nil {
			t.logger.Error("gRPC server error", "error", err)
		}
	}()

	return nil
}

func (t *GRPCTransport) Stop() error {
	t.logger.Info("stopping gRPC transport")

	if t.server != nil {
		t.server.GracefulStop()
	}

	return nil
}

func (t *GRPCTransport) RegisterEngine(engine ports.EnginePort) {
	t.engine = engine
}

func (t *GRPCTransport) RegisterRaft(raft ports.RaftNode) {
	t.raft = raft
}

func (t *GRPCTransport) ProcessTrigger(ctx context.Context, req *pb.TriggerRequest) (*pb.TriggerResponse, error) {
	if t.engine == nil {
		return &pb.TriggerResponse{
			Success: false,
			Message: "engine not registered",
		}, nil
	}

	if req.Trigger == nil {
		return &pb.TriggerResponse{
			Success: false,
			Message: "trigger is nil",
		}, nil
	}

	initialNodes := make([]domain.NodeConfig, len(req.Trigger.InitialNodes))
	for i, node := range req.Trigger.InitialNodes {
		initialNodes[i] = domain.NodeConfig{
			Name:   node.Name,
			Config: node.Config,
		}
	}

	trigger := domain.WorkflowTrigger{
		WorkflowID:   req.Trigger.WorkflowId,
		InitialNodes: initialNodes,
		InitialState: req.Trigger.InitialState,
		Metadata:     req.Trigger.Metadata,
	}

	err := t.engine.ProcessTrigger(trigger)
	if err != nil {
		t.logger.Error("failed to process trigger", "error", err, "workflow_id", trigger.WorkflowID)
		return &pb.TriggerResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.TriggerResponse{
		Success: true,
		Message: "trigger processed successfully",
	}, nil
}

func (t *GRPCTransport) RequestJoin(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	t.logger.Debug("received join request",
		"node_id", req.NodeId,
		"address", req.Address,
		"port", req.Port)

	if t.raft == nil {
		t.logger.Error("raft not registered, cannot process join request")
		return &pb.JoinResponse{
			Accepted: false,
			NodeId:   req.NodeId,
			Message:  "raft not available",
		}, nil
	}

	if !t.raft.IsLeader() {
		t.logger.Warn("not a leader, rejecting join request",
			"node_id", req.NodeId)
		return &pb.JoinResponse{
			Accepted: false,
			NodeId:   req.NodeId,
			Message:  "not a leader",
		}, nil
	}

	nodeAddress := fmt.Sprintf("%s:%d", req.Address, req.Port)
	err := t.raft.AddNode(req.NodeId, nodeAddress)
	if err != nil {
		t.logger.Error("failed to add node to raft cluster",
			"node_id", req.NodeId,
			"address", nodeAddress,
			"error", err)
		return &pb.JoinResponse{
			Accepted: false,
			NodeId:   req.NodeId,
			Message:  fmt.Sprintf("failed to add to cluster: %v", err),
		}, nil
	}

	t.logger.Debug("successfully added node to cluster",
		"node_id", req.NodeId,
		"address", nodeAddress)

	return &pb.JoinResponse{
		Accepted: true,
		NodeId:   req.NodeId,
		Message:  "successfully joined cluster",
	}, nil
}

func (t *GRPCTransport) SendTrigger(ctx context.Context, nodeAddr string, trigger domain.WorkflowTrigger) error {
	conn, err := t.getConnection(ctx, nodeAddr)
	if err != nil {
		return domain.ErrConnection
	}
	defer conn.Close()

	client := pb.NewGraftNodeClient(conn)

	pbNodes := make([]*pb.NodeConfig, len(trigger.InitialNodes))
	for i, node := range trigger.InitialNodes {
		var configBytes []byte
		if len(node.Config) > 0 {
			configBytes = node.Config
		}
		pbNodes[i] = &pb.NodeConfig{
			Name:   node.Name,
			Config: configBytes,
		}
	}

	var stateBytes []byte
	if len(trigger.InitialState) > 0 {
		stateBytes = trigger.InitialState
	}

	req := &pb.TriggerRequest{
		Trigger: &pb.WorkflowTrigger{
			WorkflowId:   trigger.WorkflowID,
			InitialNodes: pbNodes,
			InitialState: stateBytes,
			Metadata:     trigger.Metadata,
		},
	}

	resp, err := client.ProcessTrigger(ctx, req)
	if err != nil {
		return domain.ErrTimeout
	}

	if !resp.Success {
		return domain.ErrNotFound
	}

	return nil
}

func (t *GRPCTransport) SendJoinRequest(ctx context.Context, nodeAddr string, request *ports.JoinRequest) (*ports.JoinResponse, error) {
	conn, err := t.getConnection(ctx, nodeAddr)
	if err != nil {
		return nil, domain.ErrConnection
	}
	defer conn.Close()

	client := pb.NewGraftNodeClient(conn)

	req := &pb.JoinRequest{
		NodeId:   request.NodeID,
		Address:  request.Address,
		Port:     int32(request.Port),
		Metadata: request.Metadata,
	}

	resp, err := client.RequestJoin(ctx, req)
	if err != nil {
		return nil, domain.ErrTimeout
	}

	return &ports.JoinResponse{
		Accepted: resp.Accepted,
		NodeID:   resp.NodeId,
		Message:  resp.Message,
	}, nil
}

func (t *GRPCTransport) getConnection(ctx context.Context, nodeAddr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var dialOpts []grpc.DialOption
	if t.useTLS {
		var tlsCfg tls.Config
		if t.caFile != "" {
			if caBytes, err := os.ReadFile(t.caFile); err == nil {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(caBytes) {
					tlsCfg.RootCAs = pool
				}
			}
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tlsCfg)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, grpc.WithBlock())

	conn, err := grpc.DialContext(ctx, nodeAddr, dialOpts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
