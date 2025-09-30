package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/eleven-am/graft/internal/helpers/netutil"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto"
	"google.golang.org/grpc"
)

type GRPCTransport struct {
	pb.UnimplementedGraftNodeServer
	logger   *slog.Logger
	server   *grpc.Server
	engine   ports.EnginePort
	raft     ports.RaftNode
	loadSink ports.LoadSink

	address string
	port    int

	cfg domain.TransportConfig
}

func NewGRPCTransport(logger *slog.Logger, cfg domain.TransportConfig) *GRPCTransport {
	if logger == nil {
		logger = slog.Default()
	}

	return &GRPCTransport{
		logger: logger.With("component", "transport", "adapter", "grpc"),
		cfg:    cfg,
	}
}

func (t *GRPCTransport) Start(ctx context.Context, bindAddr string, port int) error {
	host, _, err := net.SplitHostPort(bindAddr)
	if err != nil {
		host = bindAddr
	}

	t.address = host

	listener, actualPort, err := netutil.ListenTCP(host, port)
	if err != nil {
		return err
	}
	t.port = actualPort

	serverOpts := []grpc.ServerOption{}
	if t.cfg.MaxMessageSizeMB > 0 {
		bytes := t.cfg.MaxMessageSizeMB * 1024 * 1024
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(bytes), grpc.MaxSendMsgSize(bytes))
	}
	if t.cfg.EnableTLS && t.cfg.TLSCertFile != "" && t.cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.cfg.TLSCertFile, t.cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("failed to load tls certs: %w", err)
		}
		tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
		if t.cfg.TLSCAFile != "" {
			caPem, err := os.ReadFile(t.cfg.TLSCAFile)
			if err == nil {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(caPem) {
					tlsCfg.ClientCAs = pool
					tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
				}
			}
		}
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}

	t.server = grpc.NewServer(serverOpts...)
	pb.RegisterGraftNodeServer(t.server, t)

	actualAddr := listener.Addr().String()
	t.logger.Info("starting gRPC transport", "address", actualAddr)

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

func (t *GRPCTransport) RegisterLoadSink(sink ports.LoadSink) {
	t.loadSink = sink
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
	timeout := 5 * time.Second
	if t.cfg.ConnectionTimeout > 0 {
		timeout = t.cfg.ConnectionTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dialOpts := []grpc.DialOption{grpc.WithBlock()}
	if t.cfg.EnableTLS {
		if t.cfg.TLSCAFile != "" {
			creds, err := credentials.NewClientTLSFromFile(t.cfg.TLSCAFile, "")
			if err != nil {
				return nil, err
			}
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
		} else {
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
		}
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(ctx, nodeAddr, dialOpts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (t *GRPCTransport) ApplyCommand(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
	if t.raft == nil {
		return &pb.ApplyResponse{
			Success: false,
			Error:   "raft not available",
		}, nil
	}

	if !t.raft.IsLeader() {
		leaderAddr := t.raft.LeaderAddr()
		clusterInfo := t.raft.GetClusterInfo()
		leaderID := ""
		if clusterInfo.Leader != nil {
			leaderID = clusterInfo.Leader.ID
		}

		return &pb.ApplyResponse{
			Success:    false,
			LeaderAddr: leaderAddr,
			LeaderId:   leaderID,
		}, nil
	}

	cmd, err := domain.UnmarshalCommand(req.Command)
	if err != nil {
		return &pb.ApplyResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	timeout := 5 * time.Second
	if t.cfg.ConnectionTimeout > 0 {
		timeout = t.cfg.ConnectionTimeout
	}

	res, err := t.raft.Apply(*cmd, timeout)
	if err != nil {
		return &pb.ApplyResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	pbEvents := make([]*pb.Event, len(res.Events))
	for i, event := range res.Events {
		pbEvents[i] = &pb.Event{
			Type:      uint32(event.Type),
			Key:       event.Key,
			Version:   event.Version,
			NodeId:    event.NodeID,
			Timestamp: event.Timestamp.Unix(),
			RequestId: event.RequestID,
		}
	}

	return &pb.ApplyResponse{
		Success: res.Success,
		Error:   res.Error,
		Version: res.Version,
		Events:  pbEvents,
	}, nil
}

func (t *GRPCTransport) PublishLoad(ctx context.Context, req *pb.LoadUpdate) (*pb.Ack, error) {
	if req.NodeId == "" {
		return &pb.Ack{Ok: false, Message: "node_id is required"}, nil
	}

	if t.loadSink == nil {
		return &pb.Ack{Ok: false, Message: "no sink registered"}, nil
	}

	pressure := clampFloat(req.Pressure, 0.0, 2.0)
	if pressure == 0 {

		pressure = clampFloat(req.Capacity, 0.0, 2.0)
	}

	update := ports.LoadUpdate{
		NodeID:          req.NodeId,
		ActiveWorkflows: int(req.ActiveWorkflows),
		TotalWeight:     req.TotalWeight,
		RecentLatencyMs: req.RecentLatencyMs,
		RecentErrorRate: clampFloat(req.RecentErrorRate, 0.0, 1.0),
		Pressure:        pressure,
		Timestamp:       req.Timestamp,
	}

	if update.TotalWeight < 0 {
		update.TotalWeight = 0
	}
	if update.RecentLatencyMs < 0 {
		update.RecentLatencyMs = 0
	}

	err := t.loadSink.ReceiveLoadUpdate(update)
	if err != nil {
		t.logger.Error("failed to process load update",
			"node_id", req.NodeId,
			"error", err)
		return &pb.Ack{Ok: false, Message: err.Error()}, nil
	}

	return &pb.Ack{Ok: true}, nil
}

func (t *GRPCTransport) SendApplyCommand(ctx context.Context, nodeAddr string, cmd *domain.Command) (*domain.CommandResult, string, error) {
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return nil, "", domain.ErrInvalidInput
	}

	conn, err := t.getConnection(ctx, nodeAddr)
	if err != nil {
		return nil, "", domain.ErrConnection
	}
	defer conn.Close()

	client := pb.NewGraftNodeClient(conn)
	req := &pb.ApplyRequest{
		Command: cmdBytes,
	}

	resp, err := client.ApplyCommand(ctx, req)
	if err != nil {
		return nil, "", domain.ErrTimeout
	}

	if resp.Success {
		events := make([]domain.Event, len(resp.Events))
		for i, pbEvent := range resp.Events {
			events[i] = domain.Event{
				Type:      domain.EventType(pbEvent.Type),
				Key:       pbEvent.Key,
				Version:   pbEvent.Version,
				NodeID:    pbEvent.NodeId,
				Timestamp: time.Unix(pbEvent.Timestamp, 0),
				RequestID: pbEvent.RequestId,
			}
		}

		result := &domain.CommandResult{
			Success: resp.Success,
			Error:   resp.Error,
			Version: resp.Version,
			Events:  events,
		}
		return result, "", nil
	}

	if resp.LeaderAddr != "" {
		return nil, resp.LeaderAddr, nil
	}

	return nil, "", domain.ErrConnection
}

func (t *GRPCTransport) SendPublishLoad(ctx context.Context, nodeAddr string, update ports.LoadUpdate) error {
	conn, err := t.getConnection(ctx, nodeAddr)
	if err != nil {
		return domain.ErrConnection
	}
	defer conn.Close()

	client := pb.NewGraftNodeClient(conn)

	req := &pb.LoadUpdate{
		NodeId:          update.NodeID,
		ActiveWorkflows: int32(update.ActiveWorkflows),
		TotalWeight:     update.TotalWeight,
		RecentLatencyMs: update.RecentLatencyMs,
		RecentErrorRate: update.RecentErrorRate,
		Pressure:        update.Pressure,

		Capacity:  update.Pressure,
		Timestamp: update.Timestamp,
	}

	timeoutCtx := ctx
	if t.cfg.ConnectionTimeout > 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, t.cfg.ConnectionTimeout)
		defer cancel()
	} else {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
	}

	resp, err := client.PublishLoad(timeoutCtx, req)
	if err != nil {
		return domain.ErrTimeout
	}

	if !resp.Ok {
		t.logger.Debug("load update rejected",
			"node_addr", nodeAddr,
			"message", resp.Message)
		return fmt.Errorf("load update rejected: %s", resp.Message)
	}

	return nil
}

func clampFloat(value, min, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}
