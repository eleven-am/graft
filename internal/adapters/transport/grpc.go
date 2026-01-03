package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
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
	engine ports.EnginePort
	raft   ports.RaftNode

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

func (t *GRPCTransport) RegisterEngine(engine ports.EnginePort) {
	t.engine = engine
}

func (t *GRPCTransport) RegisterRaft(raft ports.RaftNode) {
	t.raft = raft
}

func (t *GRPCTransport) RegisterWithServer(server grpc.ServiceRegistrar) {
	pb.RegisterGraftNodeServer(server, t)
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
		leaderAddr := t.raft.LeaderAddr()
		if leaderAddr == "" {
			return &pb.JoinResponse{
				Accepted: false,
				NodeId:   req.NodeId,
				Message:  "no leader available",
			}, nil
		}

		t.logger.Debug("proxying join request to leader",
			"node_id", req.NodeId,
			"leader_addr", leaderAddr)

		return t.proxyJoinToLeader(ctx, leaderAddr, req)
	}

	t.logger.Debug("acknowledged join request - membership handled by gossip",
		"node_id", req.NodeId,
		"address", fmt.Sprintf("%s:%d", req.Address, req.Port))

	return &pb.JoinResponse{
		Accepted: true,
		NodeId:   req.NodeId,
		Message:  "join acknowledged - membership handled by gossip layer",
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

			dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
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

func (t *GRPCTransport) GetLeaderInfo(ctx context.Context, peerAddr string) (leaderID, leaderAddr string, err error) {
	conn, err := t.getConnection(ctx, peerAddr)
	if err != nil {
		return "", "", domain.ErrConnection
	}
	defer conn.Close()

	client := pb.NewGraftNodeClient(conn)

	noop := domain.Command{Type: domain.CommandTypeNoop}
	cmdBytes, err := noop.Marshal()
	if err != nil {
		return "", "", err
	}

	req := &pb.ApplyRequest{Command: cmdBytes}
	resp, err := client.ApplyCommand(ctx, req)
	if err != nil {
		return "", "", domain.ErrTimeout
	}

	if resp.LeaderId != "" || resp.LeaderAddr != "" {
		return resp.LeaderId, resp.LeaderAddr, nil
	}

	if resp.Success {
		return "", peerAddr, nil
	}

	return "", "", nil
}

func (t *GRPCTransport) RequestAddVoter(ctx context.Context, leaderAddr, nodeID, nodeAddr string) error {
	host, portStr, err := net.SplitHostPort(nodeAddr)
	if err != nil {
		return fmt.Errorf("invalid node address %q: %w", nodeAddr, err)
	}

	var port int
	_, err = fmt.Sscanf(portStr, "%d", &port)
	if err != nil {
		return fmt.Errorf("invalid port in address %q: %w", nodeAddr, err)
	}

	joinReq := &ports.JoinRequest{
		NodeID:  nodeID,
		Address: host,
		Port:    port,
	}

	resp, err := t.SendJoinRequest(ctx, leaderAddr, joinReq)
	if err != nil {
		return err
	}

	if !resp.Accepted {
		return fmt.Errorf("add voter rejected: %s", resp.Message)
	}

	return nil
}

func (t *GRPCTransport) proxyJoinToLeader(ctx context.Context, leaderAddr string, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	conn, err := t.getConnection(ctx, leaderAddr)
	if err != nil {
		return &pb.JoinResponse{
			Accepted: false,
			NodeId:   req.NodeId,
			Message:  fmt.Sprintf("failed to connect to leader: %v", err),
		}, nil
	}
	defer conn.Close()

	client := pb.NewGraftNodeClient(conn)
	return client.RequestJoin(ctx, req)
}
