package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

type GRPCClient struct {
	logger         *slog.Logger
	config         *ClientConfig
	mu             sync.RWMutex
	connections    map[string]*grpc.ClientConn
	circuitBreaker map[string]*CircuitBreaker
}

type ClientConfig struct {
	MaxMsgSize             int
	TLS                    *ports.TLSConfig
	ConnectTimeout         time.Duration
	RequestTimeout         time.Duration
	MaxRetries             int
	RetryBackoff           time.Duration
	PoolSize               int
	KeepAliveTime          time.Duration
	KeepAliveTimeout       time.Duration
	CircuitBreakerThreshold int
	CircuitBreakerTimeout  time.Duration
	GrpcBackoffBaseDelay   time.Duration
	GrpcBackoffMaxDelay    time.Duration
	GrpcBackoffMultiplier  float64
	GrpcBackoffJitter      float64
}

type CircuitBreaker struct {
	mu              sync.RWMutex
	failureCount    int
	lastFailureTime time.Time
	state           CircuitState
	threshold       int
	timeout         time.Duration
}

type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

func NewGRPCClient(logger *slog.Logger, config *ClientConfig) *GRPCClient {
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 5 * time.Second
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 10 * time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = 100 * time.Millisecond
	}
	if config.KeepAliveTime == 0 {
		config.KeepAliveTime = 30 * time.Second
	}
	if config.KeepAliveTimeout == 0 {
		config.KeepAliveTimeout = 10 * time.Second
	}
	if config.CircuitBreakerThreshold == 0 {
		config.CircuitBreakerThreshold = 5
	}
	if config.CircuitBreakerTimeout == 0 {
		config.CircuitBreakerTimeout = 30 * time.Second
	}
	if config.GrpcBackoffBaseDelay == 0 {
		config.GrpcBackoffBaseDelay = 100 * time.Millisecond
	}
	if config.GrpcBackoffMaxDelay == 0 {
		config.GrpcBackoffMaxDelay = 15 * time.Second
	}
	if config.GrpcBackoffMultiplier == 0 {
		config.GrpcBackoffMultiplier = 1.6
	}
	if config.GrpcBackoffJitter == 0 {
		config.GrpcBackoffJitter = 0.2
	}

	return &GRPCClient{
		logger:         logger.With("component", "grpc-client"),
		config:         config,
		connections:    make(map[string]*grpc.ClientConn),
		circuitBreaker: make(map[string]*CircuitBreaker),
	}
}

func (c *GRPCClient) getConnection(ctx context.Context, address string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	conn, exists := c.connections[address]
	c.mu.RUnlock()

	if exists && conn.GetState() != connectivity.Shutdown {
		return conn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	conn, exists = c.connections[address]
	if exists && conn.GetState() != connectivity.Shutdown {
		return conn, nil
	}

	if conn != nil {
		conn.Close()
	}

	newConn, err := c.createConnection(ctx, address)
	if err != nil {
		return nil, err
	}

	c.connections[address] = newConn
	c.circuitBreaker[address] = &CircuitBreaker{
		threshold: c.config.CircuitBreakerThreshold,
		timeout:   c.config.CircuitBreakerTimeout,
		state:     CircuitClosed,
	}

	return newConn, nil
}

func (c *GRPCClient) createConnection(ctx context.Context, address string) (*grpc.ClientConn, error) {
	var dialOpts []grpc.DialOption

	dialOpts = append(dialOpts,
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  c.config.GrpcBackoffBaseDelay,
				Multiplier: c.config.GrpcBackoffMultiplier,
				Jitter:     c.config.GrpcBackoffJitter,
				MaxDelay:   c.config.GrpcBackoffMaxDelay,
			},
			MinConnectTimeout: c.config.ConnectTimeout,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.config.KeepAliveTime,
			Timeout:             c.config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	)

	if c.config.MaxMsgSize > 0 {
		dialOpts = append(dialOpts,
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(c.config.MaxMsgSize),
				grpc.MaxCallSendMsgSize(c.config.MaxMsgSize),
			),
		)
	}

	if c.config.TLS != nil && c.config.TLS.Enabled {
		creds, err := c.loadTLSCredentials()
		if err != nil {
			c.logger.Error("failed to load TLS credentials", "error", err)
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to load TLS credentials for gRPC client connection",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	c.logger.Debug("creating connection", "address", address)
	conn, err := grpc.NewClient(address, dialOpts...)
	if err != nil {
		c.logger.Error("failed to create connection", "address", address, "error", err)
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create gRPC client connection",
			Details: map[string]interface{}{
				"address": address,
				"error":   err.Error(),
			},
		}
	}

	return conn, nil
}

func (c *GRPCClient) loadTLSCredentials() (credentials.TransportCredentials, error) {
	if c.config.TLS == nil || !c.config.TLS.Enabled {
		return nil, domain.NewConfigurationError("tls", "TLS credentials requested but not configured", "enable TLS configuration in client config")
	}

	creds, err := credentials.NewClientTLSFromFile(c.config.TLS.CAFile, "")
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to load client TLS credentials from CA file",
			Details: map[string]interface{}{
				"ca_file": c.config.TLS.CAFile,
				"error":   err.Error(),
			},
		}
	}

	return creds, nil
}

func (c *GRPCClient) checkCircuitBreaker(address string) error {
	c.mu.RLock()
	cb, exists := c.circuitBreaker[address]
	c.mu.RUnlock()

	if !exists {
		return nil
	}

	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case CircuitOpen:
		if time.Since(cb.lastFailureTime) > cb.timeout {
			cb.mu.RUnlock()
			cb.mu.Lock()
			cb.state = CircuitHalfOpen
			cb.mu.Unlock()
			cb.mu.RLock()
			return nil
		}
		return domain.Error{
			Type:    domain.ErrorTypeUnavailable,
			Message: fmt.Sprintf("circuit breaker open for %s", address),
		}
	case CircuitHalfOpen, CircuitClosed:
		return nil
	default:
		return nil
	}
}

func (c *GRPCClient) recordSuccess(address string) {
	c.mu.RLock()
	cb, exists := c.circuitBreaker[address]
	c.mu.RUnlock()

	if !exists {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount = 0
	cb.state = CircuitClosed
}

func (c *GRPCClient) recordFailure(address string) {
	c.mu.RLock()
	cb, exists := c.circuitBreaker[address]
	c.mu.RUnlock()

	if !exists {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	cb.lastFailureTime = time.Now()

	if cb.failureCount >= cb.threshold {
		cb.state = CircuitOpen
	}
}

func (c *GRPCClient) JoinCluster(ctx context.Context, address string, req *ports.JoinRequest) (*ports.JoinResponse, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewClusterServiceClient(conn)

	pbReq := &pb.JoinClusterRequest{
		Node: &pb.NodeInfo{
			Id:       req.NodeID,
			Address:  req.Address,
			Metadata: req.Metadata,
		},
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.JoinCluster(reqCtx, pbReq)
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("JoinCluster failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)

	result := &ports.JoinResponse{
		Success: resp.Status.Success,
		Error:   resp.Status.Error,
	}

	if resp.Leader != nil {
		result.Leader = resp.Leader.Address
	}

	return result, nil
}

func (c *GRPCClient) LeaveCluster(ctx context.Context, address string, req *ports.LeaveRequest) (*ports.LeaveResponse, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewClusterServiceClient(conn)

	pbReq := &pb.LeaveClusterRequest{
		NodeId: req.NodeID,
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.LeaveCluster(reqCtx, pbReq)
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("LeaveCluster failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)

	return &ports.LeaveResponse{
		Success: resp.Status.Success,
		Error:   resp.Status.Error,
	}, nil
}

func (c *GRPCClient) GetLeader(ctx context.Context, address string) (*ports.LeaderInfo, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewClusterServiceClient(conn)

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.GetLeader(reqCtx, &pb.Empty{})
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("GetLeader failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)

	if !resp.Status.Success {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "GetLeader request failed",
			Details: map[string]interface{}{
				"address": address,
				"error":   resp.Status.Error,
			},
		}
	}

	if resp.Leader == nil {
		return nil, domain.NewNotFoundError("leader", "")
	}

	return &ports.LeaderInfo{
		NodeID:  resp.Leader.Id,
		Address: resp.Leader.Address,
	}, nil
}

func (c *GRPCClient) AppendEntries(ctx context.Context, address string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.AppendEntries(reqCtx, req)
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("AppendEntries failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)
	return resp, nil
}

func (c *GRPCClient) RequestVote(ctx context.Context, address string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.RequestVote(reqCtx, req)
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("RequestVote failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)
	return resp, nil
}

func (c *GRPCClient) InstallSnapshot(ctx context.Context, address string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.InstallSnapshot(reqCtx, req)
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("InstallSnapshot failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)
	return resp, nil
}

func (c *GRPCClient) ProposeStateUpdate(ctx context.Context, address string, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewWorkflowServiceClient(conn)

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.ProposeStateUpdate(reqCtx, req)
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("ProposeStateUpdate failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)
	return resp, nil
}

func (c *GRPCClient) GetWorkflowState(ctx context.Context, address string, req *pb.GetWorkflowStateRequest) (*pb.GetWorkflowStateResponse, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewWorkflowServiceClient(conn)

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.GetWorkflowState(reqCtx, req)
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("GetWorkflowState failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)
	return resp, nil
}

func (c *GRPCClient) SubmitTrigger(ctx context.Context, address string, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewWorkflowServiceClient(conn)

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.SubmitTrigger(reqCtx, req)
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("SubmitTrigger failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)
	return resp, nil
}

func (c *GRPCClient) ReadFromNode(ctx context.Context, address string, req *pb.ReadFromNodeRequest) (*pb.ReadFromNodeResponse, error) {
	if err := c.checkCircuitBreaker(address); err != nil {
		return nil, err
	}

	conn, err := c.getConnection(ctx, address)
	if err != nil {
		c.recordFailure(address)
		return nil, err
	}

	client := pb.NewStorageServiceClient(conn)

	reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	resp, err := client.ReadFromNode(reqCtx, req)
	if err != nil {
		c.recordFailure(address)
		c.logger.Error("ReadFromNode failed", "address", address, "error", err)
		return nil, err
	}

	c.recordSuccess(address)
	return resp, nil
}

func (c *GRPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for address, conn := range c.connections {
		if err := conn.Close(); err != nil {
			c.logger.Error("failed to close connection", "address", address, "error", err)
		}
	}

	c.connections = make(map[string]*grpc.ClientConn)
	c.circuitBreaker = make(map[string]*CircuitBreaker)

	return nil
}