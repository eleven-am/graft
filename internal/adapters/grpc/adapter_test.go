package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto/gen"
)

func TestGRPCTransport_Start(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	config := ports.TransportConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := transport.Start(ctx, config)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Stop()

	err = transport.Start(ctx, config)
	if err == nil {
		t.Error("Expected error when starting already started transport")
	}

	if _, ok := err.(domain.Error); !ok {
		t.Error("Expected domain.Error for duplicate start")
	}
}

func TestGRPCTransport_Stop(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	err := transport.Stop()
	if err != nil {
		t.Error("Expected no error when stopping non-started transport")
	}

	config := ports.TransportConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	ctx := context.Background()
	err = transport.Start(ctx, config)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}

	err = transport.Stop()
	if err != nil {
		t.Errorf("Failed to stop transport: %v", err)
	}

	err = transport.Stop()
	if err != nil {
		t.Error("Expected no error when stopping already stopped transport")
	}
}

func TestGRPCTransport_SetHandlers(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	handlers := &TransportHandlers{
		ClusterHandler:  &mockClusterHandler{},
		RaftHandler:     &mockRaftHandler{},
		WorkflowHandler: &mockWorkflowHandler{},
	}

	transport.SetHandlers(handlers)

	if transport.handlers != handlers {
		t.Error("Handlers not set correctly")
	}
}

func TestGRPCTransport_JoinCluster(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	config := ports.TransportConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	ctx := context.Background()
	err := transport.Start(ctx, config)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Stop()

	req := ports.JoinRequest{
		NodeID:  "node-1",
		Address: "127.0.0.1:8001",
	}

	_, err = transport.JoinCluster(ctx, req)
	if err == nil {
		t.Error("Expected error when no leader is available")
	}

	transport.UpdateLeader("leader-1", "127.0.0.1:8000")

	service := &testClusterService{
		joinFunc: func(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
			return &pb.JoinClusterResponse{
				Status: &pb.Status{Success: true},
			}, nil
		},
	}

	addr, cleanup := startTestServer(t, service)
	defer cleanup()

	transport.UpdateLeader("leader-1", addr)

	resp, err := transport.JoinCluster(ctx, req)
	if err != nil {
		t.Errorf("Failed to join cluster: %v", err)
	}

	if resp != nil && !resp.Success {
		t.Error("Expected successful join")
	}

	peers := transport.GetPeers()
	if _, exists := peers["node-1"]; !exists {
		t.Error("Peer not tracked after successful join")
	}
}

func TestGRPCTransport_LeaveCluster(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	config := ports.TransportConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	ctx := context.Background()
	err := transport.Start(ctx, config)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Stop()

	transport.trackPeer(&ports.PeerInfo{
		NodeID:    "node-1",
		Address:   "127.0.0.1:8001",
		IsHealthy: true,
	})

	req := ports.LeaveRequest{
		NodeID: "node-1",
	}

	_, err = transport.LeaveCluster(ctx, req)
	if err == nil {
		t.Error("Expected error when no leader is available")
	}

	service := &testClusterService{
		leaveFunc: func(ctx context.Context, req *pb.LeaveClusterRequest) (*pb.LeaveClusterResponse, error) {
			return &pb.LeaveClusterResponse{
				Status: &pb.Status{Success: true},
			}, nil
		},
	}

	addr, cleanup := startTestServer(t, service)
	defer cleanup()

	transport.UpdateLeader("leader-1", addr)

	resp, err := transport.LeaveCluster(ctx, req)
	if err != nil {
		t.Errorf("Failed to leave cluster: %v", err)
	}

	if resp != nil && !resp.Success {
		t.Error("Expected successful leave")
	}

	peers := transport.GetPeers()
	if _, exists := peers["node-1"]; exists {
		t.Error("Peer still tracked after successful leave")
	}
}

func TestGRPCTransport_GetLeader(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	ctx := context.Background()

	_, err := transport.GetLeader(ctx)
	if err == nil {
		t.Error("Expected error when no leader is set")
	}

	transport.UpdateLeader("leader-1", "127.0.0.1:8000")

	leader, err := transport.GetLeader(ctx)
	if err != nil {
		t.Errorf("Failed to get leader: %v", err)
	}

	if leader.NodeID != "leader-1" || leader.Address != "127.0.0.1:8000" {
		t.Error("Leader info not correct")
	}
}

func TestGRPCTransport_ForwardToLeader(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	config := ports.TransportConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	ctx := context.Background()
	err := transport.Start(ctx, config)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Stop()

	req := ports.ForwardRequest{
		Type:    "workflow_state_update",
		Payload: []byte(`{"workflow_id":"wf-1","action":"start"}`),
	}

	_, err = transport.ForwardToLeader(ctx, req)
	if err == nil {
		t.Error("Expected error when no leader is available")
	}

	workflowService := &testWorkflowService{
		proposeFunc: func(ctx context.Context, req *pb.ProposeStateUpdateRequest) (*pb.ProposeStateUpdateResponse, error) {
			return &pb.ProposeStateUpdateResponse{
				Status: &pb.Status{Success: true},
			}, nil
		},
	}

	addr, cleanup := startTestWorkflowServer(t, workflowService)
	defer cleanup()

	transport.UpdateLeader("leader-1", addr)

	resp, err := transport.ForwardToLeader(ctx, req)
	if err != nil {
		t.Errorf("Failed to forward workflow request: %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Expected successful response for workflow request")
	}

	triggerReq := ports.ForwardRequest{
		Type:    "workflow_trigger",
		Payload: []byte(`{"trigger_id":"trigger-1","event":"process"}`),
	}

	triggerService := &testWorkflowService{
		submitTriggerFunc: func(ctx context.Context, req *pb.SubmitTriggerRequest) (*pb.SubmitTriggerResponse, error) {
			return &pb.SubmitTriggerResponse{
				Status: &pb.Status{Success: true},
			}, nil
		},
	}

	addr2, cleanup2 := startTestWorkflowServer(t, triggerService)
	defer cleanup2()

	transport.UpdateLeader("leader-1", addr2)

	resp, err = transport.ForwardToLeader(ctx, triggerReq)
	if err != nil {
		t.Errorf("Failed to forward trigger request: %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Expected successful response for trigger request")
	}

	unknownReq := ports.ForwardRequest{
		Type:    "unknown_type",
		Payload: []byte{},
	}

	_, err = transport.ForwardToLeader(ctx, unknownReq)
	if err == nil {
		t.Error("Expected error for unknown request type")
	}
}

func TestGRPCTransport_PeerManagement(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	peer1 := &ports.PeerInfo{
		NodeID:    "node-1",
		Address:   "127.0.0.1:8001",
		IsHealthy: true,
	}

	peer2 := &ports.PeerInfo{
		NodeID:    "node-2",
		Address:   "127.0.0.1:8002",
		IsHealthy: true,
	}

	transport.trackPeer(peer1)
	transport.trackPeer(peer2)

	peers := transport.GetPeers()
	if len(peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(peers))
	}

	transport.removePeer("node-1")

	peers = transport.GetPeers()
	if len(peers) != 1 {
		t.Errorf("Expected 1 peer after removal, got %d", len(peers))
	}

	if _, exists := peers["node-1"]; exists {
		t.Error("node-1 should have been removed")
	}

	if _, exists := peers["node-2"]; !exists {
		t.Error("node-2 should still exist")
	}
}

func TestGRPCTransport_UpdateLeader(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	peer1 := &ports.PeerInfo{
		NodeID:    "node-1",
		Address:   "127.0.0.1:8001",
		IsHealthy: true,
		IsLeader:  false,
	}

	peer2 := &ports.PeerInfo{
		NodeID:    "node-2",
		Address:   "127.0.0.1:8002",
		IsHealthy: true,
		IsLeader:  false,
	}

	transport.trackPeer(peer1)
	transport.trackPeer(peer2)

	transport.UpdateLeader("node-1", "127.0.0.1:8001")

	peers := transport.GetPeers()
	if !peers["node-1"].IsLeader {
		t.Error("node-1 should be marked as leader")
	}

	if peers["node-2"].IsLeader {
		t.Error("node-2 should not be marked as leader")
	}

	leader, err := transport.GetLeader(context.Background())
	if err != nil {
		t.Errorf("Failed to get leader: %v", err)
	}

	if leader.NodeID != "node-1" {
		t.Error("Leader NodeID incorrect")
	}
}

func TestGRPCTransport_SendRaftMessage(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	config := ports.TransportConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	ctx := context.Background()
	err := transport.Start(ctx, config)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Stop()

	err = transport.SendRaftMessage(ctx, "unknown-node", &pb.AppendEntriesRequest{})
	if err == nil {
		t.Error("Expected error for unknown node")
	}

	transport.trackPeer(&ports.PeerInfo{
		NodeID:    "node-1",
		Address:   "127.0.0.1:8001",
		IsHealthy: false,
	})

	err = transport.SendRaftMessage(ctx, "node-1", &pb.AppendEntriesRequest{})
	if err == nil {
		t.Error("Expected error for unhealthy node")
	}

	raftService := &testRaftService{
		appendEntriesFunc: func(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
			return &pb.AppendEntriesResponse{Term: 1, Success: true}, nil
		},
	}

	addr, cleanup := startTestRaftServer(t, raftService)
	defer cleanup()

	transport.trackPeer(&ports.PeerInfo{
		NodeID:    "node-2",
		Address:   addr,
		IsHealthy: true,
	})

	err = transport.SendRaftMessage(ctx, "node-2", &pb.AppendEntriesRequest{Term: 1})
	if err != nil {
		t.Errorf("Failed to send AppendEntries: %v", err)
	}

	raftService2 := &testRaftService{
		requestVoteFunc: func(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
			return &pb.RequestVoteResponse{Term: 1, VoteGranted: true}, nil
		},
	}

	addr2, cleanup2 := startTestRaftServer(t, raftService2)
	defer cleanup2()

	transport.trackPeer(&ports.PeerInfo{
		NodeID:    "node-3",
		Address:   addr2,
		IsHealthy: true,
	})

	err = transport.SendRaftMessage(ctx, "node-3", &pb.RequestVoteRequest{Term: 1})
	if err != nil {
		t.Errorf("Failed to send RequestVote: %v", err)
	}

	raftService3 := &testRaftService{
		installSnapshotFunc: func(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
			return &pb.InstallSnapshotResponse{Term: 1, Success: true}, nil
		},
	}

	addr3, cleanup3 := startTestRaftServer(t, raftService3)
	defer cleanup3()

	transport.trackPeer(&ports.PeerInfo{
		NodeID:    "node-4",
		Address:   addr3,
		IsHealthy: true,
	})

	err = transport.SendRaftMessage(ctx, "node-4", &pb.InstallSnapshotRequest{Term: 1})
	if err != nil {
		t.Errorf("Failed to send InstallSnapshot: %v", err)
	}

	err = transport.SendRaftMessage(ctx, "node-2", "invalid-message-type")
	if err == nil {
		t.Error("Expected error for invalid message type")
	}
}

func TestGRPCTransport_ErrorHandling(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	ctx := context.Background()

	_, err := transport.JoinCluster(ctx, ports.JoinRequest{})
	if err == nil {
		t.Error("Expected error for empty request")
	}

	_, err = transport.LeaveCluster(ctx, ports.LeaveRequest{})
	if err == nil {
		t.Error("Expected error for empty request")
	}

	_, err = transport.ForwardToLeader(ctx, ports.ForwardRequest{})
	if err == nil {
		t.Error("Expected error for empty request")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	config := ports.TransportConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	_ = transport.Start(ctx, config)
}

func TestGRPCTransport_ConcurrentOperations(t *testing.T) {
	logger := slog.Default()
	transport := NewGRPCTransport(logger)

	config := ports.TransportConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	ctx := context.Background()
	err := transport.Start(ctx, config)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Stop()

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			peer := &ports.PeerInfo{
				NodeID:    fmt.Sprintf("node-%d", id),
				Address:   fmt.Sprintf("127.0.0.1:800%d", id),
				IsHealthy: true,
			}
			transport.trackPeer(peer)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	peers := transport.GetPeers()
	if len(peers) != 10 {
		t.Errorf("Expected 10 peers, got %d", len(peers))
	}

	for i := 0; i < 10; i++ {
		go func(id int) {
			nodeID := "node-" + fmt.Sprintf("%d", id)
			address := "127.0.0.1:800" + fmt.Sprintf("%d", id)
			transport.UpdateLeader(nodeID, address)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	leader, err := transport.GetLeader(ctx)
	if err != nil {
		t.Errorf("Failed to get leader after concurrent updates: %v", err)
	}
	if leader == nil || leader.NodeID == "" {
		t.Error("Expected valid leader after concurrent updates")
	}
}