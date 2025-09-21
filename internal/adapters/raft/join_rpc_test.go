package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/adapters/transport"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/stretchr/testify/assert"
)

func TestJoinViaTransportRPC(t *testing.T) {
	ctx := context.Background()
	eventManager := mocks.NewMockEventManager(t)

	storage, err := NewStorage("/tmp/rafttest-join-rpc", nil)
	assert.NoError(t, err)
	defer storage.Close()

	config := DefaultRaftConfig("node1", "cluster1", "127.0.0.1:0", "/tmp/rafttest-join-rpc", domain.ClusterPolicyStrict)
	node, err := NewNode(config, storage, eventManager, nil, nil)
	assert.NoError(t, err)

	err = node.Start(ctx, nil)
	assert.NoError(t, err)
	defer node.Stop()

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var isLeader bool
	for !isLeader {
		select {
		case <-timeout.C:
			t.Fatal("Timeout waiting for node to become leader")
		case <-ticker.C:
			isLeader = node.IsLeader()
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/join") {
			http.Error(w, "Not found", 404)
			return
		}

		var joinReq JoinRequest
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Bad request", 400)
			return
		}

		if err := json.Unmarshal(body, &joinReq); err != nil {
			http.Error(w, "Bad request", 400)
			return
		}

		resp := JoinResponse{
			Success: true,
			Message: "Node added successfully",
		}

		respBytes, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		w.Write(respBytes)
	}))
	defer server.Close()

	req := JoinRequest{
		NodeID:  "node2",
		Address: "127.0.0.1:9000",
	}

	resp, err := node.RequestJoin(server.URL, req)
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "Node added successfully", resp.Message)
}

func TestRaftJoinLogicFix(t *testing.T) {
	ctx := context.Background()

	eventManager1 := mocks.NewMockEventManager(t)
	eventManager2 := mocks.NewMockEventManager(t)

	tempDir1 := t.TempDir()
	tempDir2 := t.TempDir()

	storage1, err := NewStorage(tempDir1, nil)
	assert.NoError(t, err)
	defer storage1.Close()

	storage2, err := NewStorage(tempDir2, nil)
	assert.NoError(t, err)
	defer storage2.Close()

	addr1 := getAvailablePort(t)
	config1 := DefaultRaftConfig("node1", "cluster1", addr1, tempDir1, domain.ClusterPolicyStrict)

	addr2 := getAvailablePort(t)
	config2 := DefaultRaftConfig("node2", "cluster1", addr2, tempDir2, domain.ClusterPolicyStrict)

	transport1 := transport.NewGRPCTransport(nil, domain.TransportConfig{})
	transport2 := transport.NewGRPCTransport(nil, domain.TransportConfig{})

	node1, err := NewNode(config1, storage1, eventManager1, transport1, nil)
	assert.NoError(t, err)

	node2, err := NewNode(config2, storage2, eventManager2, transport2, nil)
	assert.NoError(t, err)

	transport1.RegisterRaft(node1)
	transport2.RegisterRaft(node2)

	grpcPort1 := 8001
	grpcPort2 := 8002

	err = transport1.Start(ctx, addr1, grpcPort1)
	assert.NoError(t, err)
	defer transport1.Stop()

	err = transport2.Start(ctx, addr2, grpcPort2)
	assert.NoError(t, err)
	defer transport2.Stop()

	err = node1.Start(ctx, nil)
	assert.NoError(t, err)
	defer node1.Stop()

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var isLeader bool
	for !isLeader {
		select {
		case <-timeout.C:
			t.Fatal("Timeout waiting for node1 to become leader")
		case <-ticker.C:
			isLeader = node1.IsLeader()
		}
	}

	err = node2.Start(ctx, nil)
	assert.NoError(t, err)
	defer node2.Stop()

	leaderAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort1)
	nodeID := config2.NodeID
	joinAddr := node2.GetLocalAddress()

	err = node2.JoinViaRPC(leaderAddr, nodeID, joinAddr)
	assert.NoError(t, err)
}
