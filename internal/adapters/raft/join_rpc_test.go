package raft

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	node, err := NewNode(config, storage, eventManager, nil)
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

	storage1, err := NewStorage("/tmp/rafttest-join-1", nil)
	assert.NoError(t, err)
	defer storage1.Close()

	storage2, err := NewStorage("/tmp/rafttest-join-2", nil)
	assert.NoError(t, err)
	defer storage2.Close()

	config1 := DefaultRaftConfig("node1", "cluster1", "127.0.0.1:0", "/tmp/rafttest-join-1", domain.ClusterPolicyStrict)
	node1, err := NewNode(config1, storage1, eventManager1, nil)
	assert.NoError(t, err)

	config2 := DefaultRaftConfig("node2", "cluster1", "127.0.0.1:0", "/tmp/rafttest-join-2", domain.ClusterPolicyStrict)
	node2, err := NewNode(config2, storage2, eventManager2, nil)
	assert.NoError(t, err)

	err = node1.Start(ctx, nil)
	assert.NoError(t, err)
	defer node1.Stop()

	time.Sleep(100 * time.Millisecond)
	assert.True(t, node1.IsLeader(), "Node1 should become leader")

	err = node2.Start(ctx, nil)
	assert.NoError(t, err)
	defer node2.Stop()

	leaderAddr := node1.GetLocalAddress()
	nodeID := config2.NodeID
	joinAddr := node2.GetLocalAddress()

	err = node2.JoinViaRPC(leaderAddr, nodeID, joinAddr)
	assert.NoError(t, err)
}
