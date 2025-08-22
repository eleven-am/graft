package raftimpl

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultRaftConfig(t *testing.T) {
	nodeID := "test-node"
	bindAddr := "127.0.0.1:8300"
	dataDir := "/tmp/test-data"

	config := DefaultRaftConfig(nodeID, bindAddr, dataDir)

	if config.NodeID != nodeID {
		t.Errorf("Expected NodeID %s, got %s", nodeID, config.NodeID)
	}

	if config.BindAddr != bindAddr {
		t.Errorf("Expected BindAddr %s, got %s", bindAddr, config.BindAddr)
	}

	expectedDataDir := filepath.Join(dataDir, nodeID)
	if config.DataDir != expectedDataDir {
		t.Errorf("Expected DataDir %s, got %s", expectedDataDir, config.DataDir)
	}


	if config.HeartbeatTimeout != 1000*time.Millisecond {
		t.Errorf("Expected HeartbeatTimeout 1s, got %v", config.HeartbeatTimeout)
	}

	if config.ElectionTimeout != 1000*time.Millisecond {
		t.Errorf("Expected ElectionTimeout 1s, got %v", config.ElectionTimeout)
	}

	if config.CommitTimeout != 50*time.Millisecond {
		t.Errorf("Expected CommitTimeout 50ms, got %v", config.CommitTimeout)
	}
}

func TestRaftNodeIsLeader(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	node := &RaftNode{
		logger: logger,
		raft:   nil,
	}

	if node.IsLeader() {
		t.Error("Expected false for nil raft")
	}
}

func TestRaftNodeLeaderAddr(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	node := &RaftNode{
		logger: logger,
		raft:   nil,
	}

	addr := node.LeaderAddr()
	if addr != "" {
		t.Errorf("Expected empty string for nil raft, got %s", addr)
	}
}

func TestRaftNodeRaft(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	node := &RaftNode{
		logger: logger,
		raft:   nil,
	}

	if node.Raft() != nil {
		t.Error("Expected nil raft")
	}
}

func TestRaftLoggerWrite(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	raftLogger := &raftLogger{logger: logger}

	testMessage := []byte("test log message")
	n, err := raftLogger.Write(testMessage)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if n != len(testMessage) {
		t.Errorf("Expected %d bytes written, got %d", len(testMessage), n)
	}
}

func TestNewRaftNodeBootstrap(t *testing.T) {
	t.Skip("Skipping integration test that requires full Raft setup")
}

func TestRaftNodeStats(t *testing.T) {
	t.Skip("Skipping integration test that requires full Raft setup")
}

func TestRaftNodeWaitForLeader(t *testing.T) {
	t.Skip("Skipping integration test that requires full Raft setup")
}

func TestRaftNodeApplyNotLeader(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	node := &RaftNode{
		logger: logger,
		raft:   nil,
		config: &RaftConfig{NodeID: "test-node"},
	}

	err := node.Apply([]byte("test"), time.Second)
	if err == nil {
		t.Error("Expected error when not leader")
	}
}

func TestRaftNodeTakeSnapshot(t *testing.T) {
	t.Skip("Skipping integration test that requires full Raft setup")
}

func TestRaftNodeAddVoter(t *testing.T) {
	t.Skip("Skipping integration test that requires full Raft setup")
}

func TestRaftNodeRemoveServer(t *testing.T) {
	t.Skip("Skipping integration test that requires full Raft setup")
}