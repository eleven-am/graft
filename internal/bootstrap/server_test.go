package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"

	pb "github.com/eleven-am/graft/internal/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type mockRaftStateProvider struct {
	lastIndex uint64
}

func (m *mockRaftStateProvider) LastIndex() uint64 {
	return m.lastIndex
}

type mockAuthInfo struct {
	credentials.TLSInfo
}

func createTestContext(t *testing.T) context.Context {
	t.Helper()
	tlsInfo := credentials.TLSInfo{
		State: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				{
					DNSNames:    []string{"localhost"},
					IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
				},
			},
		},
	}
	p := &peer.Peer{
		Addr:     &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080},
		AuthInfo: tlsInfo,
	}
	return peer.NewContext(context.Background(), p)
}

func newTestBootstrapServer(t *testing.T) (*BootstrapServer, *FencingManager) {
	return newTestBootstrapServerWithState(t, StateReady)
}

func newTestBootstrapServerWithState(t *testing.T, state NodeState) (*BootstrapServer, *FencingManager) {
	t.Helper()

	tmpDir := t.TempDir()

	secrets := NewSecretsManager(SecretsConfig{}, nil)
	fencing := NewFencingManager(nil, nil, nil, secrets, nil)

	localMeta := &ClusterMeta{
		Version:       1,
		ClusterUUID:   "test-cluster-uuid",
		ServerID:      "node-0",
		ServerAddress: "localhost:8080",
		Ordinal:       0,
		State:         state,
		BootstrapTime: time.Now(),
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}

	server := NewBootstrapServer(BootstrapServerDeps{
		Config: &BootstrapConfig{
			DataDir:       tmpDir,
			ExpectedNodes: 3,
		},
		Fencing:      fencing,
		Secrets:      secrets,
		LocalMeta:    func() *ClusterMeta { return localMeta },
		LocalRaft:    &mockRaftStateProvider{lastIndex: 100},
		UsageTracker: nil,
		TLSConfig:    tlsConfig,
	})

	return server, fencing
}

func TestBootstrapServer_GetClusterMeta(t *testing.T) {
	server, _ := newTestBootstrapServer(t)
	ctx := createTestContext(t)

	resp, err := server.GetClusterMeta(ctx, &pb.GetClusterMetaRequest{})
	if err != nil {
		t.Fatalf("GetClusterMeta() error = %v", err)
	}

	if resp.ClusterUuid != "test-cluster-uuid" {
		t.Errorf("ClusterUuid = %s, want test-cluster-uuid", resp.ClusterUuid)
	}

	if resp.ServerId != "node-0" {
		t.Errorf("ServerId = %s, want node-0", resp.ServerId)
	}

	if resp.Ordinal != 0 {
		t.Errorf("Ordinal = %d, want 0", resp.Ordinal)
	}
}

func TestBootstrapServer_GetClusterMeta_NoMeta(t *testing.T) {
	tmpDir := t.TempDir()
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert}

	server := NewBootstrapServer(BootstrapServerDeps{
		Config: &BootstrapConfig{
			DataDir:       tmpDir,
			ExpectedNodes: 3,
		},
		Fencing:   NewFencingManager(nil, nil, nil, secrets, nil),
		LocalMeta: func() *ClusterMeta { return nil },
		TLSConfig: tlsConfig,
	})

	ctx := createTestContext(t)
	_, err := server.GetClusterMeta(ctx, &pb.GetClusterMetaRequest{})
	if err != ErrMetaNotFound {
		t.Errorf("GetClusterMeta() error = %v, want %v", err, ErrMetaNotFound)
	}
}

func TestBootstrapServer_GetFencingEpoch(t *testing.T) {
	server, fencing := newTestBootstrapServer(t)
	ctx := createTestContext(t)

	fencing.mu.Lock()
	fencing.localEpoch = 5
	fencing.mu.Unlock()

	resp, err := server.GetFencingEpoch(ctx, &pb.GetFencingEpochRequest{})
	if err != nil {
		t.Fatalf("GetFencingEpoch() error = %v", err)
	}

	if resp.Epoch != 5 {
		t.Errorf("Epoch = %d, want 5", resp.Epoch)
	}
}

func TestBootstrapServer_GetLastIndex(t *testing.T) {
	server, _ := newTestBootstrapServer(t)
	ctx := createTestContext(t)

	resp, err := server.GetLastIndex(ctx, &pb.GetLastIndexRequest{})
	if err != nil {
		t.Fatalf("GetLastIndex() error = %v", err)
	}

	if resp.LastIndex != 100 {
		t.Errorf("LastIndex = %d, want 100", resp.LastIndex)
	}
}

func TestBootstrapServer_GetLastIndex_NoRaft(t *testing.T) {
	tmpDir := t.TempDir()
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert}

	server := NewBootstrapServer(BootstrapServerDeps{
		Config: &BootstrapConfig{
			DataDir:       tmpDir,
			ExpectedNodes: 3,
		},
		Fencing:   NewFencingManager(nil, nil, nil, secrets, nil),
		LocalMeta: func() *ClusterMeta { return nil },
		LocalRaft: nil,
		TLSConfig: tlsConfig,
	})

	ctx := createTestContext(t)
	resp, err := server.GetLastIndex(ctx, &pb.GetLastIndexRequest{})
	if err != nil {
		t.Fatalf("GetLastIndex() error = %v", err)
	}

	if resp.LastIndex != 0 {
		t.Errorf("LastIndex = %d, want 0", resp.LastIndex)
	}
}

func TestBootstrapServer_RequestVote_GrantVote(t *testing.T) {
	server, _ := newTestBootstrapServerWithState(t, StateBootstrapping)
	ctx := createTestContext(t)

	req := &pb.VoteRequestProto{
		CandidateId:      "node-0",
		CandidateOrdinal: 0,
		ElectionReason:   "test",
		Timestamp:        time.Now().UnixNano(),
		VoterSetHash:     []byte("hash"),
		RequiredQuorum:   2,
	}

	resp, err := server.RequestVote(ctx, req)
	if err != nil {
		t.Fatalf("RequestVote() error = %v", err)
	}

	if !resp.VoteGranted {
		t.Errorf("VoteGranted = %v, want true; reason = %s", resp.VoteGranted, resp.Reason)
	}
}

func TestBootstrapServer_RequestVote_RejectAlreadyInCluster(t *testing.T) {
	server, _ := newTestBootstrapServerWithState(t, StateReady)
	ctx := createTestContext(t)

	req := &pb.VoteRequestProto{
		CandidateId:      "node-0",
		CandidateOrdinal: 0,
		ElectionReason:   "test",
		Timestamp:        time.Now().UnixNano(),
		VoterSetHash:     []byte("hash"),
		RequiredQuorum:   2,
	}

	resp, err := server.RequestVote(ctx, req)
	if err != nil {
		t.Fatalf("RequestVote() error = %v", err)
	}

	if resp.VoteGranted {
		t.Errorf("VoteGranted = %v, want false (node in StateReady should reject)", resp.VoteGranted)
	}

	if resp.Reason != "already in cluster" {
		t.Errorf("Reason = %s, want 'already in cluster'", resp.Reason)
	}
}

func TestBootstrapServer_NotifyTokenUsage(t *testing.T) {
	server, _ := newTestBootstrapServer(t)
	ctx := createTestContext(t)

	req := &pb.TokenUsageNotificationProto{
		Version:   1,
		TokenHash: "test-hash",
		UsedAt:    time.Now().UnixNano(),
		UsedBy:    "node-0",
		NewEpoch:  5,
		Timestamp: time.Now().UnixNano(),
		SenderId:  "node-1",
	}

	resp, err := server.NotifyTokenUsage(ctx, req)
	if err != nil {
		t.Fatalf("NotifyTokenUsage() error = %v", err)
	}

	if !resp.Acknowledged {
		t.Errorf("Acknowledged = %v, want true", resp.Acknowledged)
	}
}

func TestBootstrapServer_GetTokenUsage_NoTracker(t *testing.T) {
	server, _ := newTestBootstrapServer(t)
	ctx := createTestContext(t)

	req := &pb.GetTokenUsageRequest{TokenHash: "test-hash"}

	resp, err := server.GetTokenUsage(ctx, req)
	if err != nil {
		t.Fatalf("GetTokenUsage() error = %v", err)
	}

	if resp.Used {
		t.Errorf("Used = %v, want false", resp.Used)
	}

	if resp.ResponderId != "node-0" {
		t.Errorf("ResponderId = %s, want node-0", resp.ResponderId)
	}
}

func TestBootstrapServer_GetTokenUsage_Signed(t *testing.T) {
	tmpDir := t.TempDir()

	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetFencingKey([]byte("test-fencing-key-32-bytes-long!!"))
	fencing := NewFencingManager(nil, nil, nil, secrets, nil)

	localMeta := &ClusterMeta{
		Version:       1,
		ClusterUUID:   "test-cluster-uuid",
		ServerID:      "node-0",
		ServerAddress: "localhost:8080",
		Ordinal:       0,
		State:         StateReady,
		BootstrapTime: time.Now(),
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert}

	server := NewBootstrapServer(BootstrapServerDeps{
		Config: &BootstrapConfig{
			DataDir:       tmpDir,
			ExpectedNodes: 3,
		},
		Fencing:   fencing,
		Secrets:   secrets,
		LocalMeta: func() *ClusterMeta { return localMeta },
		TLSConfig: tlsConfig,
	})

	ctx := createTestContext(t)
	req := &pb.GetTokenUsageRequest{TokenHash: "test-hash"}

	resp, err := server.GetTokenUsage(ctx, req)
	if err != nil {
		t.Fatalf("GetTokenUsage() error = %v", err)
	}

	if resp.Signature == "" {
		t.Error("Signature should not be empty when fencing key is configured")
	}

	if resp.ResponderId != "node-0" {
		t.Errorf("ResponderId = %s, want node-0", resp.ResponderId)
	}

	if resp.TokenHash != "test-hash" {
		t.Errorf("TokenHash = %s, want test-hash", resp.TokenHash)
	}
}

func TestBootstrapServer_ProposeToken(t *testing.T) {
	server, _ := newTestBootstrapServer(t)
	ctx := createTestContext(t)

	req := &pb.ProposeTokenRequest{
		ProposedEpoch:   1,
		ProposerId:      string(raft.ServerID("node-1")),
		ProposerAddress: string(raft.ServerAddress("localhost:8081")),
		VoterSetHash:    "abc123",
		QuorumSize:      2,
		Mode:            0,
		Timestamp:       time.Now().UnixNano(),
	}

	resp, err := server.ProposeToken(ctx, req)
	if err != nil {
		t.Fatalf("ProposeToken() error = %v", err)
	}

	if resp.ProposedEpoch != 1 {
		t.Errorf("ProposedEpoch = %d, want 1", resp.ProposedEpoch)
	}
}

func TestBootstrapServer_getLocalServerID(t *testing.T) {
	server, _ := newTestBootstrapServer(t)

	serverID := server.getLocalServerID()
	if serverID != "node-0" {
		t.Errorf("getLocalServerID() = %s, want node-0", serverID)
	}
}

func TestBootstrapServer_getLocalServerID_NoMeta(t *testing.T) {
	tmpDir := t.TempDir()
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	server := NewBootstrapServer(BootstrapServerDeps{
		Config: &BootstrapConfig{
			DataDir:       tmpDir,
			ExpectedNodes: 3,
		},
		Fencing:   NewFencingManager(nil, nil, nil, secrets, nil),
		LocalMeta: func() *ClusterMeta { return nil },
	})

	serverID := server.getLocalServerID()
	if serverID != "" {
		t.Errorf("getLocalServerID() = %s, want empty string", serverID)
	}
}

func TestBootstrapServer_RequireProtocolVersion_RejectsMissingHeader(t *testing.T) {
	tmpDir := t.TempDir()
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	localMeta := &ClusterMeta{
		Version:       1,
		ClusterUUID:   "test-cluster-uuid",
		ServerID:      "node-0",
		ServerAddress: "localhost:8080",
		Ordinal:       0,
		State:         StateReady,
		BootstrapTime: time.Now(),
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert}

	server := NewBootstrapServer(BootstrapServerDeps{
		Config: &BootstrapConfig{
			DataDir:                tmpDir,
			ExpectedNodes:          3,
			RequireProtocolVersion: true,
		},
		Fencing:   NewFencingManager(nil, nil, nil, secrets, nil),
		Secrets:   secrets,
		LocalMeta: func() *ClusterMeta { return localMeta },
		TLSConfig: tlsConfig,
	})

	ctx := createTestContext(t)

	_, err := server.GetClusterMeta(ctx, &pb.GetClusterMetaRequest{})
	if err != ErrProtocolVersionMissing {
		t.Errorf("GetClusterMeta() error = %v, want %v", err, ErrProtocolVersionMissing)
	}
}

func TestBootstrapServer_RequireProtocolVersion_AcceptsValidHeader(t *testing.T) {
	tmpDir := t.TempDir()
	secrets := NewSecretsManager(SecretsConfig{}, nil)

	localMeta := &ClusterMeta{
		Version:       1,
		ClusterUUID:   "test-cluster-uuid",
		ServerID:      "node-0",
		ServerAddress: "localhost:8080",
		Ordinal:       0,
		State:         StateReady,
		BootstrapTime: time.Now(),
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert}

	server := NewBootstrapServer(BootstrapServerDeps{
		Config: &BootstrapConfig{
			DataDir:                tmpDir,
			ExpectedNodes:          3,
			RequireProtocolVersion: true,
		},
		Fencing:   NewFencingManager(nil, nil, nil, secrets, nil),
		Secrets:   secrets,
		LocalMeta: func() *ClusterMeta { return localMeta },
		TLSConfig: tlsConfig,
	})

	ctx := createTestContext(t)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-bootstrap-protocol-version", "1"))

	resp, err := server.GetClusterMeta(ctx, &pb.GetClusterMetaRequest{})
	if err != nil {
		t.Fatalf("GetClusterMeta() error = %v", err)
	}

	if resp.ClusterUuid != "test-cluster-uuid" {
		t.Errorf("ClusterUuid = %s, want test-cluster-uuid", resp.ClusterUuid)
	}
}

func TestBootstrapServer_NoRequireProtocolVersion_AcceptsMissingHeader(t *testing.T) {
	server, _ := newTestBootstrapServer(t)
	ctx := createTestContext(t)

	resp, err := server.GetClusterMeta(ctx, &pb.GetClusterMetaRequest{})
	if err != nil {
		t.Fatalf("GetClusterMeta() error = %v", err)
	}

	if resp.ClusterUuid != "test-cluster-uuid" {
		t.Errorf("ClusterUuid = %s, want test-cluster-uuid", resp.ClusterUuid)
	}
}
