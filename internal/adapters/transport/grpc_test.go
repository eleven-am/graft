package transport

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto"
	"github.com/stretchr/testify/mock"
)

type mockEngine struct {
	lastTrigger domain.WorkflowTrigger
	shouldError bool
}

func (m *mockEngine) Start(ctx context.Context) error {
	return nil
}

func (m *mockEngine) Stop() error {
	return nil
}

func (m *mockEngine) ProcessTrigger(trigger domain.WorkflowTrigger) error {
	m.lastTrigger = trigger
	if m.shouldError {
		return domain.NewNotFoundError("workflow", trigger.WorkflowID)
	}
	return nil
}

func (m *mockEngine) GetWorkflowStatus(workflowID string) (*domain.WorkflowStatus, error) {
	return nil, nil
}

func (m *mockEngine) PauseWorkflow(ctx context.Context, workflowID string) error {
	return nil
}

func (m *mockEngine) ResumeWorkflow(ctx context.Context, workflowID string) error {
	return nil
}

func (m *mockEngine) StopWorkflow(ctx context.Context, workflowID string) error {
	return nil
}

func (m *mockEngine) GetMetrics() domain.ExecutionMetrics {
	return domain.ExecutionMetrics{}
}

func TestGRPCTransport_BasicLifecycle(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	transport := NewGRPCTransport(logger, domain.TransportConfig{})

	ctx := context.Background()
	err := transport.Start(ctx, "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Stop()

	mockEng := &mockEngine{}
	transport.RegisterEngine(mockEng)

	time.Sleep(100 * time.Millisecond)
}

func writeTempTLSCredentials(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	dir := t.TempDir()

	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	certFileHandle, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("failed to create cert file: %v", err)
	}
	if err := pem.Encode(certFileHandle, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("failed to encode cert: %v", err)
	}
	_ = certFileHandle.Close()

	keyFileHandle, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("failed to create key file: %v", err)
	}
	if err := pem.Encode(keyFileHandle, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}); err != nil {
		t.Fatalf("failed to encode key: %v", err)
	}
	_ = keyFileHandle.Close()

	return certPath, keyPath
}

func TestGRPCTransport_StartFailsWhenCAReadFails(t *testing.T) {
	certPath, keyPath := writeTempTLSCredentials(t)

	cfg := domain.TransportConfig{
		EnableTLS:   true,
		TLSCertFile: certPath,
		TLSKeyFile:  keyPath,
		TLSCAFile:   filepath.Join(t.TempDir(), "missing.pem"),
	}

	transport := NewGRPCTransport(nil, cfg)
	err := transport.Start(context.Background(), "127.0.0.1:0", 0)
	if err == nil {
		t.Fatal("expected error due to missing CA bundle")
	}

	if derr, ok := err.(*domain.DomainError); !ok {
		t.Fatalf("expected domain error, got %T", err)
	} else if derr.Category != domain.CategoryConfiguration {
		t.Fatalf("expected configuration error, got %v", derr.Category)
	}
}

func TestGRPCTransport_StartFailsWhenCAInvalid(t *testing.T) {
	certPath, keyPath := writeTempTLSCredentials(t)

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("not-a-cert"), 0o600); err != nil {
		t.Fatalf("failed to write invalid CA file: %v", err)
	}

	cfg := domain.TransportConfig{
		EnableTLS:   true,
		TLSCertFile: certPath,
		TLSKeyFile:  keyPath,
		TLSCAFile:   caPath,
	}

	transport := NewGRPCTransport(nil, cfg)
	err := transport.Start(context.Background(), "127.0.0.1:0", 0)
	if err == nil {
		t.Fatal("expected error due to invalid CA bundle")
	}

	if derr, ok := err.(*domain.DomainError); !ok {
		t.Fatalf("expected domain error, got %T", err)
	} else if derr.Category != domain.CategoryConfiguration {
		t.Fatalf("expected configuration error, got %v", derr.Category)
	}
}

func TestGRPCTransport_RegisterEngine(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	transport := NewGRPCTransport(logger, domain.TransportConfig{})
	mockEng := &mockEngine{}

	transport.RegisterEngine(mockEng)

	if transport.engine != mockEng {
		t.Error("Engine not properly registered")
	}
}

func TestApplyCommand_Leader(t *testing.T) {
	mockRaft := mocks.NewMockRaftNode(t)
	mockRaft.On("IsLeader").Return(true)
	mockRaft.On("Apply", mock.Anything, mock.Anything).Return(&domain.CommandResult{
		Success: true,
		Version: 42,
		Events: []domain.Event{
			{
				Type:      domain.EventPut,
				Key:       "test-key",
				Version:   42,
				NodeID:    "node1",
				Timestamp: time.Now(),
				RequestID: "req-123",
			},
		},
	}, nil)

	transport := &GRPCTransport{
		raft: mockRaft,
		cfg:  domain.TransportConfig{},
	}

	cmd := domain.NewPutCommand("test-key", []byte("test-value"), 42)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	req := &pb.ApplyRequest{
		Command: cmdBytes,
	}

	resp, err := transport.ApplyCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("ApplyCommand failed: %v", err)
	}

	if !resp.Success {
		t.Errorf("Expected success=true, got %v", resp.Success)
	}

	if resp.Version != 42 {
		t.Errorf("Expected version=42, got %d", resp.Version)
	}

	if len(resp.Events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(resp.Events))
	}

	if resp.Events[0].Key != "test-key" {
		t.Errorf("Expected event key 'test-key', got '%s'", resp.Events[0].Key)
	}
}

func TestApplyCommand_NonLeader(t *testing.T) {
	mockRaft := mocks.NewMockRaftNode(t)
	mockRaft.On("IsLeader").Return(false)
	mockRaft.On("LeaderAddr").Return("127.0.0.1:8001")
	mockRaft.On("GetClusterInfo").Return(ports.ClusterInfo{
		Leader: &ports.RaftNodeInfo{
			ID: "leader-node",
		},
	})

	transport := &GRPCTransport{
		raft: mockRaft,
		cfg:  domain.TransportConfig{},
	}

	cmd := domain.NewPutCommand("test-key", []byte("test-value"), 42)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	req := &pb.ApplyRequest{
		Command: cmdBytes,
	}

	resp, err := transport.ApplyCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("ApplyCommand failed: %v", err)
	}

	if resp.Success {
		t.Errorf("Expected success=false for non-leader, got %v", resp.Success)
	}

	if resp.LeaderAddr != "127.0.0.1:8001" {
		t.Errorf("Expected leader address '127.0.0.1:8001', got '%s'", resp.LeaderAddr)
	}

	if resp.LeaderId != "leader-node" {
		t.Errorf("Expected leader ID 'leader-node', got '%s'", resp.LeaderId)
	}
}

func TestApplyCommand_NoRaft(t *testing.T) {
	transport := &GRPCTransport{
		raft: nil,
		cfg:  domain.TransportConfig{},
	}

	req := &pb.ApplyRequest{
		Command: []byte("invalid"),
	}

	resp, err := transport.ApplyCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("ApplyCommand failed: %v", err)
	}

	if resp.Success {
		t.Errorf("Expected success=false when raft not available, got %v", resp.Success)
	}

	if resp.Error != "raft not available" {
		t.Errorf("Expected error 'raft not available', got '%s'", resp.Error)
	}
}
