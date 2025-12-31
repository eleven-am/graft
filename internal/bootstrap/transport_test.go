package bootstrap

import (
	"testing"
)

func TestNewSecureTransport(t *testing.T) {
	config := &BootstrapConfig{
		DataDir:       t.TempDir(),
		ExpectedNodes: 3,
		TLS: TLSConfig{
			Enabled: false,
		},
	}

	deps := SecureTransportDeps{
		Config: config,
	}

	_, err := NewSecureTransport(deps)
	if err != ErrTLSNotConfigured {
		t.Fatalf("NewSecureTransport() error = %v, want %v", err, ErrTLSNotConfigured)
	}
}

func TestNewSecureTransport_RequiresMTLS(t *testing.T) {
	certFile, keyFile, caFile := createTestCerts(t)

	config := &BootstrapConfig{
		DataDir:       t.TempDir(),
		ExpectedNodes: 3,
		TLS: TLSConfig{
			Enabled:  true,
			Required: false,
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   caFile,
		},
	}

	deps := SecureTransportDeps{
		Config: config,
	}

	_, err := NewSecureTransport(deps)
	if err != ErrMTLSRequired {
		t.Fatalf("NewSecureTransport() error = %v, want %v (mTLS must be required)", err, ErrMTLSRequired)
	}
}

func TestNewSecureTransport_WithTLS(t *testing.T) {
	certFile, keyFile, caFile := createTestCerts(t)

	config := &BootstrapConfig{
		DataDir:       t.TempDir(),
		ExpectedNodes: 3,
		TLS: TLSConfig{
			Enabled:  true,
			Required: true,
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   caFile,
		},
	}

	deps := SecureTransportDeps{
		Config: config,
	}

	transport, err := NewSecureTransport(deps)
	if err != nil {
		t.Fatalf("NewSecureTransport() error = %v", err)
	}

	if transport.tlsConfig == nil {
		t.Error("tlsConfig should not be nil when TLS is enabled")
	}
}

func TestSecureTransport_Close(t *testing.T) {
	certFile, keyFile, caFile := createTestCerts(t)

	config := &BootstrapConfig{
		DataDir:       t.TempDir(),
		ExpectedNodes: 3,
		TLS: TLSConfig{
			Enabled:  true,
			Required: true,
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   caFile,
		},
	}

	deps := SecureTransportDeps{
		Config: config,
	}

	transport, err := NewSecureTransport(deps)
	if err != nil {
		t.Fatalf("NewSecureTransport() error = %v", err)
	}

	err = transport.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if len(transport.connPool) != 0 {
		t.Errorf("connPool should be empty after Close(), got %d", len(transport.connPool))
	}
}

func TestSecureTransport_closeConnection(t *testing.T) {
	certFile, keyFile, caFile := createTestCerts(t)

	config := &BootstrapConfig{
		DataDir:       t.TempDir(),
		ExpectedNodes: 3,
		TLS: TLSConfig{
			Enabled:  true,
			Required: true,
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   caFile,
		},
	}

	deps := SecureTransportDeps{
		Config: config,
	}

	transport, err := NewSecureTransport(deps)
	if err != nil {
		t.Fatalf("NewSecureTransport() error = %v", err)
	}

	transport.closeConnection("nonexistent:8080")
}
