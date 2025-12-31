package bootstrap

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTLSConfigBuilder_BuildServerConfig(t *testing.T) {
	certFile, keyFile, caFile := createTestCerts(t)

	config := &TLSConfig{
		Enabled:  true,
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
		Required: true,
	}

	builder := NewTLSConfigBuilder(config, nil)

	tlsConfig, err := builder.BuildServerConfig()
	if err != nil {
		t.Fatalf("BuildServerConfig() error = %v", err)
	}

	if tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %d, want %d", tlsConfig.MinVersion, tls.VersionTLS13)
	}

	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v, want %v", tlsConfig.ClientAuth, tls.RequireAndVerifyClientCert)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("len(Certificates) = %d, want 1", len(tlsConfig.Certificates))
	}
}

func TestTLSConfigBuilder_BuildClientConfig(t *testing.T) {
	certFile, keyFile, caFile := createTestCerts(t)

	config := &TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CAFile:     caFile,
		ServerName: "test-server",
	}

	builder := NewTLSConfigBuilder(config, nil)

	tlsConfig, err := builder.BuildClientConfig()
	if err != nil {
		t.Fatalf("BuildClientConfig() error = %v", err)
	}

	if tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %d, want %d", tlsConfig.MinVersion, tls.VersionTLS13)
	}

	if tlsConfig.ServerName != "test-server" {
		t.Errorf("ServerName = %s, want %s", tlsConfig.ServerName, "test-server")
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("len(Certificates) = %d, want 1", len(tlsConfig.Certificates))
	}
}

func TestTLSConfigBuilder_NotEnabled(t *testing.T) {
	config := &TLSConfig{Enabled: false}
	builder := NewTLSConfigBuilder(config, nil)

	_, err := builder.BuildServerConfig()
	if err != ErrTLSNotConfigured {
		t.Errorf("BuildServerConfig() error = %v, want %v", err, ErrTLSNotConfigured)
	}

	_, err = builder.BuildClientConfig()
	if err != ErrTLSNotConfigured {
		t.Errorf("BuildClientConfig() error = %v, want %v", err, ErrTLSNotConfigured)
	}
}

func TestTLSConfigBuilder_MinVersion(t *testing.T) {
	certFile, keyFile, caFile := createTestCerts(t)

	config := &TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CAFile:     caFile,
		MinVersion: tls.VersionTLS12,
	}

	builder := NewTLSConfigBuilder(config, nil)

	tlsConfig, err := builder.BuildServerConfig()
	if err != nil {
		t.Fatalf("BuildServerConfig() error = %v", err)
	}

	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %d, want %d", tlsConfig.MinVersion, tls.VersionTLS12)
	}
}

func TestValidatePeerIdentity(t *testing.T) {
	tests := []struct {
		name         string
		dnsNames     []string
		ipAddresses  []net.IP
		commonName   string
		expectedAddr string
		wantErr      bool
	}{
		{
			name:         "matching DNS name",
			dnsNames:     []string{"node1.cluster.local"},
			expectedAddr: "node1.cluster.local:8080",
			wantErr:      false,
		},
		{
			name:         "matching IP address",
			ipAddresses:  []net.IP{net.ParseIP("192.168.1.1")},
			expectedAddr: "192.168.1.1:8080",
			wantErr:      false,
		},
		{
			name:         "matching common name",
			commonName:   "node1.cluster.local",
			expectedAddr: "node1.cluster.local:8080",
			wantErr:      false,
		},
		{
			name:         "no match",
			dnsNames:     []string{"node2.cluster.local"},
			expectedAddr: "node1.cluster.local:8080",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := &x509.Certificate{
				DNSNames:    tt.dnsNames,
				IPAddresses: tt.ipAddresses,
				Subject:     pkix.Name{CommonName: tt.commonName},
			}

			err := ValidatePeerIdentity(cert, tt.expectedAddr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePeerIdentity() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidatePeerIdentity_NilCert(t *testing.T) {
	err := ValidatePeerIdentity(nil, "node1:8080")
	if err != ErrPeerIdentityMismatch {
		t.Errorf("ValidatePeerIdentity() error = %v, want %v", err, ErrPeerIdentityMismatch)
	}
}

func TestMatchWildcard(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    bool
	}{
		{
			name:    "basic wildcard match",
			pattern: "*.example.com",
			want:    true,
		},
		{
			name:    "multi-level subdomain no match",
			pattern: "*.example.com",
			want:    false,
		},
		{
			name:    "exact domain no match for wildcard",
			pattern: "*.example.com",
			want:    false,
		},
		{
			name:    "non-wildcard pattern",
			pattern: "example.com",
			want:    false,
		},
		{
			name:    "wildcard with cluster.local",
			pattern: "*.cluster.local",
			want:    true,
		},
	}

	testNames := []string{
		"foo.example.com",
		"foo.bar.example.com",
		"example.com",
		"foo.example.com",
		"node1.cluster.local",
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchWildcard(testNames[i], tt.pattern)
			if got != tt.want {
				t.Errorf("matchWildcard(%s, %s) = %v, want %v", testNames[i], tt.pattern, got, tt.want)
			}
		})
	}
}

func TestMatchWildcard_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		pattern string
		want    bool
	}{
		{"single label matches", "foo.example.com", "*.example.com", true},
		{"multi-label no match", "foo.bar.example.com", "*.example.com", false},
		{"root domain no match", "example.com", "*.example.com", false},
		{"non-wildcard pattern", "foo.example.com", "foo.example.com", false},
		{"invalid wildcard position", "foo.example.com", "foo.*.com", false},
		{"empty pattern suffix", "foo.", "*.", true},
		{"k8s service pattern", "my-svc-0.my-svc.default.svc.cluster.local", "*.my-svc.default.svc.cluster.local", true},
		{"k8s multi-subdomain no match", "extra.my-svc-0.my-svc.default.svc.cluster.local", "*.my-svc.default.svc.cluster.local", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchWildcard(tt.host, tt.pattern)
			if got != tt.want {
				t.Errorf("matchWildcard(%q, %q) = %v, want %v", tt.host, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestValidatePeerIdentity_Wildcard(t *testing.T) {
	tests := []struct {
		name         string
		dnsNames     []string
		expectedAddr string
		wantErr      bool
	}{
		{
			name:         "wildcard DNS matches single subdomain",
			dnsNames:     []string{"*.cluster.local"},
			expectedAddr: "node1.cluster.local:8080",
			wantErr:      false,
		},
		{
			name:         "wildcard DNS no match multi-subdomain",
			dnsNames:     []string{"*.cluster.local"},
			expectedAddr: "node1.ns.cluster.local:8080",
			wantErr:      true,
		},
		{
			name:         "k8s headless service wildcard",
			dnsNames:     []string{"*.my-svc.default.svc.cluster.local"},
			expectedAddr: "my-svc-0.my-svc.default.svc.cluster.local:8080",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := &x509.Certificate{
				DNSNames: tt.dnsNames,
			}

			err := ValidatePeerIdentity(cert, tt.expectedAddr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePeerIdentity() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheckAllowedSANs_Wildcard(t *testing.T) {
	tests := []struct {
		name        string
		allowedSANs []string
		certDNS     []string
		wantErr     bool
	}{
		{
			name:        "wildcard in allowed SANs matches",
			allowedSANs: []string{"*.cluster.local"},
			certDNS:     []string{"node1.cluster.local"},
			wantErr:     false,
		},
		{
			name:        "wildcard in allowed SANs no match",
			allowedSANs: []string{"*.cluster.local"},
			certDNS:     []string{"node1.other.domain"},
			wantErr:     true,
		},
		{
			name:        "exact match still works",
			allowedSANs: []string{"node1.cluster.local"},
			certDNS:     []string{"node1.cluster.local"},
			wantErr:     false,
		},
		{
			name:        "mixed wildcard and exact",
			allowedSANs: []string{"specific.host", "*.cluster.local"},
			certDNS:     []string{"node1.cluster.local"},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &TLSConfig{
				AllowedSANs: tt.allowedSANs,
			}
			builder := &TLSConfigBuilder{config: config}

			cert := &x509.Certificate{
				DNSNames: tt.certDNS,
			}

			err := builder.checkAllowedSANs(cert)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkAllowedSANs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func createTestCerts(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()

	tmpDir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create CA certificate: %v", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("failed to parse CA certificate: %v", err)
	}

	caFile = filepath.Join(tmpDir, "ca.pem")
	if err := writePEMFile(caFile, "CERTIFICATE", caCertDER); err != nil {
		t.Fatalf("failed to write CA certificate: %v", err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate server key: %v", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Test Server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create server certificate: %v", err)
	}

	certFile = filepath.Join(tmpDir, "server.pem")
	if err := writePEMFile(certFile, "CERTIFICATE", serverCertDER); err != nil {
		t.Fatalf("failed to write server certificate: %v", err)
	}

	keyBytes, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		t.Fatalf("failed to marshal server key: %v", err)
	}

	keyFile = filepath.Join(tmpDir, "server-key.pem")
	if err := writePEMFile(keyFile, "EC PRIVATE KEY", keyBytes); err != nil {
		t.Fatalf("failed to write server key: %v", err)
	}

	return certFile, keyFile, caFile
}

func writePEMFile(path, blockType string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return pem.Encode(f, &pem.Block{
		Type:  blockType,
		Bytes: data,
	})
}
