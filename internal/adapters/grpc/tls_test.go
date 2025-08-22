package grpc

import (
	"os"
	"testing"

	"github.com/eleven-am/graft/internal/ports"
)

func TestLoadServerTLSCredentials(t *testing.T) {
	tests := []struct {
		name        string
		config      *ports.TLSConfig
		expectError bool
	}{
		{
			name:        "nil config",
			config:      nil,
			expectError: true,
		},
		{
			name: "disabled TLS",
			config: &ports.TLSConfig{
				Enabled: false,
			},
			expectError: true,
		},
		{
			name: "invalid cert file",
			config: &ports.TLSConfig{
				Enabled:  true,
				CertFile: "/nonexistent/cert.pem",
				KeyFile:  "/nonexistent/key.pem",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadServerTLSCredentials(tt.config)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestLoadClientTLSCredentials(t *testing.T) {
	tests := []struct {
		name        string
		config      *ports.TLSConfig
		expectError bool
	}{
		{
			name:        "nil config",
			config:      nil,
			expectError: true,
		},
		{
			name: "disabled TLS",
			config: &ports.TLSConfig{
				Enabled: false,
			},
			expectError: true,
		},
		{
			name: "invalid CA file",
			config: &ports.TLSConfig{
				Enabled: true,
				CAFile:  "/nonexistent/ca.pem",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadClientTLSCredentials(tt.config)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestLoadServerTLSCredentials_WithValidCerts(t *testing.T) {
	certPEM := `-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`

	keyPEM := `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----`

	certFile, err := os.CreateTemp("", "cert*.pem")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(certFile.Name())

	keyFile, err := os.CreateTemp("", "key*.pem")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(keyFile.Name())

	if _, err := certFile.Write([]byte(certPEM)); err != nil {
		t.Fatal(err)
	}
	certFile.Close()

	if _, err := keyFile.Write([]byte(keyPEM)); err != nil {
		t.Fatal(err)
	}
	keyFile.Close()

	config := &ports.TLSConfig{
		Enabled:  true,
		CertFile: certFile.Name(),
		KeyFile:  keyFile.Name(),
	}

	creds, err := LoadServerTLSCredentials(config)
	if err != nil {
		t.Errorf("Failed to load valid certificates: %v", err)
	}

	if creds == nil {
		t.Error("Expected credentials but got nil")
	}
}