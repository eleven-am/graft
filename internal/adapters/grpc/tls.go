package grpc

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"google.golang.org/grpc/credentials"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func LoadServerTLSCredentials(config *ports.TLSConfig) (credentials.TransportCredentials, error) {
	if config == nil || !config.Enabled {
		return nil, domain.NewConfigurationError("tls", "TLS configuration is not enabled", "enable TLS in server configuration")
	}

	cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to load server TLS certificates",
			Details: map[string]interface{}{
				"cert_file": config.CertFile,
				"key_file":  config.KeyFile,
				"error":     err.Error(),
			},
		}
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert,
	}

	if config.CAFile != "" {
		certPool := x509.NewCertPool()
		ca, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to read server CA certificate file",
				Details: map[string]interface{}{
					"ca_file": config.CAFile,
					"error":   err.Error(),
				},
			}
		}

		if !certPool.AppendCertsFromPEM(ca) {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to append server CA certificate to pool",
				Details: map[string]interface{}{
					"ca_file": config.CAFile,
				},
			}
		}

		tlsConfig.ClientCAs = certPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return credentials.NewTLS(tlsConfig), nil
}

func LoadClientTLSCredentials(config *ports.TLSConfig) (credentials.TransportCredentials, error) {
	if config == nil || !config.Enabled {
		return nil, domain.NewConfigurationError("tls", "TLS configuration is not enabled", "enable TLS in client configuration")
	}

	tlsConfig := &tls.Config{}

	if config.CAFile != "" {
		certPool := x509.NewCertPool()
		ca, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to read client CA certificate file",
				Details: map[string]interface{}{
					"ca_file": config.CAFile,
					"error":   err.Error(),
				},
			}
		}

		if !certPool.AppendCertsFromPEM(ca) {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to append client CA certificate to pool",
				Details: map[string]interface{}{
					"ca_file": config.CAFile,
				},
			}
		}

		tlsConfig.RootCAs = certPool
	}

	if config.CertFile != "" && config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to load client TLS certificates",
				Details: map[string]interface{}{
					"cert_file": config.CertFile,
					"key_file":  config.KeyFile,
					"error":     err.Error(),
				},
			}
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return credentials.NewTLS(tlsConfig), nil
}