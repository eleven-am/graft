package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

type TLSConfigBuilder struct {
	config *TLSConfig
	logger *slog.Logger
}

func NewTLSConfigBuilder(config *TLSConfig, logger *slog.Logger) *TLSConfigBuilder {
	if logger == nil {
		logger = slog.Default()
	}
	return &TLSConfigBuilder{
		config: config,
		logger: logger,
	}
}

func (b *TLSConfigBuilder) BuildServerConfig() (*tls.Config, error) {
	if !b.config.Enabled {
		return nil, ErrTLSNotConfigured
	}

	cert, err := b.loadCertificates()
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificates: %w", err)
	}

	certPool, err := b.loadCertPool()
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificates: %w", err)
	}

	minVersion := b.config.MinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS13
	}

	clientAuth := b.config.ClientAuth
	if b.config.Required {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    certPool,
		ClientAuth:   clientAuth,
		MinVersion:   minVersion,
		CipherSuites: strongCipherSuites(),
	}

	var crl *x509.RevocationList
	if b.config.CRLFile != "" {
		var err error
		crl, err = b.loadCRL()
		if err != nil {
			return nil, fmt.Errorf("failed to load CRL: %w", err)
		}
	}

	if crl != nil || len(b.config.AllowedSANs) > 0 {
		tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
				return fmt.Errorf("no verified certificate chains")
			}

			if crl != nil {
				for _, chain := range verifiedChains {
					for _, cert := range chain {
						if err := b.verifyCertWithCRL(cert, crl); err != nil {
							return err
						}
					}
				}
			}

			leafCert := verifiedChains[0][0]
			if err := b.checkAllowedSANs(leafCert); err != nil {
				return err
			}

			return nil
		}
	}

	return tlsConfig, nil
}

func (b *TLSConfigBuilder) BuildClientConfig() (*tls.Config, error) {
	if !b.config.Enabled {
		return nil, ErrTLSNotConfigured
	}

	cert, err := b.loadCertificates()
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificates: %w", err)
	}

	certPool, err := b.loadCertPool()
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificates: %w", err)
	}

	minVersion := b.config.MinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS13
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
		MinVersion:   minVersion,
		CipherSuites: strongCipherSuites(),
	}

	if b.config.ServerName != "" {
		tlsConfig.ServerName = b.config.ServerName
	}

	var crl *x509.RevocationList
	if b.config.CRLFile != "" {
		var err error
		crl, err = b.loadCRL()
		if err != nil {
			return nil, fmt.Errorf("failed to load CRL: %w", err)
		}
	}

	if crl != nil || len(b.config.AllowedSANs) > 0 {
		tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
				return fmt.Errorf("no verified certificate chains")
			}

			if crl != nil {
				for _, chain := range verifiedChains {
					for _, cert := range chain {
						if err := b.verifyCertWithCRL(cert, crl); err != nil {
							return err
						}
					}
				}
			}

			leafCert := verifiedChains[0][0]
			if err := b.checkAllowedSANs(leafCert); err != nil {
				return err
			}

			return nil
		}
	}

	return tlsConfig, nil
}

func (b *TLSConfigBuilder) loadCertificates() (tls.Certificate, error) {
	return tls.LoadX509KeyPair(b.config.CertFile, b.config.KeyFile)
}

func (b *TLSConfigBuilder) loadCertPool() (*x509.CertPool, error) {
	caCert, err := os.ReadFile(b.config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA file: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificates")
	}

	return certPool, nil
}

func (b *TLSConfigBuilder) loadCRL() (*x509.RevocationList, error) {
	crlData, err := os.ReadFile(b.config.CRLFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CRL file: %w", err)
	}

	crl, err := x509.ParseRevocationList(crlData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CRL: %w", err)
	}

	return crl, nil
}

func (b *TLSConfigBuilder) verifyCertWithCRL(cert *x509.Certificate, crl *x509.RevocationList) error {
	if crl == nil {
		return nil
	}

	if !crl.NextUpdate.IsZero() && crl.NextUpdate.Before(time.Now()) {
		b.logger.Warn("CRL has expired, skipping revocation check",
			slog.String("next_update", crl.NextUpdate.String()))
		return nil
	}

	for _, entry := range crl.RevokedCertificateEntries {
		if cert.SerialNumber.Cmp(entry.SerialNumber) == 0 {
			b.logger.Error("certificate is revoked",
				slog.String("serial", cert.SerialNumber.String()),
				slog.String("subject", cert.Subject.String()),
				slog.String("revocation_time", entry.RevocationTime.String()))
			return ErrCertificateRevoked
		}
	}

	return nil
}

func (b *TLSConfigBuilder) checkAllowedSANs(cert *x509.Certificate) error {
	if len(b.config.AllowedSANs) == 0 {
		return nil
	}

	for _, allowed := range b.config.AllowedSANs {
		for _, dns := range cert.DNSNames {
			if dns == allowed || matchWildcard(dns, allowed) {
				return nil
			}
		}
		for _, ip := range cert.IPAddresses {
			if ip.String() == allowed {
				return nil
			}
		}
	}

	return fmt.Errorf("certificate SAN not in allowed list: DNS=%v IP=%v allowed=%v",
		cert.DNSNames, cert.IPAddresses, b.config.AllowedSANs)
}

func strongCipherSuites() []uint16 {
	return []uint16{
		tls.TLS_AES_128_GCM_SHA256,
		tls.TLS_AES_256_GCM_SHA384,
		tls.TLS_CHACHA20_POLY1305_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	}
}

func matchWildcard(name, pattern string) bool {
	if !strings.HasPrefix(pattern, "*.") {
		return false
	}
	suffix := pattern[1:]
	if !strings.HasSuffix(name, suffix) {
		return false
	}
	prefix := name[:len(name)-len(suffix)]
	return !strings.Contains(prefix, ".")
}

func ValidatePeerIdentity(cert *x509.Certificate, expectedAddr string) error {
	if cert == nil {
		return ErrPeerIdentityMismatch
	}

	host, _, err := net.SplitHostPort(expectedAddr)
	if err != nil {
		host = expectedAddr
	}

	for _, dnsName := range cert.DNSNames {
		if dnsName == host || matchWildcard(host, dnsName) {
			return nil
		}
	}

	ip := net.ParseIP(host)
	if ip != nil {
		for _, certIP := range cert.IPAddresses {
			if certIP.Equal(ip) {
				return nil
			}
		}
	}

	if cert.Subject.CommonName == host || matchWildcard(host, cert.Subject.CommonName) {
		return nil
	}

	return &CertificateSANMismatchError{
		ExpectedAddress: raft.ServerAddress(expectedAddr),
		CertDNSNames:    cert.DNSNames,
		CertIPAddresses: cert.IPAddresses,
		CertCN:          cert.Subject.CommonName,
	}
}
