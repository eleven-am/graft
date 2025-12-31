package bootstrap

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type SecureTransportDeps struct {
	Config    *BootstrapConfig
	Fencing   *FencingManager
	MetaStore MetaStore
	Secrets   *SecretsManager
	Logger    *slog.Logger
}

type SecureTransport struct {
	config    *BootstrapConfig
	fencing   *FencingManager
	metaStore MetaStore
	secrets   *SecretsManager
	logger    *slog.Logger

	tlsConfig *tls.Config
	connPool  map[string]*grpc.ClientConn
	connMu    sync.RWMutex
}

func NewSecureTransport(deps SecureTransportDeps) (*SecureTransport, error) {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}

	if !deps.Config.TLS.Enabled {
		return nil, ErrTLSNotConfigured
	}

	if !deps.Config.TLS.Required {
		return nil, ErrMTLSRequired
	}

	builder := NewTLSConfigBuilder(&deps.Config.TLS, deps.Logger)
	tlsConfig, err := builder.BuildClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	t := &SecureTransport{
		config:    deps.Config,
		fencing:   deps.Fencing,
		metaStore: deps.MetaStore,
		secrets:   deps.Secrets,
		logger:    deps.Logger,
		connPool:  make(map[string]*grpc.ClientConn),
		tlsConfig: tlsConfig,
	}

	return t, nil
}

func (t *SecureTransport) getConnection(addr string) (*grpc.ClientConn, error) {
	t.connMu.RLock()
	if conn, ok := t.connPool[addr]; ok {
		t.connMu.RUnlock()
		return conn, nil
	}
	t.connMu.RUnlock()

	t.connMu.Lock()
	defer t.connMu.Unlock()

	if conn, ok := t.connPool[addr]; ok {
		return conn, nil
	}

	var opts []grpc.DialOption

	if t.tlsConfig == nil {
		return nil, ErrTLSNotConfigured
	}

	creds := credentials.NewTLS(t.tlsConfig)
	opts = append(opts, grpc.WithTransportCredentials(creds))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		t.logger.Error("failed to dial peer",
			slog.String("addr", addr),
			slog.String("error", err.Error()))
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	t.connPool[addr] = conn
	t.logger.Debug("established connection to peer", slog.String("addr", addr))
	return conn, nil
}

func (t *SecureTransport) closeConnection(addr string) {
	t.connMu.Lock()
	defer t.connMu.Unlock()

	if conn, ok := t.connPool[addr]; ok {
		if err := conn.Close(); err != nil {
			t.logger.Warn("error closing connection",
				slog.String("addr", addr),
				slog.String("error", err.Error()))
		}
		delete(t.connPool, addr)
	}
}

func (t *SecureTransport) Close() error {
	t.connMu.Lock()
	defer t.connMu.Unlock()

	var lastErr error
	for addr, conn := range t.connPool {
		if err := conn.Close(); err != nil {
			t.logger.Warn("error closing connection",
				slog.String("addr", addr),
				slog.String("error", err.Error()))
			lastErr = err
		}
	}
	t.connPool = make(map[string]*grpc.ClientConn)
	return lastErr
}
