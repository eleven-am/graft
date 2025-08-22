package grpc

import (
	"context"
	"testing"
	"log/slog"
	"fmt"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"github.com/eleven-am/graft/internal/ports"
)

func TestGRPCServer_GetAddress(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
	}

	server := NewGRPCServer(logger, config)
	
	addr := server.GetAddress()
	assert.Equal(t, "127.0.0.1:0", addr)
	
	ctx := context.Background()
	err := server.Start(ctx)
	assert.NoError(t, err)
	defer server.Stop()
	
	addr = server.GetAddress()
	assert.NotEmpty(t, addr)
	assert.Contains(t, addr, "127.0.0.1:")
	
	assert.NotEqual(t, "127.0.0.1:0", addr)
}

func TestGRPCServer_LoggingInterceptor(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)
	
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/TestMethod",
	}
	
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "response", nil
	}
	
	resp, err := server.loggingInterceptor(ctx, "request", info, handler)
	assert.NoError(t, err)
	assert.Equal(t, "response", resp)
	
	handlerWithError := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, fmt.Errorf("test error")
	}
	
	resp, err = server.loggingInterceptor(ctx, "request", info, handlerWithError)
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestGRPCServer_StreamLoggingInterceptor(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)
	
	info := &grpc.StreamServerInfo{
		FullMethod:     "/test.Service/StreamMethod",
		IsClientStream: true,
		IsServerStream: true,
	}
	
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}
	
	err := server.streamLoggingInterceptor(nil, nil, info, handler)
	assert.NoError(t, err)
	
	handlerWithError := func(srv interface{}, stream grpc.ServerStream) error {
		return fmt.Errorf("stream error")
	}
	
	err = server.streamLoggingInterceptor(nil, nil, info, handlerWithError)
	assert.Error(t, err)
	assert.Equal(t, "stream error", err.Error())
}

func TestGRPCServer_LoadTLSCredentials(t *testing.T) {
	logger := slog.Default()
	
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
		TLS:         nil,
	}
	server := NewGRPCServer(logger, config)
	
	creds, err := server.loadTLSCredentials()
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "TLS credentials requested but not configured")
	
	config.TLS = &ports.TLSConfig{
		Enabled: false,
	}
	server = NewGRPCServer(logger, config)
	
	creds, err = server.loadTLSCredentials()
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "TLS credentials requested but not configured")
	
	config.TLS = &ports.TLSConfig{
		Enabled:  true,
		CertFile: "nonexistent.crt",
		KeyFile:  "nonexistent.key",
	}
	server = NewGRPCServer(logger, config)
	
	creds, err = server.loadTLSCredentials()
	assert.Error(t, err)
	assert.Nil(t, creds)
}

func TestGRPCServer_Start_WithTLS(t *testing.T) {
	logger := slog.Default()
	
	certFile := t.TempDir() + "/test.crt"
	keyFile := t.TempDir() + "/test.key"
	
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
		TLS: &ports.TLSConfig{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  keyFile,
		},
	}
	
	server := NewGRPCServer(logger, config)
	ctx := context.Background()
	
	err := server.Start(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load TLS credentials")
}

func TestGRPCServer_Start_WithMaxMsgSize(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)
	ctx := context.Background()
	
	err := server.Start(ctx)
	assert.NoError(t, err)
	defer server.Stop()
	
	assert.NotNil(t, server.server)
}

func TestGRPCServer_SetHandlers(t *testing.T) {
	logger := slog.Default()
	config := &ServerConfig{
		BindAddress: "127.0.0.1",
		BindPort:    0,
	}

	server := NewGRPCServer(logger, config)
	
	clusterHandler := &mockClusterHandler{}
	raftHandler := &mockRaftHandler{}
	workflowHandler := &mockWorkflowHandler{}
	
	server.SetHandlers(clusterHandler, raftHandler, workflowHandler)
	
	assert.Equal(t, clusterHandler, server.clusterHandler)
	assert.Equal(t, raftHandler, server.raftHandler)
	assert.Equal(t, workflowHandler, server.workflowHandler)
}

func TestGRPCServer_Start_ListenError(t *testing.T) {
	logger := slog.Default()
	
	config := &ServerConfig{
		BindAddress: "999.999.999.999",
		BindPort:    8080,
	}

	server := NewGRPCServer(logger, config)
	ctx := context.Background()
	
	err := server.Start(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start gRPC server listener")
}