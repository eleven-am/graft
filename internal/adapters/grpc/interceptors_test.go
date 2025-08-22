package grpc

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)


func TestUnaryLoggingInterceptor(t *testing.T) {
	logger := slog.Default()
	interceptor := UnaryLoggingInterceptor(logger)

	tests := []struct {
		name        string
		handler     grpc.UnaryHandler
		expectError bool
	}{
		{
			name: "successful request",
			handler: func(ctx context.Context, req interface{}) (interface{}, error) {
				return "response", nil
			},
			expectError: false,
		},
		{
			name: "failed request with status error",
			handler: func(ctx context.Context, req interface{}) (interface{}, error) {
				return nil, status.Error(codes.Internal, "internal error")
			},
			expectError: true,
		},
		{
			name: "failed request with regular error",
			handler: func(ctx context.Context, req interface{}) (interface{}, error) {
				return nil, errors.New("regular error")
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &grpc.UnaryServerInfo{
				FullMethod: "/test.Service/Method",
			}

			resp, err := interceptor(context.Background(), "request", info, tt.handler)

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !tt.expectError && resp == nil {
				t.Error("Expected response but got nil")
			}
		})
	}
}

func TestStreamLoggingInterceptor(t *testing.T) {
	logger := slog.Default()
	interceptor := StreamLoggingInterceptor(logger)

	tests := []struct {
		name        string
		handler     grpc.StreamHandler
		expectError bool
	}{
		{
			name: "successful stream",
			handler: func(srv interface{}, stream grpc.ServerStream) error {
				return nil
			},
			expectError: false,
		},
		{
			name: "failed stream with status error",
			handler: func(srv interface{}, stream grpc.ServerStream) error {
				return status.Error(codes.Internal, "internal error")
			},
			expectError: true,
		},
		{
			name: "failed stream with regular error",
			handler: func(srv interface{}, stream grpc.ServerStream) error {
				return errors.New("regular error")
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &grpc.StreamServerInfo{
				FullMethod:     "/test.Service/StreamMethod",
				IsClientStream: true,
				IsServerStream: true,
			}

			err := interceptor(nil, nil, info, tt.handler)

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestUnaryClientLoggingInterceptor(t *testing.T) {
	logger := slog.Default()
	interceptor := UnaryClientLoggingInterceptor(logger)

	tests := []struct {
		name        string
		invoker     grpc.UnaryInvoker
		expectError bool
	}{
		{
			name: "successful call",
			invoker: func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				return nil
			},
			expectError: false,
		},
		{
			name: "failed call with status error",
			invoker: func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				return status.Error(codes.Unavailable, "service unavailable")
			},
			expectError: true,
		},
		{
			name: "failed call with regular error",
			invoker: func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				return errors.New("connection error")
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc := &grpc.ClientConn{}

			err := interceptor(
				context.Background(),
				"/test.Service/Method",
				"request",
				"reply",
				cc,
				tt.invoker,
			)

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

type mockClientStream struct {
	grpc.ClientStream
	recvErr error
	sendErr error
}

func (m *mockClientStream) RecvMsg(msg interface{}) error {
	return m.recvErr
}

func (m *mockClientStream) SendMsg(msg interface{}) error {
	return m.sendErr
}

func TestStreamClientLoggingInterceptor(t *testing.T) {
	logger := slog.Default()
	interceptor := StreamClientLoggingInterceptor(logger)

	tests := []struct {
		name        string
		streamer    grpc.Streamer
		expectError bool
	}{
		{
			name: "successful stream creation",
			streamer: func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return &mockClientStream{}, nil
			},
			expectError: false,
		},
		{
			name: "failed stream creation",
			streamer: func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return nil, status.Error(codes.Unavailable, "cannot establish stream")
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desc := &grpc.StreamDesc{
				ClientStreams: true,
				ServerStreams: true,
			}
			cc := &grpc.ClientConn{}

			stream, err := interceptor(
				context.Background(),
				desc,
				cc,
				"/test.Service/StreamMethod",
				tt.streamer,
			)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				if stream != nil {
					t.Error("Expected nil stream on error")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if stream == nil {
					t.Error("Expected stream but got nil")
				}
			}
		})
	}
}

func TestLoggingClientStream(t *testing.T) {
	logger := slog.Default()
	
	baseStream := &mockClientStream{}
	loggingStream := &loggingClientStream{
		ClientStream: baseStream,
		logger:       logger,
		method:       "/test.Service/StreamMethod",
		target:       "localhost:8080",
	}

	baseStream.recvErr = errors.New("recv error")
	err := loggingStream.RecvMsg("message")
	if err == nil {
		t.Error("Expected recv error")
	}

	baseStream.sendErr = errors.New("send error")
	err = loggingStream.SendMsg("message")
	if err == nil {
		t.Error("Expected send error")
	}

	baseStream.recvErr = nil
	err = loggingStream.RecvMsg("message")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	baseStream.sendErr = nil
	err = loggingStream.SendMsg("message")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}