package grpc

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func UnaryLoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		
		logger.Debug("request started",
			"method", info.FullMethod,
			"type", "unary",
		)

		resp, err := handler(ctx, req)
		
		duration := time.Since(start)
		code := codes.OK
		if err != nil {
			if st, ok := status.FromError(err); ok {
				code = st.Code()
			} else {
				code = codes.Unknown
			}
		}

		if err != nil {
			logger.Error("request failed",
				"method", info.FullMethod,
				"type", "unary",
				"duration_ms", duration.Milliseconds(),
				"code", code.String(),
				"error", err,
			)
		} else {
			logger.Info("request completed",
				"method", info.FullMethod,
				"type", "unary",
				"duration_ms", duration.Milliseconds(),
				"code", code.String(),
			)
		}

		return resp, err
	}
}

func StreamLoggingInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		
		logger.Debug("stream started",
			"method", info.FullMethod,
			"type", "stream",
			"is_client_stream", info.IsClientStream,
			"is_server_stream", info.IsServerStream,
		)

		err := handler(srv, ss)
		
		duration := time.Since(start)
		code := codes.OK
		if err != nil {
			if st, ok := status.FromError(err); ok {
				code = st.Code()
			} else {
				code = codes.Unknown
			}
		}

		if err != nil {
			logger.Error("stream failed",
				"method", info.FullMethod,
				"type", "stream",
				"duration_ms", duration.Milliseconds(),
				"code", code.String(),
				"error", err,
			)
		} else {
			logger.Info("stream completed",
				"method", info.FullMethod,
				"type", "stream",
				"duration_ms", duration.Milliseconds(),
				"code", code.String(),
			)
		}

		return err
	}
}

func UnaryClientLoggingInterceptor(logger *slog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		
		logger.Debug("client request started",
			"method", method,
			"type", "unary",
			"target", cc.Target(),
		)

		err := invoker(ctx, method, req, reply, cc, opts...)
		
		duration := time.Since(start)
		code := codes.OK
		if err != nil {
			if st, ok := status.FromError(err); ok {
				code = st.Code()
			} else {
				code = codes.Unknown
			}
		}

		if err != nil {
			logger.Error("client request failed",
				"method", method,
				"type", "unary",
				"target", cc.Target(),
				"duration_ms", duration.Milliseconds(),
				"code", code.String(),
				"error", err,
			)
		} else {
			logger.Debug("client request completed",
				"method", method,
				"type", "unary",
				"target", cc.Target(),
				"duration_ms", duration.Milliseconds(),
				"code", code.String(),
			)
		}

		return err
	}
}

func StreamClientLoggingInterceptor(logger *slog.Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := time.Now()
		
		logger.Debug("client stream started",
			"method", method,
			"type", "stream",
			"target", cc.Target(),
			"is_client_stream", desc.ClientStreams,
			"is_server_stream", desc.ServerStreams,
		)

		stream, err := streamer(ctx, desc, cc, method, opts...)
		
		if err != nil {
			duration := time.Since(start)
			code := codes.Unknown
			if st, ok := status.FromError(err); ok {
				code = st.Code()
			}
			
			logger.Error("client stream failed to establish",
				"method", method,
				"type", "stream",
				"target", cc.Target(),
				"duration_ms", duration.Milliseconds(),
				"code", code.String(),
				"error", err,
			)
			return nil, err
		}

		return &loggingClientStream{
			ClientStream: stream,
			logger:       logger,
			method:       method,
			target:       cc.Target(),
			start:        start,
		}, nil
	}
}

type loggingClientStream struct {
	grpc.ClientStream
	logger *slog.Logger
	method string
	target string
	start  time.Time
}

func (s *loggingClientStream) RecvMsg(m interface{}) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		duration := time.Since(s.start)
		s.logger.Debug("client stream recv error",
			"method", s.method,
			"target", s.target,
			"duration_ms", duration.Milliseconds(),
			"error", err,
		)
	}
	return err
}

func (s *loggingClientStream) SendMsg(m interface{}) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil {
		duration := time.Since(s.start)
		s.logger.Debug("client stream send error",
			"method", s.method,
			"target", s.target,
			"duration_ms", duration.Milliseconds(),
			"error", err,
		)
	}
	return err
}