package transport

import "github.com/eleven-am/graft/internal/domain"

const grpcTransportComponent = "transport.GRPCTransport"

func newTransportConfigError(message string, cause error, opts ...domain.ErrorOption) *domain.DomainError {
	merged := []domain.ErrorOption{domain.WithComponent(grpcTransportComponent)}
	if len(opts) > 0 {
		merged = append(merged, opts...)
	}
	return domain.NewConfigurationError(message, cause, merged...)
}

func newTransportNetworkError(message string, cause error, opts ...domain.ErrorOption) *domain.DomainError {
	merged := []domain.ErrorOption{domain.WithComponent(grpcTransportComponent)}
	if len(opts) > 0 {
		merged = append(merged, opts...)
	}
	return domain.NewNetworkError(message, cause, merged...)
}

func newTransportResourceError(message string, cause error, opts ...domain.ErrorOption) *domain.DomainError {
	merged := []domain.ErrorOption{domain.WithComponent(grpcTransportComponent)}
	if len(opts) > 0 {
		merged = append(merged, opts...)
	}
	return domain.NewResourceError(message, cause, merged...)
}
