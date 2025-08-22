package grpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"
)

type HealthChecker struct {
	logger   *slog.Logger
	mu       sync.RWMutex
	services map[string]grpc_health_v1.HealthCheckResponse_ServingStatus
}

func NewHealthChecker(logger *slog.Logger) *HealthChecker {
	return &HealthChecker{
		logger:   logger.With("component", "health-checker"),
		services: make(map[string]grpc_health_v1.HealthCheckResponse_ServingStatus),
	}
}

func (h *HealthChecker) SetServiceStatus(service string, status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.services[service] = status
	h.logger.Debug("service status updated", "service", service, "status", status.String())
}

func (h *HealthChecker) GetServiceStatus(service string) grpc_health_v1.HealthCheckResponse_ServingStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if status, exists := h.services[service]; exists {
		return status
	}

	return grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN
}

func (h *HealthChecker) CheckHealth(ctx context.Context, service string) (grpc_health_v1.HealthCheckResponse_ServingStatus, error) {
	status := h.GetServiceStatus(service)
	return status, nil
}

func (h *HealthChecker) WatchHealth(ctx context.Context, service string, updateChan chan<- grpc_health_v1.HealthCheckResponse_ServingStatus) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastStatus := grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentStatus := h.GetServiceStatus(service)
			if currentStatus != lastStatus {
				select {
				case updateChan <- currentStatus:
					lastStatus = currentStatus
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (h *HealthChecker) StartHealthChecks(ctx context.Context) {
	h.SetServiceStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	h.SetServiceStatus("graft.proto.ClusterService", grpc_health_v1.HealthCheckResponse_SERVING)
	h.SetServiceStatus("graft.proto.RaftService", grpc_health_v1.HealthCheckResponse_SERVING)
	h.SetServiceStatus("graft.proto.WorkflowService", grpc_health_v1.HealthCheckResponse_SERVING)

	go func() {
		<-ctx.Done()
		h.SetServiceStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		h.SetServiceStatus("graft.proto.ClusterService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		h.SetServiceStatus("graft.proto.RaftService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		h.SetServiceStatus("graft.proto.WorkflowService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}()
}