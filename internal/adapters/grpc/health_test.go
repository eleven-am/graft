package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestHealthChecker_SetAndGetServiceStatus(t *testing.T) {
	logger := slog.Default()
	checker := NewHealthChecker(logger)

	service := "test-service"
	status := grpc_health_v1.HealthCheckResponse_SERVING

	checker.SetServiceStatus(service, status)

	retrievedStatus := checker.GetServiceStatus(service)
	if retrievedStatus != status {
		t.Errorf("Expected status %v, got %v", status, retrievedStatus)
	}

	unknownStatus := checker.GetServiceStatus("unknown-service")
	if unknownStatus != grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN {
		t.Errorf("Expected SERVICE_UNKNOWN for unknown service, got %v", unknownStatus)
	}
}

func TestHealthChecker_CheckHealth(t *testing.T) {
	logger := slog.Default()
	checker := NewHealthChecker(logger)

	service := "test-service"
	expectedStatus := grpc_health_v1.HealthCheckResponse_SERVING

	checker.SetServiceStatus(service, expectedStatus)

	ctx := context.Background()
	status, err := checker.CheckHealth(ctx, service)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if status != expectedStatus {
		t.Errorf("Expected status %v, got %v", expectedStatus, status)
	}
}

func TestHealthChecker_WatchHealth(t *testing.T) {
	logger := slog.Default()
	checker := NewHealthChecker(logger)

	service := "test-service"
	updateChan := make(chan grpc_health_v1.HealthCheckResponse_ServingStatus, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go checker.WatchHealth(ctx, service, updateChan)

	time.Sleep(100 * time.Millisecond)

	checker.SetServiceStatus(service, grpc_health_v1.HealthCheckResponse_SERVING)

	select {
	case status := <-updateChan:
		if status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Errorf("Expected SERVING status, got %v", status)
		}
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for first status update")
	}

	checker.SetServiceStatus(service, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	select {
	case status := <-updateChan:
		if status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			t.Errorf("Expected NOT_SERVING status, got %v", status)
		}
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for second status update")
	}
}

func TestHealthChecker_StartHealthChecks(t *testing.T) {
	logger := slog.Default()
	checker := NewHealthChecker(logger)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	checker.StartHealthChecks(ctx)

	expectedServices := []string{
		"",
		"graft.proto.ClusterService",
		"graft.proto.RaftService",
		"graft.proto.WorkflowService",
	}

	for _, service := range expectedServices {
		status := checker.GetServiceStatus(service)
		if status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Errorf("Expected service %s to be SERVING, got %v", service, status)
		}
	}

	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)

	for _, service := range expectedServices {
		status := checker.GetServiceStatus(service)
		if status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			t.Errorf("Expected service %s to be NOT_SERVING after context cancellation, got %v", service, status)
		}
	}
}

func TestHealthChecker_MultipleStatusUpdates(t *testing.T) {
	logger := slog.Default()
	checker := NewHealthChecker(logger)

	service := "test-service"

	statuses := []grpc_health_v1.HealthCheckResponse_ServingStatus{
		grpc_health_v1.HealthCheckResponse_SERVING,
		grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN,
		grpc_health_v1.HealthCheckResponse_SERVING,
	}

	for _, status := range statuses {
		checker.SetServiceStatus(service, status)
		retrievedStatus := checker.GetServiceStatus(service)
		if retrievedStatus != status {
			t.Errorf("Expected status %v, got %v", status, retrievedStatus)
		}
	}
}

func TestHealthChecker_ConcurrentAccess(t *testing.T) {
	logger := slog.Default()
	checker := NewHealthChecker(logger)

	done := make(chan bool)
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			service := fmt.Sprintf("service-%d", id)
			status := grpc_health_v1.HealthCheckResponse_SERVING

			for j := 0; j < 100; j++ {
				checker.SetServiceStatus(service, status)
				checker.GetServiceStatus(service)
			}

			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}