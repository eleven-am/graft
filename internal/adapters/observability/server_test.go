package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type mockHealthProvider struct {
	healthy bool
	err     string
	details map[string]interface{}
}

func (m *mockHealthProvider) GetHealth() ports.HealthStatus {
	return ports.HealthStatus{
		Healthy: m.healthy,
		Error:   m.err,
		Details: m.details,
	}
}

type mockMetricsProvider struct{}

func (m *mockMetricsProvider) GetMetrics() ports.SystemMetrics {
	return ports.SystemMetrics{
		Engine: map[string]interface{}{
			"active_workflows": 5,
		},
	}
}

func TestServerHealthEndpoint(t *testing.T) {
	mockHealth := &mockHealthProvider{
		healthy: true,
		details: map[string]interface{}{
			"raft": "ok",
		},
	}
	mockMetrics := &mockMetricsProvider{}

	server := NewServer(9091, mockHealth, mockMetrics, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		server.Start(ctx)
	}()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://localhost:9091/health")
	if err != nil {
		t.Fatalf("Failed to get health endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %d", resp.StatusCode)
	}

	var health HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("Failed to decode health response: %v", err)
	}

	if health.Status != "ok" {
		t.Errorf("Expected status 'ok', got '%s'", health.Status)
	}
}

func TestServerMetricsEndpoint(t *testing.T) {
	mockHealth := &mockHealthProvider{healthy: true}
	mockMetrics := &mockMetricsProvider{}

	server := NewServer(9092, mockHealth, mockMetrics, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		server.Start(ctx)
	}()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://localhost:9092/metrics")
	if err != nil {
		t.Fatalf("Failed to get metrics endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %d", resp.StatusCode)
	}

	var metrics MetricsResponse
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatalf("Failed to decode metrics response: %v", err)
	}

	if metrics.Application.Engine == nil {
		t.Error("Expected engine metrics to be present")
	}
}
