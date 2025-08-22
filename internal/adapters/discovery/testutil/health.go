package testutil

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type MockHealthChecker struct {
	HealthyPeers map[string]bool
}

func NewMockHealthChecker() *MockHealthChecker {
	return &MockHealthChecker{
		HealthyPeers: make(map[string]bool),
	}
}

func (m *MockHealthChecker) SetPeerHealth(peerID string, healthy bool) {
	m.HealthyPeers[peerID] = healthy
}

func (m *MockHealthChecker) CheckPeerHealth(peerID string) bool {
	healthy, exists := m.HealthyPeers[peerID]
	return exists && healthy
}

func HealthCheckPeer(t *testing.T, peer ports.ServiceInfo, timeout time.Duration) bool {
	t.Helper()
	
	address := FormatPeerAddress(peer)
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

func WaitForHealthyPeer(t *testing.T, peerID string, checker *MockHealthChecker, timeout time.Duration) {
	t.Helper()
	
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for peer %s to become healthy", peerID)
		case <-ticker.C:
			if checker.CheckPeerHealth(peerID) {
				return
			}
		}
	}
}

func FormatPeerAddress(peer ports.ServiceInfo) string {
	return fmt.Sprintf("%s:%d", peer.Address, peer.Port)
}

func CreateUnhealthyPeer(id string) ports.ServiceInfo {
	return ports.ServiceInfo{
		ID:      id,
		Name:    "unhealthy-service",
		Metadata: map[string]string{
			"status": "unhealthy",
		},
	}
}