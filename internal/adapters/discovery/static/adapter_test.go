package static

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery/testutil"
	"github.com/eleven-am/graft/internal/ports"
)

func TestStaticAdapter_StartStop(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewStaticAdapter(logger)

	ctx := context.Background()
	config := ports.DiscoveryConfig{
		ServiceName: "test-service",
		ServicePort: 8080,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	os.Setenv("GRAFT_PEERS", "127.0.0.1:8081,127.0.0.1:8082")
	defer os.Unsetenv("GRAFT_PEERS")

	err := adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}

	err = adapter.Start(ctx, config)
	if err == nil {
		t.Fatal("expected error when starting already started adapter")
	}

	err = adapter.Stop()
	if err != nil {
		t.Fatalf("failed to stop adapter: %v", err)
	}

	err = adapter.Stop()
	if err == nil {
		t.Fatal("expected error when stopping already stopped adapter")
	}
}

func TestStaticAdapter_Advertise(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewStaticAdapter(logger)

	serviceInfo := ports.ServiceInfo{
		ID:      "node-1",
		Name:    "test-service",
		Address: "127.0.0.1",
		Port:    8080,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	err := adapter.Advertise(serviceInfo)
	if err == nil {
		t.Fatal("expected error when advertising before start")
	}

	ctx := context.Background()
	config := testutil.CreateTestDiscoveryConfig()

	os.Setenv("GRAFT_PEERS", "")
	defer os.Unsetenv("GRAFT_PEERS")

	err = adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	defer adapter.Stop()

	err = adapter.Advertise(serviceInfo)
	if err != nil {
		t.Fatalf("failed to advertise service: %v", err)
	}
}

func TestStaticAdapter_Discover(t *testing.T) {
	logger := testutil.CreateTestLogger()
	adapter := NewStaticAdapter(logger)

	_, err := adapter.Discover()
	if err == nil {
		t.Fatal("expected error when discovering before start")
	}

	ctx := context.Background()
	config := testutil.CreateTestDiscoveryConfig()

	os.Setenv("GRAFT_PEERS", "127.0.0.1:8081,127.0.0.1:8082")
	defer os.Unsetenv("GRAFT_PEERS")

	err = adapter.Start(ctx, config)
	if err != nil {
		t.Fatalf("failed to start adapter: %v", err)
	}
	defer adapter.Stop()

	time.Sleep(100 * time.Millisecond)

	peers, err := adapter.Discover()
	if err != nil {
		t.Fatalf("failed to discover peers: %v", err)
	}

	if len(peers) > 2 {
		t.Fatalf("expected at most 2 peers, got %d", len(peers))
	}
}

func TestConfig_LoadFromEnv(t *testing.T) {
	tests := []struct {
		name      string
		envValue  string
		wantPeers int
		wantError bool
	}{
		{
			name:      "valid peers",
			envValue:  "127.0.0.1:8081,192.168.1.10:9090",
			wantPeers: 2,
			wantError: false,
		},
		{
			name:      "empty string",
			envValue:  "",
			wantPeers: 0,
			wantError: false,
		},
		{
			name:      "invalid format",
			envValue:  "127.0.0.1",
			wantPeers: 0,
			wantError: true,
		},
		{
			name:      "invalid port",
			envValue:  "127.0.0.1:invalid",
			wantPeers: 0,
			wantError: true,
		},
		{
			name:      "with spaces",
			envValue:  "127.0.0.1:8081, 192.168.1.10:9090 ",
			wantPeers: 2,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("GRAFT_PEERS", tt.envValue)
			defer os.Unsetenv("GRAFT_PEERS")

			config, err := LoadConfig()
			if (err != nil) != tt.wantError {
				t.Errorf("LoadConfig() error = %v, wantError %v", err, tt.wantError)
				return
			}

			if !tt.wantError && len(config.Peers) != tt.wantPeers {
				t.Errorf("LoadConfig() got %d peers, want %d", len(config.Peers), tt.wantPeers)
			}
		})
	}
}

func TestConfig_LoadFromFile(t *testing.T) {
	jsonContent := `{
		"peers": [
			{
				"id": "peer-1",
				"address": "127.0.0.1",
				"port": 8081,
				"metadata": {
					"region": "us-east"
				}
			},
			{
				"id": "peer-2",
				"address": "192.168.1.10",
				"port": 9090
			}
		]
	}`

	yamlContent := `peers:
  - id: peer-1
    address: 127.0.0.1
    port: 8081
    metadata:
      region: us-east
  - id: peer-2
    address: 192.168.1.10
    port: 9090`

	tests := []struct {
		name      string
		filename  string
		content   string
		wantPeers int
		wantError bool
	}{
		{
			name:      "json file",
			filename:  "test_peers.json",
			content:   jsonContent,
			wantPeers: 2,
			wantError: false,
		},
		{
			name:      "yaml file",
			filename:  "test_peers.yaml",
			content:   yamlContent,
			wantPeers: 2,
			wantError: false,
		},
		{
			name:      "invalid json",
			filename:  "test_invalid.json",
			content:   "{invalid json}",
			wantPeers: 0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.WriteFile(tt.filename, []byte(tt.content), 0o644)
			if err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}
			defer os.Remove(tt.filename)

			config, err := loadConfigFromFile(tt.filename)
			if (err != nil) != tt.wantError {
				t.Errorf("loadConfigFromFile() error = %v, wantError %v", err, tt.wantError)
				return
			}

			if !tt.wantError && len(config.Peers) != tt.wantPeers {
				t.Errorf("loadConfigFromFile() got %d peers, want %d", len(config.Peers), tt.wantPeers)
			}
		})
	}
}

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid config",
			config: &Config{
				Peers: []PeerConfig{
					{ID: "peer-1", Address: "127.0.0.1", Port: 8080},
					{ID: "peer-2", Address: "192.168.1.1", Port: 8081},
				},
			},
			wantError: false,
		},
		{
			name: "duplicate peer ID",
			config: &Config{
				Peers: []PeerConfig{
					{ID: "peer-1", Address: "127.0.0.1", Port: 8080},
					{ID: "peer-1", Address: "192.168.1.1", Port: 8081},
				},
			},
			wantError: true,
			errorMsg:  "duplicate peer ID",
		},
		{
			name: "empty address",
			config: &Config{
				Peers: []PeerConfig{
					{ID: "peer-1", Address: "", Port: 8080},
				},
			},
			wantError: true,
			errorMsg:  "address cannot be empty",
		},
		{
			name: "invalid port",
			config: &Config{
				Peers: []PeerConfig{
					{ID: "peer-1", Address: "127.0.0.1", Port: 0},
				},
			},
			wantError: true,
			errorMsg:  "invalid port",
		},
		{
			name: "duplicate address",
			config: &Config{
				Peers: []PeerConfig{
					{ID: "peer-1", Address: "127.0.0.1", Port: 8080},
					{ID: "peer-2", Address: "127.0.0.1", Port: 8080},
				},
			},
			wantError: true,
			errorMsg:  "duplicate peer address",
		},
		{
			name: "auto-generate ID",
			config: &Config{
				Peers: []PeerConfig{
					{Address: "127.0.0.1", Port: 8080},
					{Address: "192.168.1.1", Port: 8081},
				},
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(tt.config)
			if (err != nil) != tt.wantError {
				t.Errorf("validateConfig() error = %v, wantError %v", err, tt.wantError)
			}
			if err != nil && tt.errorMsg != "" {
				if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateConfig() error = %v, want error containing %v", err, tt.errorMsg)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr || 
		   len(s) >= len(substr) && s[:len(substr)] == substr ||
		   len(s) > len(substr) && containsMiddle(s[1:len(s)-1], substr)
}

func containsMiddle(s, substr string) bool {
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestConfig_LoadConfigFromAPI(t *testing.T) {
	peers := []PeerConfig{
		{ID: "peer-1", Address: "127.0.0.1", Port: 8080},
		{ID: "peer-2", Address: "192.168.1.1", Port: 8081},
	}

	config, err := LoadConfigFromAPI(peers)
	if err != nil {
		t.Fatalf("LoadConfigFromAPI() error = %v", err)
	}

	if len(config.Peers) != 2 {
		t.Errorf("LoadConfigFromAPI() got %d peers, want 2", len(config.Peers))
	}

	invalidPeers := []PeerConfig{
		{ID: "peer-1", Address: "", Port: 8080},
	}

	_, err = LoadConfigFromAPI(invalidPeers)
	if err == nil {
		t.Fatal("expected error with invalid peer configuration")
	}
}

func TestHealthChecker_UpdatePeers(t *testing.T) {
	logger := testutil.CreateTestLogger()
	health := NewHealthChecker(logger)

	peers := []ports.Peer{
		{ID: "peer-1", Address: "127.0.0.1", Port: 8080},
		{ID: "peer-2", Address: "127.0.0.1", Port: 8081},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go health.Start(ctx, peers)
	time.Sleep(100 * time.Millisecond)

	newPeers := []ports.Peer{
		{ID: "peer-1", Address: "127.0.0.1", Port: 8080},
		{ID: "peer-3", Address: "127.0.0.1", Port: 8082},
	}

	health.UpdatePeers(newPeers)

	healthyPeers := health.GetHealthyPeers()
	if len(healthyPeers) > 2 {
		t.Errorf("expected at most 2 peers, got %d", len(healthyPeers))
	}

	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestHealthChecker_GetPeerHealth(t *testing.T) {
	logger := testutil.CreateTestLogger()
	health := NewHealthChecker(logger)

	peers := []ports.Peer{
		{ID: "peer-1", Address: "127.0.0.1", Port: 8080},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go health.Start(ctx, peers)
	time.Sleep(100 * time.Millisecond)

	peerHealth, ok := health.GetPeerHealth("peer-1")
	if !ok {
		t.Fatal("expected to find peer-1 health")
	}

	if peerHealth.Peer.ID != "peer-1" {
		t.Errorf("expected peer ID to be peer-1, got %s", peerHealth.Peer.ID)
	}

	_, ok = health.GetPeerHealth("non-existent")
	if ok {
		t.Fatal("expected not to find non-existent peer")
	}

	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestNewStaticAdapter_NilLogger(t *testing.T) {
	adapter := NewStaticAdapter(nil)
	if adapter == nil {
		t.Fatal("expected adapter to be created with nil logger")
	}
	if adapter.logger == nil {
		t.Fatal("expected logger to be set to default")
	}
}

func TestNewHealthChecker_NilLogger(t *testing.T) {
	health := NewHealthChecker(nil)
	if health == nil {
		t.Fatal("expected health checker to be created with nil logger")
	}
	if health.logger == nil {
		t.Fatal("expected logger to be set to default")
	}
}

func TestConfig_LoadFromFile_UnsupportedFormat(t *testing.T) {
	content := "unsupported content"
	filename := "test_unsupported.txt"
	
	err := os.WriteFile(filename, []byte(content), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer os.Remove(filename)

	_, err = loadConfigFromFile(filename)
	if err == nil {
		t.Fatal("expected error with unsupported file format")
	}
}

func TestHealthChecker_CheckPeerHealthy(t *testing.T) {
	logger := testutil.CreateTestLogger()
	health := NewHealthChecker(logger)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create test listener: %v", err)
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	
	peer := &PeerHealth{
		Peer: ports.Peer{
			ID:      "test-peer",
			Address: "127.0.0.1",
			Port:    port,
		},
		Healthy: false,
	}

	err = health.checkPeerWithError(peer)
	if err != nil {
		t.Fatalf("unexpected error checking peer: %v", err)
	}

	if !peer.Healthy {
		t.Error("expected peer to be healthy")
	}
	if peer.LastError != nil {
		t.Errorf("expected no error, got %v", peer.LastError)
	}
}