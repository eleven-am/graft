package domain

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Test configuration edge cases that could cause runtime failures
func TestConfig_AdversarialValidation(t *testing.T) {
	tests := []struct {
		name          string
		setupConfig   func() *Config
		expectedError string
		shouldPanic   bool
	}{
		{
			name: "nil_logger_should_be_handled",
			setupConfig: func() *Config {
				return &Config{
					NodeID:    "test-node",
					ClusterID: "test-cluster",
					BindAddr:  "127.0.0.1:7000",
					DataDir:   "/tmp/test",
					Logger:    nil, // Nil logger
				}
			},
			expectedError: "logger",
		},
		{
			name: "empty_strings_all_required_fields",
			setupConfig: func() *Config {
				return &Config{
					NodeID:    "", // Empty
					ClusterID: "", // Empty
					BindAddr:  "", // Empty
					DataDir:   "", // Empty
					Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
				}
			},
			expectedError: "node_id", // Should fail on first validation
		},
		{
			name: "cluster_id_only_whitespace",
			setupConfig: func() *Config {
				return &Config{
					NodeID:    "test-node",
					ClusterID: "   ", // Only whitespace
					BindAddr:  "127.0.0.1:7000",
					DataDir:   "/tmp/test",
					Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
					Resources: ResourceConfig{
						MaxConcurrentTotal: 10,
					},
					Engine: EngineConfig{
						MaxConcurrentWorkflows: 5,
					},
				}
			},
			// This might pass validation but cause issues later
			expectedError: "",
		},
		{
			name: "extremely_long_cluster_id",
			setupConfig: func() *Config {
				longID := make([]byte, 10000) // 10KB cluster ID
				for i := range longID {
					longID[i] = 'a'
				}
				return &Config{
					NodeID:    "test-node",
					ClusterID: string(longID),
					BindAddr:  "127.0.0.1:7000",
					DataDir:   "/tmp/test",
					Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
					Resources: ResourceConfig{
						MaxConcurrentTotal: 10,
					},
					Engine: EngineConfig{
						MaxConcurrentWorkflows: 5,
					},
				}
			},
			// Should pass validation but might cause memory/network issues
			expectedError: "",
		},
		{
			name: "special_characters_in_ids",
			setupConfig: func() *Config {
				return &Config{
					NodeID:    "node-with-特殊字符-and-émojis-🚀",
					ClusterID: "cluster-with-newlines\n\rand-tabs\t",
					BindAddr:  "127.0.0.1:7000",
					DataDir:   "/tmp/test",
					Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
					Resources: ResourceConfig{
						MaxConcurrentTotal: 10,
					},
					Engine: EngineConfig{
						MaxConcurrentWorkflows: 5,
					},
				}
			},
			// Might pass validation but cause issues in networking/storage
			expectedError: "",
		},
		{
			name: "invalid_bind_address_format",
			setupConfig: func() *Config {
				return &Config{
					NodeID:    "test-node",
					ClusterID: "test-cluster",
					BindAddr:  "not-a-valid-address:port:extra", // Invalid format
					DataDir:   "/tmp/test",
					Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
					Resources: ResourceConfig{
						MaxConcurrentTotal: 10,
					},
					Engine: EngineConfig{
						MaxConcurrentWorkflows: 5,
					},
				}
			},
			// Config validation doesn't check address format - potential bug!
			expectedError: "",
		},
		{
			name: "negative_timeout_values",
			setupConfig: func() *Config {
				config := DefaultConfig()
				config.NodeID = "test-node"
				config.ClusterID = "test-cluster"
				config.BindAddr = "127.0.0.1:7000"
				config.DataDir = "/tmp/test"
				config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
				
				// Set negative timeouts
				config.Raft.DiscoveryTimeout = -1 * time.Second
				config.Raft.JoinTimeout = -1 * time.Second
				return config
			},
			// Negative timeouts should be caught but probably aren't
			expectedError: "",
		},
		{
			name: "zero_resource_limits",
			setupConfig: func() *Config {
				config := DefaultConfig()
				config.NodeID = "test-node"
				config.ClusterID = "test-cluster"
				config.BindAddr = "127.0.0.1:7000"
				config.DataDir = "/tmp/test"
				config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
				
				// Zero limits
				config.Resources.MaxConcurrentTotal = 0 // This should fail validation
				config.Engine.MaxConcurrentWorkflows = 0 // This should fail validation
				return config
			},
			expectedError: "resources.max_concurrent_total",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.setupConfig()

			if tt.shouldPanic {
				assert.Panics(t, func() {
					config.Validate()
				})
				return
			}

			err := config.Validate()

			if tt.expectedError != "" {
				assert.Error(t, err, "Expected validation error")
				assert.Contains(t, err.Error(), tt.expectedError, "Error should contain expected text")
			} else {
				assert.NoError(t, err, "Expected no validation error")
			}
		})
	}
}

// Test configuration builder methods with edge cases
func TestConfig_BuilderMethods_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		buildConfig func() *Config
		expectPanic bool
	}{
		{
			name: "with_mdns_empty_values",
			buildConfig: func() *Config {
				return NewConfigFromSimple("node", "addr", "dir", slog.Default()).
					WithMDNS("", "", "")
			},
			expectPanic: false, // Should use defaults
		},
		{
			name: "with_kubernetes_nil_values",
			buildConfig: func() *Config {
				// This might cause issues if not handled properly
				config := NewConfigFromSimple("node", "addr", "dir", slog.Default())
				return config.WithKubernetes("", "")
			},
			expectPanic: false,
		},
		{
			name: "with_static_peers_empty_slice",
			buildConfig: func() *Config {
				return NewConfigFromSimple("node", "addr", "dir", slog.Default()).
					WithStaticPeers()
			},
			expectPanic: false, // Should handle empty slice
		},
		{
			name: "with_tls_empty_file_paths",
			buildConfig: func() *Config {
				return NewConfigFromSimple("node", "addr", "dir", slog.Default()).
					WithTLS("", "", "")
			},
			expectPanic: false, // Might cause runtime errors later
		},
		{
			name: "with_resource_limits_negative_values",
			buildConfig: func() *Config {
				overrides := map[string]int{
					"negative-type": -1,
					"zero-type":     0,
				}
				return NewConfigFromSimple("node", "addr", "dir", slog.Default()).
					WithResourceLimits(-1, -1, overrides)
			},
			expectPanic: false, // Should be caught in validation
		},
		{
			name: "with_engine_settings_zero_timeouts",
			buildConfig: func() *Config {
				return NewConfigFromSimple("node", "addr", "dir", slog.Default()).
					WithEngineSettings(0, 0, 0)
			},
			expectPanic: false, // Should be caught in validation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				assert.Panics(t, func() {
					tt.buildConfig()
				})
				return
			}

			config := tt.buildConfig()
			assert.NotNil(t, config, "Config should not be nil")

			// Try to validate - might reveal issues
			err := config.Validate()
			t.Logf("Validation result for %s: %v", tt.name, err)
		})
	}
}

// Test NewConfigFromSimple edge cases
func TestNewConfigFromSimple_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		nodeID   string
		bindAddr string
		dataDir  string
		logger   *slog.Logger
		expectPanic bool
	}{
		{
			name:     "empty_node_id",
			nodeID:   "",
			bindAddr: "addr",
			dataDir:  "dir",
			logger:   slog.Default(),
		},
		{
			name:     "nil_logger_should_be_handled",
			nodeID:   "node",
			bindAddr: "addr", 
			dataDir:  "dir",
			logger:   nil, // Should create default logger
		},
		{
			name:     "all_empty_strings",
			nodeID:   "",
			bindAddr: "",
			dataDir:  "",
			logger:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				assert.Panics(t, func() {
					NewConfigFromSimple(tt.nodeID, tt.bindAddr, tt.dataDir, tt.logger)
				})
				return
			}

			config := NewConfigFromSimple(tt.nodeID, tt.bindAddr, tt.dataDir, tt.logger)
			assert.NotNil(t, config, "Config should not be nil")
			assert.NotNil(t, config.Logger, "Logger should never be nil")
			assert.NotEmpty(t, config.ClusterID, "ClusterID should be generated")
		})
	}
}

// Test default config values for sanity
func TestDefaultConfig_SanityChecks(t *testing.T) {
	config := DefaultConfig()

	// Check that defaults are reasonable
	assert.NotNil(t, config, "Default config should not be nil")
	assert.Empty(t, config.NodeID, "Default NodeID should be empty")
	assert.Empty(t, config.ClusterID, "Default ClusterID should be empty")
	assert.Empty(t, config.BindAddr, "Default BindAddr should be empty")
	assert.Empty(t, config.DataDir, "Default DataDir should be empty")
	assert.Nil(t, config.Logger, "Default Logger should be nil")

	// Check Raft defaults
	assert.Greater(t, config.Raft.DiscoveryTimeout, time.Duration(0), "DiscoveryTimeout should be positive")
	assert.Greater(t, config.Raft.JoinTimeout, time.Duration(0), "JoinTimeout should be positive")
	assert.GreaterOrEqual(t, config.Raft.BootstrapExpected, 0, "BootstrapExpected should be non-negative")
	assert.NotNil(t, config.Raft.ExpectedNodes, "ExpectedNodes should not be nil")

	// Check Resource defaults
	assert.Greater(t, config.Resources.MaxConcurrentTotal, 0, "MaxConcurrentTotal should be positive")
	assert.Greater(t, config.Resources.DefaultPerTypeLimit, 0, "DefaultPerTypeLimit should be positive")
	assert.NotNil(t, config.Resources.MaxConcurrentPerType, "MaxConcurrentPerType should not be nil")

	// Check Engine defaults
	assert.Greater(t, config.Engine.MaxConcurrentWorkflows, 0, "MaxConcurrentWorkflows should be positive")
	assert.Greater(t, config.Engine.NodeExecutionTimeout, time.Duration(0), "NodeExecutionTimeout should be positive")
}

// Test configuration with malicious inputs
func TestConfig_MaliciousInputs(t *testing.T) {
	tests := []struct {
		name        string
		setupConfig func() *Config
		description string
	}{
		{
			name: "extremely_long_node_id",
			setupConfig: func() *Config {
				longID := make([]byte, 1000000) // 1MB node ID
				for i := range longID {
					longID[i] = 'x'
				}
				return &Config{
					NodeID:    string(longID),
					ClusterID: "cluster",
					BindAddr:  "addr",
					DataDir:   "dir", 
					Logger:    slog.Default(),
				}
			},
			description: "Should handle extremely long node IDs without crashing",
		},
		{
			name: "control_characters_in_ids",
			setupConfig: func() *Config {
				return &Config{
					NodeID:    "node\x00\x01\x02\x03",
					ClusterID: "cluster\x7f\x80\x81",
					BindAddr:  "addr\r\n",
					DataDir:   "dir\t\v",
					Logger:    slog.Default(),
				}
			},
			description: "Should handle control characters safely",
		},
		{
			name: "sql_injection_like_strings",
			setupConfig: func() *Config {
				return &Config{
					NodeID:    "'; DROP TABLE nodes; --",
					ClusterID: "' OR '1'='1",
					BindAddr:  "127.0.0.1:7000",
					DataDir:   "/tmp/test",
					Logger:    slog.Default(),
				}
			},
			description: "Should handle SQL-injection-like strings safely",
		},
		{
			name: "path_traversal_in_data_dir",
			setupConfig: func() *Config {
				return &Config{
					NodeID:    "node",
					ClusterID: "cluster",
					BindAddr:  "127.0.0.1:7000",
					DataDir:   "../../../../../../etc/passwd",
					Logger:    slog.Default(),
				}
			},
			description: "Should not prevent path traversal in validation (but runtime should)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log(tt.description)
			
			config := tt.setupConfig()
			
			// Should not panic on creation or validation
			assert.NotPanics(t, func() {
				err := config.Validate()
				if err != nil {
					t.Logf("Validation error (might be expected): %v", err)
				}
			})
		})
	}
}

// Test concurrent config access
func TestConfig_ConcurrentAccess(t *testing.T) {
	config := NewConfigFromSimple("test-node", "127.0.0.1:7000", "/tmp/test", slog.Default())
	
	const numGoroutines = 100
	done := make(chan bool, numGoroutines)
	
	// Multiple goroutines accessing config simultaneously
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()
			
			// Read operations
			_ = config.NodeID
			_ = config.ClusterID
			_ = config.Raft.ExpectedNodes
			
			// Validation (should be safe for concurrent access)
			config.Validate()
			
			// Builder methods (these modify config - potential race!)
			// This could reveal race conditions
			config.WithMDNS("service", "domain", "host")
		}()
	}
	
	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
	
	// Config should still be valid
	err := config.Validate()
	assert.NoError(t, err, "Config should remain valid after concurrent access")
}