package core

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
)

// Test bootstrap race conditions with multiple nodes starting simultaneously
func TestBootstrapRaceCondition_MultipleNodes(t *testing.T) {
	tests := []struct {
		name           string
		nodeConfigs    []nodeConfig
		expectedLeader string
		expectError    bool
	}{
		{
			name: "three_nodes_same_expected_list",
			nodeConfigs: []nodeConfig{
				{nodeID: "node-c", expectedNodes: []string{"node-a", "node-b", "node-c"}},
				{nodeID: "node-a", expectedNodes: []string{"node-a", "node-b", "node-c"}},
				{nodeID: "node-b", expectedNodes: []string{"node-a", "node-b", "node-c"}},
			},
			expectedLeader: "node-a",
		},
		{
			name: "different_expected_lists_should_fail_validation",
			nodeConfigs: []nodeConfig{
				{nodeID: "node-a", expectedNodes: []string{"node-a", "node-b"}},
				{nodeID: "node-b", expectedNodes: []string{"node-b", "node-c"}},
				{nodeID: "node-c", expectedNodes: []string{"node-a", "node-b"}},
			},
			expectError: true,
		},
		{
			name: "empty_expected_nodes_all_bootstrap",
			nodeConfigs: []nodeConfig{
				{nodeID: "node-a", expectedNodes: []string{}},
				{nodeID: "node-b", expectedNodes: []string{}},
				{nodeID: "node-c", expectedNodes: []string{}},
			},

			expectError:    false,
			expectedLeader: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			results := make([]bootstrapResult, len(tt.nodeConfigs))

			for i, config := range tt.nodeConfigs {
				wg.Add(1)
				go func(idx int, nodeConf nodeConfig) {
					defer wg.Done()

					fullConfig := &domain.Config{
						NodeID:    nodeConf.nodeID,
						ClusterID: "test-cluster",
						BindAddr:  "127.0.0.1:7000",
						DataDir:   "/tmp/test",
						Logger:    slog.Default(),
						Raft: domain.RaftConfig{
							ExpectedNodes: nodeConf.expectedNodes,
						},
						Resources: domain.ResourceConfig{
							MaxConcurrentTotal: 10,
						},
						Engine: domain.EngineConfig{
							MaxConcurrentWorkflows: 5,
						},
					}

					configErr := fullConfig.Validate()
					if configErr != nil {

						results[idx] = bootstrapResult{
							nodeID:          nodeConf.nodeID,
							shouldBootstrap: false,
							configError:     configErr,
						}
						return
					}

					manager := createTestManager(nodeConf.nodeID, nodeConf.expectedNodes)
					raftConfig := &domain.RaftConfig{
						ExpectedNodes: nodeConf.expectedNodes,
					}

					shouldBootstrap := manager.shouldBootstrap([]ports.Peer{}, raftConfig)
					results[idx] = bootstrapResult{
						nodeID:          nodeConf.nodeID,
						shouldBootstrap: shouldBootstrap,
					}
				}(i, config)
			}

			wg.Wait()

			bootstrapCount := 0
			configErrorCount := 0
			var leaders []string
			var configErrors []string

			for _, result := range results {
				if result.configError != nil {
					configErrorCount++
					configErrors = append(configErrors, fmt.Sprintf("%s: %v", result.nodeID, result.configError))
				} else if result.shouldBootstrap {
					bootstrapCount++
					leaders = append(leaders, result.nodeID)
				}
			}

			if tt.expectError {

				assert.Greater(t, configErrorCount, 0, "Expected configuration validation errors, got none. Errors: %v", configErrors)
				t.Logf("Configuration validation correctly caught %d errors: %v", configErrorCount, configErrors)
			} else if tt.expectedLeader == "" {

				assert.Equal(t, 0, configErrorCount, "Expected no configuration errors, got: %v", configErrors)
				assert.Greater(t, bootstrapCount, 1, "Expected multiple bootstraps (split brain), got %d leaders: %v", bootstrapCount, leaders)
				t.Logf("WARNING: Split brain detected with %d leaders: %v - this is a deployment configuration issue", bootstrapCount, leaders)
			} else {

				assert.Equal(t, 0, configErrorCount, "Expected no configuration errors, got: %v", configErrors)
				assert.Equal(t, 1, bootstrapCount, "Should have exactly one bootstrap, got %d leaders: %v", bootstrapCount, leaders)
				if len(leaders) == 1 {
					assert.Equal(t, tt.expectedLeader, leaders[0], "Wrong leader selected")
				}
			}
		})
	}
}

// Test edge cases in bootstrap logic that could cause failures
func TestBootstrapLogic_EdgeCases(t *testing.T) {
	tests := []struct {
		name            string
		nodeID          string
		expectedNodes   []string
		existingPeers   []ports.Peer
		forceBootstrap  bool
		shouldBootstrap bool
		expectPanic     bool
	}{
		{
			name:            "node_not_in_expected_list",
			nodeID:          "outsider",
			expectedNodes:   []string{"node-a", "node-b"},
			shouldBootstrap: false,
		},
		{
			name:            "empty_expected_with_self_only",
			nodeID:          "node-a",
			expectedNodes:   []string{},
			shouldBootstrap: true,
		},
		{
			name:            "nil_expected_nodes",
			nodeID:          "node-a",
			expectedNodes:   nil,
			shouldBootstrap: true,
		},
		{
			name:            "duplicate_nodes_in_expected",
			nodeID:          "node-a",
			expectedNodes:   []string{"node-a", "node-b", "node-a", "node-c", "node-b"},
			shouldBootstrap: true,
		},
		{
			name:            "force_bootstrap_overrides_everything",
			nodeID:          "node-z",
			expectedNodes:   []string{"node-a"},
			existingPeers:   []ports.Peer{{ID: "peer1", Address: "addr1"}},
			forceBootstrap:  true,
			shouldBootstrap: true,
		},
		{
			name:            "existing_peers_override_expected",
			nodeID:          "node-a",
			expectedNodes:   []string{"node-a"},
			existingPeers:   []ports.Peer{{ID: "peer1", Address: "addr1"}},
			shouldBootstrap: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				assert.Panics(t, func() {
					manager := createTestManager(tt.nodeID, tt.expectedNodes)
					raftConfig := &domain.RaftConfig{
						ExpectedNodes:  tt.expectedNodes,
						ForceBootstrap: tt.forceBootstrap,
					}
					manager.shouldBootstrap(tt.existingPeers, raftConfig)
				})
				return
			}

			manager := createTestManager(tt.nodeID, tt.expectedNodes)
			raftConfig := &domain.RaftConfig{
				ExpectedNodes:  tt.expectedNodes,
				ForceBootstrap: tt.forceBootstrap,
			}

			result := manager.shouldBootstrap(tt.existingPeers, raftConfig)
			assert.Equal(t, tt.shouldBootstrap, result, "Bootstrap decision mismatch for %s", tt.name)
		})
	}
}

// Test concurrent bootstrap decisions with timing variations
func TestBootstrapDecision_ConcurrentTiming(t *testing.T) {
	const numNodes = 10
	const iterations = 100

	for iteration := 0; iteration < iterations; iteration++ {
		var wg sync.WaitGroup
		results := make([]bool, numNodes)

		expectedNodes := make([]string, numNodes)
		for i := 0; i < numNodes; i++ {
			expectedNodes[i] = fmt.Sprintf("node-%02d", i)
		}

		for i := 0; i < numNodes; i++ {
			wg.Add(1)
			go func(nodeIndex int) {
				defer wg.Done()

				if nodeIndex%3 == 0 {
					time.Sleep(time.Microsecond * time.Duration(nodeIndex))
				}

				nodeID := fmt.Sprintf("node-%02d", nodeIndex)
				manager := createTestManager(nodeID, expectedNodes)
				raftConfig := &domain.RaftConfig{
					ExpectedNodes: expectedNodes,
				}

				results[nodeIndex] = manager.shouldBootstrap([]ports.Peer{}, raftConfig)
			}(i)
		}

		wg.Wait()

		bootstrapCount := 0
		leaderIndex := -1
		for i, shouldBootstrap := range results {
			if shouldBootstrap {
				bootstrapCount++
				leaderIndex = i
			}
		}

		if bootstrapCount != 1 || leaderIndex != 0 {
			t.Fatalf("Iteration %d: Expected exactly 1 bootstrap at index 0, got %d bootstraps with leader at index %d",
				iteration, bootstrapCount, leaderIndex)
		}
	}
}

// Test ExpectedNodes validation prevents split-brain
func TestExpectedNodesValidation_PreventsSplitBrain(t *testing.T) {
	tests := []struct {
		name          string
		nodeID        string
		expectedNodes []string
		expectError   bool
		errorContains string
	}{
		{
			name:          "node_in_expected_list_valid",
			nodeID:        "node-a",
			expectedNodes: []string{"node-a", "node-b", "node-c"},
			expectError:   false,
		},
		{
			name:          "node_not_in_expected_list_invalid",
			nodeID:        "node-d",
			expectedNodes: []string{"node-a", "node-b", "node-c"},
			expectError:   true,
			errorContains: "node node-d not found in ExpectedNodes list",
		},
		{
			name:          "empty_expected_list_valid",
			nodeID:        "node-a",
			expectedNodes: []string{},
			expectError:   false,
		},
		{
			name:          "single_node_in_list_valid",
			nodeID:        "node-a",
			expectedNodes: []string{"node-a"},
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &domain.Config{
				NodeID:    tt.nodeID,
				ClusterID: "test-cluster",
				BindAddr:  "127.0.0.1:7000",
				DataDir:   "/tmp/test",
				Logger:    slog.Default(),
				Raft: domain.RaftConfig{
					ExpectedNodes: tt.expectedNodes,
				},
				Resources: domain.ResourceConfig{
					MaxConcurrentTotal: 10,
				},
				Engine: domain.EngineConfig{
					MaxConcurrentWorkflows: 5,
				},
			}

			err := config.Validate()

			if tt.expectError {
				assert.Error(t, err, "Expected validation error")
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains, "Error should contain expected text")
				}
			} else {
				assert.NoError(t, err, "Expected no validation error")
			}
		})
	}
}

// Test configuration validation edge cases
func TestConfigValidation_EdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		config       *domain.Config
		expectError  bool
		errorMessage string
	}{
		{
			name: "empty_cluster_id",
			config: &domain.Config{
				NodeID:    "test-node",
				ClusterID: "",
				BindAddr:  "127.0.0.1:7000",
				DataDir:   "/tmp/test",
				Logger:    slog.Default(),
			},
			expectError:  true,
			errorMessage: "cluster_id",
		},
		{
			name: "missing_node_id_with_cluster_id",
			config: &domain.Config{
				NodeID:    "",
				ClusterID: "test-cluster",
				BindAddr:  "127.0.0.1:7000",
				DataDir:   "/tmp/test",
				Logger:    slog.Default(),
			},
			expectError:  true,
			errorMessage: "node_id",
		},
		{
			name: "valid_config_with_cluster_id",
			config: &domain.Config{
				NodeID:    "test-node",
				ClusterID: "test-cluster",
				BindAddr:  "127.0.0.1:7000",
				DataDir:   "/tmp/test",
				Logger:    slog.Default(),
				Resources: domain.ResourceConfig{
					MaxConcurrentTotal: 10,
				},
				Engine: domain.EngineConfig{
					MaxConcurrentWorkflows: 5,
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.expectError {
				assert.Error(t, err, "Expected validation error")
				if tt.errorMessage != "" {
					assert.Contains(t, err.Error(), tt.errorMessage, "Error message should contain expected text")
				}
			} else {
				assert.NoError(t, err, "Expected no validation error")
			}
		})
	}
}

// Helper types and functions
type nodeConfig struct {
	nodeID        string
	expectedNodes []string
}

type bootstrapResult struct {
	nodeID          string
	shouldBootstrap bool
	configError     error
}

func createTestManager(nodeID string, expectedNodes []string) *Manager {
	config := &domain.Config{
		NodeID:    nodeID,
		ClusterID: "test-cluster",
		Logger:    slog.Default(),
	}

	return &Manager{
		config: config,
		logger: slog.Default(),
		nodeID: nodeID,
	}
}
