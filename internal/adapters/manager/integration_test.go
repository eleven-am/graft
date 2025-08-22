package manager

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceManager_WorkflowEngineIntegration(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   5,
		MaxConcurrentPerType: map[string]int{"http_client": 2, "email_sender": 1},
		DefaultPerTypeLimit:  3,
	}

	rm := NewResourceManager(config, slog.Default())

	testScenarios := []struct {
		nodeType    string
		expectAllow bool
		description string
	}{
		{"http_client", true, "first http_client should be allowed"},
		{"http_client", true, "second http_client should be allowed"},
		{"http_client", false, "third http_client should be rejected - type limit reached"},
		{"email_sender", true, "email_sender should be allowed"},
		{"email_sender", false, "second email_sender should be rejected - type limit reached"},
		{"data_processor", true, "data_processor should be allowed - uses default limit"},
		{"data_processor", true, "second data_processor should be allowed"},
		{"data_processor", false, "third data_processor should be rejected - total limit would be reached"},
	}

	acquiredResources := make([]string, 0)

	for _, scenario := range testScenarios {
		t.Run(scenario.description, func(t *testing.T) {
			if rm.CanExecuteNode(scenario.nodeType) {
				if scenario.expectAllow {
					err := rm.AcquireNode(scenario.nodeType)
					require.NoError(t, err)
					acquiredResources = append(acquiredResources, scenario.nodeType)

					t.Logf("Successfully acquired resource for %s", scenario.nodeType)
				} else {
					t.Errorf("Expected %s to be rejected but CanExecuteNode returned true", scenario.nodeType)
				}
			} else {
				if scenario.expectAllow {
					t.Errorf("Expected %s to be allowed but CanExecuteNode returned false", scenario.nodeType)
				} else {
					t.Logf("Correctly rejected %s", scenario.nodeType)
				}
			}
		})
	}

	stats := rm.GetExecutionStats()
	assert.Equal(t, 0, stats.AvailableSlots)

	for i, nodeType := range acquiredResources {
		t.Run("release_"+nodeType+"_"+string(rune(i)), func(t *testing.T) {
			err := rm.ReleaseNode(nodeType)
			require.NoError(t, err)
		})
	}

	finalStats := rm.GetExecutionStats()
	assert.Equal(t, 0, finalStats.TotalExecuting)
	assert.Equal(t, config.MaxConcurrentTotal, finalStats.AvailableSlots)
}

func TestResourceManager_WorkflowEngineStressIntegration(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   50,
		MaxConcurrentPerType: map[string]int{"worker": 20},
		DefaultPerTypeLimit:  15,
	}

	rm := NewResourceManager(config, slog.Default())

	nodeTypes := []string{"worker", "processor", "validator"}

	var acquired []string

	for i := 0; i < 100; i++ {
		nodeType := nodeTypes[i%len(nodeTypes)]

		if rm.CanExecuteNode(nodeType) {
			err := rm.AcquireNode(nodeType)
			if err == nil {
				acquired = append(acquired, nodeType)
			}
		}
	}

	stats := rm.GetExecutionStats()
	assert.LessOrEqual(t, stats.TotalExecuting, config.MaxConcurrentTotal)

	assert.LessOrEqual(t, stats.PerTypeExecuting["worker"], config.MaxConcurrentPerType["worker"])
	assert.LessOrEqual(t, stats.PerTypeExecuting["processor"], config.DefaultPerTypeLimit)
	assert.LessOrEqual(t, stats.PerTypeExecuting["validator"], config.DefaultPerTypeLimit)

	t.Logf("Acquired %d resources (max %d), stats: %+v",
		len(acquired), config.MaxConcurrentTotal, stats)

	for _, nodeType := range acquired {
		require.NoError(t, rm.ReleaseNode(nodeType))
	}
}

func TestResourceManager_WorkflowLifecycle(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   10,
		MaxConcurrentPerType: map[string]int{"critical": 2},
		DefaultPerTypeLimit:  5,
	}

	rm := NewResourceManager(config, slog.Default())

	workflowSteps := []struct {
		nodeType      string
		action        string
		shouldSucceed bool
	}{
		{"critical", "start", true},
		{"normal", "start", true},
		{"normal", "start", true},
		{"normal", "start", true},
		{"normal", "start", true},
	}

	activeCritical := 0
	activeNormal := 0

	for i, step := range workflowSteps {
		t.Run(fmt.Sprintf("step_%d_%s_%s", i, step.nodeType, step.action), func(t *testing.T) {
			if step.action == "start" {
				if rm.CanExecuteNode(step.nodeType) {
					err := rm.AcquireNode(step.nodeType)
					if step.shouldSucceed {
						require.NoError(t, err)
						if step.nodeType == "critical" {
							activeCritical++
						} else {
							activeNormal++
						}
						t.Logf("Started %s (active: critical=%d, normal=%d)",
							step.nodeType, activeCritical, activeNormal)
					} else {
						assert.Error(t, err, "Expected acquisition to fail")
					}
				} else {
					if step.shouldSucceed {
						t.Errorf("Expected %s to be startable but resource check failed", step.nodeType)
					}
				}
			} else if step.action == "complete" {
				err := rm.ReleaseNode(step.nodeType)
				require.NoError(t, err)
				if step.nodeType == "critical" {
					activeCritical--
				} else {
					activeNormal--
				}
				t.Logf("Completed %s (active: critical=%d, normal=%d)",
					step.nodeType, activeCritical, activeNormal)
			}
		})
	}

	stats := rm.GetExecutionStats()
	assert.Equal(t, activeCritical, stats.PerTypeExecuting["critical"])
	assert.Equal(t, activeNormal, stats.PerTypeExecuting["normal"])
}
