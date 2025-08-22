package manager

import (
	"testing"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultResourceConfig(t *testing.T) {
	config := DefaultResourceConfig()

	assert.Equal(t, 50, config.MaxConcurrentTotal)
	assert.Equal(t, 10, config.DefaultPerTypeLimit)
	assert.NotNil(t, config.MaxConcurrentPerType)
	assert.Empty(t, config.MaxConcurrentPerType)
}

func TestValidateResourceConfig_Valid(t *testing.T) {
	config := ports.ResourceConfig{
		MaxConcurrentTotal:   100,
		MaxConcurrentPerType: map[string]int{"test": 50, "http": 20},
		DefaultPerTypeLimit:  30,
	}

	err := ValidateResourceConfig(config)
	assert.NoError(t, err)
}

func TestValidateResourceConfig_Invalid(t *testing.T) {
	tests := []struct {
		name   string
		config ports.ResourceConfig
		errMsg string
	}{
		{
			name: "negative total",
			config: ports.ResourceConfig{
				MaxConcurrentTotal:   -1,
				MaxConcurrentPerType: make(map[string]int),
				DefaultPerTypeLimit:  10,
			},
			errMsg: "validation failed for field MaxConcurrentTotal: must be positive",
		},
		{
			name: "zero total",
			config: ports.ResourceConfig{
				MaxConcurrentTotal:   0,
				MaxConcurrentPerType: make(map[string]int),
				DefaultPerTypeLimit:  10,
			},
			errMsg: "validation failed for field MaxConcurrentTotal: must be positive",
		},
		{
			name: "negative default",
			config: ports.ResourceConfig{
				MaxConcurrentTotal:   100,
				MaxConcurrentPerType: make(map[string]int),
				DefaultPerTypeLimit:  -1,
			},
			errMsg: "validation failed for field DefaultPerTypeLimit: must be positive",
		},
		{
			name: "nil per type map",
			config: ports.ResourceConfig{
				MaxConcurrentTotal:   100,
				MaxConcurrentPerType: nil,
				DefaultPerTypeLimit:  10,
			},
			errMsg: "validation failed for field MaxConcurrentPerType: map cannot be nil",
		},
		{
			name: "empty node type",
			config: ports.ResourceConfig{
				MaxConcurrentTotal:   100,
				MaxConcurrentPerType: map[string]int{"": 10},
				DefaultPerTypeLimit:  10,
			},
			errMsg: "validation failed for field NodeType: cannot be empty string",
		},
		{
			name: "negative per type limit",
			config: ports.ResourceConfig{
				MaxConcurrentTotal:   100,
				MaxConcurrentPerType: map[string]int{"test": -1},
				DefaultPerTypeLimit:  10,
			},
			errMsg: "validation failed for field MaxConcurrentPerType[test]: must be positive",
		},
		{
			name: "per type exceeds total",
			config: ports.ResourceConfig{
				MaxConcurrentTotal:   10,
				MaxConcurrentPerType: map[string]int{"test": 20},
				DefaultPerTypeLimit:  5,
			},
			errMsg: "validation failed for field MaxConcurrentPerType[test]: limit 20 cannot exceed MaxConcurrentTotal 10",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateResourceConfig(tt.config)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestMergeResourceConfig(t *testing.T) {
	base := ports.ResourceConfig{
		MaxConcurrentTotal:   50,
		MaxConcurrentPerType: map[string]int{"base1": 10, "base2": 20},
		DefaultPerTypeLimit:  15,
	}

	override := ports.ResourceConfig{
		MaxConcurrentTotal:   100,
		MaxConcurrentPerType: map[string]int{"base2": 30, "override1": 40},
		DefaultPerTypeLimit:  25,
	}

	result := MergeResourceConfig(base, override)

	assert.Equal(t, 100, result.MaxConcurrentTotal)
	assert.Equal(t, 25, result.DefaultPerTypeLimit)
}

func TestMergeResourceConfig_PartialOverride(t *testing.T) {
	base := ports.ResourceConfig{
		MaxConcurrentTotal:   50,
		MaxConcurrentPerType: map[string]int{"test": 10},
		DefaultPerTypeLimit:  15,
	}

	override := ports.ResourceConfig{
		MaxConcurrentTotal:   100,
		MaxConcurrentPerType: make(map[string]int),
	}

	result := MergeResourceConfig(base, override)
	_ = result
}

func TestConfigBuilder(t *testing.T) {
	config, err := NewConfigBuilder().
		WithMaxTotal(200).
		WithDefaultPerType(50).
		WithNodeTypeLimit("http", 30).
		WithNodeTypeLimit("email", 20).
		Build()

	require.NoError(t, err)
	assert.Equal(t, 200, config.MaxConcurrentTotal)
	assert.Equal(t, 50, config.DefaultPerTypeLimit)
	assert.Equal(t, 30, config.MaxConcurrentPerType["http"])
	assert.Equal(t, 20, config.MaxConcurrentPerType["email"])
}

func TestConfigBuilder_WithNodeTypeLimits(t *testing.T) {
	limits := map[string]int{
		"worker1": 10,
		"worker2": 15,
		"worker3": 5,
	}

	config, err := NewConfigBuilder().
		WithMaxTotal(100).
		WithNodeTypeLimits(limits).
		Build()

	require.NoError(t, err)
	assert.Equal(t, 100, config.MaxConcurrentTotal)

	for nodeType, expectedLimit := range limits {
		assert.Equal(t, expectedLimit, config.MaxConcurrentPerType[nodeType])
	}
}

func TestConfigBuilder_ValidationError(t *testing.T) {
	_, err := NewConfigBuilder().
		WithMaxTotal(-1).
		Build()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid resource configuration")
}

func TestConfigBuilder_MustBuild_Success(t *testing.T) {
	config := NewConfigBuilder().
		WithMaxTotal(100).
		MustBuild()

	assert.Equal(t, 100, config.MaxConcurrentTotal)
}

func TestConfigBuilder_MustBuild_Panic(t *testing.T) {
	assert.Panics(t, func() {
		NewConfigBuilder().
			WithMaxTotal(-1).
			MustBuild()
	})
}
