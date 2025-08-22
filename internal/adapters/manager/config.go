package manager

import (
	"fmt"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func DefaultResourceConfig() ports.ResourceConfig {
	return ports.ResourceConfig{
		MaxConcurrentTotal:   50,
		MaxConcurrentPerType: make(map[string]int),
		DefaultPerTypeLimit:  10,
	}
}

func ValidateResourceConfig(config ports.ResourceConfig) error {
	if config.MaxConcurrentTotal <= 0 {
		return domain.NewValidationError("MaxConcurrentTotal", fmt.Sprintf("must be positive, got %d", config.MaxConcurrentTotal))
	}

	if config.DefaultPerTypeLimit <= 0 {
		return domain.NewValidationError("DefaultPerTypeLimit", fmt.Sprintf("must be positive, got %d", config.DefaultPerTypeLimit))
	}

	if config.MaxConcurrentPerType == nil {
		return domain.NewValidationError("MaxConcurrentPerType", "map cannot be nil")
	}

	for nodeType, limit := range config.MaxConcurrentPerType {
		if nodeType == "" {
			return domain.NewValidationError("NodeType", "cannot be empty string")
		}
		if limit <= 0 {
			return domain.NewValidationError(fmt.Sprintf("MaxConcurrentPerType[%s]", nodeType), fmt.Sprintf("must be positive, got %d", limit))
		}
		if limit > config.MaxConcurrentTotal {
			return domain.NewValidationError(
				fmt.Sprintf("MaxConcurrentPerType[%s]", nodeType), 
				fmt.Sprintf("limit %d cannot exceed MaxConcurrentTotal %d", limit, config.MaxConcurrentTotal))
		}
	}

	return nil
}

func MergeResourceConfig(base, override ports.ResourceConfig) ports.ResourceConfig {
	result := ports.ResourceConfig{
		MaxConcurrentTotal:   base.MaxConcurrentTotal,
		MaxConcurrentPerType: make(map[string]int),
		DefaultPerTypeLimit:  base.DefaultPerTypeLimit,
	}

	for nodeType, limit := range base.MaxConcurrentPerType {
		result.MaxConcurrentPerType[nodeType] = limit
	}

	if override.MaxConcurrentTotal > 0 {
		result.MaxConcurrentTotal = override.MaxConcurrentTotal
	}

	if override.DefaultPerTypeLimit > 0 {
		result.DefaultPerTypeLimit = override.DefaultPerTypeLimit
	}

	for nodeType, limit := range override.MaxConcurrentPerType {
		result.MaxConcurrentPerType[nodeType] = limit
	}

	return result
}

type ConfigBuilder struct {
	config ports.ResourceConfig
}

func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: DefaultResourceConfig(),
	}
}

func (cb *ConfigBuilder) WithMaxTotal(max int) *ConfigBuilder {
	cb.config.MaxConcurrentTotal = max
	return cb
}

func (cb *ConfigBuilder) WithDefaultPerType(limit int) *ConfigBuilder {
	cb.config.DefaultPerTypeLimit = limit
	return cb
}

func (cb *ConfigBuilder) WithNodeTypeLimit(nodeType string, limit int) *ConfigBuilder {
	if cb.config.MaxConcurrentPerType == nil {
		cb.config.MaxConcurrentPerType = make(map[string]int)
	}
	cb.config.MaxConcurrentPerType[nodeType] = limit
	return cb
}

func (cb *ConfigBuilder) WithNodeTypeLimits(limits map[string]int) *ConfigBuilder {
	if cb.config.MaxConcurrentPerType == nil {
		cb.config.MaxConcurrentPerType = make(map[string]int)
	}
	for nodeType, limit := range limits {
		cb.config.MaxConcurrentPerType[nodeType] = limit
	}
	return cb
}

func (cb *ConfigBuilder) Build() (ports.ResourceConfig, error) {
	if err := ValidateResourceConfig(cb.config); err != nil {
		return ports.ResourceConfig{}, domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "invalid resource configuration",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	return cb.config, nil
}

func (cb *ConfigBuilder) MustBuild() ports.ResourceConfig {
	config, err := cb.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to build resource config: %v", err))
	}
	return config
}