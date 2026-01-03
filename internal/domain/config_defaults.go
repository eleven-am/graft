package domain

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

func DefaultConfig() *Config {
	return &Config{
		Transport:      DefaultTransportConfig(),
		Raft:           DefaultRaftConfig(),
		Resources:      DefaultResourceConfig(),
		Engine:         DefaultEngineConfig(),
		Orchestrator:   DefaultOrchestratorConfig(),
		Cluster:        DefaultClusterConfig(),
		Observability:  DefaultObservabilityConfig(),
		CircuitBreaker: DefaultCircuitBreakerSettings(),
		RateLimiter:    DefaultRateLimiterSettings(),
		Tracing:        DefaultTracingConfig(),
	}
}

func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		EnableTLS:         false,
		MaxMessageSizeMB:  10,
		ConnectionTimeout: 30 * time.Second,
	}
}

func DefaultRaftConfig() RaftConfig {
	return RaftConfig{
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  1024,
		MaxSnapshots:       5,
		MaxJoinAttempts:    10,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      500 * time.Millisecond,
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       10240,
		LeaderLeaseTimeout: 500 * time.Millisecond,

		DiscoveryTimeout: 30 * time.Second,
	}
}

func DefaultResourceConfig() ResourceConfig {
	return ResourceConfig{
		MaxConcurrentTotal:   100,
		MaxConcurrentPerType: make(map[string]int),
		DefaultPerTypeLimit:  10,
		NodePriorities:       make(map[string]int),
		HealthThresholds:     DefaultHealthConfig(),
	}
}

func DefaultHealthConfig() HealthConfig {
	return HealthConfig{
		MaxResponseTime:    5 * time.Second,
		MinSuccessRate:     0.95,
		MaxUtilizationRate: 0.90,
	}
}

func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		WorkerCount:            10,
		MaxConcurrentWorkflows: 50,
		NodeExecutionTimeout:   5 * time.Minute,
		StateUpdateInterval:    time.Second,
		RetryAttempts:          3,
		RetryBackoff:           time.Second,
	}
}

func DefaultOrchestratorConfig() OrchestratorConfig {
	return OrchestratorConfig{
		ShutdownTimeout: 30 * time.Second,
		StartupTimeout:  30 * time.Second,
		GracePeriod:     2 * time.Second,
	}
}

func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		ID:              "",
		Policy:          ClusterPolicyRecover,
		PersistenceFile: "cluster.json",
		AllowRecovery:   true,
	}
}

// DefaultLoadBalancerConfig removed in minimal mode

func DefaultObservabilityConfig() ObservabilityConfig {
	return ObservabilityConfig{
		Enabled:       true,
		ReadTimeout:   10 * time.Second,
		WriteTimeout:  10 * time.Second,
		IdleTimeout:   60 * time.Second,
		EnablePprof:   false,
		EnableMetrics: true,
	}
}

func DefaultCircuitBreakerSettings() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		Enabled: true,
		DefaultConfig: DefaultCircuitBreakerConfig{
			FailureThreshold: 5,
			SuccessThreshold: 3,
			Timeout:          30 * time.Second,
			MaxRequests:      1,
			Interval:         60 * time.Second,
		},
		ServiceOverrides: make(map[string]DefaultCircuitBreakerConfig),
	}
}

func DefaultRateLimiterSettings() RateLimiterConfig {
	return RateLimiterConfig{
		Enabled: true,
		DefaultConfig: DefaultRateLimiterConfig{
			RequestsPerSecond: 100.0,
			BurstSize:         100,
			WaitTimeout:       5 * time.Second,
			EnableMetrics:     true,
			CleanupInterval:   5 * time.Minute,
			KeyExpiry:         10 * time.Minute,
		},
		ServiceOverrides:  make(map[string]DefaultRateLimiterConfig),
		EndpointOverrides: make(map[string]DefaultRateLimiterConfig),
	}
}

func DefaultTracingConfig() TracingConfig {
	return TracingConfig{
		Enabled:          false,
		ServiceName:      "graft",
		SamplingRate:     0.1,
		JaegerEndpoint:   "",
		OTLPEndpoint:     "",
		Environment:      "development",
		ResourceTags:     make(map[string]string),
		MaxSpansPerTrace: 1000,
	}
}

func NewConfigFromSimple(nodeID, dataDir string, logger *slog.Logger) *Config {
	config := DefaultConfig()
	config.NodeID = nodeID
	config.GossipPort = 7946
	config.DataDir = dataDir
	config.Logger = logger

	if logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	if err := initializeClusterID(config); err != nil {
		config.Logger.Warn("failed to initialize cluster ID", "error", err)

		config.Cluster.ID = uuid.New().String()
	}

	return config
}

func (c *Config) WithTLS(certFile, keyFile, caFile string) *Config {
	c.Transport.EnableTLS = true
	c.Transport.TLSCertFile = certFile
	c.Transport.TLSKeyFile = keyFile
	c.Transport.TLSCAFile = caFile
	return c
}

func (c *Config) WithResourceLimits(maxTotal, defaultPerType int, perTypeOverrides map[string]int) *Config {
	c.Resources.MaxConcurrentTotal = maxTotal
	c.Resources.DefaultPerTypeLimit = defaultPerType
	if perTypeOverrides != nil {
		c.Resources.MaxConcurrentPerType = perTypeOverrides
	}
	return c
}

func (c *Config) WithEngineSettings(maxWorkflows int, nodeTimeout time.Duration, retryAttempts int) *Config {
	c.Engine.MaxConcurrentWorkflows = maxWorkflows
	c.Engine.NodeExecutionTimeout = nodeTimeout
	c.Engine.RetryAttempts = retryAttempts
	if c.Engine.WorkerCount == 0 {
		c.Engine.WorkerCount = 10
	}
	return c
}

func (c *Config) WithClusterID(clusterID string) *Config {
	c.Cluster.ID = clusterID
	return c
}

func (c *Config) WithClusterPolicy(policy ClusterPolicy) *Config {
	c.Cluster.Policy = policy
	return c
}

func (c *Config) WithClusterRecovery(enabled bool) *Config {
	c.Cluster.AllowRecovery = enabled
	return c
}

func (c *Config) WithClusterPersistence(persistenceFile string) *Config {
	c.Cluster.PersistenceFile = persistenceFile
	return c
}

func (c *Config) WithGossipPort(gossipPort int) *Config {
	c.GossipPort = gossipPort
	return c
}

func (c *Config) WithAdvertiseAddr(advertiseAddr string) *Config {
	c.AdvertiseAddr = advertiseAddr
	return c
}

func (c *Config) Validate() error {
	if c.NodeID == "" {
		return NewConfigError("node_id", ErrInvalidInput)
	}
	if c.Cluster.ID == "" {
		return NewConfigError("cluster_id", ErrInvalidInput)
	}
	if c.GossipPort <= 0 || c.GossipPort > 65535 {
		return NewConfigError("gossip_port", ErrInvalidInput)
	}
	if c.DataDir == "" {
		return NewConfigError("data_dir", ErrInvalidInput)
	}
	if c.Logger == nil {
		return NewConfigError("logger", ErrInvalidInput)
	}

	if c.Resources.MaxConcurrentTotal <= 0 {
		return NewConfigError("resources.max_concurrent_total", ErrInvalidInput)
	}

	if c.Engine.MaxConcurrentWorkflows <= 0 {
		return NewConfigError("engine.max_concurrent_workflows", ErrInvalidInput)
	}

	return nil
}

type ConfigError struct {
	Field string
	Err   error
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config field %s: %v", e.Field, e.Err)
}

func (e *ConfigError) Unwrap() error {
	return e.Err
}

func NewConfigError(field string, err error) *ConfigError {
	return &ConfigError{
		Field: field,
		Err:   err,
	}
}

type ClusterPersistence struct {
	ClusterID string `json:"cluster_id"`
	CreatedAt string `json:"created_at"`
	NodeID    string `json:"node_id"`
}

func initializeClusterID(config *Config) error {
	if config.Cluster.ID != "" {
		return nil
	}

	persistenceFile := config.Cluster.PersistenceFile
	if persistenceFile == "" {
		persistenceFile = "cluster.json"
	}
	if !filepath.IsAbs(persistenceFile) {
		persistenceFile = filepath.Join(config.DataDir, persistenceFile)
	}

	if clusterID, err := loadClusterID(persistenceFile, config.Logger); err == nil && clusterID != "" {
		config.Cluster.ID = clusterID
		config.Logger.Info("loaded existing cluster ID", "cluster_id", clusterID, "file", persistenceFile)
		return nil
	}

	newClusterID := uuid.New().String()
	if err := saveClusterID(persistenceFile, newClusterID, config.NodeID, config.Logger); err != nil {
		return NewStorageError(
			"failed to save new cluster ID",
			err,
			WithComponent("domain.initializeClusterID"),
			WithContextDetail("persistence_file", persistenceFile),
			WithNodeID(config.NodeID),
		)
	}

	config.Cluster.ID = newClusterID
	config.Logger.Info("generated new cluster ID", "cluster_id", newClusterID, "file", persistenceFile)
	return nil
}

func loadClusterID(persistenceFile string, logger *slog.Logger) (string, error) {
	data, err := os.ReadFile(persistenceFile)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", NewStorageError(
			"failed to read cluster persistence file",
			err,
			WithComponent("domain.loadClusterID"),
			WithContextDetail("persistence_file", persistenceFile),
		)
	}

	var persistence ClusterPersistence
	if err := json.Unmarshal(data, &persistence); err != nil {
		logger.Warn("failed to parse cluster persistence file, will regenerate", "error", err)
		return "", err
	}

	return persistence.ClusterID, nil
}

func saveClusterID(persistenceFile, clusterID, nodeID string, logger *slog.Logger) error {
	if err := os.MkdirAll(filepath.Dir(persistenceFile), 0755); err != nil {
		return NewStorageError(
			"failed to create persistence directory",
			err,
			WithComponent("domain.saveClusterID"),
			WithContextDetail("persistence_file", persistenceFile),
		)
	}

	persistence := ClusterPersistence{
		ClusterID: clusterID,
		CreatedAt: time.Now().Format(time.RFC3339),
		NodeID:    nodeID,
	}

	data, err := json.MarshalIndent(persistence, "", "  ")
	if err != nil {
		return NewStorageError(
			"failed to marshal cluster persistence",
			err,
			WithComponent("domain.saveClusterID"),
			WithContextDetail("persistence_file", persistenceFile),
		)
	}

	if err := os.WriteFile(persistenceFile, data, 0644); err != nil {
		return NewStorageError(
			"failed to write cluster persistence file",
			err,
			WithComponent("domain.saveClusterID"),
			WithContextDetail("persistence_file", persistenceFile),
		)
	}

	return nil
}
