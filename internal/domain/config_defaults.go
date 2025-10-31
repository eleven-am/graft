package domain

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
)

func DefaultConfig() *Config {
	return &Config{
		Discovery:      []DiscoveryConfig{},
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

func DefaultMDNSConfig() *MDNSConfig {
	return &MDNSConfig{
		Service:     "_graft._tcp",
		Domain:      "local.",
		Host:        "",
		DisableIPv6: true,
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
		MaxJoinAttempts:    5,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      500 * time.Millisecond,
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       10240,
		LeaderLeaseTimeout: 500 * time.Millisecond,

		DiscoveryTimeout: 10 * time.Second,
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

func NewConfigFromSimple(nodeID, bindAddr, dataDir string, logger *slog.Logger) *Config {
	config := DefaultConfig()
	config.NodeID = nodeID
	config.BindAddr = bindAddr
	config.DataDir = dataDir
	config.Logger = logger

	if logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	if err := initializeClusterID(config); err != nil {
		config.Logger.Warn("failed to initialize cluster ID", "error", err)

		config.ClusterID = uuid.New().String()
	}

	return config
}

func (c *Config) WithMDNS(service, domain, host string, disableIPv6 bool) *Config {
	mdnsConfig := DefaultMDNSConfig()
	if service != "" {
		mdnsConfig.Service = service
	}
	if domain != "" {
		mdnsConfig.Domain = domain
	}
	if host != "" {
		mdnsConfig.Host = host
	}
	mdnsConfig.DisableIPv6 = disableIPv6

	c.Discovery = append(c.Discovery, DiscoveryConfig{
		Type: DiscoveryMDNS,
		MDNS: mdnsConfig,
	})
	return c
}

func (c *Config) WithStaticPeers(peers ...StaticPeer) *Config {
	c.Discovery = append(c.Discovery, DiscoveryConfig{
		Type:   DiscoveryStatic,
		Static: peers,
	})
	return c
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
	c.ClusterID = clusterID
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

func (c *Config) Validate() error {
	if c.NodeID == "" {
		return NewConfigError("node_id", ErrInvalidInput)
	}
	if c.ClusterID == "" {
		return NewConfigError("cluster_id", ErrInvalidInput)
	}
	if c.BindAddr == "" {
		return NewConfigError("bind_addr", ErrInvalidInput)
	}
	if host, port, err := net.SplitHostPort(c.BindAddr); err != nil || host == "" {
		return NewConfigError("bind_addr", ErrInvalidInput)
	} else {
		if _, err := strconv.Atoi(port); err != nil {
			return NewConfigError("bind_addr", ErrInvalidInput)
		}
	}
	if c.DataDir == "" {
		return NewConfigError("data_dir", ErrInvalidInput)
	}
	if c.Logger == nil {
		return NewConfigError("logger", ErrInvalidInput)
	}

	for i, discovery := range c.Discovery {
		if err := validateDiscoveryConfig(&discovery); err != nil {
			return NewConfigError("discovery", err)
		}
		_ = i
	}

	if c.Resources.MaxConcurrentTotal <= 0 {
		return NewConfigError("resources.max_concurrent_total", ErrInvalidInput)
	}

	if c.Engine.MaxConcurrentWorkflows <= 0 {
		return NewConfigError("engine.max_concurrent_workflows", ErrInvalidInput)
	}

	return nil
}

func validateDiscoveryConfig(config *DiscoveryConfig) error {
	switch config.Type {
	case DiscoveryMDNS:
		if config.MDNS == nil {
			return NewConfigError("mdns", ErrInvalidInput)
		}
		if config.MDNS.Service == "" {
			return NewConfigError("mdns.service", ErrInvalidInput)
		}
	case DiscoveryStatic:
		if len(config.Static) == 0 {
			return NewConfigError("static", ErrInvalidInput)
		}
		for _, peer := range config.Static {
			if peer.Address == "" {
				return NewConfigError("static.address", ErrInvalidInput)
			}
			if peer.Port <= 0 {
				return NewConfigError("static.port", ErrInvalidInput)
			}
		}
	default:
		return NewConfigError("discovery.type", ErrInvalidInput)
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
		config.ClusterID = config.Cluster.ID
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
		config.ClusterID = clusterID
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

	config.ClusterID = newClusterID
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
