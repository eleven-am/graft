package domain

import (
	"log/slog"
	"time"
)

type Config struct {
	NodeID    string       `json:"node_id" yaml:"node_id"`
	ClusterID string       `json:"cluster_id" yaml:"cluster_id"`
	BindAddr  string       `json:"bind_addr" yaml:"bind_addr"`
	DataDir   string       `json:"data_dir" yaml:"data_dir"`
	Logger    *slog.Logger `json:"-" yaml:"-"`

	Discovery      []DiscoveryConfig    `json:"discovery" yaml:"discovery"`
	Transport      TransportConfig      `json:"transport" yaml:"transport"`
	Raft           RaftConfig           `json:"raft" yaml:"raft"`
	Resources      ResourceConfig       `json:"resources" yaml:"resources"`
	Engine         EngineConfig         `json:"engine" yaml:"engine"`
	Orchestrator   OrchestratorConfig   `json:"orchestrator" yaml:"orchestrator"`
	Cluster        ClusterConfig        `json:"cluster" yaml:"cluster"`
	LoadBalancer   LoadBalancerConfig   `json:"load_balancer" yaml:"load_balancer"`
	Observability  ObservabilityConfig  `json:"observability" yaml:"observability"`
	CircuitBreaker CircuitBreakerConfig `json:"circuit_breaker" yaml:"circuit_breaker"`
	RateLimiter    RateLimiterConfig    `json:"rate_limiter" yaml:"rate_limiter"`
	Tracing        TracingConfig        `json:"tracing" yaml:"tracing"`
}

type DiscoveryType string

const (
	DiscoveryMDNS   DiscoveryType = "mdns"
	DiscoveryStatic DiscoveryType = "static"
)

type DiscoveryConfig struct {
	Type   DiscoveryType `json:"type" yaml:"type"`
	MDNS   *MDNSConfig   `json:"mdns,omitempty" yaml:"mdns,omitempty"`
	Static []StaticPeer  `json:"static,omitempty" yaml:"static,omitempty"`
}

type MDNSConfig struct {
	Service string `json:"service" yaml:"service"`
	Domain  string `json:"domain" yaml:"domain"`
	Host    string `json:"host" yaml:"host"`
}

type StaticPeer struct {
	ID       string            `json:"id" yaml:"id"`
	Address  string            `json:"address" yaml:"address"`
	Port     int               `json:"port" yaml:"port"`
	Metadata map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type TransportConfig struct {
	EnableTLS         bool          `json:"enable_tls" yaml:"enable_tls"`
	TLSCertFile       string        `json:"tls_cert_file,omitempty" yaml:"tls_cert_file,omitempty"`
	TLSKeyFile        string        `json:"tls_key_file,omitempty" yaml:"tls_key_file,omitempty"`
	TLSCAFile         string        `json:"tls_ca_file,omitempty" yaml:"tls_ca_file,omitempty"`
	MaxMessageSizeMB  int           `json:"max_message_size_mb" yaml:"max_message_size_mb"`
	ConnectionTimeout time.Duration `json:"connection_timeout" yaml:"connection_timeout"`
}

type RaftConfig struct {
	SnapshotInterval   time.Duration `json:"snapshot_interval" yaml:"snapshot_interval"`
	SnapshotThreshold  uint64        `json:"snapshot_threshold" yaml:"snapshot_threshold"`
	MaxSnapshots       int           `json:"max_snapshots" yaml:"max_snapshots"`
	MaxJoinAttempts    int           `json:"max_join_attempts" yaml:"max_join_attempts"`
	HeartbeatTimeout   time.Duration `json:"heartbeat_timeout" yaml:"heartbeat_timeout"`
	ElectionTimeout    time.Duration `json:"election_timeout" yaml:"election_timeout"`
	CommitTimeout      time.Duration `json:"commit_timeout" yaml:"commit_timeout"`
	MaxAppendEntries   int           `json:"max_append_entries" yaml:"max_append_entries"`
	ShutdownOnRemove   bool          `json:"shutdown_on_remove" yaml:"shutdown_on_remove"`
	TrailingLogs       uint64        `json:"trailing_logs" yaml:"trailing_logs"`
	LeaderLeaseTimeout time.Duration `json:"leader_lease_timeout" yaml:"leader_lease_timeout"`

	DiscoveryTimeout  time.Duration `json:"discovery_timeout" yaml:"discovery_timeout"`
	BootstrapExpected int           `json:"bootstrap_expected" yaml:"bootstrap_expected"`
	ForceBootstrap    bool          `json:"force_bootstrap" yaml:"force_bootstrap"`
	ExpectedNodes     []string      `json:"expected_nodes,omitempty" yaml:"expected_nodes,omitempty"`
	RequireCluster    bool          `json:"require_cluster" yaml:"require_cluster"`
	JoinTimeout       time.Duration `json:"join_timeout" yaml:"join_timeout"`
}

type ResourceConfig struct {
	MaxConcurrentTotal   int            `json:"max_concurrent_total" yaml:"max_concurrent_total"`
	MaxConcurrentPerType map[string]int `json:"max_concurrent_per_type,omitempty" yaml:"max_concurrent_per_type,omitempty"`
	DefaultPerTypeLimit  int            `json:"default_per_type_limit" yaml:"default_per_type_limit"`
	NodePriorities       map[string]int `json:"node_priorities,omitempty" yaml:"node_priorities,omitempty"`
	HealthThresholds     HealthConfig   `json:"health_thresholds" yaml:"health_thresholds"`
}

type HealthConfig struct {
	MaxResponseTime    time.Duration `json:"max_response_time" yaml:"max_response_time"`
	MinSuccessRate     float64       `json:"min_success_rate" yaml:"min_success_rate"`
	MaxUtilizationRate float64       `json:"max_utilization_rate" yaml:"max_utilization_rate"`
}

type EngineConfig struct {
	MaxConcurrentWorkflows int                     `json:"max_concurrent_workflows" yaml:"max_concurrent_workflows"`
	NodeExecutionTimeout   time.Duration           `json:"node_execution_timeout" yaml:"node_execution_timeout"`
	StateUpdateInterval    time.Duration           `json:"state_update_interval" yaml:"state_update_interval"`
	RetryAttempts          int                     `json:"retry_attempts" yaml:"retry_attempts"`
	RetryBackoff           time.Duration           `json:"retry_backoff" yaml:"retry_backoff"`
	WorkerCount            int                     `json:"worker_count" yaml:"worker_count"`
	StateOptimization      StateOptimizationConfig `json:"state_optimization" yaml:"state_optimization"`
}

type OrchestratorConfig struct {
	ShutdownTimeout time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
	StartupTimeout  time.Duration `json:"startup_timeout" yaml:"startup_timeout"`
	GracePeriod     time.Duration `json:"grace_period" yaml:"grace_period"`
}

type ClusterPolicy int

const (
	ClusterPolicyStrict ClusterPolicy = iota
	ClusterPolicyAdopt
	ClusterPolicyReset
	ClusterPolicyRecover
)

type ClusterConfig struct {
	ID              string        `json:"id,omitempty" yaml:"id,omitempty"`
	Policy          ClusterPolicy `json:"policy" yaml:"policy"`
	PersistenceFile string        `json:"persistence_file,omitempty" yaml:"persistence_file,omitempty"`
	AllowRecovery   bool          `json:"allow_recovery" yaml:"allow_recovery"`
}

type LoadBalancerConfig struct {
	ScoreCacheTTL      time.Duration              `json:"score_cache_ttl" yaml:"score_cache_ttl"`
	FailurePolicy      string                     `json:"failure_policy" yaml:"failure_policy"` // "fail-open" or "fail-closed"
	DefaultCapacity    float64                    `json:"default_capacity" yaml:"default_capacity"`
	Algorithm          LoadBalancingAlgorithm     `json:"algorithm" yaml:"algorithm"`
	WeightedConfig     WeightedRoundRobinConfig   `json:"weighted_config" yaml:"weighted_config"`
	AdaptiveConfig     AdaptiveLoadBalancerConfig `json:"adaptive_config" yaml:"adaptive_config"`
	PublishInterval    time.Duration              `json:"publish_interval" yaml:"publish_interval"`
	PublishDebounce    time.Duration              `json:"publish_debounce" yaml:"publish_debounce"`
	AvailabilityWindow time.Duration              `json:"availability_window" yaml:"availability_window"`
}

type LoadBalancingAlgorithm string

const (
	AlgorithmRoundRobin         LoadBalancingAlgorithm = "round_robin"
	AlgorithmWeightedRoundRobin LoadBalancingAlgorithm = "weighted_round_robin"
	AlgorithmLeastConnections   LoadBalancingAlgorithm = "least_connections"
	AlgorithmLeastResponseTime  LoadBalancingAlgorithm = "least_response_time"
	AlgorithmAdaptive           LoadBalancingAlgorithm = "adaptive"
	AlgorithmConsistentHash     LoadBalancingAlgorithm = "consistent_hash"
)

type WeightedRoundRobinConfig struct {
	DefaultWeight   int            `json:"default_weight" yaml:"default_weight"`
	NodeWeights     map[string]int `json:"node_weights,omitempty" yaml:"node_weights,omitempty"`
	SmoothWeighting bool           `json:"smooth_weighting" yaml:"smooth_weighting"`
}

type AdaptiveLoadBalancerConfig struct {
	ResponseTimeWeight    float64       `json:"response_time_weight" yaml:"response_time_weight"`
	CpuUsageWeight        float64       `json:"cpu_usage_weight" yaml:"cpu_usage_weight"`
	MemoryUsageWeight     float64       `json:"memory_usage_weight" yaml:"memory_usage_weight"`
	ConnectionCountWeight float64       `json:"connection_count_weight" yaml:"connection_count_weight"`
	ErrorRateWeight       float64       `json:"error_rate_weight" yaml:"error_rate_weight"`
	AdaptationInterval    time.Duration `json:"adaptation_interval" yaml:"adaptation_interval"`
	HistoryWindow         time.Duration `json:"history_window" yaml:"history_window"`
}

type ObservabilityConfig struct {
	Enabled       bool          `json:"enabled" yaml:"enabled"`
	Port          int           `json:"port" yaml:"port"`
	ReadTimeout   time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout  time.Duration `json:"write_timeout" yaml:"write_timeout"`
	IdleTimeout   time.Duration `json:"idle_timeout" yaml:"idle_timeout"`
	EnablePprof   bool          `json:"enable_pprof" yaml:"enable_pprof"`
	EnableMetrics bool          `json:"enable_metrics" yaml:"enable_metrics"`
}

type CircuitBreakerConfig struct {
	Enabled          bool                                   `json:"enabled" yaml:"enabled"`
	DefaultConfig    DefaultCircuitBreakerConfig            `json:"default_config" yaml:"default_config"`
	ServiceOverrides map[string]DefaultCircuitBreakerConfig `json:"service_overrides,omitempty" yaml:"service_overrides,omitempty"`
}

type DefaultCircuitBreakerConfig struct {
	FailureThreshold int           `json:"failure_threshold" yaml:"failure_threshold"`
	SuccessThreshold int           `json:"success_threshold" yaml:"success_threshold"`
	Timeout          time.Duration `json:"timeout" yaml:"timeout"`
	MaxRequests      int           `json:"max_requests" yaml:"max_requests"`
	Interval         time.Duration `json:"interval" yaml:"interval"`
}

type RateLimiterConfig struct {
	Enabled           bool                                `json:"enabled" yaml:"enabled"`
	DefaultConfig     DefaultRateLimiterConfig            `json:"default_config" yaml:"default_config"`
	ServiceOverrides  map[string]DefaultRateLimiterConfig `json:"service_overrides,omitempty" yaml:"service_overrides,omitempty"`
	EndpointOverrides map[string]DefaultRateLimiterConfig `json:"endpoint_overrides,omitempty" yaml:"endpoint_overrides,omitempty"`
}

type DefaultRateLimiterConfig struct {
	RequestsPerSecond float64       `json:"requests_per_second" yaml:"requests_per_second"`
	BurstSize         int           `json:"burst_size" yaml:"burst_size"`
	WaitTimeout       time.Duration `json:"wait_timeout" yaml:"wait_timeout"`
	EnableMetrics     bool          `json:"enable_metrics" yaml:"enable_metrics"`
	CleanupInterval   time.Duration `json:"cleanup_interval" yaml:"cleanup_interval"`
	KeyExpiry         time.Duration `json:"key_expiry" yaml:"key_expiry"`
}

type TracingConfig struct {
	Enabled          bool              `json:"enabled" yaml:"enabled"`
	ServiceName      string            `json:"service_name" yaml:"service_name"`
	SamplingRate     float64           `json:"sampling_rate" yaml:"sampling_rate"`
	JaegerEndpoint   string            `json:"jaeger_endpoint,omitempty" yaml:"jaeger_endpoint,omitempty"`
	OTLPEndpoint     string            `json:"otlp_endpoint,omitempty" yaml:"otlp_endpoint,omitempty"`
	Environment      string            `json:"environment" yaml:"environment"`
	ResourceTags     map[string]string `json:"resource_tags,omitempty" yaml:"resource_tags,omitempty"`
	MaxSpansPerTrace int               `json:"max_spans_per_trace" yaml:"max_spans_per_trace"`
}
