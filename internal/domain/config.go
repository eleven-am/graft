package domain

import (
	"log/slog"
	"time"
)

type Config struct {
	NodeID   string       `json:"node_id" yaml:"node_id"`
	BindAddr string       `json:"bind_addr" yaml:"bind_addr"`
	DataDir  string       `json:"data_dir" yaml:"data_dir"`
	Logger   *slog.Logger `json:"-" yaml:"-"`

	Discovery    []DiscoveryConfig  `json:"discovery" yaml:"discovery"`
	Transport    TransportConfig    `json:"transport" yaml:"transport"`
	Raft         RaftConfig         `json:"raft" yaml:"raft"`
	Resources    ResourceConfig     `json:"resources" yaml:"resources"`
	Engine       EngineConfig       `json:"engine" yaml:"engine"`
	Orchestrator OrchestratorConfig `json:"orchestrator" yaml:"orchestrator"`
}

type DiscoveryType string

const (
	DiscoveryMDNS       DiscoveryType = "mdns"
	DiscoveryKubernetes DiscoveryType = "kubernetes"
	DiscoveryStatic     DiscoveryType = "static"
)

type DiscoveryConfig struct {
	Type       DiscoveryType     `json:"type" yaml:"type"`
	MDNS       *MDNSConfig       `json:"mdns,omitempty" yaml:"mdns,omitempty"`
	Kubernetes *KubernetesConfig `json:"kubernetes,omitempty" yaml:"kubernetes,omitempty"`
	Static     []StaticPeer      `json:"static,omitempty" yaml:"static,omitempty"`
}

type MDNSConfig struct {
	Service string `json:"service" yaml:"service"`
	Domain  string `json:"domain" yaml:"domain"`
	Host    string `json:"host" yaml:"host"`
}

type KubernetesConfig struct {
	AuthMethod     AuthMethod `json:"auth_method" yaml:"auth_method"`
	KubeconfigPath string     `json:"kubeconfig_path,omitempty" yaml:"kubeconfig_path,omitempty"`
	Token          string     `json:"token,omitempty" yaml:"token,omitempty"`
	APIServer      string     `json:"api_server,omitempty" yaml:"api_server,omitempty"`

	Namespace         string            `json:"namespace" yaml:"namespace"`
	ServiceName       string            `json:"service_name,omitempty" yaml:"service_name,omitempty"`
	AnnotationFilters map[string]string `json:"annotation_filters,omitempty" yaml:"annotation_filters,omitempty"`
	FieldSelector     string            `json:"field_selector,omitempty" yaml:"field_selector,omitempty"`
	RequireReady      bool              `json:"require_ready" yaml:"require_ready"`

	Discovery DiscoveryStrategy `json:"discovery" yaml:"discovery"`
	PeerID    PeerIDStrategy    `json:"peer_id" yaml:"peer_id"`
	Port      PortStrategy      `json:"port" yaml:"port"`

	NetworkingMode NetworkingMode `json:"networking_mode" yaml:"networking_mode"`

	WatchInterval time.Duration `json:"watch_interval" yaml:"watch_interval"`
	RetryStrategy RetryStrategy `json:"retry_strategy" yaml:"retry_strategy"`
	BufferSize    int           `json:"buffer_size" yaml:"buffer_size"`
}

type StaticPeer struct {
	ID       string            `json:"id" yaml:"id"`
	Address  string            `json:"address" yaml:"address"`
	Port     int               `json:"port" yaml:"port"`
	Metadata map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type AuthMethod int

const (
	AuthInCluster AuthMethod = iota
	AuthKubeconfig
	AuthExplicitToken
)

type DiscoveryMethod int

const (
	DiscoveryLabelSelector DiscoveryMethod = iota
	DiscoveryService
	DiscoveryDNS
	DiscoveryStatefulSet
	DiscoveryNamespace
	DiscoverySiblings
)

type PeerIDSource int

const (
	PeerIDPodName PeerIDSource = iota
	PeerIDAnnotation
	PeerIDLabel
	PeerIDTemplate
)

type PortSource int

const (
	PortNamedPort PortSource = iota
	PortAnnotation
	PortFirstPort
	PortEnvVar
)

type NetworkingMode int

const (
	NetworkingPodIP NetworkingMode = iota
	NetworkingServiceIP
	NetworkingNodePort
)

type DiscoveryStrategy struct {
	Method          DiscoveryMethod   `json:"method" yaml:"method"`
	LabelSelector   map[string]string `json:"label_selector,omitempty" yaml:"label_selector,omitempty"`
	ServiceName     string            `json:"service_name,omitempty" yaml:"service_name,omitempty"`
	StatefulSetName string            `json:"stateful_set_name,omitempty" yaml:"stateful_set_name,omitempty"`
}

type PeerIDStrategy struct {
	Source   PeerIDSource `json:"source" yaml:"source"`
	Key      string       `json:"key,omitempty" yaml:"key,omitempty"`
	Template string       `json:"template,omitempty" yaml:"template,omitempty"`
}

type PortStrategy struct {
	Source        PortSource `json:"source" yaml:"source"`
	PortName      string     `json:"port_name,omitempty" yaml:"port_name,omitempty"`
	AnnotationKey string     `json:"annotation_key,omitempty" yaml:"annotation_key,omitempty"`
	EnvVarName    string     `json:"env_var_name,omitempty" yaml:"env_var_name,omitempty"`
	DefaultPort   int        `json:"default_port,omitempty" yaml:"default_port,omitempty"`
}

type RetryStrategy struct {
	MaxRetries    int           `json:"max_retries" yaml:"max_retries"`
	InitialDelay  time.Duration `json:"initial_delay" yaml:"initial_delay"`
	MaxDelay      time.Duration `json:"max_delay" yaml:"max_delay"`
	BackoffFactor float64       `json:"backoff_factor" yaml:"backoff_factor"`
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
	MaxConcurrentWorkflows int           `json:"max_concurrent_workflows" yaml:"max_concurrent_workflows"`
	NodeExecutionTimeout   time.Duration `json:"node_execution_timeout" yaml:"node_execution_timeout"`
	StateUpdateInterval    time.Duration `json:"state_update_interval" yaml:"state_update_interval"`
	RetryAttempts          int           `json:"retry_attempts" yaml:"retry_attempts"`
	RetryBackoff           time.Duration `json:"retry_backoff" yaml:"retry_backoff"`
}

type OrchestratorConfig struct {
	ShutdownTimeout time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
	StartupTimeout  time.Duration `json:"startup_timeout" yaml:"startup_timeout"`
	GracePeriod     time.Duration `json:"grace_period" yaml:"grace_period"`
}
