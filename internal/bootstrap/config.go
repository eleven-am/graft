package bootstrap

import (
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

type BootstrapConfig struct {
	ServiceName string `json:"service_name" yaml:"service_name"`
	Namespace   string `json:"namespace" yaml:"namespace"`
	PodName     string `json:"pod_name" yaml:"pod_name"`
	DataDir     string `json:"data_dir" yaml:"data_dir"`
	Ordinal     int    `json:"ordinal" yaml:"ordinal"`

	ExpectedNodes int `json:"expected_nodes" yaml:"expected_nodes"`
	MinQuorum     int `json:"min_quorum" yaml:"min_quorum"`

	RaftPort        int    `json:"raft_port" yaml:"raft_port"`
	JoinPort        int    `json:"join_port" yaml:"join_port"`
	BindAddr        string `json:"bind_addr" yaml:"bind_addr"`
	AdvertiseAddr   string `json:"advertise_addr,omitempty" yaml:"advertise_addr,omitempty"`
	HeadlessService string `json:"headless_service,omitempty" yaml:"headless_service,omitempty"`

	BootstrapTimeout       time.Duration `json:"bootstrap_timeout" yaml:"bootstrap_timeout"`
	JoinTimeout            time.Duration `json:"join_timeout" yaml:"join_timeout"`
	LeaderWaitTimeout      time.Duration `json:"leader_wait_timeout" yaml:"leader_wait_timeout"`
	FallbackElectionWindow time.Duration `json:"fallback_election_window" yaml:"fallback_election_window"`
	HealthCheckInterval    time.Duration `json:"health_check_interval" yaml:"health_check_interval"`
	StartupTimeout         time.Duration `json:"startup_timeout" yaml:"startup_timeout"`
	DiscoveryCacheTTL      time.Duration `json:"discovery_cache_ttl" yaml:"discovery_cache_ttl"`
	PeerProbeTimeout       time.Duration `json:"peer_probe_timeout" yaml:"peer_probe_timeout"`

	Retry          RetryConfig          `json:"retry" yaml:"retry"`
	TLS            TLSConfig            `json:"tls" yaml:"tls"`
	Secrets        SecretsConfig        `json:"secrets" yaml:"secrets"`
	Membership     MembershipConfig     `json:"membership" yaml:"membership"`
	Metrics        MetricsConfig        `json:"metrics" yaml:"metrics"`
	ForceBootstrap ForceBootstrapConfig `json:"force_bootstrap,omitempty" yaml:"force_bootstrap,omitempty"`

	ForceBootstrapFile     string `json:"force_bootstrap_file,omitempty" yaml:"force_bootstrap_file,omitempty"`
	RequireProtocolVersion bool   `json:"require_protocol_version" yaml:"require_protocol_version"`
}

func (c *BootstrapConfig) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}
	if c.Ordinal < 0 {
		return fmt.Errorf("ordinal must be non-negative")
	}
	if c.ExpectedNodes < 1 {
		return fmt.Errorf("expected_nodes must be at least 1")
	}
	if c.MinQuorum > c.ExpectedNodes {
		return fmt.Errorf("min_quorum (%d) cannot exceed expected_nodes (%d)", c.MinQuorum, c.ExpectedNodes)
	}
	if c.RaftPort <= 0 || c.RaftPort > 65535 {
		return fmt.Errorf("raft_port must be between 1 and 65535")
	}
	if c.JoinPort <= 0 || c.JoinPort > 65535 {
		return fmt.Errorf("join_port must be between 1 and 65535")
	}
	if c.BootstrapTimeout <= 0 {
		return fmt.Errorf("bootstrap_timeout must be positive")
	}
	if c.JoinTimeout <= 0 {
		return fmt.Errorf("join_timeout must be positive")
	}
	if c.LeaderWaitTimeout <= 0 {
		return fmt.Errorf("leader_wait_timeout must be positive")
	}
	if err := c.Retry.Validate(); err != nil {
		return fmt.Errorf("retry config: %w", err)
	}
	if err := c.TLS.Validate(); err != nil {
		return fmt.Errorf("tls config: %w", err)
	}
	if err := c.Membership.Validate(); err != nil {
		return fmt.Errorf("membership config: %w", err)
	}
	return nil
}

func (c *BootstrapConfig) CalculateQuorum() int {
	if c.MinQuorum > 0 {
		return c.MinQuorum
	}
	return (c.ExpectedNodes / 2) + 1
}

type RetryConfig struct {
	InitialDelay   time.Duration `json:"initial_delay" yaml:"initial_delay"`
	MaxDelay       time.Duration `json:"max_delay" yaml:"max_delay"`
	Multiplier     float64       `json:"multiplier" yaml:"multiplier"`
	MaxAttempts    int           `json:"max_attempts" yaml:"max_attempts"`
	JitterFraction float64       `json:"jitter_fraction" yaml:"jitter_fraction"`
}

func (c *RetryConfig) Validate() error {
	if c.InitialDelay <= 0 {
		return fmt.Errorf("initial_delay must be positive")
	}
	if c.MaxDelay < c.InitialDelay {
		return fmt.Errorf("max_delay must be >= initial_delay")
	}
	if c.Multiplier < 1.0 {
		return fmt.Errorf("multiplier must be >= 1.0")
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("max_attempts must be at least 1")
	}
	if c.JitterFraction < 0 || c.JitterFraction > 1 {
		return fmt.Errorf("jitter_fraction must be between 0 and 1")
	}
	return nil
}

type TLSConfig struct {
	Enabled       bool               `json:"enabled" yaml:"enabled"`
	Required      bool               `json:"required" yaml:"required"`
	AllowInsecure bool               `json:"allow_insecure" yaml:"allow_insecure"`
	CertFile      string             `json:"cert_file" yaml:"cert_file"`
	KeyFile       string             `json:"key_file" yaml:"key_file"`
	CAFile        string             `json:"ca_file" yaml:"ca_file"`
	ClientAuth    tls.ClientAuthType `json:"client_auth" yaml:"client_auth"`
	ServerName    string             `json:"server_name,omitempty" yaml:"server_name,omitempty"`
	MinVersion    uint16             `json:"min_version" yaml:"min_version"`
	AllowedSANs   []string           `json:"allowed_sans,omitempty" yaml:"allowed_sans,omitempty"`
	CRLFile       string             `json:"crl_file,omitempty" yaml:"crl_file,omitempty"`
	OCSPEnabled   bool               `json:"ocsp_enabled" yaml:"ocsp_enabled"`
}

func (c *TLSConfig) Validate() error {
	if c.Required && !c.Enabled {
		return fmt.Errorf("tls cannot be required when not enabled")
	}
	if !c.Enabled {
		return nil
	}
	if c.CertFile == "" {
		return fmt.Errorf("cert_file is required when TLS is enabled")
	}
	if c.KeyFile == "" {
		return fmt.Errorf("key_file is required when TLS is enabled")
	}
	if c.CAFile == "" {
		return fmt.Errorf("ca_file is required when TLS is enabled")
	}
	return nil
}

type SecretsConfig struct {
	FencingKeyFile        string `json:"fencing_key_file" yaml:"fencing_key_file"`
	AdminTokenFile        string `json:"admin_token_file" yaml:"admin_token_file"`
	WipeKeyFile           string `json:"wipe_key_file" yaml:"wipe_key_file"`
	ForceBootstrapKeyFile string `json:"force_bootstrap_key_file,omitempty" yaml:"force_bootstrap_key_file,omitempty"`
}

type ForceBootstrapConfig struct {
	RequireDedicatedKey   bool   `json:"require_dedicated_key" yaml:"require_dedicated_key"`
	ServerIDPattern       string `json:"server_id_pattern" yaml:"server_id_pattern"`
	AllowDRQuorumOverride bool   `json:"allow_dr_quorum_override" yaml:"allow_dr_quorum_override"`
}

func (c *ForceBootstrapConfig) GetServerIDPattern() string {
	if c == nil || c.ServerIDPattern == "" {
		return "node-%d"
	}
	return c.ServerIDPattern
}

type MembershipConfig struct {
	LearnerCatchupThreshold uint64        `json:"learner_catchup_threshold" yaml:"learner_catchup_threshold"`
	SnapshotCatchupTimeout  time.Duration `json:"snapshot_catchup_timeout" yaml:"snapshot_catchup_timeout"`
	SnapshotCompleteGate    bool          `json:"snapshot_complete_gate" yaml:"snapshot_complete_gate"`
	JoinRateLimit           rate.Limit    `json:"join_rate_limit" yaml:"join_rate_limit"`
	JoinBurst               int           `json:"join_burst" yaml:"join_burst"`
	MaxConcurrentJoins      int           `json:"max_concurrent_joins" yaml:"max_concurrent_joins"`
	MaxLogSizeForJoin       uint64        `json:"max_log_size_for_join" yaml:"max_log_size_for_join"`
	DiskPressureThreshold   float64       `json:"disk_pressure_threshold" yaml:"disk_pressure_threshold"`
	DataDir                 string        `json:"data_dir" yaml:"data_dir"`

	VersionCompatibility VersionCompatibilityConfig `json:"version_compatibility" yaml:"version_compatibility"`
}

func (c *MembershipConfig) Validate() error {
	if c.JoinBurst < 1 {
		return fmt.Errorf("join_burst must be at least 1")
	}
	if c.MaxConcurrentJoins < 1 {
		return fmt.Errorf("max_concurrent_joins must be at least 1")
	}
	if c.DiskPressureThreshold < 0 || c.DiskPressureThreshold > 1 {
		return fmt.Errorf("disk_pressure_threshold must be between 0 and 1")
	}
	return nil
}

type VersionCompatibilityConfig struct {
	MinRaftProtocolVersion uint32   `json:"min_raft_protocol_version" yaml:"min_raft_protocol_version"`
	MaxRaftProtocolVersion uint32   `json:"max_raft_protocol_version" yaml:"max_raft_protocol_version"`
	MinStateMachineVersion uint32   `json:"min_state_machine_version" yaml:"min_state_machine_version"`
	MaxStateMachineVersion uint32   `json:"max_state_machine_version" yaml:"max_state_machine_version"`
	RequiredFeatureFlags   []string `json:"required_feature_flags,omitempty" yaml:"required_feature_flags,omitempty"`
	BlockedVersionPatterns []string `json:"blocked_version_patterns,omitempty" yaml:"blocked_version_patterns,omitempty"`
	AllowDowngrade         bool     `json:"allow_downgrade" yaml:"allow_downgrade"`
}

type MetricsConfig struct {
	Namespace string `json:"namespace" yaml:"namespace"`
	Subsystem string `json:"subsystem" yaml:"subsystem"`
}

func ExtractOrdinalFromPodName(podName string) (int, error) {
	parts := strings.Split(podName, "-")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid pod name format: %s", podName)
	}

	ordinalStr := parts[len(parts)-1]
	ordinal, err := strconv.Atoi(ordinalStr)
	if err != nil {
		return 0, fmt.Errorf("invalid ordinal in pod name %s: %w", podName, err)
	}

	return ordinal, nil
}
