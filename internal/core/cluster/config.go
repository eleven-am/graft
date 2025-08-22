package cluster

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

const (
	EnvGraftNodeID           = "GRAFT_NODE_ID"
	EnvGraftServiceName      = "GRAFT_SERVICE_NAME"
	EnvGraftServicePort      = "GRAFT_SERVICE_PORT"
	EnvGraftAdvertiseAddress = "GRAFT_ADVERTISE_ADDRESS"
	EnvGraftDiscoveryStrategy = "GRAFT_DISCOVERY_STRATEGY"
	EnvGraftPeers            = "GRAFT_PEERS"
	
	EnvKubernetesServiceHost = "KUBERNETES_SERVICE_HOST"
	EnvKubernetesServicePort = "KUBERNETES_SERVICE_PORT"
	EnvPodIP                 = "POD_IP"
	EnvHostIP                = "HOST_IP"
	
	KubernetesServiceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount"
	KubernetesConfigPath         = "/etc/kubernetes"
	
	DefaultServiceName      = "graft-cluster"
	DefaultServicePort      = 8080
	DefaultDiscoveryStrategy = "auto"
	DefaultLocalhostAddress = "localhost"
	DefaultUDPTestAddress   = "8.8.8.8:80"
)

type ClusterConfig struct {
	NodeID           string `json:"node_id" yaml:"node_id"`
	ServiceName      string `json:"service_name" yaml:"service_name"`
	ServicePort      int    `json:"service_port" yaml:"service_port"`
	AdvertiseAddress string `json:"advertise_address" yaml:"advertise_address"`

	Discovery DiscoveryConfig `json:"discovery" yaml:"discovery"`
	Transport TransportConfig `json:"transport" yaml:"transport"`
	Storage   StorageConfig   `json:"storage" yaml:"storage"`
	Queue     QueueConfig     `json:"queue" yaml:"queue"`
	Resources ports.ResourceConfig `json:"resources" yaml:"resources"`
	Engine    EngineConfig    `json:"engine" yaml:"engine"`
	Health    HealthConfig    `json:"health" yaml:"health"`
}

type DiscoveryConfig struct {
	Strategy    discovery.Strategy `json:"strategy" yaml:"strategy"`
	ServiceName string             `json:"service_name" yaml:"service_name"`
	ServicePort int                `json:"service_port" yaml:"service_port"`
	Peers       []string           `json:"peers" yaml:"peers"`
	Metadata    map[string]string  `json:"metadata" yaml:"metadata"`
}

type TransportConfig struct {
	ListenAddress     string              `json:"listen_address" yaml:"listen_address"`
	ListenPort        int                 `json:"listen_port" yaml:"listen_port"`
	TLS               TLSConfig           `json:"tls" yaml:"tls"`
	MaxMessageSize    int                 `json:"max_message_size" yaml:"max_message_size"`
	ConnectionTimeout time.Duration       `json:"connection_timeout" yaml:"connection_timeout"`
	KeepAlive         KeepAliveConfig     `json:"keep_alive" yaml:"keep_alive"`
}

type TLSConfig struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	CertFile string `json:"cert_file" yaml:"cert_file"`
	KeyFile  string `json:"key_file" yaml:"key_file"`
	CAFile   string `json:"ca_file" yaml:"ca_file"`
}

type KeepAliveConfig struct {
	Time                time.Duration `json:"time" yaml:"time"`
	Timeout             time.Duration `json:"timeout" yaml:"timeout"`
	PermitWithoutStream bool          `json:"permit_without_stream" yaml:"permit_without_stream"`
}

type StorageConfig struct {
	ListenAddress     string `json:"listen_address" yaml:"listen_address"`
	ListenPort        int    `json:"listen_port" yaml:"listen_port"`
	DataDir           string `json:"data_dir" yaml:"data_dir"`
	LogLevel          string `json:"log_level" yaml:"log_level"`
	SnapshotRetention int    `json:"snapshot_retention" yaml:"snapshot_retention"`
	SnapshotThreshold uint64 `json:"snapshot_threshold" yaml:"snapshot_threshold"`
	TrailingLogs      uint64 `json:"trailing_logs" yaml:"trailing_logs"`
}

type QueueConfig struct {
	DataDir                 string        `json:"data_dir" yaml:"data_dir"`
	MaxSize                 int           `json:"max_size" yaml:"max_size"`
	SyncWrites              bool          `json:"sync_writes" yaml:"sync_writes"`
	ValueLogFileSize        int64         `json:"value_log_file_size" yaml:"value_log_file_size"`
	NumVersionsToKeep       int           `json:"num_versions_to_keep" yaml:"num_versions_to_keep"`
	CompactL0OnClose        bool          `json:"compact_l0_on_close" yaml:"compact_l0_on_close"`
	NumGoroutines           int           `json:"num_goroutines" yaml:"num_goroutines"`
	MemTableSize            int64         `json:"mem_table_size" yaml:"mem_table_size"`
	BaseTableSize           int64         `json:"base_table_size" yaml:"base_table_size"`
	BaseLevelSize           int64         `json:"base_level_size" yaml:"base_level_size"`
	LevelSizeMultiplier     int           `json:"level_size_multiplier" yaml:"level_size_multiplier"`
	TableSizeMultiplier     int           `json:"table_size_multiplier" yaml:"table_size_multiplier"`
	MaxLevels               int           `json:"max_levels" yaml:"max_levels"`
	VLogPercentile          float64       `json:"v_log_percentile" yaml:"v_log_percentile"`
	ValueThreshold          int64         `json:"value_threshold" yaml:"value_threshold"`
	NumMemTables            int           `json:"num_mem_tables" yaml:"num_mem_tables"`
	BlockSize               int           `json:"block_size" yaml:"block_size"`
	BloomFalsePositive      float64       `json:"bloom_false_positive" yaml:"bloom_false_positive"`
	BlockCacheSize          int64         `json:"block_cache_size" yaml:"block_cache_size"`
	IndexCacheSize          int64         `json:"index_cache_size" yaml:"index_cache_size"`
	NumLevelZeroTables      int           `json:"num_level_zero_tables" yaml:"num_level_zero_tables"`
	NumLevelZeroTablesStall int           `json:"num_level_zero_tables_stall" yaml:"num_level_zero_tables_stall"`
	ValueLogMaxEntries      uint32        `json:"value_log_max_entries" yaml:"value_log_max_entries"`
	DetectConflicts         bool          `json:"detect_conflicts" yaml:"detect_conflicts"`
	Claims                  ClaimsConfig  `json:"claims" yaml:"claims"`
}

type ClaimsConfig struct {
	Enabled                bool          `json:"enabled" yaml:"enabled"`
	DefaultClaimDuration   time.Duration `json:"default_claim_duration" yaml:"default_claim_duration"`
	MaxClaimDuration       time.Duration `json:"max_claim_duration" yaml:"max_claim_duration"`
	CleanupInterval        time.Duration `json:"cleanup_interval" yaml:"cleanup_interval"`
	ExpiredClaimGracePeriod time.Duration `json:"expired_claim_grace_period" yaml:"expired_claim_grace_period"`
	EnableMetrics          bool          `json:"enable_metrics" yaml:"enable_metrics"`
}

type EngineConfig struct {
	MaxConcurrentWorkflows int           `json:"max_concurrent_workflows" yaml:"max_concurrent_workflows"`
	NodeExecutionTimeout   time.Duration `json:"node_execution_timeout" yaml:"node_execution_timeout"`
	StateUpdateInterval    time.Duration `json:"state_update_interval" yaml:"state_update_interval"`
	RetryAttempts          int           `json:"retry_attempts" yaml:"retry_attempts"`
	RetryBackoff           time.Duration `json:"retry_backoff" yaml:"retry_backoff"`
}

type HealthConfig struct {
	Enabled              bool                     `json:"enabled" yaml:"enabled"`
	CheckInterval        time.Duration            `json:"check_interval" yaml:"check_interval"`
	Timeout              time.Duration            `json:"timeout" yaml:"timeout"`
	MaxConsecutiveFailures int                     `json:"max_consecutive_failures" yaml:"max_consecutive_failures"`
	Components           ComponentHealthConfig    `json:"components" yaml:"components"`
}

type ComponentHealthConfig struct {
	Discovery       ComponentCheckConfig `json:"discovery" yaml:"discovery"`
	Transport       ComponentCheckConfig `json:"transport" yaml:"transport"`
	Storage         ComponentCheckConfig `json:"storage" yaml:"storage"`
	Queue           ComponentCheckConfig `json:"queue" yaml:"queue"`
	NodeRegistry    ComponentCheckConfig `json:"node_registry" yaml:"node_registry"`
	ResourceManager ComponentCheckConfig `json:"resource_manager" yaml:"resource_manager"`
	WorkflowEngine  ComponentCheckConfig `json:"workflow_engine" yaml:"workflow_engine"`
}

type ComponentCheckConfig struct {
	Enabled                bool          `json:"enabled" yaml:"enabled"`
	Timeout                time.Duration `json:"timeout" yaml:"timeout"`
	MaxConsecutiveFailures int           `json:"max_consecutive_failures" yaml:"max_consecutive_failures"`
}

func DefaultClusterConfig() ClusterConfig {
	nodeID := getEnvOrDefault(EnvGraftNodeID, generateNodeID())
	serviceName := getEnvOrDefault(EnvGraftServiceName, DefaultServiceName)
	servicePort := getEnvIntOrDefault(EnvGraftServicePort, DefaultServicePort)
	advertiseAddress := getAdvertiseAddress()

	return ClusterConfig{
		NodeID:           nodeID,
		ServiceName:      serviceName,
		ServicePort:      servicePort,
		AdvertiseAddress: advertiseAddress,
		Discovery:        DefaultDiscoveryConfig(serviceName, servicePort),
		Transport:        DefaultTransportConfig(),
		Storage:          DefaultStorageConfig(),
		Queue:            DefaultQueueConfig(),
		Resources:        DefaultResourceConfig(),
		Engine:           DefaultEngineConfig(),
		Health:           DefaultHealthConfig(),
	}
}

func DefaultDiscoveryConfig(serviceName string, servicePort int) DiscoveryConfig {
	strategyStr := getEnvOrDefault(EnvGraftDiscoveryStrategy, DefaultDiscoveryStrategy)
	return DiscoveryConfig{
		Strategy:    discovery.Strategy(strategyStr),
		ServiceName: serviceName,
		ServicePort: servicePort,
		Peers:       []string{},
		Metadata:    make(map[string]string),
	}
}

func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		ListenAddress:     getEnvOrDefault("GRAFT_TRANSPORT_LISTEN_ADDRESS", "0.0.0.0"),
		ListenPort:        getEnvIntOrDefault("GRAFT_TRANSPORT_LISTEN_PORT", 9090),
		MaxMessageSize:    getEnvIntOrDefault("GRAFT_TRANSPORT_MAX_MESSAGE_SIZE", 4*1024*1024),
		ConnectionTimeout: getEnvDurationOrDefault("GRAFT_TRANSPORT_CONNECTION_TIMEOUT", 30*time.Second),
		TLS: TLSConfig{
			Enabled:  getEnvBoolOrDefault("GRAFT_TRANSPORT_TLS_ENABLED", false),
			CertFile: getEnvOrDefault("GRAFT_TRANSPORT_TLS_CERT_FILE", ""),
			KeyFile:  getEnvOrDefault("GRAFT_TRANSPORT_TLS_KEY_FILE", ""),
			CAFile:   getEnvOrDefault("GRAFT_TRANSPORT_TLS_CA_FILE", ""),
		},
		KeepAlive: KeepAliveConfig{
			Time:                getEnvDurationOrDefault("GRAFT_TRANSPORT_KEEPALIVE_TIME", 30*time.Second),
			Timeout:             getEnvDurationOrDefault("GRAFT_TRANSPORT_KEEPALIVE_TIMEOUT", 5*time.Second),
			PermitWithoutStream: getEnvBoolOrDefault("GRAFT_TRANSPORT_KEEPALIVE_PERMIT_WITHOUT_STREAM", true),
		},
	}
}

func DefaultStorageConfig() StorageConfig {
	return StorageConfig{
		ListenAddress:     getEnvOrDefault("GRAFT_STORAGE_LISTEN_ADDRESS", "127.0.0.1"),
		ListenPort:        getEnvIntOrDefault("GRAFT_STORAGE_LISTEN_PORT", 7000),
		DataDir:           getEnvOrDefault("GRAFT_STORAGE_DATA_DIR", "./data/raft"),
		LogLevel:          getEnvOrDefault("GRAFT_STORAGE_LOG_LEVEL", "INFO"),
		SnapshotRetention: getEnvIntOrDefault("GRAFT_STORAGE_SNAPSHOT_RETENTION", 2),
		SnapshotThreshold: uint64(getEnvIntOrDefault("GRAFT_STORAGE_SNAPSHOT_THRESHOLD", 8192)),
		TrailingLogs:      uint64(getEnvIntOrDefault("GRAFT_STORAGE_TRAILING_LOGS", 10240)),
	}
}

func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		DataDir:                 getEnvOrDefault("GRAFT_QUEUE_DATA_DIR", "./data/queue"),
		MaxSize:                 getEnvIntOrDefault("GRAFT_QUEUE_MAX_SIZE", 10000),
		SyncWrites:              getEnvBoolOrDefault("GRAFT_QUEUE_SYNC_WRITES", false),
		ValueLogFileSize:        int64(getEnvIntOrDefault("GRAFT_QUEUE_VALUE_LOG_FILE_SIZE", 1073741823)),
		NumVersionsToKeep:       getEnvIntOrDefault("GRAFT_QUEUE_NUM_VERSIONS_TO_KEEP", 1),
		CompactL0OnClose:        getEnvBoolOrDefault("GRAFT_QUEUE_COMPACT_L0_ON_CLOSE", false),
		NumGoroutines:           getEnvIntOrDefault("GRAFT_QUEUE_NUM_GOROUTINES", 8),
		MemTableSize:            int64(getEnvIntOrDefault("GRAFT_QUEUE_MEM_TABLE_SIZE", 67108864)),
		BaseTableSize:           int64(getEnvIntOrDefault("GRAFT_QUEUE_BASE_TABLE_SIZE", 2097152)),
		BaseLevelSize:           int64(getEnvIntOrDefault("GRAFT_QUEUE_BASE_LEVEL_SIZE", 10485760)),
		LevelSizeMultiplier:     getEnvIntOrDefault("GRAFT_QUEUE_LEVEL_SIZE_MULTIPLIER", 10),
		TableSizeMultiplier:     getEnvIntOrDefault("GRAFT_QUEUE_TABLE_SIZE_MULTIPLIER", 2),
		MaxLevels:               getEnvIntOrDefault("GRAFT_QUEUE_MAX_LEVELS", 7),
		VLogPercentile:          getEnvFloatOrDefault("GRAFT_QUEUE_V_LOG_PERCENTILE", 0.0),
		ValueThreshold:          int64(getEnvIntOrDefault("GRAFT_QUEUE_VALUE_THRESHOLD", 32)),
		NumMemTables:            getEnvIntOrDefault("GRAFT_QUEUE_NUM_MEM_TABLES", 5),
		BlockSize:               getEnvIntOrDefault("GRAFT_QUEUE_BLOCK_SIZE", 4096),
		BloomFalsePositive:      getEnvFloatOrDefault("GRAFT_QUEUE_BLOOM_FALSE_POSITIVE", 0.01),
		BlockCacheSize:          int64(getEnvIntOrDefault("GRAFT_QUEUE_BLOCK_CACHE_SIZE", 256*1024*1024)),
		IndexCacheSize:          int64(getEnvIntOrDefault("GRAFT_QUEUE_INDEX_CACHE_SIZE", 0)),
		NumLevelZeroTables:      getEnvIntOrDefault("GRAFT_QUEUE_NUM_LEVEL_ZERO_TABLES", 5),
		NumLevelZeroTablesStall: getEnvIntOrDefault("GRAFT_QUEUE_NUM_LEVEL_ZERO_TABLES_STALL", 15),
		ValueLogMaxEntries:      uint32(getEnvIntOrDefault("GRAFT_QUEUE_VALUE_LOG_MAX_ENTRIES", 1000000)),
		DetectConflicts:         getEnvBoolOrDefault("GRAFT_QUEUE_DETECT_CONFLICTS", true),
		Claims:                  DefaultClaimsConfig(),
	}
}

func DefaultClaimsConfig() ClaimsConfig {
	return ClaimsConfig{
		Enabled:                getEnvBoolOrDefault("GRAFT_CLAIMS_ENABLED", true),
		DefaultClaimDuration:   getEnvDurationOrDefault("GRAFT_CLAIMS_DEFAULT_DURATION", 5*time.Minute),
		MaxClaimDuration:       getEnvDurationOrDefault("GRAFT_CLAIMS_MAX_DURATION", 30*time.Minute),
		CleanupInterval:        getEnvDurationOrDefault("GRAFT_CLAIMS_CLEANUP_INTERVAL", 1*time.Minute),
		ExpiredClaimGracePeriod: getEnvDurationOrDefault("GRAFT_CLAIMS_EXPIRED_GRACE_PERIOD", 30*time.Second),
		EnableMetrics:          getEnvBoolOrDefault("GRAFT_CLAIMS_ENABLE_METRICS", true),
	}
}

func DefaultResourceConfig() ports.ResourceConfig {
	return ports.ResourceConfig{
		MaxConcurrentTotal:   getEnvIntOrDefault("GRAFT_RESOURCES_MAX_CONCURRENT_TOTAL", 50),
		MaxConcurrentPerType: make(map[string]int),
		DefaultPerTypeLimit:  getEnvIntOrDefault("GRAFT_RESOURCES_DEFAULT_PER_TYPE_LIMIT", 10),
	}
}

func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		MaxConcurrentWorkflows: getEnvIntOrDefault("GRAFT_ENGINE_MAX_CONCURRENT_WORKFLOWS", 100),
		NodeExecutionTimeout:   getEnvDurationOrDefault("GRAFT_ENGINE_NODE_EXECUTION_TIMEOUT", 5*time.Minute),
		StateUpdateInterval:    getEnvDurationOrDefault("GRAFT_ENGINE_STATE_UPDATE_INTERVAL", 30*time.Second),
		RetryAttempts:          getEnvIntOrDefault("GRAFT_ENGINE_RETRY_ATTEMPTS", 3),
		RetryBackoff:           getEnvDurationOrDefault("GRAFT_ENGINE_RETRY_BACKOFF", 5*time.Second),
	}
}

func DefaultHealthConfig() HealthConfig {
	defaultComponentConfig := ComponentCheckConfig{
		Enabled:                getEnvBoolOrDefault("GRAFT_HEALTH_COMPONENT_ENABLED", true),
		Timeout:                getEnvDurationOrDefault("GRAFT_HEALTH_COMPONENT_TIMEOUT", 5*time.Second),
		MaxConsecutiveFailures: getEnvIntOrDefault("GRAFT_HEALTH_COMPONENT_MAX_FAILURES", 3),
	}

	return HealthConfig{
		Enabled:              getEnvBoolOrDefault("GRAFT_HEALTH_ENABLED", true),
		CheckInterval:        getEnvDurationOrDefault("GRAFT_HEALTH_CHECK_INTERVAL", 30*time.Second),
		Timeout:              getEnvDurationOrDefault("GRAFT_HEALTH_TIMEOUT", 10*time.Second),
		MaxConsecutiveFailures: getEnvIntOrDefault("GRAFT_HEALTH_MAX_FAILURES", 5),
		Components: ComponentHealthConfig{
			Discovery:       defaultComponentConfig,
			Transport:       defaultComponentConfig,
			Storage:         defaultComponentConfig,
			Queue:           defaultComponentConfig,
			NodeRegistry:    defaultComponentConfig,
			ResourceManager: defaultComponentConfig,
			WorkflowEngine:  defaultComponentConfig,
		},
	}
}

func ValidateClusterConfig(config ClusterConfig) error {
	if config.NodeID == "" {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "node_id is required",
		}
	}

	if config.ServiceName == "" {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "service_name is required",
		}
	}

	if config.ServicePort <= 0 || config.ServicePort > 65535 {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "service_port must be between 1 and 65535",
			Details: map[string]interface{}{
				"service_port": config.ServicePort,
			},
		}
	}

	if err := validateDiscoveryConfig(config.Discovery); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "discovery configuration validation failed",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if err := validateTransportConfig(config.Transport); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "transport configuration validation failed",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if err := validateStorageConfig(config.Storage); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "storage configuration validation failed",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if err := validateQueueConfig(config.Queue); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "queue configuration validation failed",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if err := validateEngineConfig(config.Engine); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "engine configuration validation failed",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if err := validateHealthConfig(config.Health); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "health configuration validation failed",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return nil
}

func validateDiscoveryConfig(config DiscoveryConfig) error {
	validStrategies := map[discovery.Strategy]bool{
		discovery.StrategyAuto:       true,
		discovery.StrategyStatic:     true,
		discovery.StrategyMDNS:       true,
		discovery.StrategyKubernetes: true,
	}

	if !validStrategies[config.Strategy] {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "invalid discovery strategy",
			Details: map[string]interface{}{
				"strategy":         config.Strategy,
				"valid_strategies": []string{"auto", "static", "mdns", "kubernetes"},
			},
		}
	}

	return nil
}

func validateTransportConfig(config TransportConfig) error {
	if config.ListenPort <= 0 || config.ListenPort > 65535 {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "transport listen_port must be between 1 and 65535",
			Details: map[string]interface{}{
				"listen_port": config.ListenPort,
			},
		}
	}

	if config.MaxMessageSize <= 0 {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "transport max_message_size must be greater than 0",
			Details: map[string]interface{}{
				"max_message_size": config.MaxMessageSize,
			},
		}
	}

	if config.TLS.Enabled {
		if config.TLS.CertFile == "" || config.TLS.KeyFile == "" {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "TLS cert_file and key_file are required when TLS is enabled",
			}
		}
	}

	return nil
}

func validateStorageConfig(config StorageConfig) error {
	if config.ListenPort <= 0 || config.ListenPort > 65535 {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "storage listen_port must be between 1 and 65535",
			Details: map[string]interface{}{
				"listen_port": config.ListenPort,
			},
		}
	}

	if config.DataDir == "" {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "storage data_dir is required",
		}
	}

	return nil
}

func validateQueueConfig(config QueueConfig) error {
	if config.DataDir == "" {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "queue data_dir is required",
		}
	}

	if err := validateClaimsConfig(config.Claims); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "claims configuration validation failed",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return nil
}

func validateClaimsConfig(config ClaimsConfig) error {
	if config.Enabled {
		if config.DefaultClaimDuration <= 0 {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "claims default_claim_duration must be greater than 0 when claims are enabled",
				Details: map[string]interface{}{
					"default_claim_duration": config.DefaultClaimDuration,
				},
			}
		}

		if config.MaxClaimDuration <= 0 {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "claims max_claim_duration must be greater than 0 when claims are enabled",
				Details: map[string]interface{}{
					"max_claim_duration": config.MaxClaimDuration,
				},
			}
		}

		if config.DefaultClaimDuration > config.MaxClaimDuration {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "claims default_claim_duration cannot be greater than max_claim_duration",
				Details: map[string]interface{}{
					"default_claim_duration": config.DefaultClaimDuration,
					"max_claim_duration":     config.MaxClaimDuration,
				},
			}
		}

		if config.CleanupInterval <= 0 {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "claims cleanup_interval must be greater than 0 when claims are enabled",
				Details: map[string]interface{}{
					"cleanup_interval": config.CleanupInterval,
				},
			}
		}
	}

	return nil
}

func validateEngineConfig(config EngineConfig) error {
	if config.MaxConcurrentWorkflows <= 0 {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "engine max_concurrent_workflows must be greater than 0",
			Details: map[string]interface{}{
				"max_concurrent_workflows": config.MaxConcurrentWorkflows,
			},
		}
	}

	if config.NodeExecutionTimeout <= 0 {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "engine node_execution_timeout must be greater than 0",
			Details: map[string]interface{}{
				"node_execution_timeout": config.NodeExecutionTimeout,
			},
		}
	}

	return nil
}

func validateHealthConfig(config HealthConfig) error {
	if config.Enabled {
		if config.CheckInterval <= 0 {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "health check_interval must be greater than 0 when health monitoring is enabled",
				Details: map[string]interface{}{
					"check_interval": config.CheckInterval,
				},
			}
		}

		if config.Timeout <= 0 {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "health timeout must be greater than 0 when health monitoring is enabled",
				Details: map[string]interface{}{
					"timeout": config.Timeout,
				},
			}
		}

		if config.MaxConsecutiveFailures <= 0 {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "health max_consecutive_failures must be greater than 0 when health monitoring is enabled",
				Details: map[string]interface{}{
					"max_consecutive_failures": config.MaxConsecutiveFailures,
				},
			}
		}

		if config.Timeout >= config.CheckInterval {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "health timeout cannot be greater than or equal to check_interval",
				Details: map[string]interface{}{
					"timeout":        config.Timeout,
					"check_interval": config.CheckInterval,
				},
			}
		}
	}

	return nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBoolOrDefault(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getAdvertiseAddress() string {
	advertiseAddr := getEnvOrDefault(EnvGraftAdvertiseAddress, "")
	if advertiseAddr != "" {
		return advertiseAddr
	}

	if isKubernetesEnvironment() {
		if podIP := os.Getenv(EnvPodIP); podIP != "" {
			return podIP
		}
		if hostIP := os.Getenv(EnvHostIP); hostIP != "" {
			return hostIP
		}
		if kubernetesHost := os.Getenv(EnvKubernetesServiceHost); kubernetesHost != "" {
			return kubernetesHost
		}
	}

	if localIP := getLocalIP(); localIP != "" {
		return localIP
	}

	return DefaultLocalhostAddress
}

func isKubernetesEnvironment() bool {
	if os.Getenv(EnvKubernetesServiceHost) != "" && os.Getenv(EnvKubernetesServicePort) != "" {
		return true
	}

	if _, err := os.Stat(KubernetesServiceAccountPath); err == nil {
		return true
	}

	if _, err := os.Stat(KubernetesConfigPath); err == nil {
		return true
	}

	return false
}

func getLocalIP() string {
	conn, err := net.Dial("udp", DefaultUDPTestAddress)
	if err != nil {
		return ""
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func getEnvFloatOrDefault(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			return floatValue
		}
	}
	return defaultValue
}

func getEnvDurationOrDefault(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func generateNodeID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano()%100000)
}