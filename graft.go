package graft

import (
	"context"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery"
	"github.com/eleven-am/graft/internal/api"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

const (
	StrategyAuto       = discovery.StrategyAuto
	StrategyStatic     = discovery.StrategyStatic
	StrategyMDNS       = discovery.StrategyMDNS
	StrategyKubernetes = discovery.StrategyKubernetes
)

type NodeResult = ports.NodeResult
type NextNode = ports.NextNode

type Config struct {
	NodeID      string `json:"node_id" yaml:"node_id"`
	ServiceName string `json:"service_name" yaml:"service_name"`
	ServicePort int    `json:"service_port" yaml:"service_port"`

	Discovery DiscoveryConfig `json:"discovery" yaml:"discovery"`
	Transport TransportConfig `json:"transport" yaml:"transport"`
	Storage   StorageConfig   `json:"storage" yaml:"storage"`
	Queue     QueueConfig     `json:"queue" yaml:"queue"`
	Resources ResourceConfig  `json:"resources" yaml:"resources"`
	Engine    EngineConfig    `json:"engine" yaml:"engine"`

	LogLevel string       `json:"log_level" yaml:"log_level"`
	Logger   *slog.Logger `json:"-" yaml:"-"`
}

type DiscoveryConfig struct {
	Strategy    discovery.Strategy `json:"strategy" yaml:"strategy"`
	ServiceName string             `json:"service_name" yaml:"service_name"`
	ServicePort int                `json:"service_port" yaml:"service_port"`
	Peers       []string           `json:"peers" yaml:"peers"`
	Metadata    map[string]string  `json:"metadata" yaml:"metadata"`
}

type TransportConfig struct {
	ListenAddress     string `json:"listen_address" yaml:"listen_address"`
	ListenPort        int    `json:"listen_port" yaml:"listen_port"`
	EnableTLS         bool   `json:"enable_tls" yaml:"enable_tls"`
	TLSCertFile       string `json:"tls_cert_file" yaml:"tls_cert_file"`
	TLSKeyFile        string `json:"tls_key_file" yaml:"tls_key_file"`
	TLSCAFile         string `json:"tls_ca_file" yaml:"tls_ca_file"`
	MaxMessageSizeMB  int    `json:"max_message_size_mb" yaml:"max_message_size_mb"`
	ConnectionTimeout string `json:"connection_timeout" yaml:"connection_timeout"`
}

type StorageConfig struct {
	ListenAddress     string `json:"listen_address" yaml:"listen_address"`
	ListenPort        int    `json:"listen_port" yaml:"listen_port"`
	DataDir           string `json:"data_dir" yaml:"data_dir"`
	LogLevel          string `json:"log_level" yaml:"log_level"`
	SnapshotRetention int    `json:"snapshot_retention" yaml:"snapshot_retention"`
	SnapshotThreshold int    `json:"snapshot_threshold" yaml:"snapshot_threshold"`
	TrailingLogs      int    `json:"trailing_logs" yaml:"trailing_logs"`
}

type QueueConfig struct {
	DataDir            string  `json:"data_dir" yaml:"data_dir"`
	SyncWrites         bool    `json:"sync_writes" yaml:"sync_writes"`
	ValueLogFileSizeMB int     `json:"value_log_file_size_mb" yaml:"value_log_file_size_mb"`
	MemTableSizeMB     int     `json:"mem_table_size_mb" yaml:"mem_table_size_mb"`
	NumGoroutines      int     `json:"num_goroutines" yaml:"num_goroutines"`
	BloomFalsePositive float64 `json:"bloom_false_positive" yaml:"bloom_false_positive"`
}

type ResourceConfig struct {
	MaxConcurrentTotal   int            `json:"max_concurrent_total" yaml:"max_concurrent_total"`
	MaxConcurrentPerType map[string]int `json:"max_concurrent_per_type" yaml:"max_concurrent_per_type"`
	DefaultPerTypeLimit  int            `json:"default_per_type_limit" yaml:"default_per_type_limit"`
}

type EngineConfig struct {
	MaxConcurrentWorkflows int    `json:"max_concurrent_workflows" yaml:"max_concurrent_workflows"`
	NodeExecutionTimeout   string `json:"node_execution_timeout" yaml:"node_execution_timeout"`
	StateUpdateInterval    string `json:"state_update_interval" yaml:"state_update_interval"`
	RetryAttempts          int    `json:"retry_attempts" yaml:"retry_attempts"`
	RetryBackoff           string `json:"retry_backoff" yaml:"retry_backoff"`
}

type NextNodeConfig struct {
	NodeName       string         `json:"node_name"`
	Config         interface{}    `json:"config"`
	Priority       int            `json:"priority,omitempty"`
	Delay          *time.Duration `json:"delay,omitempty"`
	IdempotencyKey *string        `json:"idempotency_key,omitempty"`
}

type NodeMetadata struct {
	ExecutionTime time.Duration     `json:"execution_time,omitempty"`
	ResourceUsage interface{}       `json:"resource_usage,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
}

type WorkflowTrigger struct {
	WorkflowID   string            `json:"workflow_id"`
	InitialState interface{}       `json:"initial_state"`
	InitialNodes []NodeConfig      `json:"initial_nodes"`
	Metadata     map[string]string `json:"metadata"`
}

type NodeConfig struct {
	Name   string      `json:"name"`
	Config interface{} `json:"config"`
}

type WorkflowState = ports.WorkflowStatus

type ClusterInfo struct {
	NodeID          string          `json:"node_id"`
	RegisteredNodes []string        `json:"registered_nodes"`
	ActiveWorkflows int64           `json:"active_workflows"`
	ResourceLimits  ResourceConfig  `json:"resource_limits"`
	ExecutionStats  ExecutionStats  `json:"execution_stats"`
	EngineMetrics   EngineMetrics   `json:"engine_metrics"`
	ClusterMembers  []ClusterMember `json:"cluster_members"`
	IsLeader        bool            `json:"is_leader"`
}

type ExecutionStats struct {
	TotalExecuting   int            `json:"total_executing"`
	TotalCapacity    int            `json:"total_capacity"`
	AvailableSlots   int            `json:"available_slots"`
	PerTypeExecuting map[string]int `json:"per_type_executing"`
	PerTypeCapacity  map[string]int `json:"per_type_capacity"`
}

type EngineMetrics struct {
	TotalWorkflows     int64      `json:"total_workflows"`
	ActiveWorkflows    int64      `json:"active_workflows"`
	CompletedWorkflows int64      `json:"completed_workflows"`
	FailedWorkflows    int64      `json:"failed_workflows"`
	QueueSizes         QueueSizes `json:"queue_sizes"`
	WorkerPoolSize     int        `json:"worker_pool_size"`
}

type QueueSizes struct {
	Ready   int `json:"ready"`
	Pending int `json:"pending"`
}

type ClusterMember struct {
	NodeID   string `json:"node_id"`
	Address  string `json:"address"`
	Status   string `json:"status"`
	IsLeader bool   `json:"is_leader"`
}

type WorkflowCompletionData = domain.WorkflowCompletionData
type ExecutedNodeData = domain.ExecutedNodeData
type CheckpointData = domain.CheckpointData
type QueueItemData = domain.QueueItemData
type IdempotencyKeyData = domain.IdempotencyKeyData
type ClaimData = domain.ClaimData

type CompletionHandler = ports.CompletionHandler
type ErrorHandler = ports.ErrorHandler

type Cluster struct {
	internal *api.GraftCluster
}

func New(config Config) (*Cluster, error) {
	internalConfig, err := api.ConvertConfig(config)
	if err != nil {
		return nil, domain.NewConfigurationError("config conversion", "failed to convert graft config to internal config", "verify configuration structure and values")
	}

	var logger *slog.Logger
	if config.Logger != nil {
		logger = config.Logger
	} else {
		logger, err = api.CreateLogger(config.LogLevel)
		if err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "failed to create logger",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}
	}

	internalCluster, err := api.NewCluster(internalConfig, logger)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create internal cluster",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return &Cluster{
		internal: internalCluster,
	}, nil
}

func DefaultConfig() Config {
	return Config{
		LogLevel: "info",
		Discovery: DiscoveryConfig{
			Strategy: "auto",
		},
		Transport: TransportConfig{
			ListenAddress:     "0.0.0.0",
			ListenPort:        9090,
			MaxMessageSizeMB:  4,
			ConnectionTimeout: "30s",
		},
		Storage: StorageConfig{
			ListenAddress:     "127.0.0.1",
			ListenPort:        7000,
			DataDir:           "./data/raft",
			LogLevel:          "INFO",
			SnapshotRetention: 2,
			SnapshotThreshold: 8192,
			TrailingLogs:      10240,
		},
		Queue: QueueConfig{
			DataDir:            "./data/queue",
			SyncWrites:         false,
			ValueLogFileSizeMB: 1024,
			MemTableSizeMB:     64,
			NumGoroutines:      8,
			BloomFalsePositive: 0.01,
		},
		Resources: ResourceConfig{
			MaxConcurrentTotal:   50,
			MaxConcurrentPerType: make(map[string]int),
			DefaultPerTypeLimit:  10,
		},
		Engine: EngineConfig{
			MaxConcurrentWorkflows: 100,
			NodeExecutionTimeout:   "5m",
			StateUpdateInterval:    "30s",
			RetryAttempts:          3,
			RetryBackoff:           "5s",
		},
	}
}

func (c *Cluster) Start(ctx context.Context) error {
	return c.internal.Start(ctx)
}

func (c *Cluster) Stop() error {
	return c.internal.Stop()
}

func (c *Cluster) RegisterNode(node interface{}) error {
	return c.internal.RegisterNode(node)
}

func (c *Cluster) StartWorkflow(workflowID string, trigger WorkflowTrigger) error {
	internalTrigger := ports.WorkflowTrigger{
		WorkflowID:   workflowID,
		InitialState: trigger.InitialState,
		InitialNodes: make([]ports.NodeConfig, len(trigger.InitialNodes)),
		Metadata:     trigger.Metadata,
	}

	for i, node := range trigger.InitialNodes {
		internalTrigger.InitialNodes[i] = ports.NodeConfig{
			Name:   node.Name,
			Config: node.Config,
		}
	}

	return c.internal.StartWorkflow(internalTrigger)
}

func (c *Cluster) GetWorkflowState(workflowID string) (*WorkflowState, error) {
	return c.internal.GetWorkflowState(workflowID)
}

func (c *Cluster) GetClusterInfo() ClusterInfo {
	info := c.internal.GetClusterInfo()
	converted := api.ConvertClusterInfo(info)

	var result ClusterInfo
	if err := api.ConvertViaJSON(converted, &result); err != nil {
		return ClusterInfo{}
	}

	return result
}

func (c *Cluster) OnComplete(handler CompletionHandler) {
	c.internal.OnComplete(handler)
}

func (c *Cluster) OnError(handler ErrorHandler) {
	c.internal.OnError(handler)
}
