package api

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery"
	"github.com/eleven-am/graft/internal/core/cluster"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func ConvertConfig(config interface{}) (cluster.ClusterConfig, error) {
	configVal := reflect.ValueOf(config)
	if configVal.Kind() == reflect.Ptr {
		configVal = configVal.Elem()
	}

	getField := func(name string) interface{} {
		field := configVal.FieldByName(name)
		if !field.IsValid() {
			return nil
		}
		return field.Interface()
	}

	getStringField := func(name string, defaultVal string) string {
		if val := getField(name); val != nil {
			if str, ok := val.(string); ok && str != "" {
				return str
			}
		}
		return defaultVal
	}

	getIntField := func(name string, defaultVal int) int {
		if val := getField(name); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				return intVal
			}
		}
		return defaultVal
	}

	nodeID := getStringField("NodeID", GenerateNodeID())
	serviceName := getStringField("ServiceName", "graft-cluster")
	servicePort := getIntField("ServicePort", 8080)

	discoveryField := configVal.FieldByName("Discovery")
	var discoveryConfig cluster.DiscoveryConfig
	if discoveryField.IsValid() {
		discoveryVal := discoveryField
		getDiscoveryField := func(name string) interface{} {
			field := discoveryVal.FieldByName(name)
			if !field.IsValid() {
				return nil
			}
			return field.Interface()
		}

		strategy := "auto"
		if val := getDiscoveryField("Strategy"); val != nil {
			if str, ok := val.(string); ok && str != "" {
				strategy = str
			}
		}

		discoveryConfig = cluster.DiscoveryConfig{
			Strategy:    discovery.Strategy(strategy),
			ServiceName: serviceName,
			ServicePort: servicePort,
			Peers:       []string{},
			Metadata:    make(map[string]string),
		}

		if val := getDiscoveryField("Peers"); val != nil {
			if peers, ok := val.([]string); ok {
				discoveryConfig.Peers = peers
			}
		}

		if val := getDiscoveryField("Metadata"); val != nil {
			if metadata, ok := val.(map[string]string); ok {
				discoveryConfig.Metadata = metadata
			}
		}
	} else {
		discoveryConfig = cluster.DiscoveryConfig{
			Strategy:    discovery.StrategyAuto,
			ServiceName: serviceName,
			ServicePort: servicePort,
			Peers:       []string{},
			Metadata:    make(map[string]string),
		}
	}

	transportField := configVal.FieldByName("Transport")
	var transportConfig cluster.TransportConfig
	if transportField.IsValid() {
		transportVal := transportField
		getTransportField := func(name string) interface{} {
			field := transportVal.FieldByName(name)
			if !field.IsValid() {
				return nil
			}
			return field.Interface()
		}

		listenAddress := "0.0.0.0"
		if val := getTransportField("ListenAddress"); val != nil {
			if str, ok := val.(string); ok && str != "" {
				listenAddress = str
			}
		}

		listenPort := 9090
		if val := getTransportField("ListenPort"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				listenPort = intVal
			}
		}

		maxMessageSizeMB := 4
		if val := getTransportField("MaxMessageSizeMB"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				maxMessageSizeMB = intVal
			}
		}

		connectionTimeoutStr := "30s"
		if val := getTransportField("ConnectionTimeout"); val != nil {
			if str, ok := val.(string); ok && str != "" {
				connectionTimeoutStr = str
			}
		}

		connectionTimeout, err := ParseDuration(connectionTimeoutStr, 30*time.Second)
		if err != nil {
			return cluster.ClusterConfig{}, domain.NewConfigurationError("transport.connection_timeout", "invalid duration format", "use Go duration format like '30s' or '1m30s'")
		}

		enableTLS := false
		if val := getTransportField("EnableTLS"); val != nil {
			if boolVal, ok := val.(bool); ok {
				enableTLS = boolVal
			}
		}

		transportConfig = cluster.TransportConfig{
			ListenAddress:     listenAddress,
			ListenPort:        listenPort,
			MaxMessageSize:    maxMessageSizeMB * 1024 * 1024,
			ConnectionTimeout: connectionTimeout,
			TLS: cluster.TLSConfig{
				Enabled: enableTLS,
			},
			KeepAlive: cluster.KeepAliveConfig{
				Time:                30 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		}

		if val := getTransportField("TLSCertFile"); val != nil {
			if str, ok := val.(string); ok {
				transportConfig.TLS.CertFile = str
			}
		}
		if val := getTransportField("TLSKeyFile"); val != nil {
			if str, ok := val.(string); ok {
				transportConfig.TLS.KeyFile = str
			}
		}
		if val := getTransportField("TLSCAFile"); val != nil {
			if str, ok := val.(string); ok {
				transportConfig.TLS.CAFile = str
			}
		}
	} else {
		transportConfig = cluster.TransportConfig{
			ListenAddress:     "0.0.0.0",
			ListenPort:        9090,
			MaxMessageSize:    4 * 1024 * 1024,
			ConnectionTimeout: 30 * time.Second,
			TLS: cluster.TLSConfig{
				Enabled: false,
			},
			KeepAlive: cluster.KeepAliveConfig{
				Time:                30 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		}
	}

	storageField := configVal.FieldByName("Storage")
	var storageConfig cluster.StorageConfig
	if storageField.IsValid() {
		storageVal := storageField
		getStorageField := func(name string) interface{} {
			field := storageVal.FieldByName(name)
			if !field.IsValid() {
				return nil
			}
			return field.Interface()
		}

		storageConfig = cluster.StorageConfig{
			ListenAddress:     getStringField("ListenAddress", "127.0.0.1"),
			ListenPort:        getIntField("ListenPort", 7000),
			DataDir:           getStringField("DataDir", "./data/raft"),
			LogLevel:          getStringField("LogLevel", "INFO"),
			SnapshotRetention: getIntField("SnapshotRetention", 2),
			SnapshotThreshold: uint64(getIntField("SnapshotThreshold", 8192)),
			TrailingLogs:      uint64(getIntField("TrailingLogs", 10240)),
		}

		if val := getStorageField("ListenAddress"); val != nil {
			if str, ok := val.(string); ok && str != "" {
				storageConfig.ListenAddress = str
			}
		}
		if val := getStorageField("ListenPort"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				storageConfig.ListenPort = intVal
			}
		}
		if val := getStorageField("DataDir"); val != nil {
			if str, ok := val.(string); ok && str != "" {
				storageConfig.DataDir = str
			}
		}
	} else {
		storageConfig = cluster.StorageConfig{
			ListenAddress:     "127.0.0.1",
			ListenPort:        7000,
			DataDir:           "./data/raft",
			LogLevel:          "INFO",
			SnapshotRetention: 2,
			SnapshotThreshold: 8192,
			TrailingLogs:      10240,
		}
	}

	queueField := configVal.FieldByName("Queue")
	var queueConfig cluster.QueueConfig
	if queueField.IsValid() {
		queueVal := queueField
		getQueueField := func(name string) interface{} {
			field := queueVal.FieldByName(name)
			if !field.IsValid() {
				return nil
			}
			return field.Interface()
		}

		dataDir := "./data/queue"
		if val := getQueueField("DataDir"); val != nil {
			if str, ok := val.(string); ok && str != "" {
				dataDir = str
			}
		}

		syncWrites := false
		if val := getQueueField("SyncWrites"); val != nil {
			if boolVal, ok := val.(bool); ok {
				syncWrites = boolVal
			}
		}

		valueLogFileSizeMB := 1024
		if val := getQueueField("ValueLogFileSizeMB"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				valueLogFileSizeMB = intVal
			}
		}

		memTableSizeMB := 64
		if val := getQueueField("MemTableSizeMB"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				memTableSizeMB = intVal
			}
		}

		numGoroutines := 8
		if val := getQueueField("NumGoroutines"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				numGoroutines = intVal
			}
		}

		bloomFalsePositive := 0.01
		if val := getQueueField("BloomFalsePositive"); val != nil {
			if floatVal, ok := val.(float64); ok && floatVal != 0.0 {
				bloomFalsePositive = floatVal
			}
		}

		queueConfig = cluster.QueueConfig{
			DataDir:                 dataDir,
			SyncWrites:              syncWrites,
			ValueLogFileSize:        int64(valueLogFileSizeMB) * 1024 * 1024,
			NumVersionsToKeep:       1,
			CompactL0OnClose:        false,
			NumGoroutines:           numGoroutines,
			MemTableSize:            int64(memTableSizeMB) * 1024 * 1024,
			BaseTableSize:           2097152,
			BaseLevelSize:           10485760,
			LevelSizeMultiplier:     10,
			TableSizeMultiplier:     2,
			MaxLevels:               7,
			VLogPercentile:          0.0,
			ValueThreshold:          32,
			NumMemTables:            5,
			BlockSize:               4096,
			BloomFalsePositive:      bloomFalsePositive,
			BlockCacheSize:          256 * 1024 * 1024,
			IndexCacheSize:          0,
			NumLevelZeroTables:      5,
			NumLevelZeroTablesStall: 15,
			ValueLogMaxEntries:      1000000,
			DetectConflicts:         true,
		}
	} else {
		queueConfig = cluster.QueueConfig{
			DataDir:                 "./data/queue",
			SyncWrites:              false,
			ValueLogFileSize:        1024 * 1024 * 1024,
			NumVersionsToKeep:       1,
			CompactL0OnClose:        false,
			NumGoroutines:           8,
			MemTableSize:            64 * 1024 * 1024,
			BaseTableSize:           2097152,
			BaseLevelSize:           10485760,
			LevelSizeMultiplier:     10,
			TableSizeMultiplier:     2,
			MaxLevels:               7,
			VLogPercentile:          0.0,
			ValueThreshold:          32,
			NumMemTables:            5,
			BlockSize:               4096,
			BloomFalsePositive:      0.01,
			BlockCacheSize:          256 * 1024 * 1024,
			IndexCacheSize:          0,
			NumLevelZeroTables:      5,
			NumLevelZeroTablesStall: 15,
			ValueLogMaxEntries:      1000000,
			DetectConflicts:         true,
		}
	}

	resourcesField := configVal.FieldByName("Resources")
	var resourceConfig ports.ResourceConfig
	if resourcesField.IsValid() {
		resourcesVal := resourcesField
		getResourceField := func(name string) interface{} {
			field := resourcesVal.FieldByName(name)
			if !field.IsValid() {
				return nil
			}
			return field.Interface()
		}

		maxConcurrentTotal := 50
		if val := getResourceField("MaxConcurrentTotal"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				maxConcurrentTotal = intVal
			}
		}

		defaultPerTypeLimit := 10
		if val := getResourceField("DefaultPerTypeLimit"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				defaultPerTypeLimit = intVal
			}
		}

		maxConcurrentPerType := make(map[string]int)
		if val := getResourceField("MaxConcurrentPerType"); val != nil {
			if mapVal, ok := val.(map[string]int); ok {
				maxConcurrentPerType = mapVal
			}
		}

		resourceConfig = ports.ResourceConfig{
			MaxConcurrentTotal:   maxConcurrentTotal,
			MaxConcurrentPerType: maxConcurrentPerType,
			DefaultPerTypeLimit:  defaultPerTypeLimit,
		}
	} else {
		resourceConfig = ports.ResourceConfig{
			MaxConcurrentTotal:   50,
			MaxConcurrentPerType: make(map[string]int),
			DefaultPerTypeLimit:  10,
		}
	}

	engineField := configVal.FieldByName("Engine")
	var engineConfig cluster.EngineConfig
	if engineField.IsValid() {
		engineVal := engineField
		getEngineField := func(name string) interface{} {
			field := engineVal.FieldByName(name)
			if !field.IsValid() {
				return nil
			}
			return field.Interface()
		}

		maxConcurrentWorkflows := 100
		if val := getEngineField("MaxConcurrentWorkflows"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				maxConcurrentWorkflows = intVal
			}
		}

		nodeExecutionTimeoutStr := "5m"
		if val := getEngineField("NodeExecutionTimeout"); val != nil {
			if str, ok := val.(string); ok && str != "" {
				nodeExecutionTimeoutStr = str
			}
		}

		stateUpdateIntervalStr := "30s"
		if val := getEngineField("StateUpdateInterval"); val != nil {
			if str, ok := val.(string); ok && str != "" {
				stateUpdateIntervalStr = str
			}
		}

		retryBackoffStr := "5s"
		if val := getEngineField("RetryBackoff"); val != nil {
			if str, ok := val.(string); ok && str != "" {
				retryBackoffStr = str
			}
		}

		retryAttempts := 3
		if val := getEngineField("RetryAttempts"); val != nil {
			if intVal, ok := val.(int); ok && intVal != 0 {
				retryAttempts = intVal
			}
		}

		nodeExecutionTimeout, err := ParseDuration(nodeExecutionTimeoutStr, 5*time.Minute)
		if err != nil {
			return cluster.ClusterConfig{}, domain.NewConfigurationError("engine.node_execution_timeout", "invalid duration format", "use Go duration format like '5m' or '300s'")
		}

		stateUpdateInterval, err := ParseDuration(stateUpdateIntervalStr, 30*time.Second)
		if err != nil {
			return cluster.ClusterConfig{}, domain.NewConfigurationError("engine.state_update_interval", "invalid duration format", "use Go duration format like '1s' or '100ms'")
		}

		retryBackoff, err := ParseDuration(retryBackoffStr, 5*time.Second)
		if err != nil {
			return cluster.ClusterConfig{}, domain.NewConfigurationError("engine.retry_backoff", "invalid duration format", "use Go duration format like '2s' or '500ms'")
		}

		engineConfig = cluster.EngineConfig{
			MaxConcurrentWorkflows: maxConcurrentWorkflows,
			NodeExecutionTimeout:   nodeExecutionTimeout,
			StateUpdateInterval:    stateUpdateInterval,
			RetryAttempts:          retryAttempts,
			RetryBackoff:           retryBackoff,
		}
	} else {
		engineConfig = cluster.EngineConfig{
			MaxConcurrentWorkflows: 100,
			NodeExecutionTimeout:   5 * time.Minute,
			StateUpdateInterval:    30 * time.Second,
			RetryAttempts:          3,
			RetryBackoff:           5 * time.Second,
		}
	}

	return cluster.ClusterConfig{
		NodeID:      nodeID,
		ServiceName: serviceName,
		ServicePort: servicePort,
		Discovery:   discoveryConfig,
		Transport:   transportConfig,
		Storage:     storageConfig,
		Queue:       queueConfig,
		Resources:   resourceConfig,
		Engine:      engineConfig,
	}, nil
}

func ConvertWorkflowStatus(status ports.WorkflowStatus) interface{} {
	state := map[string]interface{}{
		"workflow_id":    status.WorkflowID,
		"status":         string(status.Status),
		"current_state":  status.CurrentState,
		"started_at":     status.StartedAt.Format(time.RFC3339),
		"executed_nodes": make([]interface{}, len(status.ExecutedNodes)),
		"pending_nodes":  make([]interface{}, len(status.PendingNodes)),
		"ready_nodes":    make([]interface{}, len(status.ReadyNodes)),
		"last_error":     status.LastError,
	}

	if status.CompletedAt != nil {
		completedAt := status.CompletedAt.Format(time.RFC3339)
		state["completed_at"] = &completedAt
	}

	for i, node := range status.ExecutedNodes {
		completedAt := node.ExecutedAt.Add(node.Duration).Format(time.RFC3339)
		executedNode := map[string]interface{}{
			"node_name":    node.NodeName,
			"started_at":   node.ExecutedAt.Format(time.RFC3339),
			"completed_at": completedAt,
			"status":       string(node.Status),
			"output":       node.Results,
			"error":        node.Error,
		}
		state["executed_nodes"].([]interface{})[i] = executedNode
	}

	for i, node := range status.PendingNodes {
		var dependencies []string
		if node.Reason != "" {
			dependencies = []string{node.Reason}
		}
		pendingNode := map[string]interface{}{
			"node_name":     node.NodeName,
			"config":        node.Config,
			"priority":      node.Priority,
			"dependencies":  dependencies,
		}
		state["pending_nodes"].([]interface{})[i] = pendingNode
	}

	for i, node := range status.ReadyNodes {
		readyNode := map[string]interface{}{
			"node_name": node.NodeName,
			"config":    node.Config,
			"priority":  node.Priority,
			"queued_at": node.QueuedAt.Format(time.RFC3339),
		}
		state["ready_nodes"].([]interface{})[i] = readyNode
	}

	return state
}

func ConvertClusterInfo(info ports.ClusterInfo) interface{} {
	return map[string]interface{}{
		"node_id":          info.NodeID,
		"registered_nodes": info.RegisteredNodes,
		"active_workflows": info.ActiveWorkflows,
		"resource_limits": map[string]interface{}{
			"max_concurrent_total":    info.ResourceLimits.MaxConcurrentTotal,
			"max_concurrent_per_type": info.ResourceLimits.MaxConcurrentPerType,
			"default_per_type_limit":  info.ResourceLimits.DefaultPerTypeLimit,
		},
		"execution_stats": map[string]interface{}{
			"total_executing":    info.ExecutionStats.TotalExecuting,
			"total_capacity":     info.ExecutionStats.TotalCapacity,
			"available_slots":    info.ExecutionStats.AvailableSlots,
			"per_type_executing": info.ExecutionStats.PerTypeExecuting,
			"per_type_capacity":  info.ExecutionStats.PerTypeCapacity,
		},
		"engine_metrics": map[string]interface{}{
			"total_workflows":     info.EngineMetrics.TotalWorkflows,
			"active_workflows":    info.EngineMetrics.ActiveWorkflows,
			"completed_workflows": info.EngineMetrics.CompletedWorkflows,
			"failed_workflows":    info.EngineMetrics.FailedWorkflows,
			"queue_sizes": map[string]interface{}{
				"ready":   info.EngineMetrics.QueueSizes.Ready,
				"pending": info.EngineMetrics.QueueSizes.Pending,
			},
			"worker_pool_size": info.EngineMetrics.WorkerPoolSize,
		},
		"cluster_members": ConvertClusterMembers(info.ClusterMembers),
		"is_leader":       info.IsLeader,
	}
}

func ConvertClusterMembers(members []ports.ClusterMember) []interface{} {
	result := make([]interface{}, len(members))
	for i, member := range members {
		result[i] = map[string]interface{}{
			"node_id":   member.NodeID,
			"address":   member.Address,
			"status":    member.Status,
			"is_leader": member.IsLeader,
		}
	}
	return result
}

func CreateLogger(level string) (*slog.Logger, error) {
	var logLevel slog.Level
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "info", "":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		return nil, domain.NewConfigurationError("log_level", fmt.Sprintf("unsupported level: %s", level), "use: DEBUG, INFO, WARN, or ERROR")
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	handler := slog.NewTextHandler(os.Stdout, opts)
	logger := slog.New(handler)

	return logger, nil
}

func ParseDuration(value string, defaultValue time.Duration) (time.Duration, error) {
	if value == "" {
		return defaultValue, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, domain.NewConfigurationError("duration", fmt.Sprintf("invalid format: %s", value), "use Go duration format like '1s', '100ms', '5m'")
	}

	return duration, nil
}

func GenerateNodeID() string {
	return fmt.Sprintf("graft-node-%d", time.Now().UnixNano()%100000)
}