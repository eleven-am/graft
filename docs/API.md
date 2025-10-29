# Graft API Documentation

## Overview

Graft is a distributed workflow orchestration framework written in Go. It provides a simple yet powerful API for building and executing complex workflows across multiple nodes in a cluster.

## Core Concepts

### Cluster
The main entry point for Graft operations. A cluster manages nodes, workflows, and distributed coordination.

### Node
A processing unit that executes a specific task within a workflow. Nodes implement the `Node` interface.

### Workflow
A collection of interconnected nodes that process data and state through a defined execution flow.

## Installation

```bash
go get github.com/eleven-am/graft
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "log/slog"
    "os"
    "time"
    
    "github.com/eleven-am/graft"
)

func main() {
    // Create logger
    logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
    
    // Create manager with direct parameters
    manager := graft.New("example-node", "localhost:7000", "./data", logger)
    if manager == nil {
        log.Fatal("failed to create manager")
    }
    
    // Configure discovery before starting
    manager.Discovery().MDNS("", "")  // or Static()
    
    // Start the manager with gRPC port
    if err := manager.Start(context.Background(), 8080); err != nil {
        log.Fatal(err)
    }
    defer manager.Stop(context.Background())
    
    // Register a simple node
    node := &SimpleProcessor{}
    if err := manager.RegisterNode(node); err != nil {
        log.Fatal(err)
    }
    
    // Start a workflow with typed state
    trigger := graft.WorkflowTrigger{
        WorkflowID: "example-workflow",
        InitialState: ProcessorState{
            Input: "hello world",
        },
        InitialNodes: []graft.NodeConfig{
            {
                Name:   "simple-processor",
                Config: ProcessorConfig{DelayMS: 100},
            },
        },
    }
    
    if err := manager.StartWorkflow(trigger); err != nil {
        log.Fatal(err)
    }
}

// Define typed structures
type ProcessorConfig struct {
    DelayMS int `json:"delay_ms"`
}

type ProcessorState struct {
    Input string `json:"input"`
}

type ProcessorResult struct {
    Output    string    `json:"output"`
    Timestamp time.Time `json:"timestamp"`
}

type SimpleProcessor struct{}

func (p *SimpleProcessor) GetName() string {
    return "simple-processor"
}

func (p *SimpleProcessor) Execute(ctx context.Context, state ProcessorState, config ProcessorConfig) (graft.NodeResult, error) {
    result := ProcessorResult{
        Output:    fmt.Sprintf("processed: %s", state.Input),
        Timestamp: time.Now(),
    }
    
    if workflowCtx, ok := graft.GetWorkflowContext(ctx); ok {
        fmt.Printf("Processing in workflow %s, node %s (execution: %s)\n", 
            workflowCtx.WorkflowID, workflowCtx.NodeName, workflowCtx.ExecutionID)
    }
    
    return graft.NodeResult{
        Data: result,
    }, nil
}
```

## Core API

### Manager Interface

```go
type Manager interface {
    Discovery() DiscoveryManager
    Start(ctx context.Context, grpcPort int) error
    Stop(ctx context.Context) error
    RegisterNode(node interface{}) error
    UnregisterNode(nodeName string) error
    RegisterConnector(name string, factory ConnectorFactory) error
    StartConnector(connectorName string, config ConnectorConfig) error
    StartWorkflow(trigger WorkflowTrigger) error
    GetWorkflowState(workflowID string) (*WorkflowStatus, error)
    GetClusterInfo() ClusterInfo
    OnComplete(handler CompletionHandler)
    OnError(handler ErrorHandler)
    PauseWorkflow(ctx context.Context, workflowID string) error
    ResumeWorkflow(ctx context.Context, workflowID string) error
    StopWorkflow(ctx context.Context, workflowID string) error
}
```

#### Methods

**Discovery() DiscoveryManager**
- Returns the discovery manager for configuring service discovery
- Must be called before Start() to configure discovery
- Supports built-in MDNS and Static; external providers via Add(provider)

**Start(ctx context.Context, grpcPort int) error**
- Starts the manager and begins listening for gRPC connections
- grpcPort specifies the port for gRPC communication
- Must be called before any workflow operations
- Context can be used for graceful shutdown

**Stop(ctx context.Context) error**
- Gracefully shuts down the manager
- Completes running workflows before stopping
- Should be called in defer or signal handlers
- Context controls shutdown timeout

**RegisterNode(node interface{}) error**
- Registers a node implementation with the manager
- Node must implement GetName() method and Execute method via reflection
- Node names must be unique within the cluster

**UnregisterNode(nodeName string) error**
- Removes a previously registered node
- Returns error if node doesn't exist
- Active workflows using the node will continue

**RegisterConnector(name string, factory ConnectorFactory) error**
- Registers a long-lived connector factory (listeners, stream consumers)
- Factory receives the stored config payload and must return a fresh `ConnectorPort` per config ID
- Registration is idempotent across the cluster; duplicates are rejected by name

**StartConnector(connectorName string, config ConnectorConfig) error**
- Persists the provided config (must return a stable ID) and requests the connector to start
- All nodes watch for configs; the load balancer selects an owner and restarts it on failure or rebalance
- Subsequent calls with the same config ID are ignored unless the payload changes

**StartWorkflow(trigger WorkflowTrigger) error**
- Initiates a new workflow execution
- WorkflowID in trigger must be unique across the cluster
- Returns error if workflow already exists

**GetWorkflowState(workflowID string) (*WorkflowStatus, error)**
- Retrieves current state of a running or completed workflow
- Includes execution history and current status
- Returns error if workflow not found

**GetClusterInfo() ClusterInfo**
- Returns cluster statistics and member information
- Includes resource utilization and performance metrics
- Useful for monitoring and debugging

**OnComplete(handler CompletionHandler)**
- Registers a callback for successful workflow completion
- Handler receives final workflow state
- Multiple handlers can be registered

**OnError(handler ErrorHandler)**
- Registers a callback for workflow failures
- Handler receives error details and partial state
- Multiple handlers can be registered

**PauseWorkflow(ctx context.Context, workflowID string) error**
- Pauses execution of a running workflow
- Workflow can be resumed later with ResumeWorkflow
- Returns error if workflow not found or not running

**ResumeWorkflow(ctx context.Context, workflowID string) error**
- Resumes execution of a paused workflow
- Workflow continues from where it was paused
- Returns error if workflow not found or not paused

**StopWorkflow(ctx context.Context, workflowID string) error**
- Terminates a running or paused workflow
- Workflow is marked as cancelled and cannot be resumed
- Returns error if workflow not found

### Node Interface

Graft uses a simplified Node interface with automatic type discovery via reflection:

```go
type NodeInterface interface {
    GetName() string
}
```

Every node MUST also implement an Execute method (discovered via reflection):

```go
// Execute method signature patterns:
// Full signature with state and config:
func (n *MyNode) Execute(ctx context.Context, state StateType, config ConfigType) (graft.NodeResult, error)

// With state only:
func (n *MyNode) Execute(ctx context.Context, state StateType) (graft.NodeResult, error)

// Minimal (context only):
func (n *MyNode) Execute(ctx context.Context) (graft.NodeResult, error)
```

#### Type-Safe Execute Method

The Execute method uses typed parameters instead of maps, providing compile-time type safety:

```go
func (n *MyNode) Execute(ctx context.Context, state MyStateType, config MyConfigType) (graft.NodeResult, error) {
    // Process with full type safety
    result := MyResultType{
        ProcessedData: state.Input + " processed",
        Timestamp: time.Now(),
    }
    
    return graft.NodeResult{
        Data: result,
        NextNodes: []graft.NextNodeConfig{
            {NodeName: "next-node", Config: NextConfig{...}},
        },
    }, nil
}
```

#### NodeResult Structure

```go
type NodeResult struct {
    Data         interface{}      `json:"data"`
    NextNodes    []NextNodeConfig `json:"next_nodes,omitempty"`
    StateUpdates interface{}      `json:"state_updates,omitempty"`
    Metadata     NodeMetadata     `json:"metadata,omitempty"`
}
```

**Fields:**
- `Data`: The result data from node execution (any typed struct)
- `NextNodes`: Optional list of nodes to execute next with their configs
- `StateUpdates`: Optional workflow state updates
- `Metadata`: Optional metadata about the execution

#### Automatic Type Discovery

Graft automatically extracts type information from the Execute method signature using reflection:
- Config type from the third parameter
- State type from the second parameter
- Result type from NodeResult.Data

This eliminates the need for schema methods while maintaining type safety.

#### Methods

**GetName() string**
- Returns unique identifier for the node type
- Used for workflow configuration and routing
- Must be consistent across cluster restarts

### Connector Basics

Connectors are long-lived listeners managed separately from workflow nodes. They are registered like nodes but are scheduled via leases and the load balancer so only one node owns a given config ID at a time.

1. Define a config struct with `GetID() string` returning a stable identifier (for example a queue ARN or topic name).
2. Implement a factory `func([]byte) (ConnectorPort, error)` that unmarshals the config, stores it on a connector instance, and returns the instance.
3. Register the factory with `manager.RegisterConnector("name", factory)` once at startup, then call `manager.StartConnector("name", cfg)` for each config you need.

```go
type BillingConfig struct {
    Topic string `json:"topic"`
}

func (c *BillingConfig) GetID() string { return c.Topic }

type KafkaConnector struct {
    cfg *BillingConfig
}

func (c *KafkaConnector) GetName() string { return "kafka" }
func (c *KafkaConnector) GetConfig() ConnectorConfig { return c.cfg }
func (c *KafkaConnector) Start(ctx context.Context) error { /* subscribe & emit workflows */ return nil }
func (c *KafkaConnector) Stop(ctx context.Context) error  { /* close consumers */ return nil }

func NewKafkaConnector(configJSON []byte) (ConnectorPort, error) {
    var cfg BillingConfig
    if err := json.Unmarshal(configJSON, &cfg); err != nil {
        return nil, err
    }
    return &KafkaConnector{cfg: &cfg}, nil
}

manager.RegisterConnector("kafka", NewKafkaConnector)
manager.StartConnector("kafka", &BillingConfig{Topic: "billing"})
```

The manager persists each config, every node watches for changes, and the least-loaded node claims the lease. When ownership changes (failure, shutdown, new peer), the next node restarts the connector with the same config.

### Optional Interfaces

### Optional Methods

#### CanStart Method

Nodes can optionally implement a `CanStart` method (discovered via reflection) to control execution conditions:

```go
// CanStart method signature patterns:
// Full signature with state and config:
func (n *MyNode) CanStart(ctx context.Context, state StateType, config ConfigType) bool

// With state only:
func (n *MyNode) CanStart(ctx context.Context, state StateType) bool

// Minimal (context only):
func (n *MyNode) CanStart(ctx context.Context) bool
```

**CanStart** - Optional method for conditional execution
- Returns true if node can execute with current state
- Used for conditional workflow branching
- Called before each node execution attempt
- Uses the same typed parameters as the Execute method

## Configuration

### Manager Creation

```go
// Create a manager with required parameters
manager := graft.New(nodeID, bindAddr, dataDir, logger)

// Parameters:
// - nodeID: Unique identifier for this node in the cluster
// - bindAddr: Address and port for Raft consensus (e.g., "localhost:7000")
// - dataDir: Directory path for persistent storage
// - logger: *slog.Logger instance for structured logging
```

### Discovery Configuration

Configure service discovery before starting the manager:

```go
// mDNS Discovery (automatic local discovery)
manager.Discovery().MDNS("", "")

// Static Discovery (predefined peer list)
manager.Discovery().Static([]graft.Peer{
    {ID: "peer1", Address: "host1", Port: 7001},
    {ID: "peer2", Address: "host2", Port: 7002},
})
```

**Discovery Strategies:**
- **MDNS**: Automatically discovers peers on the local network using multicast DNS (built-in)
- **Static**: Uses a predefined list of peer addresses (built-in)
- **External providers**: Implement the Provider interface and call `manager.Discovery().Add(provider)`

### Resource Configuration

Resource limits are configured internally with sensible defaults:

```go
// Default resource configuration (internal):
resourceConfig := &ports.ResourceConfig{
    MaxConcurrentTotal:   100,               // Total concurrent workflows
    DefaultPerTypeLimit:  10,                // Default per-node-type limit
    MaxConcurrentPerType: map[string]int{},  // Custom per-type limits
    HealthThresholds:     ports.HealthConfig{}, // Health check thresholds
}
```

To customize resource limits, you would need to modify the internal configuration in future releases.

### Startup Configuration

```go
// Start the manager with gRPC port
ctx := context.Background()
grpcPort := 8080
if err := manager.Start(ctx, grpcPort); err != nil {
    log.Fatal(err)
}

// Graceful shutdown
defer manager.Stop(ctx)
```

### Logging Configuration

```go
// Create structured logger with desired level and format
logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,  // or LevelDebug, LevelWarn, LevelError
    AddSource: true,        // Include source file info
}))

// JSON format for production
jsonLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))
```

## Data Types

### WorkflowTrigger

```go
type WorkflowTrigger struct {
    WorkflowID   string        `json:"workflow_id"`
    InitialState interface{}   `json:"initial_state"`  // Typed state struct
    InitialNodes []NodeConfig  `json:"initial_nodes"`
    Metadata     map[string]string `json:"metadata,omitempty"`
}
```

### WorkflowStatus

```go
type WorkflowStatus struct {
    WorkflowID    string      `json:"workflow_id"`
    Status        string      `json:"status"`
    CurrentState  interface{} `json:"current_state"`  // Typed state struct
    StartedAt     time.Time   `json:"started_at"`
    CompletedAt   *time.Time  `json:"completed_at,omitempty"`
    ExecutedNodes []ExecutedNodeData `json:"executed_nodes"`
    PendingNodes  []NodeConfig       `json:"pending_nodes"`
    ReadyNodes    []NodeConfig       `json:"ready_nodes"`
    LastError     *string     `json:"last_error,omitempty"`
}
```

**Status Values:**
- `"pending"`: Workflow queued but not started
- `"running"`: Currently executing
- `"completed"`: Successfully finished
- `"failed"`: Terminated due to error
- `"cancelled"`: Manually stopped
- `"paused"`: Execution temporarily suspended

### NodeConfig

```go
type NodeConfig struct {
    Name   string      `json:"name"`
    Config interface{} `json:"config"`  // Typed config struct
}
```

### ClusterInfo

```go
type ClusterInfo struct {
    NodeID   string        `json:"node_id"`
    Status   string        `json:"status"`         // "running" or "stopped"
    IsLeader bool          `json:"is_leader"`
    Peers    []string      `json:"peers"`
    Metrics  EngineMetrics `json:"metrics"`
}
```

### EngineMetrics

```go
type EngineMetrics struct {
    TotalWorkflows     int64 `json:"total_workflows"`
    ActiveWorkflows    int64 `json:"active_workflows"`
    CompletedWorkflows int64 `json:"completed_workflows"`
    FailedWorkflows    int64 `json:"failed_workflows"`
    NodesExecuted      int64 `json:"nodes_executed"`
}
```

## Workflow Context

### Accessing Workflow Metadata

Nodes can access workflow-level metadata during execution using the `GetWorkflowContext` function:

```go
func (n *MyNode) Execute(ctx context.Context, state MyState, config MyConfig) (graft.NodeResult, error) {
    if workflowCtx, ok := graft.GetWorkflowContext(ctx); ok {
        log.Printf("Executing node %s in workflow %s", workflowCtx.NodeName, workflowCtx.WorkflowID)
        log.Printf("Execution ID: %s", workflowCtx.ExecutionID)
        log.Printf("Started at: %s", workflowCtx.StartedAt)
        log.Printf("Is leader: %t", workflowCtx.ClusterInfo.IsLeader)
        log.Printf("Retry count: %d", workflowCtx.RetryCount)
        
        if workflowCtx.Metadata != nil {
            for key, value := range workflowCtx.Metadata {
                log.Printf("Metadata %s: %s", key, value)
            }
        }
    }
    
    // Your node logic here
    return graft.NodeResult{Data: result}, nil
}
```

### WorkflowContext Structure

```go
type WorkflowContext struct {
    WorkflowID     string            `json:"workflow_id"`      // Unique workflow identifier
    NodeName       string            `json:"node_name"`        // Current node name
    ExecutionID    string            `json:"execution_id"`     // Unique execution identifier
    StartedAt      time.Time         `json:"started_at"`       // Workflow start time
    Metadata       map[string]string `json:"metadata"`         // Workflow metadata
    ClusterInfo    ClusterBasicInfo  `json:"cluster_info"`     // Cluster state information
    PreviousNode   *string           `json:"previous_node,omitempty"` // Previous node (if any)
    RetryCount     int               `json:"retry_count"`      // Current retry attempt
    Priority       int               `json:"priority"`         // Execution priority
}

type ClusterBasicInfo struct {
    NodeID   string `json:"node_id"`   // Current cluster node ID
    IsLeader bool   `json:"is_leader"` // Whether this node is Raft leader
    Status   string `json:"status"`    // Cluster status
}
```

### Conditional Logic Based on Context

```go
func (n *ConditionalNode) Execute(ctx context.Context, state MyState, config MyConfig) (graft.NodeResult, error) {
    result := MyResult{ProcessedAt: time.Now()}
    
    if workflowCtx, ok := graft.GetWorkflowContext(ctx); ok {
        if workflowCtx.ClusterInfo.IsLeader {
            result.Message = "Processing on leader node"
        } else {
            result.Message = "Processing on follower node"
        }
        
        if workflowCtx.RetryCount > 0 {
            result.Message += fmt.Sprintf(" (retry %d)", workflowCtx.RetryCount)
        }
        
        if env, exists := workflowCtx.Metadata["environment"]; exists {
            result.Environment = env
        }
    }
    
    return graft.NodeResult{Data: result}, nil
}
```

## Advanced Features

### Workflow Chaining

Nodes can specify next nodes to execute by returning `NextNodes` in the result:

```go
func (n *ConditionalNode) Execute(ctx context.Context, state MyState, config MyConfig) (graft.NodeResult, error) {
    processedValue := state.Value * 2
    
    result := graft.NodeResult{
        Data: MyResult{
            ProcessedValue: processedValue,
            Timestamp:      time.Now(),
        },
    }
    
    // Conditionally add next nodes
    if state.Value > 10 {
        result.NextNodes = []graft.NextNode{
            {
                NodeName: "high-value-processor",
                Config: HighValueConfig{
                    Threshold: 100,
                },
            },
        }
    }
    
    return result, nil
}
```

### Error Handling

```go
// Register error handler
manager.OnError(func(ctx context.Context, data domain.WorkflowErrorData) error {
    log.Printf("Workflow %s failed: %v", data.WorkflowID, data.Error)
    // Implement custom error handling logic
    return nil
})

// Register completion handler  
manager.OnComplete(func(ctx context.Context, data domain.WorkflowCompletionData) error {
    log.Printf("Workflow %s completed successfully", data.WorkflowID)
    // Implement post-processing logic
    return nil
})
```

### Workflow Control

```go
// Pause a running workflow
if err := manager.PauseWorkflow(ctx, "workflow-123"); err != nil {
    log.Printf("Failed to pause workflow: %v", err)
}

// Resume a paused workflow
if err := manager.ResumeWorkflow(ctx, "workflow-123"); err != nil {
    log.Printf("Failed to resume workflow: %v", err)
}

// Stop a workflow permanently
if err := manager.StopWorkflow(ctx, "workflow-123"); err != nil {
    log.Printf("Failed to stop workflow: %v", err)
}
```

### Monitoring

```go
info := manager.GetClusterInfo()
fmt.Printf("Node ID: %s\n", info.NodeID)
fmt.Printf("Status: %s\n", info.Status)
fmt.Printf("Is Leader: %t\n", info.IsLeader)
fmt.Printf("Active workflows: %d\n", info.Metrics.ActiveWorkflows)
fmt.Printf("Completed workflows: %d\n", info.Metrics.CompletedWorkflows)
fmt.Printf("Failed workflows: %d\n", info.Metrics.FailedWorkflows)
```

## Best Practices

### Node Design
- Keep nodes stateless and idempotent
- Use timeouts for external service calls
- Implement proper error handling
- Define clear input/output schemas

### Workflow Design
- Break complex processes into smaller nodes
- Use conditional execution with `CanStart`
- Implement proper error recovery strategies
- Monitor resource utilization

### Cluster Configuration
- Set appropriate resource limits
- Configure timeouts based on workload
- Use TLS in production environments
- Monitor cluster health and performance

### Error Handling
- Implement both error and completion handlers
- Log workflow state for debugging
- Use retries for transient failures
- Implement circuit breakers for external dependencies

## Examples

See the `examples/` directory for complete working examples:
- `basic/`: Simple single-node workflow
- `multi-node/`: Complex data processing pipeline
- `error-handling/`: Error recovery and retry patterns
- `production/`: Production deployment configuration
