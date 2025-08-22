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
go get github.com/eleven-am/graft/pkg/graft
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/eleven-am/graft/pkg/graft"
)

func main() {
    // Create cluster with default configuration
    config := graft.DefaultConfig()
    config.NodeID = "example-node"
    
    cluster, err := graft.New(config)
    if err != nil {
        log.Fatal(err)
    }
    
    // Start the cluster
    if err := cluster.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
    defer cluster.Stop()
    
    // Register a simple node
    node := &SimpleProcessor{}
    if err := cluster.RegisterNode(node); err != nil {
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
    
    if err := cluster.StartWorkflow("example-workflow", trigger); err != nil {
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
    // Process with type safety
    result := ProcessorResult{
        Output:    fmt.Sprintf("processed: %s", state.Input),
        Timestamp: time.Now(),
    }
    
    return graft.NodeResult{
        Data: result,
    }, nil
}
```

## Core API

### Cluster Interface

```go
type Cluster interface {
    Start(ctx context.Context) error
    Stop() error
    RegisterNode(node Node) error
    StartWorkflow(workflowID string, trigger WorkflowTrigger) error
    GetWorkflowState(workflowID string) (WorkflowState, error)
    GetClusterInfo() ClusterInfo
    OnComplete(handler CompletionHandler)
    OnError(handler ErrorHandler)
}
```

#### Methods

**Start(ctx context.Context) error**
- Starts the cluster and begins listening for connections
- Must be called before any workflow operations
- Context can be used for graceful shutdown

**Stop() error**
- Gracefully shuts down the cluster
- Completes running workflows before stopping
- Should be called in defer or signal handlers

**RegisterNode(node Node) error**
- Registers a node implementation with the cluster
- Node must implement the `Node` interface
- Node names must be unique within the cluster

**StartWorkflow(workflowID string, trigger WorkflowTrigger) error**
- Initiates a new workflow execution
- WorkflowID must be unique across the cluster
- Returns error if workflow already exists

**GetWorkflowState(workflowID string) (WorkflowState, error)**
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

### Config Structure

```go
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

    LogLevel string `json:"log_level" yaml:"log_level"`
}
```

### Discovery Configuration

```go
type DiscoveryConfig struct {
    Strategy    string            `json:"strategy" yaml:"strategy"`
    ServiceName string            `json:"service_name" yaml:"service_name"`
    ServicePort int               `json:"service_port" yaml:"service_port"`
    Peers       []string          `json:"peers" yaml:"peers"`
    Metadata    map[string]string `json:"metadata" yaml:"metadata"`
}
```

**Strategies:**
- `"auto"`: Automatically detect best strategy
- `"static"`: Use predefined peer list
- `"mdns"`: Use multicast DNS for local discovery
- `"kubernetes"`: Use Kubernetes service discovery

### Transport Configuration

```go
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
```

### Resource Configuration

```go
type ResourceConfig struct {
    MaxConcurrentTotal   int            `json:"max_concurrent_total" yaml:"max_concurrent_total"`
    MaxConcurrentPerType map[string]int `json:"max_concurrent_per_type" yaml:"max_concurrent_per_type"`
    DefaultPerTypeLimit  int            `json:"default_per_type_limit" yaml:"default_per_type_limit"`
}
```

### Engine Configuration

```go
type EngineConfig struct {
    MaxConcurrentWorkflows int    `json:"max_concurrent_workflows" yaml:"max_concurrent_workflows"`
    NodeExecutionTimeout   string `json:"node_execution_timeout" yaml:"node_execution_timeout"`
    StateUpdateInterval    string `json:"state_update_interval" yaml:"state_update_interval"`
    RetryAttempts          int    `json:"retry_attempts" yaml:"retry_attempts"`
    RetryBackoff           string `json:"retry_backoff" yaml:"retry_backoff"`
}
```

## Data Types

### WorkflowTrigger

```go
type WorkflowTrigger struct {
    WorkflowID   string                   `json:"workflow_id"`
    InitialState map[string]interface{}   `json:"initial_state"`
    InitialNodes []NodeConfig             `json:"initial_nodes"`
    Metadata     map[string]string        `json:"metadata"`
}
```

### WorkflowState

```go
type WorkflowState struct {
    WorkflowID    string                 `json:"workflow_id"`
    Status        string                 `json:"status"`
    CurrentState  map[string]interface{} `json:"current_state"`
    StartedAt     string                 `json:"started_at"`
    CompletedAt   *string                `json:"completed_at,omitempty"`
    ExecutedNodes []ExecutedNode         `json:"executed_nodes"`
    PendingNodes  []PendingNode          `json:"pending_nodes"`
    ReadyNodes    []ReadyNode            `json:"ready_nodes"`
    LastError     *string                `json:"last_error,omitempty"`
}
```

**Status Values:**
- `"pending"`: Workflow queued but not started
- `"running"`: Currently executing
- `"completed"`: Successfully finished
- `"failed"`: Terminated due to error
- `"cancelled"`: Manually stopped

### ClusterInfo

```go
type ClusterInfo struct {
    NodeID            string            `json:"node_id"`
    RegisteredNodes   []string          `json:"registered_nodes"`
    ActiveWorkflows   int64             `json:"active_workflows"`
    ResourceLimits    ResourceConfig    `json:"resource_limits"`
    ExecutionStats    ExecutionStats    `json:"execution_stats"`
    EngineMetrics     EngineMetrics     `json:"engine_metrics"`
    ClusterMembers    []ClusterMember   `json:"cluster_members"`
    IsLeader          bool              `json:"is_leader"`
}
```

## Advanced Features

### Workflow Chaining

Nodes can specify next nodes to execute by including `_next_nodes` in their output:

```go
func (n *ConditionalNode) Execute(ctx context.Context, config, state map[string]interface{}) (map[string]interface{}, error) {
    value := state["value"].(int)
    
    result := map[string]interface{}{
        "processed_value": value * 2,
    }
    
    if value > 10 {
        result["_next_nodes"] = []interface{}{
            map[string]interface{}{
                "node_name": "high-value-processor",
                "config": map[string]interface{}{
                    "threshold": 100,
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
cluster.OnError(func(workflowID string, finalState map[string]interface{}, err error) {
    log.Printf("Workflow %s failed: %v", workflowID, err)
    // Implement custom error handling logic
})

// Register completion handler
cluster.OnComplete(func(workflowID string, finalState map[string]interface{}) {
    log.Printf("Workflow %s completed successfully", workflowID)
    // Implement post-processing logic
})
```

### Resource Management

```go
config := graft.DefaultConfig()
config.Resources = graft.ResourceConfig{
    MaxConcurrentTotal: 100,
    MaxConcurrentPerType: map[string]int{
        "heavy-processor": 5,
        "light-processor": 20,
    },
    DefaultPerTypeLimit: 10,
}
```

### Monitoring

```go
info := cluster.GetClusterInfo()
fmt.Printf("Active workflows: %d\n", info.ActiveWorkflows)
fmt.Printf("Resource utilization: %d/%d\n", 
    info.ExecutionStats.TotalExecuting, 
    info.ExecutionStats.TotalCapacity)
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