# Graft - Distributed Workflow Orchestration Engine

## What is Graft?

Graft is a production-ready distributed workflow orchestration engine for Go applications. It enables you to build complex, multi-step workflows that can be distributed across multiple machines while maintaining consistency and fault tolerance.

## Core Concepts

### Workflows
A workflow is a series of connected steps (nodes) that process data through transformations. Each workflow:
- Has a unique ID for tracking
- Maintains state that flows through nodes
- Can branch conditionally based on node decisions
- Persists across system failures
- Emits events for monitoring

### Nodes
Nodes are the individual processing units in your workflow. They:
- Receive the current workflow state
- Perform specific operations
- Return updated state and next nodes to execute
- Can execute conditionally using `CanStart`
- Support various execution signatures for flexibility

### Cluster
Graft automatically manages a distributed cluster where:
- Workflows can execute on any available node
- State is replicated for fault tolerance
- Leader election happens automatically
- Nodes can join/leave dynamically

## Quick Start

### Installation
```bash
go get github.com/eleven-am/graft
```

### Basic Usage

#### 1. Define Your Workflow Node

You can use either custom structs for type safety or maps for flexibility:

**Option A: Using Custom Structs (Recommended)**
```go
type ProcessDataNode struct{}

type DataState struct {
    Input     string      `json:"input"`
    Processed interface{} `json:"processed,omitempty"`
    Valid     bool        `json:"valid,omitempty"`
}

type DataConfig struct {
    Threshold float64 `json:"threshold"`
    Mode      string  `json:"mode"`
}

func (n *ProcessDataNode) GetName() string {
    return "ProcessData"
}

func (n *ProcessDataNode) Execute(ctx context.Context, state *DataState, config *DataConfig) (*graft.NodeResult, error) {
    // Access workflow context
    workflowCtx, _ := graft.GetWorkflowContext(ctx)
    
    // Process your data with type safety
    processedData := processData(state.Input)
    state.Processed = processedData
    
    // Update state and determine next nodes
    return &graft.NodeResult{
        GlobalState: state,  // Automatically serialized to JSON
        NextNodes: []graft.NextNode{
            {
                NodeName: "ValidateData",
                Config:   &DataConfig{Threshold: 0.8, Mode: "strict"},  // Can use struct
            },
        },
    }, nil
}

// Optional: Conditional execution
func (n *ProcessDataNode) CanStart(ctx context.Context, state *DataState, config *DataConfig) bool {
    // Type-safe check
    return state.Input != ""
}
```

**Option B: Using Maps (More Flexible)**
```go
type ProcessDataNode struct{}

func (n *ProcessDataNode) GetName() string {
    return "ProcessData"
}

func (n *ProcessDataNode) Execute(ctx context.Context, state map[string]interface{}, config map[string]interface{}) (*graft.NodeResult, error) {
    // Access workflow context
    workflowCtx, _ := graft.GetWorkflowContext(ctx)
    
    // Process your data
    processedData := processData(state["input"])
    
    // Update state and determine next nodes
    return &graft.NodeResult{
        GlobalState: map[string]interface{}{
            "processed": processedData,
        },
        NextNodes: []graft.NextNode{
            {
                NodeName: "ValidateData",
                Config:   map[string]interface{}{"threshold": 0.8},
            },
        },
    }, nil
}

// Optional: Conditional execution
func (n *ProcessDataNode) CanStart(ctx context.Context, state map[string]interface{}, config map[string]interface{}) bool {
    // Only start if input exists
    _, hasInput := state["input"]
    return hasInput
}
```

#### 2. Initialize and Start Graft
```go
func main() {
    logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
    
    // Create manager
    manager := graft.New(
        "node-1",           // Unique node ID
        "localhost:7000",   // Raft bind address
        "./data",           // Data directory
        logger,
    )
    
    // Register your nodes
    manager.RegisterNode(&ProcessDataNode{})
    manager.RegisterNode(&ValidateDataNode{})
    
    // Start the cluster
    ctx := context.Background()
    grpcPort := 8080
    if err := manager.Start(ctx, grpcPort); err != nil {
        log.Fatal(err)
    }
    
    // Trigger a workflow
    trigger := graft.WorkflowTrigger{
        WorkflowID: "workflow-123",
        InitialNodes: []graft.NodeConfig{
            {Name: "ProcessData", Config: map[string]interface{}{}},
        },
        InitialState: map[string]interface{}{
            "input": "raw data",
        },
        Metadata: map[string]string{
            "source": "api",
            "priority": "high",
        },
    }
    
    if err := manager.StartWorkflow(trigger); err != nil {
        log.Printf("Failed to start workflow: %v", err)
    }
}
```

## Node Development Guide

### Node Interface Patterns

Graft supports multiple node signatures for flexibility:

```go
// Full signature with context
func (n *MyNode) Execute(ctx context.Context, state interface{}, config interface{}) (*graft.NodeResult, error)

// Without context
func (n *MyNode) Execute(state interface{}, config interface{}) (*graft.NodeResult, error)

// State only
func (n *MyNode) Execute(state interface{}) (*graft.NodeResult, error)
```

### State Management

State flows through your workflow as JSON-compatible data. You can use `map[string]interface{}` for flexibility or **your own structs** for type safety:

#### Using Custom Structs (Recommended for Type Safety)
```go
type MyWorkflowState struct {
    Input       string   `json:"input"`
    Result      float64  `json:"result"`
    ProcessedAt string   `json:"processed_at"`
    Items       []string `json:"items"`
}

type MyNodeConfig struct {
    Threshold float64 `json:"threshold"`
    MaxRetries int    `json:"max_retries"`
}

func (n *MyNode) Execute(ctx context.Context, state *MyWorkflowState, config *MyNodeConfig) (*graft.NodeResult, error) {
    // Type-safe access to state
    result := process(state.Input)
    
    // Update state with type safety
    state.Result = result
    state.ProcessedAt = time.Now().Format(time.RFC3339)
    
    return &graft.NodeResult{
        GlobalState: state,  // Your struct will be automatically serialized to JSON
        NextNodes: determineNextNodes(result),
    }, nil
}
```

#### Using Maps (Flexible but Less Type-Safe)
```go
func (n *MyNode) Execute(ctx context.Context, state map[string]interface{}, config map[string]interface{}) (*graft.NodeResult, error) {
    // Read from state
    input := state["input"].(string)
    
    // Process
    result := process(input)
    
    // Return updated state
    return &graft.NodeResult{
        GlobalState: map[string]interface{}{
            "input": input,
            "result": result,
        },
        NextNodes: determineNextNodes(result),
    }, nil
}
```

### Branching and Flow Control

Nodes can dynamically determine the next steps:

```go
func determineNextNodes(result interface{}) []graft.NextNode {
    if result.(float64) > 0.5 {
        return []graft.NextNode{
            {NodeName: "HighValuePath"},
        }
    }
    return []graft.NextNode{
        {NodeName: "LowValuePath"},
    }
}
```

### Advanced Node Features

#### Priority and Scheduling
```go
return &graft.NodeResult{
    GlobalState: updatedState,
    NextNodes: []graft.NextNode{
        {
            NodeName: "UrgentTask",
            Priority: 10,  // Higher priority executes first
        },
        {
            NodeName: "BackgroundTask",
            Priority: 1,
            Delay:    &fiveMinutes,  // Delay execution
        },
    },
}
```

#### Idempotency
```go
// Ensure a node executes only once for a given key
return &graft.NodeResult{
    GlobalState: state,
    NextNodes: []graft.NextNode{
        {
            NodeName:       "SendEmail",
            IdempotencyKey: &emailID,  // Won't re-execute for same key
        },
    },
}
```

## Cluster Configuration

### Service Discovery Options

#### mDNS (Local Development)
```go
config := &domain.Config{
    NodeID:   "node-1",
    BindAddr: "localhost:7000",
    DataDir:  "./data",
    Discovery: []domain.DiscoveryConfig{
        {Type: domain.DiscoveryMDNS},
    },
}
manager := graft.NewWithConfig(config)
```

#### Kubernetes (Production)
```go
config := &domain.Config{
    // ... basic config ...
    Discovery: []domain.DiscoveryConfig{
        {
            Type: domain.DiscoveryKubernetes,
            Kubernetes: &domain.KubernetesConfig{
                ServiceName: "graft-cluster",
                Namespace:   "default",
            },
        },
    },
}
```

#### Static Peers
```go
config := &domain.Config{
    // ... basic config ...
    Discovery: []domain.DiscoveryConfig{
        {
            Type: domain.DiscoveryStatic,
            Static: []domain.StaticPeer{
                {ID: "node-1", Address: "10.0.0.1", Port: 7000},
                {ID: "node-2", Address: "10.0.0.2", Port: 7000},
            },
        },
    },
}
```

### Cluster Policies

Control how nodes join your cluster:

- **OPEN**: Any node can join (development)
- **RESTRICTED**: Nodes need expected list (staging)
- **STRICT**: Only pre-defined nodes allowed (production)

```go
config := &domain.Config{
    // ... other config ...
    Cluster: domain.ClusterConfig{
        Policy: domain.ClusterPolicyRestricted,
        ExpectedNodes: []string{"node-1", "node-2", "node-3"},
    },
}
```

## Event Handling

Monitor workflow execution through events:

```go
// Workflow events
manager.OnWorkflowStarted(func(event *graft.WorkflowStartedEvent) {
    log.Printf("Workflow %s started", event.WorkflowID)
})

manager.OnWorkflowCompleted(func(event *graft.WorkflowCompletedEvent) {
    log.Printf("Workflow %s completed in %v", event.WorkflowID, event.Duration)
})

manager.OnWorkflowError(func(event *graft.WorkflowErrorEvent) {
    log.Printf("Workflow %s failed: %v", event.WorkflowID, event.Error)
})

// Node events
manager.OnNodeStarted(func(event *graft.NodeStartedEvent) {
    log.Printf("Node %s started in workflow %s", event.NodeName, event.WorkflowID)
})

manager.OnNodeCompleted(func(event *graft.NodeCompletedEvent) {
    log.Printf("Node %s completed in %dms", event.NodeName, event.Duration.Milliseconds())
})
```

## Workflow Management

### Query Workflow Status
```go
status, err := manager.GetWorkflowStatus("workflow-123")
if err != nil {
    log.Printf("Error: %v", err)
    return
}

fmt.Printf("Workflow State: %s\n", status.Status)
fmt.Printf("Executed Nodes: %d\n", len(status.ExecutedNodes))
fmt.Printf("Pending Nodes: %d\n", len(status.PendingNodes))
```

### Control Workflow Execution
```go
// Pause a running workflow
err := manager.PauseWorkflow(ctx, "workflow-123")

// Resume a paused workflow
err := manager.ResumeWorkflow(ctx, "workflow-123")

// Stop a workflow
err := manager.StopWorkflow(ctx, "workflow-123")
```

### Cluster Information
```go
info := manager.GetClusterInfo()
fmt.Printf("Node ID: %s\n", info.NodeID)
fmt.Printf("Is Leader: %v\n", info.IsLeader)
fmt.Printf("Cluster Peers: %v\n", info.Peers)
fmt.Printf("Active Workflows: %d\n", info.Metrics.ActiveWorkflows)
```

## Best Practices

### 1. Node Design
- **Keep nodes focused**: Each node should do one thing well
- **Make nodes idempotent**: Design for retry safety
- **Handle errors gracefully**: Return errors for retry, don't panic
- **Use context**: Access workflow metadata via `GetWorkflowContext(ctx)`

### 2. State Management
- **Use structs for type safety**: Define custom structs for your workflow state
- **Keep state serializable**: All types must be JSON-compatible
- **Avoid large state**: Store references, not large data
- **Version your state schema**: Plan for evolution
- **Use immutable updates**: Don't modify input state directly

### 3. Workflow Design
- **Plan for failure**: Design with retries and error paths
- **Use conditional execution**: Leverage `CanStart` for efficiency
- **Set appropriate timeouts**: Prevent stuck workflows
- **Monitor with events**: Track execution and performance

### 4. Cluster Operations
- **Start with 3+ nodes**: For production fault tolerance
- **Use appropriate discovery**: mDNS for dev, K8s for production
- **Choose the right policy**: STRICT for production clusters
- **Monitor cluster health**: Check leader status and peer count

### 5. Performance Considerations
- **Configure worker count**: Based on CPU cores and workload
- **Use priorities**: For time-sensitive operations
- **Batch when possible**: Reduce state update frequency
- **Monitor metrics**: Track execution times and queue depth

## Common Patterns

### Parallel Processing
```go
// Fan-out to multiple nodes
return &graft.NodeResult{
    GlobalState: state,
    NextNodes: []graft.NextNode{
        {NodeName: "ProcessorA"},
        {NodeName: "ProcessorB"},
        {NodeName: "ProcessorC"},
    },
}
```

### Conditional Branching
```go
func (n *RouterNode) Execute(ctx context.Context, state map[string]interface{}, config map[string]interface{}) (*graft.NodeResult, error) {
    value := state["score"].(float64)
    
    var nextNode string
    switch {
    case value > 0.8:
        nextNode = "HighScorePath"
    case value > 0.5:
        nextNode = "MediumScorePath"
    default:
        nextNode = "LowScorePath"
    }
    
    return &graft.NodeResult{
        GlobalState: state,
        NextNodes: []graft.NextNode{{NodeName: nextNode}},
    }, nil
}
```

### Error Handling with Retry
```go
func (n *RetryableNode) Execute(ctx context.Context, state map[string]interface{}, config map[string]interface{}) (*graft.NodeResult, error) {
    retryCount := 0
    if val, ok := state["retryCount"].(float64); ok {
        retryCount = int(val)
    }
    
    result, err := riskyOperation()
    if err != nil {
        if retryCount < 3 {
            // Update retry count and re-queue self
            state["retryCount"] = retryCount + 1
            return &graft.NodeResult{
                GlobalState: state,
                NextNodes: []graft.NextNode{
                    {
                        NodeName: "RetryableNode",
                        Delay:    &backoffDelay,
                    },
                },
            }, nil
        }
        // Max retries exceeded, move to error handling
        return &graft.NodeResult{
            GlobalState: state,
            NextNodes: []graft.NextNode{{NodeName: "ErrorHandler"}},
        }, nil
    }
    
    // Success, continue workflow
    state["result"] = result
    delete(state, "retryCount")
    return &graft.NodeResult{
        GlobalState: state,
        NextNodes: []graft.NextNode{{NodeName: "NextStep"}},
    }, nil
}
```

## Troubleshooting

### Common Issues

**Workflow not starting:**
- Ensure cluster has a leader: `manager.GetClusterInfo().IsLeader`
- Check node registration: `manager.ListNodes()`
- Verify initial node exists and is registered

**Nodes not executing:**
- Check `CanStart` conditions
- Verify state has required fields
- Review logs for execution errors

**Cluster not forming:**
- Confirm network connectivity between nodes
- Check discovery configuration
- Verify Raft bind addresses are accessible
- Review cluster policy settings

**State not persisting:**
- Ensure data directory has write permissions
- Check disk space availability
- Verify state is JSON-serializable

## Performance Tuning

### Engine Configuration
```go
config := &domain.Config{
    // ... other config ...
    Engine: domain.EngineConfig{
        WorkerCount:     10,              // Concurrent workers
        MaxRetries:      3,               // Per-node retry limit
        RetryDelay:      5 * time.Second, // Base retry delay
        QueueBufferSize: 1000,            // Queue capacity
    },
}
```

### Resource Considerations
- **Workers**: Set to 2x CPU cores for I/O-bound work
- **Queue Buffer**: Increase for bursty workloads
- **Data Directory**: Use SSD for better performance
- **Network**: Low latency between cluster nodes critical

## Security Considerations

- **Cluster Policy**: Use STRICT in production
- **Network**: Secure Raft and gRPC ports
- **State**: Don't store sensitive data directly
- **Nodes**: Validate all inputs and configs
- **Discovery**: Use Kubernetes RBAC when applicable

## Migration and Compatibility

- Workflows persist across restarts
- State format should be versioned
- Nodes can be updated independently
- Rolling cluster updates supported
- Backward compatibility within major versions

## Cross-Node Developer Messaging

Graft provides a built-in cluster command system that allows developers to send messages and commands across nodes in the cluster.

### Quick Start

#### 1. Register Command Handlers

Use type-safe handlers for your commands:

```go
type DeployParams struct {
    Service string `json:"service"`
    Version string `json:"version"`
    Replicas int   `json:"replicas"`
}

// Register a typed handler using the wrapper
manager.RegisterCommandHandler("deploy", 
    graft.WrapHandler(func(ctx context.Context, from string, params DeployParams) error {
        log.Printf("Deploy command from %s: %+v", from, params)
        return deployService(params.Service, params.Version, params.Replicas)
    }),
)

// Or register a generic handler
manager.RegisterCommandHandler("scale", 
    func(ctx context.Context, from string, params interface{}) error {
        log.Printf("Scale command from %s: %+v", from, params)
        return scaleService(params)
    },
)
```

#### 2. Broadcast Commands

Send commands to all nodes in the cluster:

```go
// Using typed parameters
deployCmd := &graft.DevCommand{
    Command: "deploy",
    Params: DeployParams{
        Service:  "api",
        Version:  "1.2.3", 
        Replicas: 3,
    },
}

// Broadcast to all nodes
if err := manager.BroadcastCommand(ctx, deployCmd); err != nil {
    log.Printf("Failed to broadcast deploy command: %v", err)
}

// Using map parameters for flexibility
scaleCmd := &graft.DevCommand{
    Command: "scale",
    Params: map[string]interface{}{
        "service": "worker",
        "count":   10,
    },
}

if err := manager.BroadcastCommand(ctx, scaleCmd); err != nil {
    log.Printf("Failed to broadcast scale command: %v", err)
}
```

### How It Works

1. **Commands are distributed via Raft**: All cluster commands go through the same consensus mechanism as workflows, ensuring ordering and consistency
2. **Type-safe handlers**: Use `graft.WrapHandler[T]()` to wrap typed handlers that automatically convert `interface{}` parameters to your specific struct types
3. **No storage overhead**: Unlike workflows, dev commands execute immediately without persisting to storage
4. **Cross-node execution**: Commands execute on every node in the cluster simultaneously

### Advanced Usage

#### Custom Command Types

Define your own command structures for type safety:

```go
type ConfigUpdateParams struct {
    Key   string      `json:"key"`
    Value interface{} `json:"value"`
    Scope string      `json:"scope"` // "global", "local", etc.
}

type LogLevelParams struct {
    Level  string `json:"level"`  // "debug", "info", "warn", "error"
    Module string `json:"module"` // Optional module filter
}

// Register handlers
manager.RegisterCommandHandler("config-update",
    graft.WrapHandler(func(ctx context.Context, from string, params ConfigUpdateParams) error {
        return updateConfig(params.Key, params.Value, params.Scope)
    }),
)

manager.RegisterCommandHandler("set-log-level",
    graft.WrapHandler(func(ctx context.Context, from string, params LogLevelParams) error {
        return setLogLevel(params.Level, params.Module)
    }),
)
```

#### Error Handling

Command handlers should return errors for failed operations:

```go
manager.RegisterCommandHandler("risky-operation",
    graft.WrapHandler(func(ctx context.Context, from string, params RiskyParams) error {
        if err := validateParams(params); err != nil {
            return fmt.Errorf("invalid params: %w", err)
        }
        
        if err := performOperation(params); err != nil {
            return fmt.Errorf("operation failed: %w", err)
        }
        
        log.Printf("Operation completed successfully from node %s", from)
        return nil
    }),
)
```

### Use Cases

- **Configuration updates**: Push config changes to all nodes
- **Log level changes**: Adjust logging across the cluster
- **Cache invalidation**: Clear caches on all nodes
- **Feature flag updates**: Toggle features cluster-wide
- **Health checks**: Trigger diagnostics across nodes
- **Deployment coordination**: Coordinate rolling updates
- **Maintenance tasks**: Run cleanup operations cluster-wide

### Best Practices

1. **Use type-safe handlers**: Prefer `graft.WrapHandler[T]()` for compile-time safety
2. **Keep commands simple**: Commands should be lightweight operations
3. **Handle errors gracefully**: Return descriptive errors from handlers
4. **Use meaningful command names**: Use clear, descriptive names like "deploy", "scale", "update-config"
5. **Validate parameters**: Always validate input parameters in handlers
6. **Log command execution**: Log both successful and failed command executions
7. **Don't block**: Keep command handlers fast and non-blocking

## Need Help?

- Check examples in `/examples` directory
- Review test files for usage patterns
- Enable debug logging for troubleshooting
- Monitor cluster metrics and events
- Consider workflow visualization tools for complex flows