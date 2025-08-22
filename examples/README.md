# Graft Examples

This directory contains practical examples demonstrating how to use Graft for different scenarios. Each example includes working code, configuration files, and detailed documentation.

## Getting Started

All examples can be run directly with `go run main.go` and include their own README with specific instructions.

## Examples Overview

### [Basic](./basic/) - Simple Single-Node Workflow
Use case: Learning the basics, development, simple automation

- Single-node cluster setup
- Basic workflow execution
- Simple configuration
- Error handling fundamentals

```bash
cd examples/basic && go run main.go
```

### [Multi-Node](./multi-node/) - Complex Workflow Pipeline
Use case: Data pipelines, multi-step processing, workflow dependencies

- Multi-step data processing pipeline
- Node dependencies and sequencing
- State passing between nodes
- Custom node implementations
- Conditional workflow branching

```bash
cd examples/multi-node && go run main.go
```

### [Production](./production/) - Production-Ready Cluster
Use case: Production deployments, high availability, scalability

- 3-node cluster setup
- Persistent storage configuration
- TLS security
- Health monitoring
- Docker Compose deployment

```bash
cd examples/production && docker-compose up
```

### [Discovery](./discovery/) - Service Discovery Strategies
Use case: Understanding different deployment patterns

#### [Static](./discovery/static/) - Static Peer Configuration
- Fixed peer list configuration
- Simple setup for known infrastructure
- Docker networking examples

#### [mDNS](./discovery/mdns/) - Zero-Config Discovery
- Automatic node discovery on local network
- Development and testing scenarios
- IoT and edge computing use cases

#### [Kubernetes](./discovery/kubernetes/) - Kubernetes Native
- Kubernetes service discovery
- Pod-to-pod communication
- Kubernetes manifests and helm charts

### [Monitoring](./monitoring/) - Observability and Metrics
Use case: Production monitoring, troubleshooting, performance tuning

- Prometheus metrics collection
- Custom health checks
- Performance monitoring dashboards
- Alerting configuration
- Log aggregation setup

```bash
cd examples/monitoring && docker-compose up
```

### [Error Handling](./error-handling/) - Resilience Patterns
Use case: Building robust workflows, handling failures gracefully

- Retry mechanisms and backoff strategies
- Circuit breaker patterns
- Dead letter queues
- Workflow recovery
- Error notification systems

## Quick Start Checklist

1. **Install Go 1.23+** - Graft requires Go 1.23 or later
2. **Clone the repository** - `git clone github.com/eleven-am/graft`
3. **Choose an example** - Start with `basic` if you're new to Graft
4. **Follow the example README** - Each has specific setup instructions
5. **Run the example** - Most use `go run main.go`

## Example Patterns

### Node Implementation Pattern
```go
// Define typed structures for type-safe execution
type MyConfig struct {
    Timeout  time.Duration `json:"timeout"`
    MaxRetries int        `json:"max_retries"`
}

type MyState struct {
    Input    string `json:"input"`
    Metadata map[string]string `json:"metadata"`
}

type MyResult struct {
    ProcessedData string    `json:"processed_data"`
    Timestamp     time.Time `json:"timestamp"`
}

// Implement the node
type MyNode struct {
    name string
}

func (n *MyNode) GetName() string {
    return n.name
}

func (n *MyNode) Execute(ctx context.Context, state MyState, config MyConfig) (graft.NodeResult, error) {
    // Your processing logic with full type safety
    result := MyResult{
        ProcessedData: fmt.Sprintf("Processed: %s", state.Input),
        Timestamp:     time.Now(),
    }
    
    return graft.NodeResult{
        Data: result,
        NextNodes: []graft.NextNodeConfig{
            {NodeName: "next-processor", Config: MyConfig{Timeout: 5 * time.Second}},
        },
    }, nil
}

// Optional: Control when node can execute
func (n *MyNode) CanStart(ctx context.Context, state MyState, config MyConfig) bool {
    return state.Input != ""
}
```

### Configuration Pattern
```go
config := graft.DefaultConfig()
config.NodeID = "unique-node-id"
config.ServiceName = "my-cluster"

// Customize for your needs
config.Resources.MaxConcurrentTotal = 100
config.Engine.MaxConcurrentWorkflows = 50

cluster, err := graft.New(config)
```

### Workflow Trigger Pattern
```go
// Using typed state and config
trigger := graft.WorkflowTrigger{
    WorkflowID: "unique-workflow-id",
    InitialState: MyState{
        Input: "your data here",
        Metadata: map[string]string{"key": "value"},
    },
    InitialNodes: []graft.NodeConfig{
        {
            Name: "first-node", 
            Config: MyConfig{
                Timeout: 10 * time.Second,
                MaxRetries: 3,
            },
        },
    },
    Metadata: map[string]string{
        "source": "api",
        "priority": "high",
    },
}

err := cluster.StartWorkflow(trigger.WorkflowID, trigger)
```

## Common Configuration Options

### Development Setup
```go
config := graft.DefaultConfig()
config.Discovery.Strategy = graft.StrategyStatic
config.Storage.DataDir = "./data"
config.LogLevel = "debug"
```

### Production Setup
```go
config := graft.DefaultConfig()
config.Discovery.Strategy = graft.StrategyKubernetes
config.Transport.EnableTLS = true
config.Transport.TLSCertFile = "/etc/ssl/cert.pem"
config.Storage.DataDir = "/var/lib/graft"
config.Resources.MaxConcurrentTotal = 1000
```

## Additional Resources

- **[API Reference](../API.md)** - Complete API documentation
- **[Architecture Guide](../ARCHITECTURE.md)** - System design and internals
- **[Deployment Guide](../DEPLOYMENT.md)** - Production deployment patterns
- **[Performance Guide](../PERFORMANCE.md)** - Tuning and optimization

## Troubleshooting

### Common Issues

**Port Conflicts**
- Ensure ports 8080 (service) and 9090 (transport) are available
- Modify `config.ServicePort` and `config.Transport.ListenPort` if needed

**Permission Errors**
- Check filesystem permissions for data directories
- Ensure the process has write access to storage paths

**Network Issues**
- Verify firewall settings allow required ports
- Check DNS resolution for peer nodes
- Validate TLS certificates if using encryption

### Getting Help

1. Check the example-specific README files
2. Review the main documentation
3. Search existing GitHub issues
4. Create a new issue with example reproduction steps

