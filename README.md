# Graft

[![Go Version](https://img.shields.io/github/go-mod/go-version/eleven-am/graft)](https://golang.org/dl/)
[![License](https://img.shields.io/github/license/eleven-am/graft)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/eleven-am/graft)](https://goreportcard.com/report/github.com/eleven-am/graft)

Graft is an embeddable Go library for building distributed, fault-tolerant workflow orchestration into your applications. It provides the core components for durable, stateful execution of complex tasks, leveraging HashiCorp Raft for consensus and BadgerDB for persistent storage.

By integrating Graft, you can add resilient, long-running process management directly within your existing Go services without needing to manage a separate orchestration system.

## Features

- **Fault-Tolerant & Consistent**: Built on HashiCorp Raft for strong consistency, automatic leader election, and state replication
- **Embeddable & Lightweight**: Designed to be integrated as a library within your existing Go applications
- **Dynamic Workflow Execution**: Define workflows as typed Go structs with automatic type discovery, chainable at runtime
- **Pluggable Service Discovery**: Static peer lists, mDNS, and external providers
- **Resource Management**: Control workflow concurrency with global and per-type execution limits
- **Secure Transport**: gRPC communication with TLS support and health checking
- **Persistent State**: BadgerDB for high-performance local storage with replication

## Quick Start

### Installation

```bash
go get github.com/eleven-am/graft
```

### Basic Example

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

// Define typed structures for type-safe workflow execution
type ProcessorConfig struct {
    Format string `json:"format"`
}

type ProcessorState struct {
    Input string `json:"input"`
}

type ProcessorResult struct {
    Result    string    `json:"result"`
    Timestamp time.Time `json:"timestamp"`
}

// Type-safe processing node
type ProcessorNode struct {}

func (n *ProcessorNode) GetName() string {
    return "processor"
}

func (n *ProcessorNode) Execute(ctx context.Context, state ProcessorState, config ProcessorConfig) (graft.NodeResult, error) {
    format := config.Format
    if format == "" {
        format = "processed: %s"
    }
    
    result := ProcessorResult{
        Result:    fmt.Sprintf(format, state.Input),
        Timestamp: time.Now(),
    }
    
    if workflowCtx, ok := graft.GetWorkflowContext(ctx); ok {
        fmt.Printf("Processing in workflow %s on node %s\n", workflowCtx.WorkflowID, workflowCtx.ClusterInfo.NodeID)
    }
    
    return graft.NodeResult{
        Data: result,
    }, nil
}

func main() {
    // Create logger
    logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
    
    // Create and start manager
    manager := graft.New("node-1", "localhost:7000", "./data", logger)
    if manager == nil {
        log.Fatal("failed to create manager")
    }
    
    // Configure discovery (optional)
    manager.Discovery().MDNS("", "")  // or Static()
    
    ctx := context.Background()
    if err := manager.Start(ctx, 8080); err != nil {  // gRPC port
        log.Fatal(err)
    }
    defer manager.Stop(ctx)
    
    // Register type-safe workflow node
    if err := manager.RegisterNode(&ProcessorNode{}); err != nil {
        log.Fatal(err)
    }
    
    // Start workflow with typed data
    trigger := graft.WorkflowTrigger{
        WorkflowID: "my-workflow-001",
        InitialState: ProcessorState{
            Input: "Hello, World!",
        },
        InitialNodes: []graft.NodeConfig{
            {Name: "processor", Config: ProcessorConfig{Format: "processed: %s"}},
        },
    }
    
    if err := manager.StartWorkflow(trigger); err != nil {
        log.Fatal(err)
    }
    
    // Check workflow status
    state, err := manager.GetWorkflowState("my-workflow-001")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Workflow Status: %s\n", state.Status)
    fmt.Printf("Result: %+v\n", state.CurrentState)
}
```

## Documentation

- **[Examples](./examples/document-pipeline/README.md)** - Working examples for common use cases
- **[API Reference](./docs/API.md)** - Complete API documentation
- **[Architecture](./docs/ARCHITECTURE.md)** - System design and components
- **[Deployment Guide](./docs/DEPLOYMENT.md)** - Production deployment patterns

## How It Works

Graft provides a simple `Node` interface that you implement for your workflow steps. The library handles distribution, state management, and execution coordination across your cluster.

```go
// Your typed workflow logic
type MyConfig struct {
    BatchSize int `json:"batch_size"`
}

type MyState struct {
    Data string `json:"data"`
}

type MyResult struct {
    Processed string    `json:"processed"`
    Timestamp time.Time `json:"timestamp"`
}

type MyProcessor struct{}

func (n *MyProcessor) GetName() string {
    return "my-processor"
}

func (n *MyProcessor) Execute(ctx context.Context, state MyState, config MyConfig) (graft.NodeResult, error) {
    // Your business logic with full type safety
    result := processData(state.Data, config.BatchSize)
    
    return graft.NodeResult{
        Data: MyResult{
            Processed: result,
            Timestamp: time.Now(),
        },
    }, nil
}
```

For detailed architecture information, see [Architecture Documentation](./docs/ARCHITECTURE.md).

## Use Cases

Graft is designed for scenarios requiring durable, distributed task execution: ETL pipelines, microservice orchestration, batch processing, and event-driven workflows. The embedded approach eliminates the operational overhead of managing separate orchestration infrastructure.

## Architecture

Graft is designed for performance and resilience through proven architectural patterns:

- **Asynchronous Processing**: Workflow submission is decoupled from execution for high-throughput ingestion
- **Efficient State Replication**: Raft consensus ensures only the leader processes workflows, with batch replication to followers  
- **Persistent Queuing**: BadgerDB-backed queues ensure workflow durability across node restarts
- **Controlled Concurrency**: Resource manager prevents overload with configurable execution limits
- **Log-Structured Storage**: BadgerDB's LSM-tree design optimized for high write throughput

*Formal performance benchmarks are planned for future releases.*

## Configuration

Basic configuration example:

```go
// Create with default settings
logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
manager := graft.New("node-1", "localhost:7000", "./data", logger)

// Configure discovery before starting
manager.Discovery().MDNS("", "")  // mDNS discovery (defaults: _graft._tcp, local.)
// or manager.Discovery().Static([]graft.Peer{{ID: "peer1", Address: "host", Port: 7001}})  // Static peers

// Start with gRPC port
if err := manager.Start(context.Background(), 8080); err != nil {
    log.Fatal(err)
}
```

For complete configuration options, see [Configuration Guide](./docs/API.md#configuration).

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [HashiCorp Raft](https://github.com/hashicorp/raft) for consensus protocol
- [BadgerDB](https://github.com/dgraph-io/badger) for high-performance storage
- [gRPC](https://grpc.io/) for efficient network communication
- [Prometheus](https://prometheus.io/) for monitoring and metrics
- With love from [Roy Ossai](https://github.com/eleven-am)
