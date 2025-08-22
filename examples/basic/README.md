# Basic Workflow Example

Simple single-node cluster demonstrating basic Graft workflow functionality.

## Overview

This example shows:
- Single-node cluster setup
- Basic node implementation
- Simple workflow execution
- Status monitoring

## Files

- `main.go` - Main application
- `config.yaml` - Configuration file
- `nodes/` - Custom node implementations

## Running

```bash
go run main.go
```

## Configuration

The example uses minimal configuration suitable for development:

```yaml
node_id: "node-1"
service_name: "basic-example"
service_port: 8080

discovery:
  strategy: "static"
  
transport:
  listen_address: "127.0.0.1"
  listen_port: 9090

storage:
  data_dir: "./data/basic"
  
resources:
  max_concurrent_total: 10
  default_per_type_limit: 5
```

## Workflow

1. Starts a single-node cluster
2. Registers a simple processing node
3. Executes a workflow with the processing node
4. Displays workflow status and results

## Expected Output

```
Cluster started successfully
Node registered: data-processor
Workflow started: workflow-001
Workflow completed with result: {"processed_data": "Hello, World! (processed)", "timestamp": "..."}
```

## Next Steps

- Modify the ProcessorNode to add custom logic
- Add more nodes to create a pipeline
- Try the multi-node example for complex workflows