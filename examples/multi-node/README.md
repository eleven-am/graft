# Multi-Node Pipeline Example

Complex data processing pipeline demonstrating multi-step workflows with node dependencies and parallel execution.

## Overview

This example demonstrates:
- Multi-step data processing pipeline
- Node dependencies and sequencing  
- State passing between nodes
- Parallel workflow execution
- Resource allocation per node type
- Conditional branching based on validation

## Pipeline Flow

```
Raw Data → Data Processor → Data Validator → Data Store
```

1. **Data Processor**: Processes raw input data
2. **Data Validator**: Validates processed data against rules
3. **Data Store**: Stores valid data or rejects invalid data

## Files

- `main.go` - Pipeline implementation with three node types
- `config.yaml` - Configuration with per-node resource limits
- `README.md` - This documentation

## Running

```bash
go run main.go
```

## Configuration

Enhanced configuration for multi-node processing:

```yaml
resources:
  max_concurrent_total: 20
  max_concurrent_per_type:
    data-processor: 8
    data-validator: 6  
    data-store: 4

engine:
  max_concurrent_workflows: 10
  node_execution_timeout: "45s"
```

## Node Implementations

### DataProcessor
- Processes raw input strings
- Adds metadata (size, timestamp, processor ID)
- Simulates processing delay

### DataValidator  
- Validates processed data size against minimum threshold
- Configurable validation rules via node config
- Returns validation status and rules applied

### DataStore
- Stores valid data with generated storage ID
- Rejects invalid data with reason
- Simulates storage operations

## Workflow Examples

The example starts 4 parallel workflows with different data sizes:
- `workflow-001`: "sample_data_large" (passes validation)
- `workflow-002`: "small" (may fail validation)
- `workflow-003`: "medium_sized_data" (passes validation)
- `workflow-004`: "huge_dataset_for_processing" (passes validation)

## Expected Output

```
Cluster started successfully
Registered 3 nodes in pipeline
Started workflow: workflow-001
Started workflow: workflow-002
Started workflow: workflow-003
Started workflow: workflow-004
Workflow workflow-001 status: running
...
Workflow workflow-001 completed successfully!
Final state: {"storage_status": "stored", "storage_id": "store_1640995200", ...}
```

## Key Concepts

### State Passing
Each node receives the accumulated state from previous nodes and adds its output to the state for the next node.

### Resource Management
Different node types have specific resource limits to optimize processing based on their computational requirements.

### Conditional Logic
The DataStore node conditionally processes data based on validation results from the previous node.

### Parallel Execution
Multiple workflows execute simultaneously, demonstrating cluster resource sharing and coordination.

## Next Steps

- Modify validation rules in DataValidator configuration
- Add error handling and retry logic
- Implement custom branching based on data characteristics
- Add monitoring and metrics collection