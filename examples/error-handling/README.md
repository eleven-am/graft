# Error Handling Example

Demonstrates resilience patterns, failure handling, and recovery mechanisms in distributed workflow execution.

## Overview

This example shows:
- Random failure simulation and error types
- Retry mechanisms with exponential backoff
- Timeout handling and context cancellation
- Error callbacks and monitoring
- Workflow recovery patterns
- Failure analysis and reporting

## Error Handling Patterns

### 1. Random Failures
- Configurable failure rates (0.0 to 1.0)
- Multiple error types: network timeouts, service unavailable, data corruption
- Demonstrates transient vs permanent failures

### 2. Retry Logic
- Automatic retry with configurable attempts
- Exponential backoff between retries
- Eventually successful after N attempts

### 3. Timeout Management
- Context-aware processing with cancellation
- Configurable execution timeouts
- Graceful handling of long-running operations

## Node Types

### UnreliableProcessor
- Simulates random failures based on configurable rate
- Multiple realistic error scenarios
- Used to test error handling robustness

### RetryProcessor  
- Fails initially but succeeds after retries
- Demonstrates eventual consistency patterns
- Tracks retry attempts in workflow state

### TimeoutProcessor
- Respects context cancellation
- Configurable processing duration
- Tests timeout handling mechanisms

## Configuration

```yaml
engine:
  node_execution_timeout: "10s"
  retry_attempts: 3
  retry_backoff: "1s"

resources:
  max_concurrent_per_type:
    unreliable-processor: 3
    retry-processor: 3
    timeout-processor: 2
```

## Running

```bash
go run main.go
```

## Test Scenarios

The example runs 5 different error scenarios:

1. **Low Failure Rate** (20% failure) - Most succeed
2. **High Failure Rate** (80% failure) - Most fail  
3. **Retry Logic** - Initial failures, eventual success
4. **Fast Processing** (500ms) - Completes under timeout
5. **Slow Processing** (15s) - Exceeds timeout, gets cancelled

## Error Callbacks

```go
cluster.OnError(func(workflowID string, nodeExecution graft.NodeExecution, err error) {
    fmt.Printf("[ERROR] Workflow %s, Node %s failed: %v\n", 
        workflowID, nodeExecution.NodeName, err)
})

cluster.OnComplete(func(workflowID string, finalState map[string]interface{}) {
    fmt.Printf("[SUCCESS] Workflow %s completed\n", workflowID)
})
```

## Expected Output

```
Starting error handling demonstrations...
Started workflow: unreliable-001 (Low failure rate processor)
Started workflow: unreliable-002 (High failure rate processor)
Started workflow: retry-001 (Processor with retry logic)
Started workflow: timeout-fast (Fast processing)
Started workflow: timeout-slow (Slow processing - will timeout)

[15:04:05] Workflow unreliable-001 (Low failure rate) status: completed
  ✓ SUCCESS: Final state: map[processed_data:processed_reliable_data ...]

[ERROR] Workflow unreliable-002, Node unreliable-processor failed: processing failed: service unavailable
[15:04:08] Workflow unreliable-002 (High failure rate) status: failed
  ✗ FAILED: Workflow failed permanently

Error Handling Demo Summary:
  Total Workflows: 5
  Successful: 2  
  Failed: 2
  Timed Out: 1
```

## Key Concepts

### Resilience Patterns
- Circuit breaker simulation through failure rates
- Bulkhead isolation via resource limits
- Timeout patterns with context cancellation

### Failure Classification
- **Transient Failures**: Network timeouts, temporary unavailability
- **Permanent Failures**: Data corruption, invalid input
- **Resource Failures**: Exhaustion, capacity limits

### Recovery Strategies
- **Retry with Backoff**: Exponential delays between attempts
- **Timeout Management**: Prevent indefinite blocking
- **Error Propagation**: Structured error reporting

### Monitoring and Observability
- Real-time error callbacks
- Detailed failure reporting
- Execution history tracking
- Resource utilization monitoring

## Production Considerations

### Error Handling Best Practices
- Set appropriate timeout values for different node types
- Configure retry attempts based on failure patterns
- Implement circuit breakers for external dependencies
- Monitor error rates and patterns

### Resource Management
- Limit concurrent executions of failure-prone nodes
- Reserve resources for retry operations
- Implement backpressure mechanisms

### Alerting and Monitoring
- Set up alerts for high error rates
- Track retry success/failure patterns
- Monitor timeout frequency
- Analyze failure root causes

## Next Steps

- Implement custom retry strategies
- Add dead letter queue functionality
- Create failure notification systems
- Build comprehensive monitoring dashboards