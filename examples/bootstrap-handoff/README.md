# Bootstrap Handoff Validation Suite

This example demonstrates and validates the bootstrap handoff mechanism that eliminates startup delays while maintaining cluster consistency.

## Overview

The bootstrap handoff feature allows nodes to start immediately as provisional leaders, then automatically detect and join senior peers without fixed delays. This directory contains comprehensive tests that validate:

- **Multi-node handoff behavior**: Provisional → detecting → ready state transitions
- **Workflow continuity**: Processing continues during handoff transitions
- **Metadata propagation**: Boot ID and timestamp distribution
- **Performance characteristics**: Handoff timing and throughput impact
- **Edge cases**: Rapid launches, network issues, load scenarios

## Architecture

```
bootstrap-handoff/
├── main.go                    # Test orchestrator and runner
├── harness/                   # Testing infrastructure
│   ├── node_launcher.go       # Multi-node orchestration
│   ├── assertions.go          # Behavior validation
│   ├── workflow_monitor.go    # Continuity testing
│   └── metrics_collector.go   # Performance measurement
├── scenarios/                 # Test scenarios
│   ├── basic_handoff.go       # 2-node handoff validation
│   ├── staggered_launch.go    # Multi-node cluster formation
│   └── workflow_continuity.go # Processing during transitions
└── config/                    # Test configuration
    └── test_config.go         # Node and timeout settings
```

## Running Tests

### Full Validation Suite
```bash
cd examples/bootstrap-handoff
go run . test_workflow_node.go
```

### Individual Scenarios
```bash
# Basic 2-node handoff
go run . test_workflow_node.go -scenario basic

# Staggered multi-node launch
go run . test_workflow_node.go -scenario staggered

# Workflow continuity validation
go run . test_workflow_node.go -scenario continuity
```

### Go Test Integration
```bash
cd tests/bootstrap
go test -v ./...
```

## Test Scenarios

### 1. Basic Handoff
**Validates**: Core handoff mechanism with 2 nodes

- Launches provisional leader (node-1)
- Verifies provisional state and readiness
- Launches senior node (node-2)
- Asserts demotion sequence: provisional → detecting → ready
- Confirms cluster formation and metadata propagation
- Measures handoff timing and state transitions

**Expected Result**: Handoff completes in <30s with proper state transitions

### 2. Staggered Launch
**Validates**: Multi-node cluster formation with timing delays

- Launches 3 nodes with staggered delays
- Tracks readiness progression across all nodes
- Validates final cluster topology (1 leader, 2 followers)
- Verifies metadata propagation across all peers
- Measures cluster formation time

**Expected Result**: All nodes ready with single leader elected

### 3. Workflow Continuity
**Validates**: Processing continues during handoff transitions

- Starts continuous workflow processing on provisional leader
- Records workflow events before handoff
- Triggers handoff by launching senior node
- Monitors workflow processing during transition
- Validates processing continues after handoff
- Analyzes throughput before/during/after handoff

**Expected Result**: Workflows process before, during, and after handoff

## Harness Components

### Node Launcher
- **Staggered Deployment**: Launch nodes with configurable delays
- **Readiness Monitoring**: Real-time state tracking
- **Resource Management**: Automatic cleanup and isolation
- **Port Management**: Dynamic port allocation

### Assertion Framework
- **State Validation**: Verify readiness state transitions
- **Metadata Verification**: Boot ID and timestamp propagation
- **Cluster Topology**: Leader election and peer relationships
- **Timing Validation**: Handoff completion within SLAs

### Workflow Monitor
- **Event Tracking**: Comprehensive workflow lifecycle events
- **Continuity Analysis**: Processing patterns during handoff
- **Performance Impact**: Throughput measurements
- **Load Generation**: Continuous workflow submission

### Metrics Collection
- **Handoff Timing**: Detailed transition measurements
- **Performance Impact**: Throughput and latency analysis
- **State Transitions**: Complete state change tracking
- **Resource Usage**: Memory and CPU impact during handoff

## Configuration

### Test Configuration
```go
type TestConfig struct {
    HandoffTimeout     time.Duration // Max time for handoff completion
    WorkflowTimeout    time.Duration // Max time for workflow processing
    ReadinessTimeout   time.Duration // Max time to wait for readiness
    StaggerDelay       time.Duration // Delay between node launches
    ValidationInterval time.Duration // Frequency of state checks
}
```

### Node Configuration
```go
type NodeConfig struct {
    NodeID   string            // Unique node identifier
    RaftAddr string            // Raft protocol address
    GRPCPort int               // gRPC service port
    DataDir  string            // Data storage directory
    Discovery DiscoveryConfig  // Peer discovery settings
}
```

## Validation Criteria

### Handoff Behavior
- ✅ Provisional leader starts immediately
- ✅ Senior peer detection within discovery timeout
- ✅ Demotion sequence completes successfully
- ✅ Cluster formation achieves expected topology
- ✅ No split-brain scenarios

### Performance Requirements
- ✅ Handoff completes in <30 seconds
- ✅ Workflow throughput impact <50%
- ✅ Readiness achieved in <10 seconds
- ✅ No permanent workflow failures during handoff

### Metadata Propagation
- ✅ Boot IDs are unique across all nodes
- ✅ Launch timestamps enable seniority determination
- ✅ Metadata visible via discovery mechanisms
- ✅ Health endpoints reflect bootstrap state

## Common Issues and Solutions

### Discovery Timeouts
**Problem**: Nodes fail to discover each other
**Solution**:
- Verify MDNS is enabled: `manager.MDNS("service", "local")`
- Check network connectivity between nodes
- Increase discovery timeout in configuration

### Readiness Failures
**Problem**: `WaitUntilReady()` times out
**Solution**:
- Check for port conflicts (Raft and gRPC)
- Verify discovery configuration
- Review logs for bootstrap handoff messages
- Increase readiness timeout

### Workflow Interruptions
**Problem**: Workflows fail during handoff
**Solution**:
- Expected during brief transition period
- Implement retry logic in application code
- Monitor intake pause/resume events

### State Assertion Failures
**Problem**: Node states don't match expectations
**Solution**:
- Check timing of assertions (nodes may still be transitioning)
- Verify discovery events are propagating
- Review bootstrap metadata in health endpoints

## Performance Baseline

### Expected Timings
- **Single Node Ready**: 50-200ms
- **2-Node Handoff**: 500-2000ms
- **3-Node Formation**: 1-5 seconds
- **Workflow Impact**: <2 second pause

### Resource Usage
- **Memory Overhead**: <10MB per node
- **CPU Impact**: <5% during handoff
- **Network Traffic**: Discovery broadcasts + join RPCs

## Integration with CI/CD

### Test Commands
```bash
# Short validation (essential scenarios only)
go test -short -timeout 2m ./tests/bootstrap/

# Full validation suite (all edge cases)
go test -timeout 5m ./tests/bootstrap/

# Performance benchmarks
go test -bench=. ./tests/bootstrap/
```

### Expected Results
All tests should pass consistently with:
- No flaky failures
- Predictable timing characteristics
- Proper resource cleanup

## Extending Tests

### Adding New Scenarios
1. Create new file in `scenarios/` directory
2. Implement scenario function with signature:
   ```go
   func RunNewScenario(ctx context.Context, launcher *harness.NodeLauncher) error
   ```
3. Add to test suite in `main.go`

### Custom Assertions
1. Add assertion function to `harness/assertions.go`
2. Use existing assertion patterns for consistency
3. Include detailed error messages and actual data

### Additional Metrics
1. Extend `MetricsCollector` with new measurements
2. Add to performance report generation
3. Include in validation criteria

---

This validation suite ensures the bootstrap handoff mechanism works reliably across all deployment scenarios while maintaining the performance and consistency guarantees expected from Graft clusters.