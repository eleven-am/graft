# Graft Architecture Documentation

## Overview

Graft is a distributed workflow orchestration framework built on modern distributed systems principles. It provides fault-tolerant, scalable workflow execution across multiple nodes using a ports and adapters architecture with pluggable components.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Graft Cluster                            │
├─────────────────────────────────────────────────────────────┤
│      ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│      │    Node A   │  │    Node B   │  │    Node C   │      │
│      │  (Leader)   │  │ (Follower)  │  │ (Follower)  │      │
│      └─────────────┘  └─────────────┘  └─────────────┘      │
│             │                 │               │             │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              Raft Consensus Layer                       ││
│  └─────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────┐│
│  │               gRPC Transport Layer                      ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Core Layer (`internal/core/`)

**Purpose**: Central orchestration and component coordination.

**Key Components**:
- `core.Manager`: Main cluster coordinator and public API surface
- `core.Orchestrator`: Workflow lifecycle management and execution coordination
- `core.PortFactory`: Component factory and dependency injection
- `core.NodeWrapper`: Node execution wrapper with type safety

**Responsibilities**:
- Component integration and startup/shutdown
- Public API implementation
- Configuration management and validation
- Workflow context injection and metadata management

### 2. Transport Layer (`internal/adapters/transport/`)

**Purpose**: Handles all network communication between cluster nodes.

**Key Components**:
- `transport.GRPCTransport`: gRPC-based transport implementation with server and client functionality
- Protocol buffer definitions in `internal/proto/transport.proto`

**Features**:
- TLS encryption support
- Connection pooling and management
- Circuit breaker pattern for fault tolerance
- Health checking and monitoring
- Message compression and size limits

**RPC Services**:
- Cluster operations (join, leave, heartbeat)
- Raft consensus (leader election, log replication)
- Workflow operations (trigger, status, state)

### 3. Storage Layer (`internal/adapters/storage/` and `internal/adapters/raft/`)

**Purpose**: Provides distributed, consistent data storage using Raft consensus.

**Key Components**:
- `raft.RaftAdapter`: Raft consensus implementation and cluster coordination
- `raft.FSM`: Finite State Machine for data operations and state transitions
- `raft.Store`: Persistent storage backend with BadgerDB integration
- `storage.StorageAdapter`: High-level storage abstraction with serialization

**Features**:
- Linearizable consistency
- Automatic leader election
- Log replication and snapshotting
- BadgerDB for persistent storage
- Configurable retention policies
- Type-safe serialization and deserialization

**Data Types**:
- Workflow state and metadata
- Node registry information
- Cluster membership data
- Queue state persistence

### 4. Queue System (`internal/adapters/queue/`)

**Purpose**: Manages workflow task queuing and execution scheduling.

**Key Components**:
- `queue.QueueAdapter`: BadgerDB-based queue implementation
- Queue operations: enqueue, dequeue, priority handling
- State management: ready, pending, executing

**Features**:
- Priority-based scheduling
- Persistent queue state
- Atomic operations
- Configurable batch processing
- Dead letter handling

### 5. Workflow Engine (`internal/adapters/engine/`)

**Purpose**: Orchestrates workflow execution and state management.

**Key Components**:
- `engine.Engine`: Main workflow coordinator and execution orchestrator
- `engine.Executor`: Individual node execution handler with type safety
- `engine.StateManager`: Workflow state persistence and state transitions
- `engine.Coordinator`: Cross-node workflow coordination and DAG management
- `engine.DataCollector`: Workflow data aggregation and persistence
- `engine.Evaluator`: Node readiness evaluation and execution conditions

**Features**:
- Concurrent workflow execution
- Resource management and throttling
- Retry logic and error handling
- State transitions and persistence
- Event-driven architecture
- DAG reconstruction and validation
- Workflow context injection

### 6. Discovery System (`internal/adapters/discovery/`)

**Purpose**: Enables automatic peer discovery and cluster formation.

**Key Components**:
- `discovery.MDNSDiscovery`: Multicast DNS discovery implementation
- `discovery.KubernetesDiscovery`: Kubernetes service discovery implementation
- Array-based configuration supporting multiple simultaneous discovery methods

**Strategies**:
- **mDNS**: Local network multicast discovery for development and local clusters
- **Kubernetes**: Service/endpoint discovery for containerized deployments
- **Multi-Discovery**: Simultaneous use of multiple discovery methods through array configuration
- **Hybrid**: Intelligent fallback between discovery methods

### 7. Resource Management (`internal/adapters/resource_manager/`)

**Purpose**: Controls resource allocation and execution limits.

**Key Components**:
- `resource_manager.ResourceManager`: Resource allocation controller
- `resource_manager.Pool`: Resource pool management and tracking
- `resource_manager.Priority`: Priority-based resource allocation
- `resource_manager.Health`: Resource health monitoring

**Features**:
- Global and per-type resource limits
- Dynamic resource allocation
- Usage monitoring and reporting
- Backpressure mechanisms
- Priority-based resource scheduling
- Health-aware resource management

### 8. Node Registry (`internal/adapters/node_registry/`)

**Purpose**: Manages available workflow node types and their metadata.

**Key Components**:
- `node_registry.NodeRegistry`: In-memory node type registry
- Node validation and schema management
- Runtime node lookup and instantiation
- Type-safe node execution via reflection

### 9. Semaphore Management (`internal/adapters/semaphore/`)

**Purpose**: Provides distributed semaphore functionality for resource coordination.

**Key Components**:
- `semaphore.SemaphoreAdapter`: Distributed semaphore implementation
- Atomic semaphore operations across cluster nodes
- Resource contention management

### 10. Domain Layer (`internal/domain/`)

**Purpose**: Core business logic and domain models.

**Key Components**:
- `domain.Config`: Configuration structures with builder pattern support
- `domain.WorkflowContext`: Workflow execution context and metadata
- `domain.Commands`: Raft command definitions for state changes
- `domain.Errors`: Domain-specific error types
- `domain.LifecycleEvents`: Event definitions for workflow lifecycle

### 11. Ports (`internal/ports/`)

**Purpose**: Interface definitions for dependency inversion and testing.

**Key Interfaces**:
- Port definitions for all adapters (Discovery, Transport, Storage, etc.)
- Mock implementations for testing
- Clear separation between core logic and adapter implementations

## Data Flow Architecture

### Workflow Execution Flow

```
1. Workflow Trigger
   ↓
2. Initial Validation
   ↓
3. State Persistence (Raft)
   ↓
4. Queue Ready Nodes
   ↓
5. Resource Allocation
   ↓
6. Node Execution
   ↓
7. State Update (Raft)
   ↓
8. Queue Next Nodes
   ↓
9. Completion/Error Handling
```

### Inter-Node Communication

```
Client Request
   ↓
gRPC Transport
   ↓
Leader Node
   ↓
Raft Consensus
   ↓
All Followers
   ↓
Local Processing
   ↓
Response to Client
```

## Consistency and Fault Tolerance

### Raft Consensus

Graft uses the Raft consensus algorithm to ensure data consistency across the cluster:

**Leader Election**:
- Nodes start as followers
- Election timeout triggers candidate state
- Majority vote determines leader
- Heartbeats maintain leadership

**Log Replication**:
- All writes go through the leader
- Leader replicates to followers
- Commits require majority acknowledgment
- Followers apply committed entries

**Safety Guarantees**:
- Linearizability for all operations
- No data loss with majority availability
- Automatic recovery from network partitions

### Failure Handling

**Node Failures**:
- Automatic leader re-election
- Workflow migration to healthy nodes
- State recovery from replicated logs
- Health monitoring and detection

**Network Partitions**:
- Majority partition continues operation
- Minority partition enters read-only mode
- Automatic reconciliation on healing
- No split-brain scenarios

## Scalability Considerations

### Horizontal Scaling

**Cluster Growth**:
- Dynamic node addition without downtime
- Automatic load redistribution
- Service discovery integration
- Rolling upgrades support

**Resource Distribution**:
- Per-node resource allocation
- Dynamic load balancing
- Backpressure propagation
- Queue distribution strategies

### Performance Optimization

**Transport Layer**:
- Connection pooling and reuse
- Message batching and compression
- Circuit breakers for fault isolation
- Configurable timeouts and retries

**Storage Layer**:
- Snapshot compaction
- Configurable retention policies
- Asynchronous replication
- Batch write optimization

**Execution Engine**:
- Concurrent workflow processing
- Resource-aware scheduling
- Priority-based execution
- State caching strategies

## Security Architecture

### Transport Security

**TLS Encryption**:
- Mutual TLS authentication
- Certificate-based identity
- Configurable cipher suites
- Certificate rotation support

**Access Control**:
- Node identity verification
- Operation-level authorization
- Audit logging
- Rate limiting

### Data Security

**At-Rest Encryption**:
- Configurable storage encryption
- Key management integration
- Secure key rotation
- Compliance framework support

**In-Transit Security**:
- End-to-end encryption
- Message integrity verification
- Replay attack prevention
- Secure key exchange

## Monitoring and Observability

### Metrics Collection

**System Metrics**:
- Resource utilization
- Queue depths and throughput
- Network latency and errors
- Storage performance

**Business Metrics**:
- Workflow success/failure rates
- Execution time distributions
- Node type utilization
- Error categorization

### Logging Architecture

**Structured Logging**:
- JSON format with consistent fields
- Correlation IDs for request tracing
- Configurable log levels
- Centralized log aggregation

**Audit Logging**:
- Security event tracking
- Compliance requirement support
- Tamper-evident log storage
- Automated alerting

### Health Monitoring

**Health Checks**:
- Component-level health status
- Dependency health monitoring
- Configurable check intervals
- Automated failover triggers

**Circuit Breakers**:
- Automatic failure detection
- Graceful degradation
- Recovery monitoring
- Configurable thresholds

## Configuration Management

### Configuration Structure

Graft supports both simple and advanced configuration approaches:

**Simple Configuration**:
```go
manager := graft.New("node-1", "localhost:7000", "./data", logger)
```

**Advanced Configuration**:
```yaml
# Node identity
node_id: "node-1"
bind_addr: "0.0.0.0:9090"
data_dir: "/var/lib/graft"

# Discovery configuration (array-based)
discovery:
  - type: "mdns"
    service_name: "_graft._tcp"
    domain: "local."
  - type: "kubernetes"
    service_name: "graft-service"
    namespace: "production"

# Transport configuration
transport:
  enable_tls: true
  cert_file: "/etc/graft/tls/cert.pem"
  key_file: "/etc/graft/tls/key.pem"
  ca_file: "/etc/graft/tls/ca.pem"

# Resource limits
resources:
  max_concurrent_total: 100
  default_per_type_limit: 10
  per_type_limits:
    heavy-processor: 5
    light-processor: 20

# Engine configuration
engine:
  max_concurrent_workflows: 50
  node_execution_timeout: "5m"
  retry_attempts: 3
  retry_backoff: "5s"

# Orchestrator configuration
orchestrator:
  buffer_size: 1000
  shutdown_timeout: "30s"
```

### Configuration Builder Pattern

```go
config := graft.NewConfigBuilder("node-1", "0.0.0.0:7000", "/data").
    WithMDNS("_graft._tcp", "local.", "").
    WithKubernetes("graft-service", "production").
    WithTLS("/certs/tls.crt", "/certs/tls.key", "/certs/ca.crt").
    WithResourceLimits(200, 20, map[string]int{
        "ml-training": 2,
        "data-export": 10,
    }).
    WithEngineSettings(100, 10*time.Minute, 5).
    Build()

manager := graft.NewWithConfig(config)
```

### Environment Integration

**Container Support**:
- Docker image optimization
- Multi-stage builds
- Health check endpoints
- Graceful shutdown handling

**Kubernetes Integration**:
- Service discovery via endpoints
- ConfigMap and Secret support
- StatefulSet deployment patterns
- Horizontal Pod Autoscaling

**Configuration Sources**:
- YAML/JSON files
- Environment variables
- Command-line arguments
- Remote configuration services

## Deployment Patterns

### Single-Node Development

```go
// Minimal configuration for development
manager := graft.New("dev-node", "localhost:7000", "./data/dev", logger)
manager.Discovery().MDNS()
```

### Multi-Node Production

```go
// Production cluster configuration
config := graft.NewConfigBuilder("prod-node", "0.0.0.0:7000", "/var/lib/graft").
    WithKubernetes("graft-cluster", "production").
    WithTLS("/etc/certs/tls.crt", "/etc/certs/tls.key", "/etc/certs/ca.crt").
    WithResourceLimits(200, 20, map[string]int{
        "data-processor": 50,
        "ml-trainer": 5,
    }).
    Build()

manager := graft.NewWithConfig(config)
```

### Cloud-Native Deployment

**Kubernetes Manifests**:
- StatefulSet for data persistence
- Service for cluster communication
- ConfigMap for configuration
- Secret for TLS certificates
- ServiceMonitor for Prometheus

**Docker Compose**:
- Multi-node local testing
- Volume mounts for persistence
- Network configuration
- Environment variable injection

## Extension Points

### Custom Node Types

Implement the `NodeInterface` and provide an Execute method:

```go
// Minimal interface requirement
type NodeInterface interface {
    GetName() string
}

// Required Execute method (discovered via reflection)
type MyNode struct{}

func (n *MyNode) GetName() string {
    return "my-node"
}

func (n *MyNode) Execute(ctx context.Context, state MyStateType, config MyConfigType) (graft.NodeResult, error) {
    // Access workflow context metadata
    workflowCtx, _ := graft.GetWorkflowContext(ctx)
    
    // Process with full type safety
    result := MyResultType{
        ProcessedData: processData(state, config),
        WorkflowID: workflowCtx.WorkflowID,
    }
    
    return graft.NodeResult{
        Data: result,
        NextNodes: []graft.NextNodeConfig{
            {NodeName: "next-node", Config: nextConfig},
        },
    }, nil
}

// Optional: Control execution conditions
func (n *MyNode) CanStart(ctx context.Context, state MyStateType, config MyConfigType) bool {
    return state.IsReady
}
```

Graft automatically discovers the Execute method signature via reflection, extracting:
- State type from the second parameter
- Config type from the third parameter (if present)
- Result type from NodeResult.Data field
- Workflow context is injected automatically via context.Context

This approach eliminates boilerplate while maintaining type safety and providing runtime metadata access.

### Custom Discovery Strategies

Implement the `DiscoveryPort` interface:

```go
type DiscoveryPort interface {
    Start(ctx context.Context) error
    Stop() error
    GetPeers() []PeerInfo
    Subscribe(handler PeerEventHandler) error
}
```

### Custom Transport Adapters

Implement the `TransportPort` interface:

```go
type TransportPort interface {
    Start(ctx context.Context, config TransportConfig) error
    Stop() error
    SendClusterMessage(ctx context.Context, target string, message ClusterMessage) error
    SendRaftMessage(ctx context.Context, target string, message []byte) error
    SendWorkflowMessage(ctx context.Context, target string, message WorkflowMessage) error
}
```

## Future Architecture Considerations

### Planned Enhancements

**Multi-Region Support**:
- Cross-region replication
- Latency-aware routing
- Regional failover strategies

**Advanced Scheduling**:
- Resource-aware scheduling
- Affinity/anti-affinity rules
- Cost-optimization algorithms

**Enhanced Security**:
- Zero-trust architecture
- Identity-based access control
- Encryption key rotation

**Observability Improvements**:
- Distributed tracing
- Advanced metrics collection
- Anomaly detection
- Capacity planning tools
