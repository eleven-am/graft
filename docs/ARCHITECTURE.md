# Graft Architecture Documentation

## Overview

Graft is a distributed workflow orchestration framework built on modern distributed systems principles. It provides fault-tolerant, scalable workflow execution across multiple nodes using a microkernel architecture with pluggable components.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Graft Cluster                            │
├─────────────────────────────────────────────────────────────┤
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│   │    Node A   │  │    Node B   │  │    Node C   │         │
│   │  (Leader)   │  │ (Follower)  │  │ (Follower)  │         │
│   └─────────────┘  └─────────────┘  └─────────────┘         │
│         │                 │                 │               │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              Raft Consensus Layer                       ││
│  └─────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────┐│
│  │               gRPC Transport Layer                      ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Cluster Management

**Purpose**: Orchestrates distributed coordination and member management.

**Key Classes**:
- `cluster.Cluster`: Main cluster coordinator
- `cluster.Config`: Cluster configuration management
- `cluster.Factory`: Component factory and dependency injection

**Responsibilities**:
- Node discovery and membership management
- Leader election and consensus coordination
- Workflow lifecycle management
- Component integration and startup/shutdown

### 2. Transport Layer

**Purpose**: Handles all network communication between cluster nodes.

**Key Classes**:
- `grpc.GRPCTransport`: gRPC-based transport implementation
- `grpc.TransportServer`: Server-side RPC handlers
- `grpc.TransportClient`: Client-side RPC operations

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

### 3. Storage Layer

**Purpose**: Provides distributed, consistent data storage using Raft consensus.

**Key Classes**:
- `raftimpl.Adapter`: Raft consensus implementation
- `raftimpl.FSM`: Finite State Machine for data operations
- `raftimpl.Store`: Persistent storage backend

**Features**:
- Linearizable consistency
- Automatic leader election
- Log replication and snapshotting
- BadgerDB for persistent storage
- Configurable retention policies

**Data Types**:
- Workflow state and metadata
- Node registry information
- Cluster membership data
- Queue state persistence

### 4. Queue System

**Purpose**: Manages workflow task queuing and execution scheduling.

**Key Classes**:
- `badger.Queue`: BadgerDB-based queue implementation
- Queue operations: enqueue, dequeue, priority handling
- State management: ready, pending, executing

**Features**:
- Priority-based scheduling
- Persistent queue state
- Atomic operations
- Configurable batch processing
- Dead letter handling

### 5. Workflow Engine

**Purpose**: Orchestrates workflow execution and state management.

**Key Classes**:
- `engine.Engine`: Main workflow coordinator
- `engine.NodeExecutor`: Individual node execution handler
- `engine.StateManager`: Workflow state persistence
- `engine.Coordinator`: Cross-node workflow coordination

**Features**:
- Concurrent workflow execution
- Resource management and throttling
- Retry logic and error handling
- State transitions and persistence
- Event-driven architecture

### 6. Discovery System

**Purpose**: Enables automatic peer discovery and cluster formation.

**Key Classes**:
- `discovery.Manager`: Main discovery coordinator
- `static.Adapter`: Static peer list discovery
- `mdns.Adapter`: Multicast DNS discovery
- `kubernetes.Adapter`: Kubernetes service discovery

**Strategies**:
- **Static**: Predefined peer list
- **mDNS**: Local network multicast discovery
- **Kubernetes**: Service/endpoint discovery
- **Auto**: Intelligent strategy selection

### 7. Resource Management

**Purpose**: Controls resource allocation and execution limits.

**Key Classes**:
- `manager.ResourceManager`: Resource allocation controller
- Resource tracking and limits enforcement
- Per-node-type concurrency control

**Features**:
- Global and per-type resource limits
- Dynamic resource allocation
- Usage monitoring and reporting
- Backpressure mechanisms

### 8. Node Registry

**Purpose**: Manages available workflow node types and their metadata.

**Key Classes**:
- `memory.Registry`: In-memory node type registry
- Node validation and schema management
- Runtime node lookup and instantiation

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

```yaml
# Node identity
node_id: "node-1"
service_name: "graft-cluster"

# Discovery configuration
discovery:
  strategy: "auto"
  service_name: "graft"
  peers: ["node-1:9090", "node-2:9090"]

# Transport configuration
transport:
  listen_address: "0.0.0.0"
  listen_port: 9090
  enable_tls: true
  tls_cert_file: "/etc/graft/tls/cert.pem"
  tls_key_file: "/etc/graft/tls/key.pem"

# Resource limits
resources:
  max_concurrent_total: 100
  max_concurrent_per_type:
    heavy-processor: 5
    light-processor: 20
  default_per_type_limit: 10

# Engine configuration
engine:
  max_concurrent_workflows: 50
  node_execution_timeout: "5m"
  retry_attempts: 3
  retry_backoff: "5s"
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

```yaml
# Minimal configuration for development
node_id: "dev-node"
discovery:
  strategy: "static"
  peers: ["localhost:9090"]
storage:
  data_dir: "./data/dev"
resources:
  max_concurrent_total: 10
```

### Multi-Node Production

```yaml
# Production cluster configuration
discovery:
  strategy: "kubernetes"
  service_name: "graft-cluster"
transport:
  enable_tls: true
  tls_cert_file: "/etc/certs/tls.crt"
  tls_key_file: "/etc/certs/tls.key"
storage:
  data_dir: "/var/lib/graft"
  snapshot_retention: 5
resources:
  max_concurrent_total: 200
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
// Example with typed parameters:
type MyNode struct{}

func (n *MyNode) GetName() string {
    return "my-node"
}

func (n *MyNode) Execute(ctx context.Context, state MyStateType, config MyConfigType) (graft.NodeResult, error) {
    // Process with full type safety
    result := MyResultType{
        ProcessedData: processData(state, config),
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

This approach eliminates boilerplate while maintaining type safety.

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
    // ... other transport methods
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
