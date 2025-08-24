# Graft Configuration Guide

Graft provides two approaches to configuration: **Simple** for quick setup and **Advanced** for comprehensive control.

## Quick Start (Simple Configuration)

For simple use cases, use the basic constructor:

```go
import (
    "log/slog"
    "github.com/eleven-am/graft"
)

// Create with minimal setup
manager := graft.New("node-1", "localhost:7000", "./data", slog.Default())

// Configure discovery dynamically
manager.Discovery().MDNS()                    // Add mDNS discovery
manager.Discovery().Static([]graft.Peer{...}) // Add static peers

// Start the system
err := manager.Start(ctx, 8080)
```

## Advanced Configuration (Power Users)

For production deployments and advanced features, use the configuration builder:

```go
import (
    "time"
    "github.com/eleven-am/graft"
)

// Build comprehensive configuration
config := graft.NewConfigBuilder("node-1", "localhost:7000", "./data").
    // Discovery: supports multiple methods simultaneously
    WithMDNS("_graft._tcp", "local.", "").
    WithKubernetes("graft-service", "production").
    WithStaticPeers(graft.StaticPeer{
        ID: "peer-1", Address: "10.1.1.1", Port: 7000,
    }).
    
    // Security: TLS configuration
    WithTLS("/path/to/cert.pem", "/path/to/key.pem", "/path/to/ca.pem").
    
    // Resources: fine-tune capacity and limits
    WithResourceLimits(200, 20, map[string]int{
        "heavy-compute": 5,
        "database":      10,
        "api-calls":     50,
    }).
    
    // Engine: workflow execution settings
    WithEngineSettings(100, 10*time.Minute, 5).
    
    Build()

// Create manager with advanced config
manager := graft.NewWithConfig(config)
err := manager.Start(ctx, 8080)
```

## Configuration Reference

### Discovery Methods

Graft supports **multiple discovery methods simultaneously**:

#### mDNS Discovery
```go
WithMDNS(service, domain, host)
// service: "_graft._tcp" (default)
// domain:  "local." (default)  
// host:    "" (auto-detect)
```

#### Kubernetes Discovery
```go
WithKubernetes(serviceName, namespace)
// Automatic peer discovery in K8s clusters
// Supports service discovery, pod discovery, StatefulSets
```

#### Static Peers
```go
WithStaticPeers(graft.StaticPeer{
    ID:       "node-2",
    Address:  "192.168.1.10", 
    Port:     7000,
    Metadata: map[string]string{"zone": "us-west-1"},
})
```

### Resource Management

Control workflow execution capacity:

```go
WithResourceLimits(
    maxTotal,        // 200 - total concurrent workflows
    defaultPerType,  // 20  - default per node type
    perTypeOverrides // map[string]int - specific limits
)
```

### Engine Settings

Fine-tune workflow execution:

```go
WithEngineSettings(
    maxWorkflows,   // 100 - max concurrent workflows
    nodeTimeout,    // 10*time.Minute - node execution timeout  
    retryAttempts   // 5 - retry failed nodes
)
```

### Security (TLS)

Enable secure transport:

```go
WithTLS(certFile, keyFile, caFile)
```

## Configuration Examples

### High-Throughput Setup
```go
config := graft.NewConfigBuilder("worker-node", "0.0.0.0:7000", "/data").
    WithMDNS("", "", "").  // Quick discovery
    WithResourceLimits(500, 50, nil).  // High capacity
    WithEngineSettings(200, 5*time.Minute, 3).  // Fast execution
    Build()
```

### Production Kubernetes
```go
config := graft.NewConfigBuilder(os.Getenv("NODE_ID"), "0.0.0.0:7000", "/data").
    WithKubernetes("graft-service", "production").
    WithTLS("/certs/tls.crt", "/certs/tls.key", "/certs/ca.crt").
    WithResourceLimits(100, 15, map[string]int{
        "ml-training": 2,     // Limit expensive ML jobs
        "data-export": 5,     // Control data operations
    }).
    Build()
```

### Hybrid Discovery
```go
config := graft.NewConfigBuilder("hybrid-node", "10.0.0.5:7000", "/data").
    WithMDNS("", "", "").     // Local network discovery
    WithKubernetes("graft", "default").  // K8s cluster discovery  
    WithStaticPeers(          // External data centers
        graft.StaticPeer{ID: "dc1-leader", Address: "dc1.company.com", Port: 7000},
        graft.StaticPeer{ID: "dc2-leader", Address: "dc2.company.com", Port: 7000},
    ).
    Build()
```

## Migration from Simple to Advanced

Existing code using `graft.New()` continues to work unchanged. To migrate:

**Before:**
```go
manager := graft.New("node-1", "localhost:7000", "./data", logger)
manager.Discovery().MDNS()
```

**After:**  
```go
config := graft.NewConfigBuilder("node-1", "localhost:7000", "./data").
    WithMDNS("", "", "").  // Same as .MDNS()
    Build()
manager := graft.NewWithConfig(config)
```

## Configuration Validation

All configurations are validated at startup. Common validation errors:

- **Empty NodeID/BindAddr/DataDir**: Required fields
- **Invalid discovery config**: Missing required fields per discovery type
- **Resource limits ≤ 0**: Must be positive integers
- **Invalid TLS files**: Files must exist and be readable

## Default Values

| Setting | Default Value | Description |
|---------|---------------|-------------|
| Max Concurrent Workflows | 50 | Total workflow capacity |
| Node Execution Timeout | 5 minutes | Per-node timeout |
| Retry Attempts | 3 | Failed node retries |
| Snapshot Interval | 120 seconds | Raft snapshots |
| Heartbeat Timeout | 1 second | Raft heartbeats |
| Max Message Size | 10 MB | gRPC transport |

## Best Practices

1. **Use builder pattern** for production deployments
2. **Combine discovery methods** for resilience  
3. **Set appropriate resource limits** based on workload
4. **Enable TLS** for production clusters
5. **Monitor resource utilization** and adjust limits
6. **Test configuration changes** in staging first