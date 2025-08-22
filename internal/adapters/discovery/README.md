# Discovery System

The Graft discovery system provides peer discovery capabilities across different environments using a pluggable adapter architecture.

## Architecture

The discovery system follows a ports/adapters pattern with:

- **Manager**: Orchestrates discovery operations and manages adapters
- **Adapters**: Environment-specific implementations (mDNS, Static, Kubernetes)
- **Health Checking**: Built-in peer health monitoring
- **Metrics**: Prometheus metrics for operational monitoring
- **Backoff**: Exponential backoff for failure recovery

## Supported Discovery Strategies

### Auto Detection
The manager automatically detects the best strategy:
- Kubernetes: Detected by `/var/run/secrets/kubernetes.io` presence
- Static: Detected by `GRAFT_PEERS` environment variable
- mDNS: Default fallback for local network discovery

### Manual Strategy Selection
```go
manager := discovery.NewManager(logger, discovery.StrategyMDNS)
manager := discovery.NewManager(logger, discovery.StrategyStatic)
manager := discovery.NewManager(logger, discovery.StrategyKubernetes)
```

## Usage Examples

### Basic Usage
```go
package main

import (
    "context"
    "log/slog"
    "time"
    
    "github.com/eleven-am/graft/internal/adapters/discovery"
    "github.com/eleven-am/graft/internal/ports"
)

func main() {
    logger := slog.Default()
    
    // Create manager with auto-detection
    manager := discovery.NewManager(logger, discovery.StrategyAuto)
    
    // Start discovery
    config := ports.DiscoveryConfig{
        ServiceName: "graft-worker",
        ServicePort: 8080,
        Metadata: map[string]string{
            "version": "1.0.0",
            "region":  "us-west-2",
        },
    }
    
    ctx := context.Background()
    if err := manager.Start(ctx, config); err != nil {
        panic(err)
    }
    defer manager.Stop()
    
    // Advertise service
    serviceInfo := ports.ServiceInfo{
        ID:      "worker-001",
        Name:    "graft-worker",
        Address: "192.168.1.100",
        Port:    8080,
        Metadata: map[string]string{
            "instance": "primary",
        },
    }
    
    if err := manager.Advertise(serviceInfo); err != nil {
        panic(err)
    }
    
    // Discover peers periodically
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            peers, err := manager.Discover()
            if err != nil {
                logger.Error("discovery failed", "error", err)
                continue
            }
            
            logger.Info("discovered peers", "count", len(peers))
            for _, peer := range peers {
                logger.Info("peer found",
                    "id", peer.ID,
                    "address", peer.Address,
                    "port", peer.Port)
            }
        }
    }
}
```

### mDNS Discovery
```go
// Automatic local network discovery
manager := discovery.NewManager(logger, discovery.StrategyMDNS)

config := ports.DiscoveryConfig{
    ServiceName: "graft-worker",
    ServicePort: 8080,
    Domain:      "local.", // Optional, defaults to "local."
}
```

### Static Discovery
```go
// Configure via environment variable
// GRAFT_PEERS=192.168.1.10:8080,192.168.1.11:8080,192.168.1.12:8080

manager := discovery.NewManager(logger, discovery.StrategyStatic)

config := ports.DiscoveryConfig{
    ServiceName: "graft-worker",
    ServicePort: 8080,
}
```

### Kubernetes Discovery
```go
// Discovers services via Kubernetes API
manager := discovery.NewManager(logger, discovery.StrategyKubernetes)

config := ports.DiscoveryConfig{
    ServiceName: "graft-worker",
    ServicePort: 8080,
    Namespace:   "production", // Optional, defaults to current namespace
}
```

## Configuration

### Environment Variables

#### Static Discovery
- `GRAFT_PEERS`: Comma-separated list of peer addresses (e.g., `host1:port1,host2:port2`)

#### Kubernetes Discovery
- `KUBECONFIG`: Path to kubeconfig file (for out-of-cluster usage)
- Kubernetes service account with appropriate RBAC permissions (for in-cluster usage)

### Static Peer Configuration File
Create `peers.json` or `peers.yaml`:

```json
{
  "peers": [
    {
      "id": "worker-001",
      "address": "192.168.1.10",
      "port": 8080,
      "metadata": {
        "region": "us-west-1",
        "zone": "a"
      }
    },
    {
      "id": "worker-002", 
      "address": "192.168.1.11",
      "port": 8080,
      "metadata": {
        "region": "us-west-1",
        "zone": "b"
      }
    }
  ]
}
```

## Kubernetes RBAC

For Kubernetes discovery, apply the following RBAC configuration:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: graft-discovery
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: graft-discovery
rules:
- apiGroups: [""]
  resources: ["services", "endpoints"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: graft-discovery
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: graft-discovery
subjects:
- kind: ServiceAccount
  name: graft-discovery
  namespace: default
```

## Monitoring

The discovery system exports Prometheus metrics:

### Counters
- `graft_discovery_operations_total{adapter, operation, status}`
- `graft_discovery_failures_total{adapter, error_type}`
- `graft_health_check_operations_total{adapter, status}`

### Histograms
- `graft_discovery_duration_seconds{adapter, operation}`
- `graft_health_check_duration_seconds{adapter}`

### Gauges
- `graft_discovery_peers_found{adapter}`
- `graft_discovery_backoff_duration_seconds{adapter}`
- `graft_healthy_peers_count{adapter}`
- `graft_unhealthy_peers_count{adapter}`

### Recording Metrics
```go
import "github.com/eleven-am/graft/internal/adapters/discovery/metrics"

// Record discovery operation
metrics.RecordDiscoveryOperation("mdns", "discover", "success")

// Record timing
metrics.RecordDiscoveryDuration("mdns", "discover", 1.5)

// Update peer counts
metrics.SetPeersFound("mdns", 5)
```

## Health Checking

The static adapter includes built-in health checking:

- **Check Interval**: 30 seconds
- **Timeout**: 5 seconds per peer
- **Method**: TCP connection test
- **Backoff**: Exponential backoff on failures (1.5x multiplier, max 60s)

Unhealthy peers are automatically excluded from discovery results.

## Error Handling

The discovery system uses structured errors with types:

- `ErrorTypeConflict`: Manager already started/stopped
- `ErrorTypeUnavailable`: Manager not started
- `ErrorTypeInternal`: Configuration or adapter errors
- `ErrorTypeValidation`: Invalid strategy or parameters

```go
peers, err := manager.Discover()
if err != nil {
    var domainErr domain.Error
    if errors.As(err, &domainErr) {
        switch domainErr.Type {
        case domain.ErrorTypeUnavailable:
            // Manager not started
        case domain.ErrorTypeInternal:
            // Adapter failure
        }
    }
}
```

## Failure Recovery

All adapters implement exponential backoff for failure recovery:

- **Initial Backoff**: 1 second
- **Multiplier**: 1.5x
- **Maximum Backoff**: 60 seconds
- **Reset**: On successful operation

This prevents thundering herd effects and reduces load during outages.

## Testing

### Unit Tests
```bash
go test ./internal/adapters/discovery/...
```

### Integration Tests
```bash
# Test with mDNS (requires network)
go test -tags integration ./internal/adapters/discovery/mdns/...

# Test with Kubernetes (requires cluster)
go test -tags integration ./internal/adapters/discovery/kubernetes/...
```

### Example Test
```go
func TestDiscoveryIntegration(t *testing.T) {
    logger := slog.Default()
    manager := discovery.NewManager(logger, discovery.StrategyMDNS)
    
    config := ports.DiscoveryConfig{
        ServiceName: "test-service",
        ServicePort: 9999,
    }
    
    ctx := context.Background()
    err := manager.Start(ctx, config)
    require.NoError(t, err)
    defer manager.Stop()
    
    // Test service advertisement
    serviceInfo := ports.ServiceInfo{
        ID:      "test-001",
        Name:    "test-service",
        Address: "127.0.0.1",
        Port:    9999,
    }
    
    err = manager.Advertise(serviceInfo)
    require.NoError(t, err)
    
    // Test peer discovery
    time.Sleep(2 * time.Second) // Allow discovery time
    peers, err := manager.Discover()
    require.NoError(t, err)
    assert.GreaterOrEqual(t, len(peers), 1)
}
```

## Troubleshooting

### mDNS Issues
- **Firewall**: Ensure UDP port 5353 is open
- **Network**: mDNS requires multicast support
- **Platform**: Some cloud environments block multicast

### Kubernetes Issues  
- **RBAC**: Verify service account has required permissions
- **Network Policy**: Ensure API server access is allowed
- **Namespace**: Check service and endpoint visibility

### Static Issues
- **Configuration**: Verify peer addresses are reachable
- **Health Checks**: Check TCP connectivity on specified ports
- **Environment**: Ensure GRAFT_PEERS is properly formatted

### General Debugging
```go
// Enable debug logging
logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

manager := discovery.NewManager(logger, discovery.StrategyAuto)
```