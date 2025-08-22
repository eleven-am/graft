# Static Discovery Example

Demonstrates static peer configuration for known infrastructure deployments where node addresses are predetermined.

## Overview

This example shows:
- Static peer list configuration
- Fixed node addressing
- Manual cluster membership
- Docker networking with known containers
- Simple multi-node setup

## Use Cases

- Development environments with fixed infrastructure
- Small production clusters with known nodes
- Docker Compose deployments
- Testing scenarios with predictable topology

## Configuration

Static discovery uses a predefined list of peer nodes:

```go
config.Discovery.Strategy = graft.StrategyStatic
config.Discovery.Peers = []string{
    "node-2:8080",
    "node-3:8080",
}
```

## Running Locally

### Single Node
```bash
go run main.go
```

### Multi-Node with Environment Variables
```bash
# Terminal 1
NODE_ID=static-node-1 SERVICE_PORT=8081 TRANSPORT_PORT=9091 PEERS=localhost:8082,localhost:8083 go run main.go

# Terminal 2  
NODE_ID=static-node-2 SERVICE_PORT=8082 TRANSPORT_PORT=9092 PEERS=localhost:8081,localhost:8083 go run main.go

# Terminal 3
NODE_ID=static-node-3 SERVICE_PORT=8083 TRANSPORT_PORT=9093 PEERS=localhost:8081,localhost:8082 go run main.go
```

## Docker Compose Deployment

```bash
docker-compose up --build
```

This creates a 3-node cluster:
- Node 1: localhost:8081 (leader, starts workflows)
- Node 2: localhost:8082 (follower) 
- Node 3: localhost:8083 (follower)

## Environment Variables

- `NODE_ID` - Unique node identifier
- `SERVICE_PORT` - HTTP service port
- `TRANSPORT_PORT` - gRPC transport port  
- `PEERS` - Comma-separated list of peer addresses

## Expected Behavior

1. All nodes start with known peer addresses
2. Cluster forms immediately using static configuration
3. Leader election occurs among the configured peers
4. Workflows are distributed across available nodes
5. No discovery protocol overhead

## Advantages

- Simple configuration
- Immediate cluster formation
- No discovery protocol overhead
- Predictable network topology
- Works in isolated networks

## Limitations

- Manual peer management
- No automatic node discovery
- Fixed cluster topology
- Requires restart to add/remove nodes
- Configuration drift potential

## Monitoring

Check cluster status:
```bash
curl http://localhost:8081/health
curl http://localhost:8082/health  
curl http://localhost:8083/health
```

View logs:
```bash
docker-compose logs -f static-node-1
```

## Next Steps

- Try the mDNS example for automatic discovery
- Compare with Kubernetes service discovery
- Implement custom peer management logic