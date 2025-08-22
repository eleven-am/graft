# Production Deployment Example

Production-ready 3-node Graft cluster with persistent storage, monitoring, and high availability configuration.

## Overview

This example demonstrates:
- 3-node cluster deployment with Docker Compose
- Persistent storage with volume mounts
- Health monitoring and metrics collection
- Graceful shutdown handling
- Environment-based configuration
- Production resource limits
- Prometheus and Grafana integration

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Node 1    │    │   Node 2    │    │   Node 3    │
│  (Leader)   │◄──►│  (Follower) │◄──►│  (Follower) │
│  :8081      │    │  :8082      │    │  :8083      │
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
              ┌─────────────▼─────────────┐
              │      Monitoring Stack     │
              │  Prometheus + Grafana     │
              │    :9090      :3000       │
              └───────────────────────────┘
```

## Files

- `docker-compose.yml` - Multi-node cluster deployment
- `Dockerfile` - Production container image
- `main.go` - Production node implementation
- `config.yaml` - Base configuration template
- `prometheus.yml` - Metrics collection configuration
- `README.md` - This documentation

## Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 4GB available memory
- Ports 8081-8083, 9090-9093, 3000 available

### Deployment

```bash
# Clone and navigate to production example
cd examples/production

# Start the cluster
docker-compose up -d

# View logs
docker-compose logs -f

# Check cluster status
curl http://localhost:8081/health
curl http://localhost:8082/health  
curl http://localhost:8083/health
```

### Monitoring Access

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **Node APIs**: 
  - Node 1: http://localhost:8081
  - Node 2: http://localhost:8082
  - Node 3: http://localhost:8083

## Configuration

### Environment Variables

Each node is configured via environment variables:

```yaml
environment:
  - NODE_ID=node-1
  - SERVICE_NAME=production-cluster
  - SERVICE_PORT=8080
  - TRANSPORT_PORT=9090
  - DATA_DIR=/data
  - PEER_NODE_2=node-2:8080
  - PEER_NODE_3=node-3:8080
  - LOG_LEVEL=info
  - ENABLE_TLS=false
```

### Resource Limits

Production resource allocation:

```yaml
resources:
  max_concurrent_total: 100
  max_concurrent_per_type:
    critical-processor: 30
    batch-processor: 10
    
engine:
  max_concurrent_workflows: 50
  node_execution_timeout: "60s"
```

### TLS Configuration

To enable TLS:
1. Place certificates in `./certs/` directory
2. Set `ENABLE_TLS=true` in environment
3. Certificates must be named `cert.pem` and `key.pem`

## Node Types

### CriticalProcessor
- High-priority task processing
- 500ms processing time
- 30 concurrent instance limit
- Immediate execution priority

### BatchProcessor  
- Bulk operation processing
- Configurable batch sizes
- 10 concurrent instance limit
- Background processing priority

## Cluster Operations

### Scaling

Add additional nodes by extending docker-compose.yml:

```yaml
node-4:
  build: .
  environment:
    - NODE_ID=node-4
    - PEER_NODE_1=node-1:8080
    - PEER_NODE_2=node-2:8080
    - PEER_NODE_3=node-3:8080
  ports:
    - "8084:8080"
    - "9094:9090"
```

### Health Checks

Each node includes comprehensive health checks:
- Container-level health checks every 10 seconds
- Application-level health endpoints
- Cluster membership validation
- Storage system checks

### Data Persistence

Persistent volumes ensure data survival across container restarts:
- `node1_data`, `node2_data`, `node3_data` - Node storage
- `prometheus_data` - Metrics storage
- `grafana_data` - Dashboard configurations

## Monitoring

### Prometheus Metrics

Key metrics collected:
- Workflow execution rates
- Node resource utilization  
- Cluster membership status
- Storage and queue statistics
- Error rates and latencies

### Grafana Dashboards

Pre-configured dashboards show:
- Cluster overview and health
- Workflow execution patterns
- Resource utilization trends
- Performance and latency metrics

## Production Considerations

### Performance
- Each node handles 100 concurrent operations
- 50 simultaneous workflows per node
- Sub-second workflow dispatch latency
- Horizontal scaling support

### Reliability
- Raft consensus ensures consistency
- Automatic leader election and failover
- Persistent storage with replication
- Graceful shutdown handling

### Security
- TLS encryption support
- Non-root container execution
- Certificate-based authentication
- Network isolation via Docker networks

## Troubleshooting

### Common Issues

**Cluster Formation**
```bash
# Check node connectivity
docker-compose exec node-1 curl http://node-2:8080/health
docker-compose exec node-1 curl http://node-3:8080/health
```

**Storage Issues**
```bash
# Check volume mounts
docker volume ls
docker volume inspect production_node1_data
```

**Network Problems**
```bash
# Inspect network configuration
docker network ls
docker network inspect production_graft-network
```

### Log Analysis

```bash
# View specific node logs
docker-compose logs node-1
docker-compose logs node-2
docker-compose logs node-3

# Follow all logs
docker-compose logs -f

# View Prometheus logs
docker-compose logs prometheus
```

## Maintenance

### Updates

```bash
# Update and restart cluster
docker-compose pull
docker-compose up -d --force-recreate
```

### Backup

```bash
# Backup persistent data
docker run --rm -v production_node1_data:/data -v $(pwd):/backup alpine tar czf /backup/node1_backup.tar.gz -C /data .
```

### Cleanup

```bash
# Stop and remove everything
docker-compose down -v --remove-orphans

# Remove persistent volumes (WARNING: Data loss)
docker volume prune
```