# Graft Deployment Guide

## Overview

This guide covers deploying Graft in various environments, from local development to production Kubernetes clusters. Graft is designed to run in distributed environments with automatic peer discovery and fault tolerance.

## Prerequisites

### System Requirements

**Minimum Requirements (per node)**:
- CPU: 2 cores
- Memory: 2GB RAM
- Storage: 10GB available space
- Network: 1Gbps network connectivity

**Recommended Production Requirements (per node)**:
- CPU: 4+ cores
- Memory: 8GB+ RAM
- Storage: 100GB+ SSD storage
- Network: 10Gbps network connectivity

### Software Dependencies

- Go 1.23+ (for building from source)
- Docker (for containerized deployment)
- Kubernetes 1.20+ (for Kubernetes deployment)

## Local Development Setup

### Single Node Development

Create a development configuration:

```yaml
# config/dev.yaml
node_id: "dev-node-1"
service_name: "graft-dev"
log_level: "debug"

discovery:
  strategy: "static"
  peers: ["localhost:9090"]

transport:
  listen_address: "127.0.0.1"
  listen_port: 9090
  enable_tls: false

storage:
  listen_address: "127.0.0.1"
  listen_port: 7000
  data_dir: "./data/dev/raft"

queue:
  data_dir: "./data/dev/queue"
  sync_writes: false

resources:
  max_concurrent_total: 10
  default_per_type_limit: 3

engine:
  max_concurrent_workflows: 5
  node_execution_timeout: "30s"
  retry_attempts: 2
```

Run the development server:

```bash
# Build the application
go build -o graft ./cmd/graft

# Create data directory
mkdir -p data/dev

# Run with development config
./graft --config config/dev.yaml
```

### Multi-Node Local Testing

Use Docker Compose for local multi-node testing:

```yaml
# docker-compose.dev.yml
version: '3.8'

services:
  graft-node-1:
    build: .
    ports:
      - "9090:9090"
      - "7000:7000"
    environment:
      - GRAFT_NODE_ID=node-1
      - GRAFT_DISCOVERY_PEERS=graft-node-1:9090,graft-node-2:9090,graft-node-3:9090
    volumes:
      - ./data/node-1:/app/data
    networks:
      - graft-network

  graft-node-2:
    build: .
    ports:
      - "9091:9090"
      - "7001:7000"
    environment:
      - GRAFT_NODE_ID=node-2
      - GRAFT_DISCOVERY_PEERS=graft-node-1:9090,graft-node-2:9090,graft-node-3:9090
    volumes:
      - ./data/node-2:/app/data
    networks:
      - graft-network

  graft-node-3:
    build: .
    ports:
      - "9092:9090"
      - "7002:7000"
    environment:
      - GRAFT_NODE_ID=node-3
      - GRAFT_DISCOVERY_PEERS=graft-node-1:9090,graft-node-2:9090,graft-node-3:9090
    volumes:
      - ./data/node-3:/app/data
    networks:
      - graft-network

networks:
  graft-network:
    driver: bridge
```

Start the cluster:

```bash
# Start the cluster
docker-compose -f docker-compose.dev.yml up

# Scale to more nodes
docker-compose -f docker-compose.dev.yml up --scale graft-node=5
```

## Container Deployment

### Docker Image

Create a production Dockerfile:

```dockerfile
# Dockerfile
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o graft ./cmd/graft

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app

COPY --from=builder /app/graft .
COPY --from=builder /app/config ./config

# Create non-root user
RUN addgroup -g 1001 graft && \
    adduser -D -s /bin/sh -u 1001 -G graft graft && \
    mkdir -p /app/data && \
    chown -R graft:graft /app

USER graft

EXPOSE 9090 7000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:9090/health || exit 1

CMD ["./graft", "--config", "config/production.yaml"]
```

Build and run:

```bash
# Build the image
docker build -t graft:latest .

# Run with custom configuration
docker run -d \
  --name graft-node \
  -p 9090:9090 \
  -p 7000:7000 \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/data:/app/data \
  -e GRAFT_NODE_ID=docker-node-1 \
  graft:latest
```

### Docker Compose Production

```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  graft-node-1:
    image: graft:latest
    ports:
      - "9090:9090"
      - "7000:7000"
    environment:
      - GRAFT_NODE_ID=prod-node-1
      - GRAFT_LOG_LEVEL=info
      - GRAFT_DISCOVERY_STRATEGY=static
      - GRAFT_DISCOVERY_PEERS=graft-node-1:9090,graft-node-2:9090,graft-node-3:9090
      - GRAFT_TRANSPORT_ENABLE_TLS=true
      - GRAFT_STORAGE_DATA_DIR=/app/data/raft
      - GRAFT_QUEUE_DATA_DIR=/app/data/queue
    volumes:
      - graft-node-1-data:/app/data
      - ./certs:/app/certs:ro
    networks:
      - graft-network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "--quiet", "--tries=1", "--spider", "http://localhost:9090/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  graft-node-2:
    image: graft:latest
    ports:
      - "9091:9090"
      - "7001:7000"
    environment:
      - GRAFT_NODE_ID=prod-node-2
      - GRAFT_LOG_LEVEL=info
      - GRAFT_DISCOVERY_STRATEGY=static
      - GRAFT_DISCOVERY_PEERS=graft-node-1:9090,graft-node-2:9090,graft-node-3:9090
      - GRAFT_TRANSPORT_ENABLE_TLS=true
      - GRAFT_STORAGE_DATA_DIR=/app/data/raft
      - GRAFT_QUEUE_DATA_DIR=/app/data/queue
    volumes:
      - graft-node-2-data:/app/data
      - ./certs:/app/certs:ro
    networks:
      - graft-network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "--quiet", "--tries=1", "--spider", "http://localhost:9090/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  graft-node-3:
    image: graft:latest
    ports:
      - "9092:9090"
      - "7002:7000"
    environment:
      - GRAFT_NODE_ID=prod-node-3
      - GRAFT_LOG_LEVEL=info
      - GRAFT_DISCOVERY_STRATEGY=static
      - GRAFT_DISCOVERY_PEERS=graft-node-1:9090,graft-node-2:9090,graft-node-3:9090
      - GRAFT_TRANSPORT_ENABLE_TLS=true
      - GRAFT_STORAGE_DATA_DIR=/app/data/raft
      - GRAFT_QUEUE_DATA_DIR=/app/data/queue
    volumes:
      - graft-node-3-data:/app/data
      - ./certs:/app/certs:ro
    networks:
      - graft-network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "--quiet", "--tries=1", "--spider", "http://localhost:9090/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  graft-node-1-data:
  graft-node-2-data:
  graft-node-3-data:

networks:
  graft-network:
    driver: bridge
```

## Kubernetes Deployment

### Namespace and RBAC

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: graft-system
  labels:
    app.kubernetes.io/name: graft
    app.kubernetes.io/component: system

---
# k8s/rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: graft
  namespace: graft-system
  labels:
    app.kubernetes.io/name: graft

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: graft
  labels:
    app.kubernetes.io/name: graft
rules:
- apiGroups: [""]
  resources: ["endpoints", "services"]
  verbs: ["get", "list", "watch"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: graft
  labels:
    app.kubernetes.io/name: graft
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: graft
subjects:
- kind: ServiceAccount
  name: graft
  namespace: graft-system
```

### ConfigMap

```yaml
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: graft-config
  namespace: graft-system
  labels:
    app.kubernetes.io/name: graft
data:
  config.yaml: |
    log_level: "info"
    service_name: "graft-cluster"
    
    discovery:
      # discovery handled via external provider in application code
      service_name: "graft"
    
    transport:
      listen_address: "0.0.0.0"
      listen_port: 9090
      enable_tls: true
      tls_cert_file: "/etc/certs/tls.crt"
      tls_key_file: "/etc/certs/tls.key"
      tls_ca_file: "/etc/certs/ca.crt"
      max_message_size_mb: 4
      connection_timeout: "30s"
    
    storage:
      listen_address: "0.0.0.0"
      listen_port: 7000
      data_dir: "/var/lib/graft/raft"
      snapshot_retention: 5
      snapshot_threshold: 8192
      trailing_logs: 10240
    
    queue:
      data_dir: "/var/lib/graft/queue"
      sync_writes: true
      value_log_file_size_mb: 1024
      mem_table_size_mb: 64
      num_goroutines: 8
    
    resources:
      max_concurrent_total: 100
      default_per_type_limit: 10
    
    engine:
      max_concurrent_workflows: 50
      node_execution_timeout: "5m"
      state_update_interval: "30s"
      retry_attempts: 3
      retry_backoff: "5s"
```

### TLS Certificates

```yaml
# k8s/tls-secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: graft-tls
  namespace: graft-system
  labels:
    app.kubernetes.io/name: graft
type: kubernetes.io/tls
data:
  ca.crt: LS0tLS1CRUdJTi... # Base64 encoded CA certificate
  tls.crt: LS0tLS1CRUdJTi... # Base64 encoded server certificate
  tls.key: LS0tLS1CRUdJTi... # Base64 encoded private key
```

### StatefulSet

```yaml
# k8s/statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: graft
  namespace: graft-system
  labels:
    app.kubernetes.io/name: graft
    app.kubernetes.io/component: cluster
spec:
  serviceName: graft-headless
  replicas: 3
  selector:
    matchLabels:
      app.kubernetes.io/name: graft
      app.kubernetes.io/component: cluster
  template:
    metadata:
      labels:
        app.kubernetes.io/name: graft
        app.kubernetes.io/component: cluster
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: graft
      securityContext:
        runAsNonRoot: true
        runAsUser: 1001
        runAsGroup: 1001
        fsGroup: 1001
      containers:
      - name: graft
        image: graft:latest
        imagePullPolicy: IfNotPresent
        env:
        - name: GRAFT_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        ports:
        - name: transport
          containerPort: 9090
          protocol: TCP
        - name: raft
          containerPort: 7000
          protocol: TCP
        volumeMounts:
        - name: config
          mountPath: /app/config
        - name: tls-certs
          mountPath: /etc/certs
          readOnly: true
        - name: data
          mountPath: /var/lib/graft
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 2
            memory: 4Gi
        livenessProbe:
          httpGet:
            path: /health
            port: transport
            scheme: HTTPS
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /ready
            port: transport
            scheme: HTTPS
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 3
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          capabilities:
            drop:
            - ALL
      volumes:
      - name: config
        configMap:
          name: graft-config
      - name: tls-certs
        secret:
          secretName: graft-tls
      terminationGracePeriodSeconds: 60
  volumeClaimTemplates:
  - metadata:
      name: data
      labels:
        app.kubernetes.io/name: graft
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 100Gi
```

### Services

```yaml
# k8s/services.yaml
apiVersion: v1
kind: Service
metadata:
  name: graft
  namespace: graft-system
  labels:
    app.kubernetes.io/name: graft
    app.kubernetes.io/component: cluster
  annotations:
    service.alpha.kubernetes.io/tolerate-unready-endpoints: "true"
spec:
  type: ClusterIP
  ports:
  - name: transport
    port: 9090
    targetPort: transport
    protocol: TCP
  - name: raft
    port: 7000
    targetPort: raft
    protocol: TCP
  selector:
    app.kubernetes.io/name: graft
    app.kubernetes.io/component: cluster

---
apiVersion: v1
kind: Service
metadata:
  name: graft-headless
  namespace: graft-system
  labels:
    app.kubernetes.io/name: graft
    app.kubernetes.io/component: cluster
  annotations:
    service.alpha.kubernetes.io/tolerate-unready-endpoints: "true"
spec:
  clusterIP: None
  ports:
  - name: transport
    port: 9090
    targetPort: transport
    protocol: TCP
  - name: raft
    port: 7000
    targetPort: raft
    protocol: TCP
  selector:
    app.kubernetes.io/name: graft
    app.kubernetes.io/component: cluster
```

### Deployment Commands

```bash
# Apply all manifests
kubectl apply -f k8s/

# Check deployment status
kubectl get pods -n graft-system -l app.kubernetes.io/name=graft

# View logs
kubectl logs -n graft-system -l app.kubernetes.io/name=graft -f

# Scale the cluster
kubectl scale statefulset graft -n graft-system --replicas=5

# Rolling update
kubectl set image statefulset/graft graft=graft:v1.1.0 -n graft-system
```

## Configuration Management

### Environment Variables

Graft supports configuration via environment variables with the `GRAFT_` prefix:

```bash
# Node configuration
export GRAFT_NODE_ID="prod-node-1"
export GRAFT_SERVICE_NAME="graft-cluster"
export GRAFT_LOG_LEVEL="info"

# Discovery configuration
# Configure discovery via your application (e.g., external k8s provider)
export GRAFT_DISCOVERY_SERVICE_NAME="graft"

# Transport configuration
export GRAFT_TRANSPORT_LISTEN_ADDRESS="0.0.0.0"
export GRAFT_TRANSPORT_LISTEN_PORT="9090"
export GRAFT_TRANSPORT_ENABLE_TLS="true"

# Storage configuration
export GRAFT_STORAGE_DATA_DIR="/var/lib/graft/raft"
export GRAFT_STORAGE_SNAPSHOT_RETENTION="5"

# Queue configuration
export GRAFT_QUEUE_DATA_DIR="/var/lib/graft/queue"
export GRAFT_QUEUE_SYNC_WRITES="true"

# Resource limits
export GRAFT_RESOURCES_MAX_CONCURRENT_TOTAL="100"
export GRAFT_RESOURCES_DEFAULT_PER_TYPE_LIMIT="10"

# Engine configuration
export GRAFT_ENGINE_MAX_CONCURRENT_WORKFLOWS="50"
export GRAFT_ENGINE_NODE_EXECUTION_TIMEOUT="5m"
```

### Configuration File Precedence

1. Command-line flags (highest priority)
2. Environment variables
3. Configuration file
4. Default values (lowest priority)

### Secrets Management

For production deployments, use secure secret management:

**Kubernetes Secrets**:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: graft-secrets
  namespace: graft-system
type: Opaque
data:
  database-password: <base64-encoded-password>
  api-key: <base64-encoded-api-key>
```

**HashiCorp Vault Integration**:
```yaml
# Use Vault Agent or CSI driver
spec:
  template:
    metadata:
      annotations:
        vault.hashicorp.com/agent-inject: "true"
        vault.hashicorp.com/role: "graft"
        vault.hashicorp.com/agent-inject-secret-config: "secret/graft/config"
```

## Monitoring and Observability

### Prometheus Metrics

```yaml
# k8s/servicemonitor.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: graft
  namespace: graft-system
  labels:
    app.kubernetes.io/name: graft
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: graft
  endpoints:
  - port: transport
    path: /metrics
    interval: 30s
    scrapeTimeout: 10s
```

### Grafana Dashboard

Key metrics to monitor:
- Cluster membership and leader status
- Workflow execution rates and success/failure ratios
- Resource utilization and queue depths
- Network latency and error rates
- Storage performance and disk usage

### Logging Configuration

```yaml
# Structured logging configuration
log_level: "info"
log_format: "json"
log_output: "stdout"

# Log correlation
correlation_id_header: "X-Correlation-ID"
request_id_header: "X-Request-ID"
```

### Health Checks

Graft provides several health check endpoints:

- `/health`: Overall service health
- `/ready`: Readiness for traffic
- `/metrics`: Prometheus metrics
- `/debug/pprof`: Go profiling endpoints

## Security Hardening

### TLS Configuration

Generate production certificates:

```bash
# Create CA
openssl genrsa -out ca-key.pem 4096
openssl req -new -x509 -days 365 -key ca-key.pem -sha256 -out ca.pem

# Create server certificate
openssl genrsa -out server-key.pem 4096
openssl req -subj "/CN=graft" -sha256 -new -key server-key.pem -out server.csr
openssl x509 -req -days 365 -sha256 -in server.csr -CA ca.pem -CAkey ca-key.pem -out server.pem
```

### Network Policies

```yaml
# k8s/network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: graft-network-policy
  namespace: graft-system
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: graft
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: graft-system
    ports:
    - protocol: TCP
      port: 9090
    - protocol: TCP
      port: 7000
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: graft-system
    ports:
    - protocol: TCP
      port: 9090
    - protocol: TCP
      port: 7000
  - to: []
    ports:
    - protocol: TCP
      port: 53
    - protocol: UDP
      port: 53
```

### Pod Security Standards

```yaml
# k8s/pod-security-policy.yaml
apiVersion: policy/v1beta1
kind: PodSecurityPolicy
metadata:
  name: graft-psp
  namespace: graft-system
spec:
  privileged: false
  allowPrivilegeEscalation: false
  requiredDropCapabilities:
    - ALL
  volumes:
    - 'configMap'
    - 'emptyDir'
    - 'projected'
    - 'secret'
    - 'downwardAPI'
    - 'persistentVolumeClaim'
  runAsUser:
    rule: 'MustRunAsNonRoot'
  seLinux:
    rule: 'RunAsAny'
  fsGroup:
    rule: 'RunAsAny'
```

## Performance Tuning

### Resource Allocation

**CPU Allocation**:
- 1 core minimum per node
- 1 additional core per 20 concurrent workflows
- Consider CPU-intensive node types

**Memory Allocation**:
- 2GB base memory per node
- 100MB per active workflow
- Additional memory for caching and buffers

**Storage Configuration**:
- Use SSD storage for better performance
- Separate volumes for Raft logs and queue data
- Configure appropriate disk I/O limits

### Kernel Parameters

For high-performance deployments:

```bash
# Increase file descriptor limits
echo "fs.file-max = 1000000" >> /etc/sysctl.conf

# Network tuning
echo "net.core.rmem_max = 268435456" >> /etc/sysctl.conf
echo "net.core.wmem_max = 268435456" >> /etc/sysctl.conf
echo "net.ipv4.tcp_rmem = 4096 65536 268435456" >> /etc/sysctl.conf
echo "net.ipv4.tcp_wmem = 4096 65536 268435456" >> /etc/sysctl.conf

# Apply settings
sysctl -p
```

## Troubleshooting

### Common Issues

**Split Brain Prevention**:
- Always deploy odd number of nodes (3, 5, 7)
- Ensure proper network connectivity
- Monitor cluster membership status

**Performance Issues**:
- Check resource utilization metrics
- Adjust concurrency limits
- Monitor queue depths
- Review network latency

**Storage Issues**:
- Monitor disk space usage
- Check Raft log compaction
- Verify backup/restore procedures

### Debugging Commands

```bash
# Check cluster status
kubectl exec -it graft-0 -n graft-system -- /app/graft status

# View Raft state
kubectl exec -it graft-0 -n graft-system -- /app/graft raft-status

# Export metrics
kubectl port-forward svc/graft 9090:9090 -n graft-system
curl http://localhost:9090/metrics

# Collect logs
kubectl logs -n graft-system -l app.kubernetes.io/name=graft --tail=1000
```

### Log Analysis

Common log patterns to monitor:

```bash
# Leader election events
grep "leader election" /var/log/graft/graft.log

# Network connectivity issues
grep "connection refused\|timeout" /var/log/graft/graft.log

# Storage errors
grep "raft\|storage\|snapshot" /var/log/graft/graft.log

# Workflow execution errors
grep "workflow.*failed\|execution.*error" /var/log/graft/graft.log
```

This deployment guide provides comprehensive coverage of running Graft in various environments with proper security, monitoring, and performance considerations.
