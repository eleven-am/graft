# Kubernetes Discovery Example

Native Kubernetes service discovery using the Kubernetes API for pod-to-pod communication in containerized environments.

## Overview

This example demonstrates:
- Kubernetes API-based service discovery
- Headless service configuration
- Pod-to-pod cluster formation
- Dynamic scaling with replica sets
- RBAC permissions for discovery
- Persistent volume integration

## Use Cases

- Cloud-native applications
- Container orchestration environments
- Microservice architectures  
- Auto-scaling workloads
- Production Kubernetes deployments

## Configuration

Kubernetes discovery uses the Kubernetes API:

```go
config.Discovery.Strategy = graft.StrategyKubernetes
config.Discovery.KubernetesServiceName = "graft-k8s-cluster"
config.Discovery.KubernetesNamespace = "default"
```

## Deployment

### Build Container Image
```bash
# From project root
docker build -t graft-k8s:latest -f examples/discovery/kubernetes/Dockerfile .
```

### Deploy to Kubernetes
```bash
kubectl apply -f deployment.yaml
```

### Scale the Deployment
```bash
kubectl scale deployment graft-k8s-cluster --replicas=5
```

## Kubernetes Resources

### Service (Headless)
```yaml
apiVersion: v1
kind: Service
metadata:
  name: graft-k8s-cluster
spec:
  clusterIP: None  # Headless for direct pod discovery
  selector:
    app: graft-k8s
```

### RBAC Permissions
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: graft-k8s-discovery
rules:
- apiGroups: [""]
  resources: ["endpoints", "services", "pods"]
  verbs: ["get", "list", "watch"]
```

## Environment Variables

Kubernetes automatically provides:
- `HOSTNAME` - Pod name used as node ID
- Service and namespace configuration via downward API
- Automatic DNS resolution for headless services

## How It Works

1. **Pod Registration**: Pods register with Kubernetes service
2. **Endpoint Discovery**: Query Kubernetes API for service endpoints  
3. **Cluster Formation**: Connect to discovered pod IPs
4. **Dynamic Updates**: Watch for pod additions/removals
5. **Leader Election**: Raft consensus among discovered pods

## Expected Behavior

1. Pods start and register with Kubernetes service
2. Each pod discovers others via Kubernetes API
3. Cluster forms using pod-to-pod communication
4. Scaling up adds new nodes automatically
5. Scaling down removes nodes gracefully

## Monitoring

Check pod status:
```bash
kubectl get pods -l app=graft-k8s
kubectl logs -l app=graft-k8s
```

Check service endpoints:
```bash
kubectl get endpoints graft-k8s-cluster
kubectl describe service graft-k8s-cluster
```

Port forward to access pods:
```bash
kubectl port-forward service/graft-k8s-cluster 8080:8080
curl http://localhost:8080/health
```

## Scaling Operations

### Manual Scaling
```bash
# Scale up
kubectl scale deployment graft-k8s-cluster --replicas=6

# Scale down  
kubectl scale deployment graft-k8s-cluster --replicas=2
```

### Auto-scaling
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: graft-k8s-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: graft-k8s-cluster
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

## Persistent Storage

For production deployments, use persistent volumes:

```yaml
volumeClaimTemplates:
- metadata:
    name: data-storage
  spec:
    accessModes: ["ReadWriteOnce"]
    resources:
      requests:
        storage: 10Gi
    storageClassName: fast-ssd
```

## Advantages

- Native Kubernetes integration
- Automatic service discovery
- Dynamic scaling support
- Built-in health checking
- Load balancing and service mesh compatibility

## Limitations

- Requires Kubernetes environment
- RBAC permissions needed
- API server dependency
- Container networking overhead
- Platform-specific configuration

## Troubleshooting

### RBAC Issues
```bash
kubectl auth can-i get endpoints --as=system:serviceaccount:default:graft-k8s
```

### Network Policies
```bash
kubectl get networkpolicies
kubectl describe networkpolicy <policy-name>
```

### DNS Resolution
```bash
kubectl exec -it <pod-name> -- nslookup graft-k8s-cluster
```

## Next Steps

- Implement persistent volume claims
- Add ingress controllers
- Configure service mesh integration
- Set up monitoring and alerting
- Implement rolling updates