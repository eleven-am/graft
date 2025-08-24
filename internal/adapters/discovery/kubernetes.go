package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type KubernetesAdapter struct {
	mu        sync.RWMutex
	logger    *slog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	clientset *kubernetes.Clientset
	peers     map[string]ports.Peer
	nodeInfo  ports.NodeInfo
	config    *ports.KubernetesConfig

	peerIDTemplate *template.Template
}

func NewKubernetesAdapter(config *ports.KubernetesConfig, logger *slog.Logger) *KubernetesAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	if config == nil {
		config = &ports.KubernetesConfig{}
	}

	adapter := &KubernetesAdapter{
		logger: logger.With("component", "discovery", "adapter", "kubernetes"),
		peers:  make(map[string]ports.Peer),
		config: config,
	}

	if config.PeerID.Source == ports.PeerIDTemplate && config.PeerID.Template != "" {
		if tmpl, err := template.New("peerID").Parse(config.PeerID.Template); err == nil {
			adapter.peerIDTemplate = tmpl
		} else {
			logger.Error("failed to parse peer ID template", "template", config.PeerID.Template, "error", err)
		}
	}

	return adapter
}

func (k *KubernetesAdapter) Start(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.ctx != nil {
		return domain.NewDiscoveryError("kubernetes", "start", domain.ErrAlreadyStarted)
	}

	k.ctx, k.cancel = context.WithCancel(ctx)
	k.logger.Debug("starting Kubernetes discovery adapter")

	clientset, err := k.createKubernetesClient()
	if err != nil {
		k.cancel()
		return domain.NewDiscoveryError("kubernetes", "client_setup", err)
	}
	k.clientset = clientset

	if k.config.Namespace == "" {
		k.config.Namespace = k.detectNamespace()
	}

	go k.discoveryLoop()

	k.logger.Debug("Kubernetes discovery adapter started",
		"namespace", k.config.Namespace,
		"discovery_method", k.config.Discovery.Method)

	return nil
}

func (k *KubernetesAdapter) Stop() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.cancel == nil {
		return domain.NewDiscoveryError("kubernetes", "stop", domain.ErrNotStarted)
	}

	k.logger.Debug("stopping Kubernetes discovery adapter")

	k.cancel()
	k.ctx = nil
	k.cancel = nil
	k.peers = make(map[string]ports.Peer)

	k.logger.Debug("Kubernetes discovery adapter stopped")
	return nil
}

func (k *KubernetesAdapter) GetPeers() []ports.Peer {
	k.mu.RLock()
	defer k.mu.RUnlock()

	peers := make([]ports.Peer, 0, len(k.peers))
	for _, peer := range k.peers {
		peers = append(peers, peer)
	}

	return peers
}

func (k *KubernetesAdapter) Announce(nodeInfo ports.NodeInfo) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.nodeInfo = nodeInfo
	k.logger.Debug("node info stored (K8s handles announcement via pod labels)",
		"id", nodeInfo.ID,
		"address", nodeInfo.Address,
		"port", nodeInfo.Port)

	return nil
}

func (k *KubernetesAdapter) createKubernetesClient() (*kubernetes.Clientset, error) {
	var config *rest.Config
	var err error

	switch k.config.AuthMethod {
	case ports.AuthKubeconfig:
		kubeconfigPath := k.config.KubeconfigPath
		if kubeconfigPath == "" {
			kubeconfigPath = os.Getenv("KUBECONFIG")
			if kubeconfigPath == "" {
				if homeDir, _ := os.UserHomeDir(); homeDir != "" {
					kubeconfigPath = fmt.Sprintf("%s/.kube/config", homeDir)
				}
			}
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	case ports.AuthExplicitToken:
		config = &rest.Config{
			Host:        k.config.APIServer,
			BearerToken: k.config.Token,
			TLSClientConfig: rest.TLSClientConfig{
				Insecure: false,
			},
		}
	default:
		config, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to build kubernetes config: %w", err)
	}

	return kubernetes.NewForConfig(config)
}

func (k *KubernetesAdapter) detectNamespace() string {
	if nsBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return strings.TrimSpace(string(nsBytes))
	}
	k.logger.Warn("could not read namespace from service account, using default")
	return "default"
}

func (k *KubernetesAdapter) discoveryLoop() {
	interval := k.config.WatchInterval
	if interval == 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	k.performDiscovery()

	for {
		k.mu.RLock()
		ctx := k.ctx
		k.mu.RUnlock()

		if ctx == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			k.performDiscovery()
		}
	}
}

func (k *KubernetesAdapter) performDiscovery() {
	k.mu.RLock()
	clientset := k.clientset
	ctx := k.ctx
	k.mu.RUnlock()

	if clientset == nil || ctx == nil {
		return
	}

	switch k.config.Discovery.Method {
	case ports.DiscoveryLabelSelector:
		k.discoverByLabelSelector(ctx, clientset)
	case ports.DiscoveryService:
		k.discoverByService(ctx, clientset)
	case ports.DiscoveryStatefulSet:
		k.discoverByStatefulSet(ctx, clientset)
	case ports.DiscoveryNamespace:
		k.discoverByNamespace(ctx, clientset)
	case ports.DiscoverySiblings:
		k.discoverSiblings(ctx, clientset)
	default:
		k.logger.Error("unsupported discovery method", "method", k.config.Discovery.Method)
	}
}

func (k *KubernetesAdapter) discoverByLabelSelector(ctx context.Context, clientset *kubernetes.Clientset) {
	selector := k.buildLabelSelector()
	if selector == "" {
		k.logger.Error("label selector discovery requires LabelSelector configuration")
		return
	}

	listOptions := metav1.ListOptions{
		LabelSelector: selector,
		FieldSelector: k.config.FieldSelector,
	}

	pods, err := clientset.CoreV1().Pods(k.config.Namespace).List(ctx, listOptions)
	if err != nil {
		k.logger.Error("failed to list pods", "error", err)
		return
	}

	for _, pod := range pods.Items {
		k.processPod(&pod, "DISCOVERED")
	}
}

func (k *KubernetesAdapter) discoverByService(ctx context.Context, clientset *kubernetes.Clientset) {
	serviceName := k.config.Discovery.ServiceName
	if serviceName == "" {
		k.logger.Error("service discovery requires ServiceName configuration")
		return
	}

	endpoints, err := clientset.CoreV1().Endpoints(k.config.Namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		k.logger.Error("failed to get service endpoints", "service", serviceName, "error", err)
		return
	}

	for _, subset := range endpoints.Subsets {
		for _, addr := range subset.Addresses {
			if addr.TargetRef != nil && addr.TargetRef.Kind == "Pod" {
				pod, err := clientset.CoreV1().Pods(k.config.Namespace).Get(ctx, addr.TargetRef.Name, metav1.GetOptions{})
				if err == nil {
					k.processPod(pod, "DISCOVERED")
				}
			}
		}
	}
}

func (k *KubernetesAdapter) discoverByStatefulSet(ctx context.Context, clientset *kubernetes.Clientset) {
	stsName := k.config.Discovery.StatefulSetName
	if stsName == "" {
		k.logger.Error("StatefulSet discovery requires StatefulSetName configuration")
		return
	}

	sts, err := clientset.AppsV1().StatefulSets(k.config.Namespace).Get(ctx, stsName, metav1.GetOptions{})
	if err != nil {
		k.logger.Error("failed to get StatefulSet", "name", stsName, "error", err)
		return
	}

	replicas := int32(0)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	for i := int32(0); i < replicas; i++ {
		podName := fmt.Sprintf("%s-%d", stsName, i)
		pod, err := clientset.CoreV1().Pods(k.config.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err == nil {
			k.processPod(pod, "DISCOVERED")
		}
	}
}

func (k *KubernetesAdapter) discoverByNamespace(ctx context.Context, clientset *kubernetes.Clientset) {
	pods, err := clientset.CoreV1().Pods(k.config.Namespace).List(ctx, metav1.ListOptions{
		FieldSelector: k.config.FieldSelector,
	})
	if err != nil {
		k.logger.Error("failed to list pods in namespace", "error", err)
		return
	}

	for _, pod := range pods.Items {
		k.processPod(&pod, "DISCOVERED")
	}
}

func (k *KubernetesAdapter) discoverSiblings(ctx context.Context, clientset *kubernetes.Clientset) {
	currentPodName := os.Getenv("POD_NAME")
	if currentPodName == "" {
		k.logger.Error("sibling discovery requires POD_NAME environment variable")
		return
	}

	currentPod, err := clientset.CoreV1().Pods(k.config.Namespace).Get(ctx, currentPodName, metav1.GetOptions{})
	if err != nil {
		k.logger.Error("failed to get current pod", "name", currentPodName, "error", err)
		return
	}

	for _, ownerRef := range currentPod.OwnerReferences {
		if ownerRef.Kind == "StatefulSet" {
			k.config.Discovery.StatefulSetName = ownerRef.Name
			k.discoverByStatefulSet(ctx, clientset)
			return
		}
	}

	k.logger.Warn("current pod is not part of a StatefulSet, falling back to label selector")
	if len(k.config.Discovery.LabelSelector) > 0 {
		k.discoverByLabelSelector(ctx, clientset)
	}
}

func (k *KubernetesAdapter) buildLabelSelector() string {
	if len(k.config.Discovery.LabelSelector) == 0 {
		return ""
	}

	var parts []string
	for key, value := range k.config.Discovery.LabelSelector {
		parts = append(parts, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(parts, ",")
}

func (k *KubernetesAdapter) processPod(pod *v1.Pod, eventType string) {
	if k.shouldSkipPod(pod) {
		return
	}

	peerID := k.extractPeerID(pod)
	if peerID == "" {
		return
	}

	if peerID == k.nodeInfo.ID {
		return
	}

	port := k.extractPort(pod)
	address := k.extractAddress(pod)

	peer := ports.Peer{
		ID:      peerID,
		Address: address,
		Port:    port,
		Metadata: map[string]string{
			"namespace": pod.Namespace,
			"node":      pod.Spec.NodeName,
			"phase":     string(pod.Status.Phase),
		},
	}

	k.mu.Lock()
	k.peers[peer.ID] = peer
	k.mu.Unlock()
}

func (k *KubernetesAdapter) shouldSkipPod(pod *v1.Pod) bool {
	if pod.Status.Phase != v1.PodRunning {
		return true
	}

	if k.config.RequireReady && !k.isPodReady(pod) {
		return true
	}

	if pod.Status.PodIP == "" {
		return true
	}

	for key, value := range k.config.AnnotationFilters {
		if pod.Annotations[key] != value {
			return true
		}
	}

	return false
}

func (k *KubernetesAdapter) isPodReady(pod *v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

func (k *KubernetesAdapter) extractPeerID(pod *v1.Pod) string {
	switch k.config.PeerID.Source {
	case ports.PeerIDAnnotation:
		if k.config.PeerID.Key != "" {
			return pod.Annotations[k.config.PeerID.Key]
		}
	case ports.PeerIDLabel:
		if k.config.PeerID.Key != "" {
			return pod.Labels[k.config.PeerID.Key]
		}
	case ports.PeerIDTemplate:
		if k.peerIDTemplate != nil {
			var buf strings.Builder
			if err := k.peerIDTemplate.Execute(&buf, pod); err == nil {
				return buf.String()
			}
		}
	default:
		return pod.Name
	}
	return pod.Name
}

func (k *KubernetesAdapter) extractPort(pod *v1.Pod) int {
	switch k.config.Port.Source {
	case ports.PortNamedPort:
		if k.config.Port.PortName != "" {
			for _, container := range pod.Spec.Containers {
				for _, port := range container.Ports {
					if port.Name == k.config.Port.PortName {
						return int(port.ContainerPort)
					}
				}
			}
		}
	case ports.PortAnnotation:
		if k.config.Port.AnnotationKey != "" {
			if portStr := pod.Annotations[k.config.Port.AnnotationKey]; portStr != "" {
				if port := parseInt(portStr); port > 0 {
					return port
				}
			}
		}
	case ports.PortEnvVar:
		if k.config.Port.EnvVarName != "" {
			for _, container := range pod.Spec.Containers {
				for _, env := range container.Env {
					if env.Name == k.config.Port.EnvVarName && env.Value != "" {
						if port := parseInt(env.Value); port > 0 {
							return port
						}
					}
				}
			}
		}
	case ports.PortFirstPort:
		for _, container := range pod.Spec.Containers {
			if len(container.Ports) > 0 {
				return int(container.Ports[0].ContainerPort)
			}
		}
	}

	if k.config.Port.DefaultPort > 0 {
		return k.config.Port.DefaultPort
	}
	return 8080
}

func (k *KubernetesAdapter) extractAddress(pod *v1.Pod) string {
	switch k.config.NetworkingMode {
	case ports.NetworkingServiceIP:
		if serviceIP := k.resolveServiceIP(); serviceIP != "" {
			return serviceIP
		}
		k.logger.Warn("failed to resolve service IP, falling back to pod IP", "pod", pod.Name)
		return pod.Status.PodIP
	case ports.NetworkingNodePort:
		if nodeAddress := k.resolveNodeAddress(pod); nodeAddress != "" {
			return nodeAddress
		}
		k.logger.Warn("failed to resolve node address, falling back to pod IP", "pod", pod.Name)
		return pod.Status.PodIP
	default:
		return pod.Status.PodIP
	}
}

func (k *KubernetesAdapter) resolveServiceIP() string {
	if k.config.Discovery.ServiceName == "" {
		k.logger.Debug("no service name configured for service IP resolution")
		return ""
	}

	ctx := context.Background()
	service, err := k.clientset.CoreV1().Services(k.config.Namespace).Get(ctx, k.config.Discovery.ServiceName, metav1.GetOptions{})
	if err != nil {
		k.logger.Error("failed to get service for IP resolution", "service", k.config.Discovery.ServiceName, "error", err)
		return ""
	}

	if service.Spec.ClusterIP == "" || service.Spec.ClusterIP == "None" {
		k.logger.Debug("service has no cluster IP", "service", k.config.Discovery.ServiceName)
		return ""
	}

	return service.Spec.ClusterIP
}

func (k *KubernetesAdapter) resolveNodeAddress(pod *v1.Pod) string {
	if pod.Spec.NodeName == "" {
		k.logger.Debug("pod has no node name", "pod", pod.Name)
		return ""
	}

	ctx := context.Background()
	node, err := k.clientset.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		k.logger.Error("failed to get node for address resolution", "node", pod.Spec.NodeName, "error", err)
		return ""
	}

	for _, addr := range node.Status.Addresses {
		if addr.Type == v1.NodeExternalIP && addr.Address != "" {
			return addr.Address
		}
	}

	for _, addr := range node.Status.Addresses {
		if addr.Type == v1.NodeInternalIP && addr.Address != "" {
			return addr.Address
		}
	}

	k.logger.Debug("no suitable node address found", "node", pod.Spec.NodeName)
	return ""
}

func parseInt(s string) int {
	var result int
	for _, r := range s {
		if r >= '0' && r <= '9' {
			result = result*10 + int(r-'0')
		} else {
			return 0
		}
	}
	return result
}
