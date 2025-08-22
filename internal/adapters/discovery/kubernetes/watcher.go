package kubernetes

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type ServiceEndpoint struct {
	IP   string
	Port int32
}

type ServiceInfo struct {
	Name      string
	Namespace string
	Labels    map[string]string
	Endpoints []ServiceEndpoint
}

type ServiceWatcher struct {
	clientset   kubernetes.Interface
	logger      *slog.Logger
	services    map[string]*ServiceInfo
	mu          sync.RWMutex
	started     bool
	cancel      context.CancelFunc
	namespace   string
}

func NewServiceWatcher(clientset kubernetes.Interface, logger *slog.Logger) *ServiceWatcher {
	if logger == nil {
		logger = slog.Default()
	}

	return &ServiceWatcher{
		clientset: clientset,
		logger:    logger.With("component", "k8s-watcher"),
		services:  make(map[string]*ServiceInfo),
	}
}

func (w *ServiceWatcher) Start(ctx context.Context, config ports.DiscoveryConfig) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.started {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "service watcher already started",
			Details: map[string]interface{}{},
		}
	}

	w.namespace = w.getNamespace(config.Namespace)
	w.logger.Info("starting kubernetes service watcher", "namespace", w.namespace, "serviceName", config.ServiceName)

	watchCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	if err := w.initialDiscovery(config.ServiceName); err != nil {
		cancel()
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "initial discovery failed",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	go w.watchServices(watchCtx, config.ServiceName)
	go w.watchEndpoints(watchCtx, config.ServiceName)

	w.started = true
	w.logger.Info("kubernetes service watcher started successfully")

	return nil
}

func (w *ServiceWatcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.started {
		return
	}

	w.logger.Info("stopping kubernetes service watcher")

	if w.cancel != nil {
		w.cancel()
	}

	w.services = make(map[string]*ServiceInfo)
	w.started = false

	w.logger.Info("kubernetes service watcher stopped")
}

func (w *ServiceWatcher) GetServices() []*ServiceInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()

	services := make([]*ServiceInfo, 0, len(w.services))
	for _, service := range w.services {
		services = append(services, service)
	}

	return services
}

func (w *ServiceWatcher) initialDiscovery(serviceName string) error {
	services, err := w.clientset.CoreV1().Services(w.namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s", serviceName),
	})
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list services",
			Details: map[string]interface{}{
				"service_name": serviceName,
				"namespace":    w.namespace,
				"error":        err.Error(),
			},
		}
	}

	endpoints, err := w.clientset.CoreV1().Endpoints(w.namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to list endpoints",
			Details: map[string]interface{}{
				"namespace": w.namespace,
				"error":     err.Error(),
			},
		}
	}

	endpointsMap := make(map[string]*corev1.Endpoints)
	for i := range endpoints.Items {
		ep := &endpoints.Items[i]
		endpointsMap[ep.Name] = ep
	}

	for i := range services.Items {
		service := &services.Items[i]
		w.processService(service, endpointsMap[service.Name])
	}

	w.logger.Info("initial discovery completed", "services", len(w.services))
	return nil
}

func (w *ServiceWatcher) watchServices(ctx context.Context, serviceName string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		w.logger.Debug("starting service watch")
		
		watchlist, err := w.clientset.CoreV1().Services(w.namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", serviceName),
		})
		if err != nil {
			w.logger.Error("failed to watch services", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for event := range watchlist.ResultChan() {
			if event.Object == nil {
				continue
			}

			service, ok := event.Object.(*corev1.Service)
			if !ok {
				continue
			}

			switch event.Type {
			case watch.Added, watch.Modified:
				w.handleServiceEvent(service)
			case watch.Deleted:
				w.handleServiceDeletion(service)
			}
		}

		watchlist.Stop()
	}
}

func (w *ServiceWatcher) watchEndpoints(ctx context.Context, serviceName string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		w.logger.Debug("starting endpoints watch")
		
		watchlist, err := w.clientset.CoreV1().Endpoints(w.namespace).Watch(ctx, metav1.ListOptions{
			FieldSelector: fields.Everything().String(),
		})
		if err != nil {
			w.logger.Error("failed to watch endpoints", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for event := range watchlist.ResultChan() {
			if event.Object == nil {
				continue
			}

			endpoints, ok := event.Object.(*corev1.Endpoints)
			if !ok {
				continue
			}

			switch event.Type {
			case watch.Added, watch.Modified:
				w.handleEndpointsEvent(endpoints)
			case watch.Deleted:
				w.handleEndpointsDeletion(endpoints)
			}
		}

		watchlist.Stop()
	}
}

func (w *ServiceWatcher) handleServiceEvent(service *corev1.Service) {
	w.mu.Lock()
	defer w.mu.Unlock()

	serviceKey := fmt.Sprintf("%s/%s", service.Namespace, service.Name)
	
	serviceInfo := &ServiceInfo{
		Name:      service.Name,
		Namespace: service.Namespace,
		Labels:    service.Labels,
		Endpoints: []ServiceEndpoint{},
	}

	if existingService, exists := w.services[serviceKey]; exists {
		serviceInfo.Endpoints = existingService.Endpoints
	}

	w.services[serviceKey] = serviceInfo

	w.logger.Debug("service updated", "name", service.Name, "namespace", service.Namespace)
}

func (w *ServiceWatcher) handleServiceDeletion(service *corev1.Service) {
	w.mu.Lock()
	defer w.mu.Unlock()

	serviceKey := fmt.Sprintf("%s/%s", service.Namespace, service.Name)
	delete(w.services, serviceKey)

	w.logger.Debug("service deleted", "name", service.Name, "namespace", service.Namespace)
}

func (w *ServiceWatcher) handleEndpointsEvent(endpoints *corev1.Endpoints) {
	w.mu.Lock()
	defer w.mu.Unlock()

	serviceKey := fmt.Sprintf("%s/%s", endpoints.Namespace, endpoints.Name)
	
	serviceInfo, exists := w.services[serviceKey]
	if !exists {
		serviceInfo = &ServiceInfo{
			Name:      endpoints.Name,
			Namespace: endpoints.Namespace,
			Labels:    map[string]string{},
			Endpoints: []ServiceEndpoint{},
		}
		w.services[serviceKey] = serviceInfo
	}

	serviceInfo.Endpoints = []ServiceEndpoint{}
	
	for _, subset := range endpoints.Subsets {
		for _, address := range subset.Addresses {
			for _, port := range subset.Ports {
				endpoint := ServiceEndpoint{
					IP:   address.IP,
					Port: port.Port,
				}
				serviceInfo.Endpoints = append(serviceInfo.Endpoints, endpoint)
			}
		}
	}

	w.logger.Debug("endpoints updated", 
		"name", endpoints.Name, 
		"namespace", endpoints.Namespace,
		"endpoints", len(serviceInfo.Endpoints))
}

func (w *ServiceWatcher) handleEndpointsDeletion(endpoints *corev1.Endpoints) {
	w.mu.Lock()
	defer w.mu.Unlock()

	serviceKey := fmt.Sprintf("%s/%s", endpoints.Namespace, endpoints.Name)
	
	if serviceInfo, exists := w.services[serviceKey]; exists {
		serviceInfo.Endpoints = []ServiceEndpoint{}
	}

	w.logger.Debug("endpoints deleted", "name", endpoints.Name, "namespace", endpoints.Namespace)
}

func (w *ServiceWatcher) processService(service *corev1.Service, endpoints *corev1.Endpoints) {
	serviceKey := fmt.Sprintf("%s/%s", service.Namespace, service.Name)
	
	serviceInfo := &ServiceInfo{
		Name:      service.Name,
		Namespace: service.Namespace,
		Labels:    service.Labels,
		Endpoints: []ServiceEndpoint{},
	}

	if endpoints != nil {
		for _, subset := range endpoints.Subsets {
			for _, address := range subset.Addresses {
				for _, port := range subset.Ports {
					endpoint := ServiceEndpoint{
						IP:   address.IP,
						Port: port.Port,
					}
					serviceInfo.Endpoints = append(serviceInfo.Endpoints, endpoint)
				}
			}
		}
	}

	w.services[serviceKey] = serviceInfo
}

func (w *ServiceWatcher) getNamespace(configNamespace string) string {
	if configNamespace != "" {
		return configNamespace
	}
	
	namespace := "default"
	
	if ns, err := w.getCurrentNamespace(); err == nil && ns != "" {
		namespace = ns
	}

	return namespace
}

func (w *ServiceWatcher) getCurrentNamespace() (string, error) {
	namespaceBytes, err := w.clientset.CoreV1().Namespaces().Get(context.Background(), "default", metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	
	return namespaceBytes.Name, nil
}