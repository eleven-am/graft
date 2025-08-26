package graft

import (
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

// Config is the main configuration structure that defines all settings for a Graft manager.
// It includes service discovery, networking, clustering, resource management, and engine settings.
type Config = domain.Config

// Service Discovery Types

// DiscoveryType specifies the method used for service discovery (mDNS, Kubernetes, or static).
type DiscoveryType = domain.DiscoveryType

// DiscoveryConfig contains the configuration for service discovery mechanisms.
type DiscoveryConfig = domain.DiscoveryConfig

// MDNSConfig configures multicast DNS-based service discovery for local network environments.
type MDNSConfig = domain.MDNSConfig

// KubernetesConfig configures Kubernetes-based service discovery using various K8s primitives.
type KubernetesConfig = domain.KubernetesConfig

// StaticPeer represents a statically configured peer in the cluster.
type StaticPeer = domain.StaticPeer

// Network and Transport Configuration

// TransportConfig defines network transport settings including TLS configuration.
type TransportConfig = domain.TransportConfig

// Clustering Configuration

// RaftConfig contains settings for the Raft consensus algorithm used for cluster coordination.
type RaftConfig = domain.RaftConfig

// Resource Management

// ResourceConfig defines resource allocation and limiting settings for workflow execution.
type ResourceConfig = domain.ResourceConfig

// HealthConfig configures health monitoring and failure detection settings.
type HealthConfig = domain.HealthConfig

// Engine Configuration

// EngineConfig contains settings for the workflow execution engine including timeouts and retry policies.
type EngineConfig = domain.EngineConfig

// OrchestratorConfig defines high-level orchestration behavior and coordination settings.
type OrchestratorConfig = domain.OrchestratorConfig

// Strategy and Method Types

// AuthMethod specifies how to authenticate with Kubernetes clusters.
type AuthMethod = domain.AuthMethod

// DiscoveryMethod defines the specific mechanism used for peer discovery within a service discovery type.
type DiscoveryMethod = domain.DiscoveryMethod

// PeerIDSource specifies how peer IDs are determined in Kubernetes environments.
type PeerIDSource = domain.PeerIDSource

// PortSource defines how to determine the port for peer communication in Kubernetes.
type PortSource = domain.PortSource

// NetworkingMode specifies the networking approach for peer communication.
type NetworkingMode = domain.NetworkingMode

// DiscoveryStrategy, PeerIDStrategy, PortStrategy, and RetryStrategy define various behavioral strategies.
type DiscoveryStrategy = domain.DiscoveryStrategy
type PeerIDStrategy = domain.PeerIDStrategy
type PortStrategy = domain.PortStrategy
type RetryStrategy = domain.RetryStrategy

// Service Discovery Constants
const (
	// MDNS enables multicast DNS-based service discovery for local networks.
	MDNS = domain.DiscoveryMDNS

	// Kubernetes enables Kubernetes-based service discovery using K8s APIs.
	Kubernetes = domain.DiscoveryKubernetes

	// Static uses a predefined list of peers for service discovery.
	Static = domain.DiscoveryStatic
)

// Kubernetes Authentication Methods
const (
	// AuthInCluster uses the service account token when running inside a Kubernetes cluster.
	AuthInCluster = domain.AuthInCluster

	// AuthKubeconfig uses a kubeconfig file for authentication.
	AuthKubeconfig = domain.AuthKubeconfig

	// AuthExplicitToken uses an explicitly provided authentication token.
	AuthExplicitToken = domain.AuthExplicitToken
)

// Kubernetes Discovery Methods
const (
	// DiscoveryLabelSelector discovers peers using Kubernetes label selectors.
	DiscoveryLabelSelector = domain.DiscoveryLabelSelector

	// DiscoveryService discovers peers through a Kubernetes service.
	DiscoveryService = domain.DiscoveryService

	// DiscoveryDNS discovers peers using DNS queries.
	DiscoveryDNS = domain.DiscoveryDNS

	// DiscoveryStatefulSet discovers peers within a StatefulSet.
	DiscoveryStatefulSet = domain.DiscoveryStatefulSet

	// DiscoveryNamespace discovers all peers within a Kubernetes namespace.
	DiscoveryNamespace = domain.DiscoveryNamespace

	// DiscoverySiblings discovers peer pods that are siblings (same labels/selectors).
	DiscoverySiblings = domain.DiscoverySiblings
)

// Kubernetes Peer ID Sources
const (
	// PeerIDPodName uses the Kubernetes pod name as the peer ID.
	PeerIDPodName = domain.PeerIDPodName

	// PeerIDAnnotation extracts the peer ID from a pod annotation.
	PeerIDAnnotation = domain.PeerIDAnnotation

	// PeerIDLabel extracts the peer ID from a pod label.
	PeerIDLabel = domain.PeerIDLabel

	// PeerIDTemplate generates the peer ID using a template.
	PeerIDTemplate = domain.PeerIDTemplate
)

// Kubernetes Port Discovery
const (
	// PortNamedPort uses a named port from the pod specification.
	PortNamedPort = domain.PortNamedPort

	// PortAnnotation extracts the port from a pod annotation.
	PortAnnotation = domain.PortAnnotation

	// PortFirstPort uses the first exposed port from the container.
	PortFirstPort = domain.PortFirstPort

	// PortEnvVar reads the port from an environment variable.
	PortEnvVar = domain.PortEnvVar
)

// Kubernetes Networking Modes
const (
	// NetworkingPodIP uses the pod's IP address for communication.
	NetworkingPodIP = domain.NetworkingPodIP

	// NetworkingServiceIP uses the service IP for communication.
	NetworkingServiceIP = domain.NetworkingServiceIP

	// NetworkingNodePort uses NodePort services for communication.
	NetworkingNodePort = domain.NetworkingNodePort
)

// DefaultConfig returns a Config with sensible defaults for most use cases.
// This includes basic settings for local development and testing.
func DefaultConfig() *Config {
	return domain.DefaultConfig()
}

// DefaultMDNSConfig returns a default mDNS configuration for local service discovery.
// Suitable for development and local network deployments.
func DefaultMDNSConfig() *MDNSConfig {
	return domain.DefaultMDNSConfig()
}

// DefaultKubernetesConfig returns a default Kubernetes service discovery configuration.
// Uses in-cluster authentication and common discovery patterns.
func DefaultKubernetesConfig() *KubernetesConfig {
	return domain.DefaultKubernetesConfig()
}

// DefaultTransportConfig returns default network transport settings.
// Includes reasonable timeouts and connection limits without TLS.
func DefaultTransportConfig() TransportConfig {
	return domain.DefaultTransportConfig()
}

// DefaultRaftConfig returns default Raft consensus algorithm settings.
// Configured with standard timeouts and replication settings.
func DefaultRaftConfig() RaftConfig {
	return domain.DefaultRaftConfig()
}

// DefaultResourceConfig returns default resource management settings.
// Includes sensible limits for concurrent workflow execution.
func DefaultResourceConfig() ResourceConfig {
	return domain.DefaultResourceConfig()
}

// DefaultEngineConfig returns default workflow engine settings.
// Configured with standard timeouts and retry policies.
func DefaultEngineConfig() EngineConfig {
	return domain.DefaultEngineConfig()
}

// DefaultOrchestratorConfig returns default orchestration settings.
// Includes standard coordination and scheduling configurations.
func DefaultOrchestratorConfig() OrchestratorConfig {
	return domain.DefaultOrchestratorConfig()
}

// ConfigBuilder provides a fluent interface for building Graft configurations.
// It uses the builder pattern to allow easy customization of various settings.
//
// Example usage:
//
//	config := graft.NewConfigBuilder("node-1", "0.0.0.0:8080", "./data").
//	    WithMDNS("graft", "local", "").
//	    WithTLS("cert.pem", "key.pem", "ca.pem").
//	    WithEngineSettings(100, 30*time.Second, 3).
//	    Build()
//	manager := graft.NewWithConfig(config)
type ConfigBuilder struct {
	config *Config
}

// NewConfigBuilder creates a new configuration builder with the essential parameters.
//
// Parameters:
//   - nodeID: Unique identifier for this node in the cluster
//   - bindAddr: Address to bind network services to (e.g., "0.0.0.0:8080")
//   - dataDir: Directory path for persistent storage
//
// Returns a ConfigBuilder with default settings that can be further customized.
func NewConfigBuilder(nodeID, bindAddr, dataDir string) *ConfigBuilder {
	config := DefaultConfig()
	config.NodeID = nodeID
	config.BindAddr = bindAddr
	config.DataDir = dataDir
	return &ConfigBuilder{config: config}
}

// WithMDNS configures multicast DNS service discovery for local network environments.
//
// Parameters:
//   - service: mDNS service name (e.g., "graft")
//   - domain: mDNS domain (e.g., "local")
//   - host: Specific host to advertise (empty string uses default)
//
// This is ideal for development environments and local clusters.
func (cb *ConfigBuilder) WithMDNS(service, domain, host string) *ConfigBuilder {
	cb.config.WithMDNS(service, domain, host)
	return cb
}

// WithKubernetes configures Kubernetes-based service discovery.
//
// Parameters:
//   - serviceName: Name of the Kubernetes service
//   - namespace: Kubernetes namespace (empty string uses current namespace)
//
// This enables automatic peer discovery in Kubernetes deployments.
func (cb *ConfigBuilder) WithKubernetes(serviceName, namespace string) *ConfigBuilder {
	cb.config.WithKubernetes(serviceName, namespace)
	return cb
}

// WithStaticPeers configures a static list of cluster peers.
//
// Parameters:
//   - peers: List of StaticPeer configurations with addresses and IDs
//
// Use this when you have a fixed set of known cluster members.
func (cb *ConfigBuilder) WithStaticPeers(peers ...StaticPeer) *ConfigBuilder {
	cb.config.WithStaticPeers(peers...)
	return cb
}

// WithTLS enables TLS encryption for all network communication.
//
// Parameters:
//   - certFile: Path to the TLS certificate file
//   - keyFile: Path to the private key file
//   - caFile: Path to the certificate authority file
//
// Essential for production deployments to secure inter-node communication.
func (cb *ConfigBuilder) WithTLS(certFile, keyFile, caFile string) *ConfigBuilder {
	cb.config.WithTLS(certFile, keyFile, caFile)
	return cb
}

// WithResourceLimits configures resource allocation limits for workflow execution.
//
// Parameters:
//   - maxTotal: Maximum total number of concurrent node executions
//   - defaultPerType: Default limit per node type
//   - perTypeOverrides: Map of node type names to their specific limits
//
// This helps prevent resource exhaustion and ensures fair resource distribution.
func (cb *ConfigBuilder) WithResourceLimits(maxTotal, defaultPerType int, perTypeOverrides map[string]int) *ConfigBuilder {
	cb.config.WithResourceLimits(maxTotal, defaultPerType, perTypeOverrides)
	return cb
}

// WithEngineSettings configures workflow execution engine parameters.
//
// Parameters:
//   - maxWorkflows: Maximum number of concurrent workflows
//   - nodeTimeout: Timeout for individual node execution
//   - retryAttempts: Number of retry attempts for failed nodes
//
// These settings control the engine's execution behavior and fault tolerance.
func (cb *ConfigBuilder) WithEngineSettings(maxWorkflows int, nodeTimeout time.Duration, retryAttempts int) *ConfigBuilder {
	cb.config.WithEngineSettings(maxWorkflows, nodeTimeout, retryAttempts)
	return cb
}

// Build returns the constructed configuration.
// This finalizes the configuration and returns it ready for use with NewWithConfig().
func (cb *ConfigBuilder) Build() *Config {
	return cb.config
}
