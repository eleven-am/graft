package graft

import (
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

// Config is the main configuration structure that defines all settings for a Graft manager.
// It includes service discovery, networking, clustering, resource management, and engine settings.
type Config = domain.Config

// Service Discovery Types

// DiscoveryType specifies the method used for service discovery (mDNS or static).
type DiscoveryType = domain.DiscoveryType

// DiscoveryConfig contains the configuration for service discovery mechanisms.
type DiscoveryConfig = domain.DiscoveryConfig

// MDNSConfig configures multicast DNS-based service discovery for local network environments.
type MDNSConfig = domain.MDNSConfig

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

// Service Discovery Constants
const (
	// MDNS enables multicast DNS-based service discovery for local networks.
	MDNS = domain.DiscoveryMDNS

	// Static uses a predefined list of peers for service discovery.
	Static = domain.DiscoveryStatic
)

type ClusterPolicy = domain.ClusterPolicy

const (
	ClusterPolicyStrict  ClusterPolicy = domain.ClusterPolicyStrict
	ClusterPolicyAdopt   ClusterPolicy = domain.ClusterPolicyAdopt
	ClusterPolicyReset   ClusterPolicy = domain.ClusterPolicyReset
	ClusterPolicyRecover ClusterPolicy = domain.ClusterPolicyRecover
)

type ClusterConfig = domain.ClusterConfig

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
func (cb *ConfigBuilder) WithMDNS(service, domain, host string, disableIPv6 bool) *ConfigBuilder {
	cb.config.WithMDNS(service, domain, host, disableIPv6)
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

func (cb *ConfigBuilder) WithClusterID(clusterID string) *ConfigBuilder {
	cb.config.WithClusterID(clusterID)
	return cb
}

func (cb *ConfigBuilder) WithClusterPolicy(policy ClusterPolicy) *ConfigBuilder {
	cb.config.WithClusterPolicy(policy)
	return cb
}

func (cb *ConfigBuilder) WithClusterRecovery(enabled bool) *ConfigBuilder {
	cb.config.WithClusterRecovery(enabled)
	return cb
}

func (cb *ConfigBuilder) WithClusterPersistence(persistenceFile string) *ConfigBuilder {
	cb.config.WithClusterPersistence(persistenceFile)
	return cb
}

func (cb *ConfigBuilder) WithBootstrapInsecure() *ConfigBuilder {
	cb.config.WithBootstrapInsecure()
	return cb
}

func (cb *ConfigBuilder) WithK8sBootstrap(replicas int) *ConfigBuilder {
	cb.config.WithK8sBootstrap(replicas)
	return cb
}

// Build returns the constructed configuration.
// This finalizes the configuration and returns it ready for use with NewWithConfig().
func (cb *ConfigBuilder) Build() *Config {
	return cb.config
}
