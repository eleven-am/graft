package graft

import (
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type Config = domain.Config

type DiscoveryType = domain.DiscoveryType
type DiscoveryConfig = domain.DiscoveryConfig
type MDNSConfig = domain.MDNSConfig
type KubernetesConfig = domain.KubernetesConfig
type StaticPeer = domain.StaticPeer
type TransportConfig = domain.TransportConfig
type RaftConfig = domain.RaftConfig
type ResourceConfig = domain.ResourceConfig
type HealthConfig = domain.HealthConfig
type EngineConfig = domain.EngineConfig
type OrchestratorConfig = domain.OrchestratorConfig

type AuthMethod = domain.AuthMethod
type DiscoveryMethod = domain.DiscoveryMethod
type PeerIDSource = domain.PeerIDSource
type PortSource = domain.PortSource
type NetworkingMode = domain.NetworkingMode
type DiscoveryStrategy = domain.DiscoveryStrategy
type PeerIDStrategy = domain.PeerIDStrategy
type PortStrategy = domain.PortStrategy
type RetryStrategy = domain.RetryStrategy

const (
	MDNS       = domain.DiscoveryMDNS
	Kubernetes = domain.DiscoveryKubernetes
	Static     = domain.DiscoveryStatic

	AuthInCluster     = domain.AuthInCluster
	AuthKubeconfig    = domain.AuthKubeconfig
	AuthExplicitToken = domain.AuthExplicitToken

	DiscoveryLabelSelector = domain.DiscoveryLabelSelector
	DiscoveryService       = domain.DiscoveryService
	DiscoveryDNS           = domain.DiscoveryDNS
	DiscoveryStatefulSet   = domain.DiscoveryStatefulSet
	DiscoveryNamespace     = domain.DiscoveryNamespace
	DiscoverySiblings      = domain.DiscoverySiblings

	PeerIDPodName    = domain.PeerIDPodName
	PeerIDAnnotation = domain.PeerIDAnnotation
	PeerIDLabel      = domain.PeerIDLabel
	PeerIDTemplate   = domain.PeerIDTemplate

	PortNamedPort  = domain.PortNamedPort
	PortAnnotation = domain.PortAnnotation
	PortFirstPort  = domain.PortFirstPort
	PortEnvVar     = domain.PortEnvVar

	NetworkingPodIP     = domain.NetworkingPodIP
	NetworkingServiceIP = domain.NetworkingServiceIP
	NetworkingNodePort  = domain.NetworkingNodePort
)

func DefaultConfig() *Config {
	return domain.DefaultConfig()
}

func DefaultMDNSConfig() *MDNSConfig {
	return domain.DefaultMDNSConfig()
}

func DefaultKubernetesConfig() *KubernetesConfig {
	return domain.DefaultKubernetesConfig()
}

func DefaultTransportConfig() TransportConfig {
	return domain.DefaultTransportConfig()
}

func DefaultRaftConfig() RaftConfig {
	return domain.DefaultRaftConfig()
}

func DefaultResourceConfig() ResourceConfig {
	return domain.DefaultResourceConfig()
}

func DefaultEngineConfig() EngineConfig {
	return domain.DefaultEngineConfig()
}

func DefaultOrchestratorConfig() OrchestratorConfig {
	return domain.DefaultOrchestratorConfig()
}

type ConfigBuilder struct {
	config *Config
}

func NewConfigBuilder(nodeID, bindAddr, dataDir string) *ConfigBuilder {
	config := DefaultConfig()
	config.NodeID = nodeID
	config.BindAddr = bindAddr
	config.DataDir = dataDir
	return &ConfigBuilder{config: config}
}

func (cb *ConfigBuilder) WithMDNS(service, domain, host string) *ConfigBuilder {
	cb.config.WithMDNS(service, domain, host)
	return cb
}

func (cb *ConfigBuilder) WithKubernetes(serviceName, namespace string) *ConfigBuilder {
	cb.config.WithKubernetes(serviceName, namespace)
	return cb
}

func (cb *ConfigBuilder) WithStaticPeers(peers ...StaticPeer) *ConfigBuilder {
	cb.config.WithStaticPeers(peers...)
	return cb
}

func (cb *ConfigBuilder) WithTLS(certFile, keyFile, caFile string) *ConfigBuilder {
	cb.config.WithTLS(certFile, keyFile, caFile)
	return cb
}

func (cb *ConfigBuilder) WithResourceLimits(maxTotal, defaultPerType int, perTypeOverrides map[string]int) *ConfigBuilder {
	cb.config.WithResourceLimits(maxTotal, defaultPerType, perTypeOverrides)
	return cb
}

func (cb *ConfigBuilder) WithEngineSettings(maxWorkflows int, nodeTimeout time.Duration, retryAttempts int) *ConfigBuilder {
	cb.config.WithEngineSettings(maxWorkflows, nodeTimeout, retryAttempts)
	return cb
}

func (cb *ConfigBuilder) Build() *Config {
	return cb.config
}
