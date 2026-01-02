package graft

import (
	"time"

	"github.com/eleven-am/auto-consensus/bootstrap"
	"github.com/eleven-am/auto-consensus/discovery"
	"github.com/eleven-am/auto-consensus/gossip"
	"github.com/eleven-am/graft/internal/domain"
)

type Config = domain.Config

type TransportConfig = domain.TransportConfig

type RaftConfig = domain.RaftConfig

type ResourceConfig = domain.ResourceConfig

type HealthConfig = domain.HealthConfig

type EngineConfig = domain.EngineConfig

type OrchestratorConfig = domain.OrchestratorConfig

type ClusterPolicy = domain.ClusterPolicy

const (
	ClusterPolicyStrict  ClusterPolicy = domain.ClusterPolicyStrict
	ClusterPolicyAdopt   ClusterPolicy = domain.ClusterPolicyAdopt
	ClusterPolicyReset   ClusterPolicy = domain.ClusterPolicyReset
	ClusterPolicyRecover ClusterPolicy = domain.ClusterPolicyRecover
)

type ClusterConfig = domain.ClusterConfig

type BootstrapConfig = bootstrap.Config

type MDNSConfig = discovery.MDNSConfig

type DNSConfig = discovery.DNSConfig

type NetworkMode = gossip.NetworkMode

const (
	NetworkModeLAN NetworkMode = gossip.LAN
	NetworkModeWAN NetworkMode = gossip.WAN
)

type Discoverer = discovery.Discoverer

func NewMDNSDiscoverer(cfg MDNSConfig) *discovery.MDNSDiscoverer {
	return discovery.NewMDNS(cfg)
}

func NewDNSDiscoverer(cfg DNSConfig) *discovery.DNSDiscoverer {
	return discovery.NewDNS(cfg)
}

func NewStaticDiscoverer(peers []string) *discovery.StaticDiscoverer {
	return discovery.NewStatic(peers)
}

func DefaultConfig() *Config {
	return domain.DefaultConfig()
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
	config.AdvertiseAddr = bindAddr
	config.DataDir = dataDir
	return &ConfigBuilder{config: config}
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

func (cb *ConfigBuilder) WithAdvertiseAddr(advertiseAddr string) *ConfigBuilder {
	cb.config.AdvertiseAddr = advertiseAddr
	return cb
}

func (cb *ConfigBuilder) WithDiscoverers(discoverers ...Discoverer) *ConfigBuilder {
	for _, d := range discoverers {
		cb.config.Discoverers = append(cb.config.Discoverers, d)
	}
	return cb
}

func (cb *ConfigBuilder) WithGossipAddr(bindAddr, advertiseAddr string) *ConfigBuilder {
	cb.config.WithGossipAddr(bindAddr, advertiseAddr)
	return cb
}

func (cb *ConfigBuilder) Build() *Config {
	return cb.config
}
