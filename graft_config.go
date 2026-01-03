package graft

import (
	"time"

	autoconsensus "github.com/eleven-am/auto-consensus"
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

type MDNSConfig = autoconsensus.MDNSConfig

type DNSConfig = autoconsensus.DNSConfig

type Discoverer = autoconsensus.Discoverer

func NewMDNSDiscoverer(cfg MDNSConfig) Discoverer {
	return autoconsensus.NewMDNSDiscovery(cfg)
}

func NewDNSDiscoverer(cfg DNSConfig) Discoverer {
	return autoconsensus.NewDNSDiscovery(cfg)
}

func NewStaticDiscoverer(peers []string) Discoverer {
	return autoconsensus.NewStaticDiscovery(peers)
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

func NewConfigBuilder(nodeID, dataDir string) *ConfigBuilder {
	config := DefaultConfig()
	config.NodeID = nodeID
	config.GossipPort = 7946
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

func (cb *ConfigBuilder) WithGossipPort(gossipPort int) *ConfigBuilder {
	cb.config.WithGossipPort(gossipPort)
	return cb
}

func (cb *ConfigBuilder) Build() *Config {
	return cb.config
}
