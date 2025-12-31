package bootstrap

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"time"

	"github.com/eleven-am/graft/internal/adapters/discovery"
	"github.com/eleven-am/graft/internal/bootstrap"
	"github.com/eleven-am/graft/internal/domain"
)

type FactoryDeps struct {
	Config           *domain.Config
	DiscoveryManager *discovery.Manager
	Logger           *slog.Logger
}

type Factory struct {
	config           *domain.Config
	discoveryManager *discovery.Manager
	logger           *slog.Logger
}

func NewFactory(deps FactoryDeps) (*Factory, error) {
	if deps.Config == nil {
		return nil, fmt.Errorf("config is required")
	}

	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Factory{
		config:           deps.Config,
		discoveryManager: deps.DiscoveryManager,
		logger:           logger.With("component", "bootstrap_factory"),
	}, nil
}

func (f *Factory) CreateBootstrapper() (*bootstrap.Bootstrapper, error) {
	bootCfg := f.buildBootstrapConfig()

	if err := bootCfg.Validate(); err != nil {
		return nil, fmt.Errorf("bootstrap config validation failed: %w", err)
	}

	metaStore := bootstrap.NewFileMetaStore(bootCfg.DataDir, f.logger)

	secretsManager := f.createSecretsManager(bootCfg)
	if secretsManager != nil {
		if err := secretsManager.LoadSecrets(); err != nil {
			f.logger.Warn("failed to load secrets", "error", err)
		}
	}

	discoveryAdapter, err := f.createDiscoveryAdapter(bootCfg)
	if err != nil {
		return nil, fmt.Errorf("create discovery adapter: %w", err)
	}

	transport, err := f.createTransport(bootCfg)
	if err != nil {
		f.logger.Warn("transport creation failed, fencing will be disabled", "error", err)
	}

	tokenStore := bootstrap.NewFileTokenStore(bootCfg.DataDir, f.logger)
	epochStore := bootstrap.NewFileEpochStore(bootCfg.DataDir, f.logger)

	var fencingManager *bootstrap.FencingManager
	if f.config.Bootstrap.FencingEnabled && transport != nil && secretsManager != nil {
		fencingManager = bootstrap.NewFencingManager(
			bootCfg,
			tokenStore,
			epochStore,
			secretsManager,
			f.logger,
		)
	}

	stateMachine, err := f.createStateMachine(metaStore, fencingManager)
	if err != nil {
		return nil, fmt.Errorf("create state machine: %w", err)
	}

	recoveryManager := f.createRecoveryManager(bootCfg, metaStore)

	var fallbackElection *bootstrap.FallbackElection
	if discoveryAdapter != nil && transport != nil && fencingManager != nil {
		fallbackElection = f.createFallbackElection(bootCfg, discoveryAdapter, transport, fencingManager, stateMachine, secretsManager)
	}

	var forceBootstrapExecutor *bootstrap.ForceBootstrapExecutor
	if transport != nil && fencingManager != nil {
		forceBootstrapExecutor = f.createForceBootstrapExecutor(bootCfg, discoveryAdapter, transport, fencingManager, metaStore, stateMachine, secretsManager)
	}

	bootstrapper := bootstrap.NewBootstrapper(bootstrap.BootstrapperDeps{
		Config:                 bootCfg,
		MetaStore:              metaStore,
		FencingManager:         fencingManager,
		StateMachine:           stateMachine,
		RecoveryManager:        recoveryManager,
		Discovery:              discoveryAdapter,
		Transport:              transport,
		SecretsManager:         secretsManager,
		FallbackElection:       fallbackElection,
		ForceBootstrapExecutor: forceBootstrapExecutor,
		Logger:                 f.logger,
		StateCheckInterval:     time.Second,
		ReadyTimeout:           f.config.Bootstrap.ReadyTimeout,
		ShutdownTimeout:        f.config.Orchestrator.ShutdownTimeout,
		StaleCheckInterval:     f.config.Bootstrap.StaleCheckInterval,
	})

	return bootstrapper, nil
}

func (f *Factory) buildBootstrapConfig() *bootstrap.BootstrapConfig {
	domainCfg := f.config.Bootstrap
	ordinal := f.resolveOrdinal()
	bindHost := f.config.BindAddr
	raftPort := domainCfg.BasePort

	if host, portStr, err := net.SplitHostPort(bindHost); err == nil {
		if host != "" {
			bindHost = host
		}
		if portStr != "" {
			if port, err := strconv.Atoi(portStr); err == nil && port > 0 {
				raftPort = port
			}
		}
	}

	return &bootstrap.BootstrapConfig{
		ServiceName:   domainCfg.ServiceName,
		Namespace:     "",
		PodName:       fmt.Sprintf("%s-%d", domainCfg.ServiceName, ordinal),
		DataDir:       f.config.DataDir,
		Ordinal:       ordinal,
		ExpectedNodes: domainCfg.Replicas,
		MinQuorum:     f.calculateQuorum(domainCfg.Replicas, domainCfg.FencingQuorum),
		RaftPort:      raftPort,
		JoinPort:      raftPort + 1,
		BindAddr:      bindHost,
		AdvertiseAddr: "",

		BootstrapTimeout:       30 * time.Second,
		JoinTimeout:            60 * time.Second,
		LeaderWaitTimeout:      domainCfg.LeaderWaitTimeout,
		FallbackElectionWindow: 10 * time.Second,
		HealthCheckInterval:    5 * time.Second,
		StartupTimeout:         domainCfg.ReadyTimeout,
		DiscoveryCacheTTL:      30 * time.Second,
		PeerProbeTimeout:       5 * time.Second,

		Retry: bootstrap.RetryConfig{
			InitialDelay:   100 * time.Millisecond,
			MaxDelay:       5 * time.Second,
			Multiplier:     1.5,
			MaxAttempts:    10,
			JitterFraction: 0.2,
		},

		TLS: f.buildTLSConfig(domainCfg),

		Secrets: bootstrap.SecretsConfig{
			FencingKeyFile:        domainCfg.FencingKeyPath,
			ForceBootstrapKeyFile: domainCfg.ForceBootstrapKeyPath,
			AdminTokenFile:        "",
			WipeKeyFile:           "",
		},

		Membership: bootstrap.MembershipConfig{
			LearnerCatchupThreshold: 1000,
			SnapshotCatchupTimeout:  5 * time.Minute,
			SnapshotCompleteGate:    true,
			JoinRateLimit:           1,
			JoinBurst:               3,
			MaxConcurrentJoins:      2,
			MaxLogSizeForJoin:       100 * 1024 * 1024,
			DiskPressureThreshold:   0.9,
			DataDir:                 f.config.DataDir,
		},

		Metrics: bootstrap.MetricsConfig{
			Namespace: "graft",
			Subsystem: "bootstrap",
		},

		ForceBootstrap: bootstrap.ForceBootstrapConfig{
			RequireDedicatedKey:   domainCfg.RequireDedicatedKey,
			ServerIDPattern:       fmt.Sprintf("%s-%%d", domainCfg.ServiceName),
			AllowDRQuorumOverride: false,
		},

		ForceBootstrapFile:     filepath.Join(domainCfg.ForceBootstrapTokenDir, "force-bootstrap-token.json"),
		RequireProtocolVersion: true,
	}
}

func (f *Factory) buildTLSConfig(domainCfg domain.BootstrapConfig) bootstrap.TLSConfig {
	if !domainCfg.TLSEnabled {
		return bootstrap.TLSConfig{
			Enabled:       false,
			Required:      false,
			AllowInsecure: domainCfg.TLSAllowInsecure,
		}
	}

	return bootstrap.TLSConfig{
		Enabled:    true,
		Required:   true,
		CertFile:   domainCfg.TLSCertPath,
		KeyFile:    domainCfg.TLSKeyPath,
		CAFile:     domainCfg.TLSCAPath,
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS13,
	}
}

func (f *Factory) resolveOrdinal() int {
	if f.config.Bootstrap.HeadlessService != "" {
		return f.config.Bootstrap.Ordinal
	}

	if f.discoveryManager != nil {
		deadline := time.Now().Add(15 * time.Second)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for time.Now().Before(deadline) {
			if ordinal := f.discoveryManager.GetSelfOrdinal(); ordinal >= 0 {
				f.logger.Info("resolved ordinal from discovery", "ordinal", ordinal)
				return ordinal
			}
			<-ticker.C
		}
		f.logger.Warn("timed out waiting for ordinal from discovery, using config")
	}

	return f.config.Bootstrap.Ordinal
}

func (f *Factory) calculateQuorum(replicas, configuredQuorum int) int {
	if configuredQuorum > 0 && configuredQuorum <= replicas {
		return configuredQuorum
	}
	return (replicas / 2) + 1
}

func (f *Factory) createSecretsManager(bootCfg *bootstrap.BootstrapConfig) *bootstrap.SecretsManager {
	if bootCfg.Secrets.FencingKeyFile == "" && bootCfg.Secrets.ForceBootstrapKeyFile == "" {
		return nil
	}
	return bootstrap.NewSecretsManager(bootCfg.Secrets, f.logger)
}

func (f *Factory) createDiscoveryAdapter(bootCfg *bootstrap.BootstrapConfig) (bootstrap.PeerDiscovery, error) {
	if f.discoveryManager == nil {
		return nil, nil
	}

	adapter, err := discovery.NewBootstrapDiscoveryAdapter(discovery.BootstrapDiscoveryConfig{
		Manager:         f.discoveryManager,
		ServiceName:     bootCfg.ServiceName,
		HeadlessService: f.config.Bootstrap.HeadlessService,
		BasePort:        bootCfg.RaftPort,
		Replicas:        bootCfg.ExpectedNodes,
		LocalOrdinal:    bootCfg.Ordinal,
		Logger:          f.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create bootstrap discovery adapter: %w", err)
	}

	return adapter, nil
}

func (f *Factory) createTransport(bootCfg *bootstrap.BootstrapConfig) (bootstrap.BootstrapTransport, error) {
	if !bootCfg.TLS.Enabled && !bootCfg.TLS.AllowInsecure {
		return nil, fmt.Errorf("TLS is required for bootstrap transport")
	}

	transport, err := bootstrap.NewSecureTransport(bootstrap.SecureTransportDeps{
		Config: bootCfg,
		Logger: f.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create secure transport: %w", err)
	}

	return transport, nil
}

func (f *Factory) createStateMachine(metaStore bootstrap.MetaStore, fencingManager *bootstrap.FencingManager) (*bootstrap.StateMachine, error) {
	sm, err := bootstrap.NewStateMachine(bootstrap.StateMachineDeps{
		MetaStore:      metaStore,
		FencingManager: fencingManager,
		Logger:         f.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create state machine: %w", err)
	}
	return sm, nil
}

func (f *Factory) createRecoveryManager(
	bootCfg *bootstrap.BootstrapConfig,
	metaStore bootstrap.MetaStore,
) *bootstrap.RecoveryManager {
	return bootstrap.NewRecoveryManager(bootstrap.RecoveryManagerDeps{
		Config:    bootCfg,
		MetaStore: metaStore,
		Logger:    f.logger,
	})
}

func (f *Factory) createFallbackElection(
	bootCfg *bootstrap.BootstrapConfig,
	discoveryAdapter bootstrap.PeerDiscovery,
	transport bootstrap.BootstrapTransport,
	fencingManager *bootstrap.FencingManager,
	stateMachine *bootstrap.StateMachine,
	secretsManager *bootstrap.SecretsManager,
) *bootstrap.FallbackElection {
	return bootstrap.NewFallbackElection(bootstrap.FallbackElectionDeps{
		Config:       bootCfg,
		Discovery:    discoveryAdapter,
		Transport:    transport,
		Fencing:      fencingManager,
		StateMachine: stateMachine,
		Secrets:      secretsManager,
		Logger:       f.logger,
	})
}

func (f *Factory) createForceBootstrapExecutor(
	bootCfg *bootstrap.BootstrapConfig,
	discoveryAdapter bootstrap.PeerDiscovery,
	transport bootstrap.BootstrapTransport,
	fencingManager *bootstrap.FencingManager,
	metaStore bootstrap.MetaStore,
	stateMachine *bootstrap.StateMachine,
	secretsManager *bootstrap.SecretsManager,
) *bootstrap.ForceBootstrapExecutor {
	return bootstrap.NewForceBootstrapExecutor(bootstrap.ForceBootstrapExecutorDeps{
		Config:       bootCfg,
		Discovery:    discoveryAdapter,
		Transport:    transport,
		Fencing:      fencingManager,
		MetaStore:    metaStore,
		StateMachine: stateMachine,
		Secrets:      secretsManager,
		Logger:       f.logger,
	})
}
