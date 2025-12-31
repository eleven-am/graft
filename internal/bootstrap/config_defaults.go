package bootstrap

import (
	"crypto/tls"
	"time"

	"golang.org/x/time/rate"
)

func DefaultBootstrapConfig() *BootstrapConfig {
	return &BootstrapConfig{
		ExpectedNodes: 3,

		RaftPort: 7946,
		JoinPort: 7947,

		BootstrapTimeout:       5 * time.Minute,
		JoinTimeout:            2 * time.Minute,
		LeaderWaitTimeout:      60 * time.Second,
		FallbackElectionWindow: 30 * time.Second,
		HealthCheckInterval:    5 * time.Second,
		StartupTimeout:         5 * time.Minute,
		DiscoveryCacheTTL:      5 * time.Second,

		Retry:      DefaultRetryConfig(),
		TLS:        DefaultTLSConfig(),
		Secrets:    DefaultSecretsConfig(),
		Membership: DefaultMembershipConfig(),
		Metrics:    DefaultMetricsConfig(),
	}
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		InitialDelay:   1 * time.Second,
		MaxDelay:       30 * time.Second,
		Multiplier:     2.0,
		MaxAttempts:    10,
		JitterFraction: 0.1,
	}
}

func DefaultTLSConfig() TLSConfig {
	return TLSConfig{
		Enabled:    false,
		Required:   false,
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS13,
	}
}

func DefaultSecretsConfig() SecretsConfig {
	return SecretsConfig{
		FencingKeyFile: "/etc/graft/secrets/fencing.key",
		AdminTokenFile: "/etc/graft/secrets/admin.token",
		WipeKeyFile:    "/etc/graft/secrets/wipe.key",
	}
}

func DefaultMembershipConfig() MembershipConfig {
	return MembershipConfig{
		LearnerCatchupThreshold: 1000,
		SnapshotCatchupTimeout:  5 * time.Minute,
		SnapshotCompleteGate:    true,
		JoinRateLimit:           rate.Limit(1),
		JoinBurst:               3,
		MaxConcurrentJoins:      1,
		MaxLogSizeForJoin:       1 << 30,
		DiskPressureThreshold:   0.9,
		VersionCompatibility:    DefaultVersionCompatibilityConfig(),
	}
}

func DefaultVersionCompatibilityConfig() VersionCompatibilityConfig {
	return VersionCompatibilityConfig{
		MinRaftProtocolVersion: 1,
		MaxRaftProtocolVersion: 3,
		MinStateMachineVersion: 1,
		MaxStateMachineVersion: 1,
		AllowDowngrade:         false,
	}
}

func DefaultMetricsConfig() MetricsConfig {
	return MetricsConfig{
		Namespace: "graft",
		Subsystem: "bootstrap",
	}
}
