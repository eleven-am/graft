package bootstrap

import (
	"testing"
	"time"
)

func TestBootstrapConfig_Validate(t *testing.T) {
	validConfig := func() *BootstrapConfig {
		cfg := DefaultBootstrapConfig()
		cfg.DataDir = "/tmp/graft"
		return cfg
	}

	t.Run("valid config", func(t *testing.T) {
		cfg := validConfig()
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("missing data_dir", func(t *testing.T) {
		cfg := validConfig()
		cfg.DataDir = ""
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for missing data_dir")
		}
	})

	t.Run("invalid expected_nodes", func(t *testing.T) {
		cfg := validConfig()
		cfg.ExpectedNodes = 0
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for expected_nodes < 1")
		}
	})

	t.Run("invalid raft_port low", func(t *testing.T) {
		cfg := validConfig()
		cfg.RaftPort = 0
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for raft_port <= 0")
		}
	})

	t.Run("invalid raft_port high", func(t *testing.T) {
		cfg := validConfig()
		cfg.RaftPort = 70000
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for raft_port > 65535")
		}
	})

	t.Run("invalid join_port", func(t *testing.T) {
		cfg := validConfig()
		cfg.JoinPort = -1
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for invalid join_port")
		}
	})

	t.Run("invalid bootstrap_timeout", func(t *testing.T) {
		cfg := validConfig()
		cfg.BootstrapTimeout = 0
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for bootstrap_timeout <= 0")
		}
	})

	t.Run("invalid join_timeout", func(t *testing.T) {
		cfg := validConfig()
		cfg.JoinTimeout = -1 * time.Second
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for join_timeout <= 0")
		}
	})

	t.Run("invalid leader_wait_timeout", func(t *testing.T) {
		cfg := validConfig()
		cfg.LeaderWaitTimeout = 0
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for leader_wait_timeout <= 0")
		}
	})

	t.Run("min_quorum exceeds expected_nodes", func(t *testing.T) {
		cfg := validConfig()
		cfg.ExpectedNodes = 3
		cfg.MinQuorum = 5
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail when min_quorum > expected_nodes")
		}
	})

	t.Run("min_quorum equals expected_nodes", func(t *testing.T) {
		cfg := validConfig()
		cfg.ExpectedNodes = 3
		cfg.MinQuorum = 3
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil for min_quorum == expected_nodes", err)
		}
	})
}

func TestBootstrapConfig_CalculateQuorum(t *testing.T) {
	tests := []struct {
		name          string
		expectedNodes int
		minQuorum     int
		wantQuorum    int
	}{
		{"3 nodes default", 3, 0, 2},
		{"5 nodes default", 5, 0, 3},
		{"7 nodes default", 7, 0, 4},
		{"1 node default", 1, 0, 1},
		{"override min quorum", 5, 4, 4},
		{"override to 1", 3, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &BootstrapConfig{
				ExpectedNodes: tt.expectedNodes,
				MinQuorum:     tt.minQuorum,
			}
			got := cfg.CalculateQuorum()
			if got != tt.wantQuorum {
				t.Errorf("CalculateQuorum() = %v, want %v", got, tt.wantQuorum)
			}
		})
	}
}

func TestRetryConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := DefaultRetryConfig()
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("invalid initial_delay", func(t *testing.T) {
		cfg := DefaultRetryConfig()
		cfg.InitialDelay = 0
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for initial_delay <= 0")
		}
	})

	t.Run("max_delay less than initial_delay", func(t *testing.T) {
		cfg := DefaultRetryConfig()
		cfg.MaxDelay = 500 * time.Millisecond
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail when max_delay < initial_delay")
		}
	})

	t.Run("invalid multiplier", func(t *testing.T) {
		cfg := DefaultRetryConfig()
		cfg.Multiplier = 0.5
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for multiplier < 1.0")
		}
	})

	t.Run("invalid max_attempts", func(t *testing.T) {
		cfg := DefaultRetryConfig()
		cfg.MaxAttempts = 0
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for max_attempts < 1")
		}
	})

	t.Run("jitter_fraction negative", func(t *testing.T) {
		cfg := DefaultRetryConfig()
		cfg.JitterFraction = -0.1
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for jitter_fraction < 0")
		}
	})

	t.Run("jitter_fraction too high", func(t *testing.T) {
		cfg := DefaultRetryConfig()
		cfg.JitterFraction = 1.5
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for jitter_fraction > 1")
		}
	})
}

func TestTLSConfig_Validate(t *testing.T) {
	t.Run("disabled TLS valid", func(t *testing.T) {
		cfg := DefaultTLSConfig()
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("enabled TLS missing cert_file", func(t *testing.T) {
		cfg := DefaultTLSConfig()
		cfg.Enabled = true
		cfg.KeyFile = "/path/to/key"
		cfg.CAFile = "/path/to/ca"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for missing cert_file")
		}
	})

	t.Run("enabled TLS missing key_file", func(t *testing.T) {
		cfg := DefaultTLSConfig()
		cfg.Enabled = true
		cfg.CertFile = "/path/to/cert"
		cfg.CAFile = "/path/to/ca"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for missing key_file")
		}
	})

	t.Run("enabled TLS missing ca_file", func(t *testing.T) {
		cfg := DefaultTLSConfig()
		cfg.Enabled = true
		cfg.CertFile = "/path/to/cert"
		cfg.KeyFile = "/path/to/key"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for missing ca_file")
		}
	})

	t.Run("enabled TLS all files present", func(t *testing.T) {
		cfg := DefaultTLSConfig()
		cfg.Enabled = true
		cfg.CertFile = "/path/to/cert"
		cfg.KeyFile = "/path/to/key"
		cfg.CAFile = "/path/to/ca"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("required but not enabled", func(t *testing.T) {
		cfg := DefaultTLSConfig()
		cfg.Required = true
		cfg.Enabled = false
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail when required=true but enabled=false")
		}
	})
}

func TestMembershipConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := DefaultMembershipConfig()
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("invalid join_burst", func(t *testing.T) {
		cfg := DefaultMembershipConfig()
		cfg.JoinBurst = 0
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for join_burst < 1")
		}
	})

	t.Run("invalid max_concurrent_joins", func(t *testing.T) {
		cfg := DefaultMembershipConfig()
		cfg.MaxConcurrentJoins = 0
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for max_concurrent_joins < 1")
		}
	})

	t.Run("disk_pressure_threshold negative", func(t *testing.T) {
		cfg := DefaultMembershipConfig()
		cfg.DiskPressureThreshold = -0.1
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for disk_pressure_threshold < 0")
		}
	})

	t.Run("disk_pressure_threshold too high", func(t *testing.T) {
		cfg := DefaultMembershipConfig()
		cfg.DiskPressureThreshold = 1.5
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() should fail for disk_pressure_threshold > 1")
		}
	})
}

func TestDefaultConfigs(t *testing.T) {
	t.Run("DefaultBootstrapConfig", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		if cfg.ExpectedNodes != 3 {
			t.Errorf("ExpectedNodes = %d, want 3", cfg.ExpectedNodes)
		}
		if cfg.RaftPort != 7946 {
			t.Errorf("RaftPort = %d, want 7946", cfg.RaftPort)
		}
		if cfg.JoinPort != 7947 {
			t.Errorf("JoinPort = %d, want 7947", cfg.JoinPort)
		}
	})

	t.Run("DefaultRetryConfig", func(t *testing.T) {
		cfg := DefaultRetryConfig()
		if cfg.InitialDelay != 1*time.Second {
			t.Errorf("InitialDelay = %v, want 1s", cfg.InitialDelay)
		}
		if cfg.MaxAttempts != 10 {
			t.Errorf("MaxAttempts = %d, want 10", cfg.MaxAttempts)
		}
	})

	t.Run("DefaultTLSConfig", func(t *testing.T) {
		cfg := DefaultTLSConfig()
		if cfg.Enabled {
			t.Error("Enabled should be false by default")
		}
	})

	t.Run("DefaultMembershipConfig", func(t *testing.T) {
		cfg := DefaultMembershipConfig()
		if cfg.LearnerCatchupThreshold != 1000 {
			t.Errorf("LearnerCatchupThreshold = %d, want 1000", cfg.LearnerCatchupThreshold)
		}
	})

	t.Run("DefaultMetricsConfig", func(t *testing.T) {
		cfg := DefaultMetricsConfig()
		if cfg.Namespace != "graft" {
			t.Errorf("Namespace = %s, want graft", cfg.Namespace)
		}
		if cfg.Subsystem != "bootstrap" {
			t.Errorf("Subsystem = %s, want bootstrap", cfg.Subsystem)
		}
	})
}
