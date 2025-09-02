package observability

import "time"

type Config struct {
	Enabled       bool          `json:"enabled" yaml:"enabled"`
	Port          int           `json:"port" yaml:"port"`
	ReadTimeout   time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout  time.Duration `json:"write_timeout" yaml:"write_timeout"`
	IdleTimeout   time.Duration `json:"idle_timeout" yaml:"idle_timeout"`
	EnablePprof   bool          `json:"enable_pprof" yaml:"enable_pprof"`
	EnableMetrics bool          `json:"enable_metrics" yaml:"enable_metrics"`
}

func DefaultConfig() Config {
	return Config{
		Enabled:       true,
		Port:          9090,
		ReadTimeout:   10 * time.Second,
		WriteTimeout:  10 * time.Second,
		IdleTimeout:   60 * time.Second,
		EnablePprof:   false,
		EnableMetrics: true,
	}
}
