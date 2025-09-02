package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type Server struct {
	port      int
	server    *http.Server
	logger    *slog.Logger
	manager   ports.HealthCheckProvider
	metrics   ports.MetricsProvider
	startTime time.Time
}

type HealthResponse struct {
	Status     string            `json:"status"`
	Timestamp  time.Time         `json:"timestamp"`
	Version    string            `json:"version,omitempty"`
	Uptime     string            `json:"uptime"`
	Components map[string]string `json:"components,omitempty"`
	Error      string            `json:"error,omitempty"`
}

type MetricsResponse struct {
	Timestamp   time.Time           `json:"timestamp"`
	System      SystemMetrics       `json:"system"`
	Application ports.SystemMetrics `json:"application"`
}

type SystemMetrics struct {
	Runtime RuntimeMetrics `json:"runtime"`
	Memory  MemoryMetrics  `json:"memory"`
	Process ProcessMetrics `json:"process"`
}

type RuntimeMetrics struct {
	GoVersion    string `json:"go_version"`
	GOOS         string `json:"goos"`
	GOARCH       string `json:"goarch"`
	NumCPU       int    `json:"num_cpu"`
	NumGoroutine int    `json:"num_goroutine"`
}

type MemoryMetrics struct {
	Alloc         uint64  `json:"alloc_bytes"`
	TotalAlloc    uint64  `json:"total_alloc_bytes"`
	Sys           uint64  `json:"sys_bytes"`
	Lookups       uint64  `json:"lookups"`
	Mallocs       uint64  `json:"mallocs"`
	Frees         uint64  `json:"frees"`
	HeapAlloc     uint64  `json:"heap_alloc_bytes"`
	HeapSys       uint64  `json:"heap_sys_bytes"`
	HeapIdle      uint64  `json:"heap_idle_bytes"`
	HeapInuse     uint64  `json:"heap_inuse_bytes"`
	HeapReleased  uint64  `json:"heap_released_bytes"`
	HeapObjects   uint64  `json:"heap_objects"`
	StackInuse    uint64  `json:"stack_inuse_bytes"`
	StackSys      uint64  `json:"stack_sys_bytes"`
	GCCPUFraction float64 `json:"gc_cpu_fraction"`
	NumGC         uint32  `json:"gc_cycles"`
	PauseTotalNs  uint64  `json:"gc_pause_total_ns"`
}

type ProcessMetrics struct {
	PID    int           `json:"pid"`
	Uptime time.Duration `json:"uptime_ns"`
}

func NewServer(port int, manager ports.HealthCheckProvider, metrics ports.MetricsProvider, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	return &Server{
		port:      port,
		logger:    logger,
		manager:   manager,
		metrics:   metrics,
		startTime: time.Now(),
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/live", s.handleLive)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/metrics/prometheus", s.handlePrometheusMetrics)
	mux.HandleFunc("/debug/vars", s.handleDebugVars)
	mux.HandleFunc("/debug/pprof/", s.handlePprof)
	mux.HandleFunc("/", s.handleRoot)

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      s.withLogging(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	s.logger.Info("starting observability server", "port", s.port)

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("observability server error", "error", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.logger.Info("shutting down observability server")
	return s.server.Shutdown(shutdownCtx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := HealthResponse{
		Status:    "ok",
		Timestamp: time.Now(),
		Uptime:    time.Since(s.startTime).String(),
	}

	if s.manager != nil {
		health := s.manager.GetHealth()
		if !health.Healthy {
			response.Status = "unhealthy"
			response.Error = health.Error
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		if health.Details != nil {
			response.Components = make(map[string]string)
			for k, v := range health.Details {
				response.Components[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.manager != nil {
		health := s.manager.GetHealth()
		if !health.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("not ready"))
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ready"))
}

func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("live"))
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := MetricsResponse{
		Timestamp: time.Now(),
		System:    s.collectSystemMetrics(),
	}

	if s.metrics != nil {
		response.Application = s.metrics.GetMetrics()
	}

	json.NewEncoder(w).Encode(response)
}

func (s *Server) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	systemMetrics := s.collectSystemMetrics()

	fmt.Fprintf(w, "# HELP graft_uptime_seconds Time since the service started\n")
	fmt.Fprintf(w, "# TYPE graft_uptime_seconds gauge\n")
	fmt.Fprintf(w, "graft_uptime_seconds %d\n", int64(time.Since(s.startTime).Seconds()))

	fmt.Fprintf(w, "# HELP graft_go_goroutines Number of goroutines\n")
	fmt.Fprintf(w, "# TYPE graft_go_goroutines gauge\n")
	fmt.Fprintf(w, "graft_go_goroutines %d\n", systemMetrics.Runtime.NumGoroutine)

	fmt.Fprintf(w, "# HELP graft_go_memstats_alloc_bytes Number of bytes allocated\n")
	fmt.Fprintf(w, "# TYPE graft_go_memstats_alloc_bytes gauge\n")
	fmt.Fprintf(w, "graft_go_memstats_alloc_bytes %d\n", systemMetrics.Memory.Alloc)

	fmt.Fprintf(w, "# HELP graft_go_memstats_heap_alloc_bytes Number of heap bytes allocated\n")
	fmt.Fprintf(w, "# TYPE graft_go_memstats_heap_alloc_bytes gauge\n")
	fmt.Fprintf(w, "graft_go_memstats_heap_alloc_bytes %d\n", systemMetrics.Memory.HeapAlloc)

	fmt.Fprintf(w, "# HELP graft_go_memstats_heap_objects Number of allocated objects\n")
	fmt.Fprintf(w, "# TYPE graft_go_memstats_heap_objects gauge\n")
	fmt.Fprintf(w, "graft_go_memstats_heap_objects %d\n", systemMetrics.Memory.HeapObjects)

	fmt.Fprintf(w, "# HELP graft_go_gc_duration_seconds Time spent in garbage collection\n")
	fmt.Fprintf(w, "# TYPE graft_go_gc_duration_seconds gauge\n")
	fmt.Fprintf(w, "graft_go_gc_duration_seconds %f\n", float64(systemMetrics.Memory.PauseTotalNs)/1e9)

	if s.metrics != nil {
		appMetrics := s.metrics.GetMetrics()
		s.writePrometheusApplicationMetrics(w, appMetrics)
	}
}

func (s *Server) writePrometheusApplicationMetrics(w http.ResponseWriter, metrics ports.SystemMetrics) {
	s.writeMetricsSection(w, "engine", metrics.Engine)
	s.writeMetricsSection(w, "cluster", metrics.Cluster)
	s.writeMetricsSection(w, "raft", metrics.Raft)
	s.writeMetricsSection(w, "queue", metrics.Queue)
	s.writeMetricsSection(w, "storage", metrics.Storage)
}

func (s *Server) writeMetricsSection(w http.ResponseWriter, section string, data interface{}) {
	if data == nil {
		return
	}

	switch m := data.(type) {
	case map[string]interface{}:
		for key, value := range m {
			metricName := fmt.Sprintf("graft_%s_%s", section, key)
			if numVal, ok := value.(int64); ok {
				fmt.Fprintf(w, "# HELP %s %s metric\n", metricName, section)
				fmt.Fprintf(w, "# TYPE %s counter\n", metricName)
				fmt.Fprintf(w, "%s %d\n", metricName, numVal)
			} else if floatVal, ok := value.(float64); ok {
				fmt.Fprintf(w, "# HELP %s %s metric\n", metricName, section)
				fmt.Fprintf(w, "# TYPE %s gauge\n", metricName)
				fmt.Fprintf(w, "%s %f\n", metricName, floatVal)
			} else if intVal, ok := value.(int); ok {
				fmt.Fprintf(w, "# HELP %s %s metric\n", metricName, section)
				fmt.Fprintf(w, "# TYPE %s counter\n", metricName)
				fmt.Fprintf(w, "%s %d\n", metricName, intVal)
			}
		}
	default:
		if val := s.extractNumericValue(data); val != nil {
			metricName := fmt.Sprintf("graft_%s", section)
			if intVal, ok := val.(int64); ok {
				fmt.Fprintf(w, "# HELP %s %s metric\n", metricName, section)
				fmt.Fprintf(w, "# TYPE %s counter\n", metricName)
				fmt.Fprintf(w, "%s %d\n", metricName, intVal)
			} else if floatVal, ok := val.(float64); ok {
				fmt.Fprintf(w, "# HELP %s %s metric\n", metricName, section)
				fmt.Fprintf(w, "# TYPE %s gauge\n", metricName)
				fmt.Fprintf(w, "%s %f\n", metricName, floatVal)
			}
		}
	}
}

func (s *Server) extractNumericValue(data interface{}) interface{} {
	switch v := data.(type) {
	case int, int8, int16, int32, int64:
		return v
	case uint, uint8, uint16, uint32, uint64:
		return v
	case float32, float64:
		return v
	default:
		return nil
	}
}

func (s *Server) handleDebugVars(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := map[string]interface{}{
		"uptime":    time.Since(s.startTime).String(),
		"timestamp": time.Now(),
		"system":    s.collectSystemMetrics(),
	}

	if s.metrics != nil {
		vars["application"] = s.metrics.GetMetrics()
	}

	json.NewEncoder(w).Encode(vars)
}

func (s *Server) handlePprof(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("pprof endpoints not implemented yet"))
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	html := `<!DOCTYPE html>
<html>
<head>
    <title>Graft Observability</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .endpoint { margin: 10px 0; }
        .endpoint a { text-decoration: none; color: #007cba; }
        .endpoint a:hover { text-decoration: underline; }
        .description { color: #666; font-size: 14px; margin-left: 20px; }
    </style>
</head>
<body>
    <h1>Graft Observability Endpoints</h1>
    <div class="endpoint">
        <a href="/health">/health</a>
        <div class="description">Overall service health status</div>
    </div>
    <div class="endpoint">
        <a href="/ready">/ready</a>
        <div class="description">Kubernetes readiness probe endpoint</div>
    </div>
    <div class="endpoint">
        <a href="/live">/live</a>
        <div class="description">Kubernetes liveness probe endpoint</div>
    </div>
    <div class="endpoint">
        <a href="/metrics">/metrics</a>
        <div class="description">JSON formatted metrics</div>
    </div>
    <div class="endpoint">
        <a href="/metrics/prometheus">/metrics/prometheus</a>
        <div class="description">Prometheus formatted metrics</div>
    </div>
    <div class="endpoint">
        <a href="/debug/vars">/debug/vars</a>
        <div class="description">Debug variables and detailed system info</div>
    </div>
</body>
</html>`
	w.Write([]byte(html))
}

func (s *Server) collectSystemMetrics() SystemMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return SystemMetrics{
		Runtime: RuntimeMetrics{
			GoVersion:    runtime.Version(),
			GOOS:         runtime.GOOS,
			GOARCH:       runtime.GOARCH,
			NumCPU:       runtime.NumCPU(),
			NumGoroutine: runtime.NumGoroutine(),
		},
		Memory: MemoryMetrics{
			Alloc:         m.Alloc,
			TotalAlloc:    m.TotalAlloc,
			Sys:           m.Sys,
			Lookups:       m.Lookups,
			Mallocs:       m.Mallocs,
			Frees:         m.Frees,
			HeapAlloc:     m.HeapAlloc,
			HeapSys:       m.HeapSys,
			HeapIdle:      m.HeapIdle,
			HeapInuse:     m.HeapInuse,
			HeapReleased:  m.HeapReleased,
			HeapObjects:   m.HeapObjects,
			StackInuse:    m.StackInuse,
			StackSys:      m.StackSys,
			GCCPUFraction: m.GCCPUFraction,
			NumGC:         m.NumGC,
			PauseTotalNs:  m.PauseTotalNs,
		},
		Process: ProcessMetrics{
			PID:    runtime.GOMAXPROCS(0),
			Uptime: time.Since(s.startTime),
		},
	}
}

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		wrapped := &responseWriter{ResponseWriter: w, statusCode: 200}
		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		s.logger.Debug("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.statusCode,
			"duration", duration,
			"remote_addr", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}
