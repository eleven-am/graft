package core

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type Orchestrator struct {
	logger         *slog.Logger
	workflowEngine ports.EnginePort
	transport      ports.TransportPort
	discovery      ports.DiscoveryManager
	raft           ports.RaftPort

	mu              sync.RWMutex
	isStarting      bool
	isStarted       bool
	isShuttingDown  bool
	isShutdown      bool
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	shutdownTimeout time.Duration
	startupTimeout  time.Duration
	gracePeriod     time.Duration

	address  string
	nodeID   string
	raftPort int
}

type OrchestratorConfig struct {
	ShutdownTimeout time.Duration
	StartupTimeout  time.Duration
	GracePeriod     time.Duration
}

func NewOrchestrator(
	logger *slog.Logger,
	workflowEngine ports.EnginePort,
	transport ports.TransportPort,
	discovery ports.DiscoveryManager,
	raft ports.RaftPort,
	config OrchestratorConfig,
	address string,
	nodeID string,
	raftPort int,
) *Orchestrator {
	if logger == nil {
		logger = slog.Default()
	}

	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = 30 * time.Second
	}

	if config.StartupTimeout == 0 {
		config.StartupTimeout = 30 * time.Second
	}

	if config.GracePeriod == 0 {
		config.GracePeriod = 2 * time.Second
	}

	return &Orchestrator{
		logger:          logger.With("component", "orchestrator"),
		workflowEngine:  workflowEngine,
		transport:       transport,
		discovery:       discovery,
		raft:            raft,
		shutdownTimeout: config.ShutdownTimeout,
		startupTimeout:  config.StartupTimeout,
		gracePeriod:     config.GracePeriod,
		address:         address,
		nodeID:          nodeID,
		raftPort:        raftPort,
	}
}

func (s *Orchestrator) RegisterContext(ctx context.Context, cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		s.logger.Warn("context already cancelled when registering")
	default:
	}

	s.cancel = cancel
}

func (s *Orchestrator) AddWorker() {
	s.wg.Add(1)
}

func (s *Orchestrator) WorkerDone() {
	s.wg.Done()
}

func (s *Orchestrator) IsShuttingDown() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isShuttingDown
}

func (s *Orchestrator) IsShutdown() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isShutdown
}

func (s *Orchestrator) IsStarting() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isStarting
}

func (s *Orchestrator) IsStarted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isStarted
}

func (s *Orchestrator) IsReady() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isStarted && !s.isShuttingDown && !s.isShutdown
}

type ComponentResult struct {
	Component string
	Error     error
	Duration  time.Duration
}

type ComponentShutdownResult = ComponentResult
type ComponentStartupResult = ComponentResult

func (s *Orchestrator) Startup(ctx context.Context, port int) error {
	address := s.address

	s.mu.Lock()
	if s.isStarting {
		s.mu.Unlock()
		return domain.NewConfigError("startup", domain.ErrAlreadyStarted)
	}

	if s.isStarted {
		s.mu.Unlock()
		return domain.NewConfigError("startup", domain.ErrAlreadyStarted)
	}

	s.isStarting = true
	s.mu.Unlock()

	s.logger.Info("initiating graceful startup",
		"timeout", s.startupTimeout,
		"grace_period", s.gracePeriod,
		"address", address)

	startupCtx, startupCancel := context.WithTimeout(ctx, s.startupTimeout)
	defer startupCancel()

	results := s.executeStartupSequence(startupCtx, port)

	var finalErr error
	for _, result := range results {
		if result.Error != nil {
			s.logger.Error("component startup failed",
				"component", result.Component,
				"error", result.Error,
				"duration", result.Duration)
			finalErr = result.Error
		} else {
			s.logger.Debug("component startup successful",
				"component", result.Component,
				"duration", result.Duration)
		}
	}

	s.mu.Lock()
	s.isStarted = finalErr == nil
	s.isStarting = false
	s.mu.Unlock()

	if finalErr != nil {
		s.logger.Error("graceful startup completed with errors", "error", finalErr)
		return domain.NewResourceError("startup", "graceful_startup", finalErr)
	}

	s.logger.Info("graceful startup completed successfully")
	return nil
}

func (s *Orchestrator) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.isShuttingDown {
		s.mu.Unlock()
		return domain.NewConfigError("shutdown", domain.ErrAlreadyStarted)
	}

	if s.isShutdown {
		s.mu.Unlock()
		return domain.NewConfigError("shutdown", domain.ErrAlreadyShutdown)
	}

	s.isShuttingDown = true
	s.mu.Unlock()

	s.logger.Info("initiating graceful shutdown",
		"timeout", s.shutdownTimeout,
		"grace_period", s.gracePeriod)

	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, s.shutdownTimeout)
	defer shutdownCancel()

	if s.cancel != nil {
		s.cancel()
	}

	workersChan := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(workersChan)
	}()

	select {
	case <-workersChan:
		s.logger.Info("all workers stopped gracefully")
	case <-shutdownCtx.Done():
		s.logger.Warn("timeout waiting for workers to stop")
	}

	results := s.executeShutdownSequence(shutdownCtx)
	var finalErr error
	for _, result := range results {
		if result.Error != nil {
			s.logger.Error("component shutdown failed",
				"component", result.Component,
				"error", result.Error,
				"duration", result.Duration)
			finalErr = result.Error
		} else {
			s.logger.Debug("component shutdown successful",
				"component", result.Component,
				"duration", result.Duration)
		}
	}

	s.mu.Lock()
	s.isShutdown = true
	s.isShuttingDown = false
	s.mu.Unlock()

	if finalErr != nil {
		s.logger.Error("graceful shutdown completed with errors", "error", finalErr)
		return domain.NewResourceError("shutdown", "graceful_shutdown", finalErr)
	}

	s.logger.Info("graceful shutdown completed successfully")
	return nil
}

func (s *Orchestrator) executeStartupSequence(ctx context.Context, port int) []ComponentStartupResult {
	startupComponents := []struct {
		name      string
		startFunc func(context.Context, string, int) error
	}{
		{"transport", s.startupTransport},
		{"discovery", s.startupDiscovery},
		{"raft", func(ctx context.Context, addr string, p int) error { return s.startupRaft(ctx) }},
		{"workflow_engine", func(ctx context.Context, addr string, p int) error { return s.startupWorkflowEngine(ctx) }},
	}

	results := make([]ComponentStartupResult, 0, len(startupComponents))

	for _, component := range startupComponents {
		if ctx.Err() != nil {
			results = append(results, ComponentStartupResult{
				Component: component.name,
				Error:     domain.NewResourceError("startup", "cancelled", ctx.Err()),
				Duration:  0,
			})
			continue
		}

		start := time.Now()
		s.logger.Debug("starting component", "component", component.name)

		err := component.startFunc(ctx, s.address, port)
		duration := time.Since(start)

		results = append(results, ComponentStartupResult{
			Component: component.name,
			Error:     err,
			Duration:  duration,
		})

		if err == nil && s.gracePeriod > 0 {
			select {
			case <-time.After(s.gracePeriod):
			case <-ctx.Done():
				break
			}
		}
	}

	return results
}

func (s *Orchestrator) executeShutdownSequence(ctx context.Context) []ComponentShutdownResult {
	shutdownComponents := []struct {
		name     string
		stopFunc func() error
	}{
		{"workflow_engine", s.shutdownWorkflowEngine},
		{"transport", s.shutdownTransport},
		{"discovery", s.shutdownDiscovery},
		{"raft", s.shutdownRaft},
	}

	results := make([]ComponentShutdownResult, 0, len(shutdownComponents))

	for _, component := range shutdownComponents {
		if ctx.Err() != nil {
			results = append(results, ComponentShutdownResult{
				Component: component.name,
				Error:     domain.NewResourceError("shutdown", "cancelled", ctx.Err()),
				Duration:  0,
			})
			continue
		}

		start := time.Now()
		s.logger.Debug("shutting down component", "component", component.name)

		err := component.stopFunc()
		duration := time.Since(start)

		results = append(results, ComponentShutdownResult{
			Component: component.name,
			Error:     err,
			Duration:  duration,
		})

		if err == nil && s.gracePeriod > 0 {
			select {
			case <-time.After(s.gracePeriod):
			case <-ctx.Done():
				break
			}
		}
	}

	return results
}

func (s *Orchestrator) startupRaft(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("raft start panicked", "panic", r)
			err = domain.NewResourceError("raft", "startup_panic", domain.ErrInternalError)
		}
	}()

	if s.raft == nil {
		return domain.NewConfigError("raft", domain.ErrNotStarted)
	}

	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		var existingPeers []ports.Peer
		if s.discovery != nil {
			existingPeers = s.discovery.GetPeers()
		}

		if len(existingPeers) > 0 {
			for _, peer := range existingPeers {
				nodeInfo := ports.NodeInfo{
					ID:      s.nodeID,
					Address: s.address,
					Port:    s.raftPort,
				}

				err := s.transport.RequestJoinFromPeer(ctx, peer.Address, nodeInfo)
				if err == nil {
					err = s.raft.Start(ctx, existingPeers)
					if err != nil {
						return err
					}

					if s.transport != nil {
						s.transport.SetRaft(s.raft)
					}

					return nil
				}

				s.logger.Warn("join attempt failed", "peer", peer.Address, "attempt", attempt+1, "error", err)
			}
		}

		if attempt < maxRetries-1 {
			time.Sleep(2 * time.Second)
		}
	}

	s.logger.Info("no existing cluster found or join failed, bootstrapping new cluster")
	err = s.raft.Start(ctx, []ports.Peer{})
	if err != nil {
		return err
	}

	if s.transport != nil {
		s.transport.SetRaft(s.raft)
	}

	return nil
}

func (s *Orchestrator) startupDiscovery(ctx context.Context, address string, port int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("discovery start panicked", "panic", r)
			err = domain.NewResourceError("raft", "startup_panic", domain.ErrInternalError)
		}
	}()

	if s.discovery == nil {
		return domain.NewConfigError("discovery", domain.ErrNotStarted)
	}

	return s.discovery.Start(ctx, address, port)
}

func (s *Orchestrator) startupTransport(ctx context.Context, address string, port int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("transport start panicked", "panic", r)
			err = domain.NewResourceError("raft", "startup_panic", domain.ErrInternalError)
		}
	}()

	if s.transport == nil {
		return nil
	}
	return s.transport.Start(ctx, address, port)
}

func (s *Orchestrator) startupWorkflowEngine(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("workflow engine start panicked", "panic", r)
			err = domain.NewResourceError("raft", "startup_panic", domain.ErrInternalError)
		}
	}()

	if s.workflowEngine == nil {
		return nil
	}
	return s.workflowEngine.Start(ctx)
}

func (s *Orchestrator) shutdownWorkflowEngine() (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("workflow engine stop panicked", "panic", r)
			err = domain.NewResourceError("component", "shutdown_panic", domain.ErrInternalError)
		}
	}()

	if s.workflowEngine == nil {
		return nil
	}

	return s.workflowEngine.Stop()
}

func (s *Orchestrator) shutdownTransport() (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("transport stop panicked", "panic", r)
			err = domain.NewResourceError("component", "shutdown_panic", domain.ErrInternalError)
		}
	}()

	if s.transport == nil {
		return nil
	}

	return s.transport.Stop()
}

func (s *Orchestrator) shutdownDiscovery() (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("discovery stop panicked", "panic", r)
			err = domain.NewResourceError("component", "shutdown_panic", domain.ErrInternalError)
		}
	}()

	if s.discovery == nil {
		return nil
	}

	return s.discovery.Stop()
}

func (s *Orchestrator) shutdownRaft() (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("raft stop panicked", "panic", r)
			err = domain.NewResourceError("component", "shutdown_panic", domain.ErrInternalError)
		}
	}()

	if s.raft == nil {
		return nil
	}

	return s.raft.Stop()
}

func (s *Orchestrator) EmergencyShutdown() error {
	s.mu.Lock()
	s.isShuttingDown = true
	s.mu.Unlock()

	s.logger.Warn("initiating emergency shutdown")

	if s.cancel != nil {
		s.cancel()
	}

	var wg sync.WaitGroup
	errors := make(chan error, 4)

	components := map[string]func() error{
		"workflow_engine": s.shutdownWorkflowEngine,
		"transport":       s.shutdownTransport,
		"discovery":       s.shutdownDiscovery,
		"raft":            s.shutdownRaft,
	}

	for name, stopFunc := range components {
		wg.Add(1)
		go func(compName string, fn func() error) {
			defer wg.Done()
			if err := fn(); err != nil {
				s.logger.Error("emergency shutdown component failed",
					"component", compName, "error", err)
				errors <- err
			}
		}(name, stopFunc)
	}

	wg.Wait()
	close(errors)

	var lastErr error
	for err := range errors {
		lastErr = err
	}

	s.mu.Lock()
	s.isShutdown = true
	s.isShuttingDown = false
	s.mu.Unlock()

	if lastErr != nil {
		s.logger.Error("emergency shutdown completed with errors", "error", lastErr)
		return domain.NewResourceError("shutdown", "emergency_shutdown", lastErr)
	}

	s.logger.Warn("emergency shutdown completed")
	return nil
}

func (s *Orchestrator) ForceShutdown(timeout time.Duration) error {
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	done := make(chan error, 1)
	go func() {
		done <- s.EmergencyShutdown()
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		s.logger.Error("force shutdown timed out", "timeout", timeout)
		return domain.NewResourceError("shutdown", "force_shutdown",
			domain.ErrTimeout)
	}
}
