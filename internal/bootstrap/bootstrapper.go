package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"github.com/eleven-am/graft/internal/ports"
)

const (
	DefaultStateCheckInterval = 1 * time.Second
	DefaultReadyTimeout       = 30 * time.Second
	DefaultShutdownTimeout    = 10 * time.Second
	DefaultStaleCheckInterval = 30 * time.Second
)

type BootstrapperDeps struct {
	Config                 *BootstrapConfig
	MetaStore              MetaStore
	FencingManager         *FencingManager
	StateMachine           *StateMachine
	RecoveryManager        *RecoveryManager
	Seeder                 ports.Seeder
	Transport              BootstrapTransport
	SecretsManager         *SecretsManager
	FallbackElection       *FallbackElection
	ForceBootstrapExecutor *ForceBootstrapExecutor
	Logger                 *slog.Logger

	StateCheckInterval time.Duration
	ReadyTimeout       time.Duration
	ShutdownTimeout    time.Duration
	StaleCheckInterval time.Duration
}

type Bootstrapper struct {
	config                 *BootstrapConfig
	metaStore              MetaStore
	fencingManager         *FencingManager
	stateMachine           *StateMachine
	recoveryManager        *RecoveryManager
	seeder                 ports.Seeder
	transport              BootstrapTransport
	secretsManager         *SecretsManager
	fallbackElection       *FallbackElection
	forceBootstrapExecutor *ForceBootstrapExecutor
	logger                 *slog.Logger

	stateCheckInterval  time.Duration
	readyTimeout        time.Duration
	shutdownTimeout     time.Duration
	staleCheckInterval  time.Duration
	leaderWaitStartTime time.Time
	lastStaleCheck      time.Time
	discoveredPeers     []ports.Peer

	mu       sync.RWMutex
	started  bool
	stopping bool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	readyCh  chan struct{}
	readyErr error
}

func NewBootstrapper(deps BootstrapperDeps) *Bootstrapper {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	stateCheckInterval := deps.StateCheckInterval
	if stateCheckInterval <= 0 {
		stateCheckInterval = DefaultStateCheckInterval
	}

	readyTimeout := deps.ReadyTimeout
	if readyTimeout <= 0 {
		readyTimeout = DefaultReadyTimeout
	}

	shutdownTimeout := deps.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = DefaultShutdownTimeout
	}

	staleCheckInterval := deps.StaleCheckInterval
	if staleCheckInterval <= 0 {
		staleCheckInterval = DefaultStaleCheckInterval
	}

	return &Bootstrapper{
		config:                 deps.Config,
		metaStore:              deps.MetaStore,
		fencingManager:         deps.FencingManager,
		stateMachine:           deps.StateMachine,
		recoveryManager:        deps.RecoveryManager,
		seeder:                 deps.Seeder,
		transport:              deps.Transport,
		secretsManager:         deps.SecretsManager,
		fallbackElection:       deps.FallbackElection,
		forceBootstrapExecutor: deps.ForceBootstrapExecutor,
		logger:                 logger.With("component", "bootstrapper"),
		stateCheckInterval:     stateCheckInterval,
		readyTimeout:           readyTimeout,
		shutdownTimeout:        shutdownTimeout,
		staleCheckInterval:     staleCheckInterval,
		readyCh:                make(chan struct{}),
	}
}

func (b *Bootstrapper) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.started || b.stopping {
		b.mu.Unlock()
		return fmt.Errorf("bootstrapper already started or stopping")
	}
	b.started = true
	b.ctx, b.cancel = context.WithCancel(ctx)
	b.readyCh = make(chan struct{})
	b.readyErr = nil
	loopCtx := b.ctx
	b.wg.Add(1)
	b.mu.Unlock()

	if err := b.initialize(loopCtx); err != nil {
		b.mu.Lock()
		b.started = false
		b.readyErr = err
		close(b.readyCh)
		b.mu.Unlock()
		b.wg.Done()
		return fmt.Errorf("initialize: %w", err)
	}

	go func() {
		defer b.wg.Done()
		b.runStateLoop(loopCtx)
	}()

	b.logger.Info("bootstrapper started")
	return nil
}

func (b *Bootstrapper) Stop() error {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return nil
	}

	if b.stopping {
		b.mu.Unlock()
		return nil
	}

	b.stopping = true
	cancel := b.cancel
	b.cancel = nil
	b.ctx = nil
	b.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	waitDone := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(waitDone)
	}()

	timedOut := false
	select {
	case <-waitDone:
	case <-time.After(b.shutdownTimeout):
		b.logger.Warn("shutdown timeout exceeded, forcing stop")
		timedOut = true
	}

	if timedOut {
		<-waitDone
	}

	b.mu.Lock()
	b.started = false
	b.stopping = false
	b.mu.Unlock()

	if err := b.persistState(); err != nil {
		b.logger.Error("failed to persist state on shutdown",
			slog.Any("error", err))
	}

	b.logger.Info("bootstrapper stopped")
	return nil
}

func (b *Bootstrapper) Ready() <-chan struct{} {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.readyCh
}

func (b *Bootstrapper) ReadyError() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.readyErr
}

func (b *Bootstrapper) IsReady() bool {
	select {
	case <-b.Ready():
		return b.ReadyError() == nil
	default:
		return false
	}
}

func (b *Bootstrapper) CurrentState() NodeState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.currentStateUnsafe()
}

func (b *Bootstrapper) GetServerID() raft.ServerID {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.getOwnServerID()
}

func (b *Bootstrapper) currentStateUnsafe() NodeState {
	if b.stateMachine == nil {
		return StateUninitialized
	}
	return b.stateMachine.CurrentState()
}

func (b *Bootstrapper) GetMeta() *ClusterMeta {
	if b.metaStore == nil {
		return nil
	}
	meta, err := b.metaStore.LoadMeta()
	if err != nil {
		return nil
	}
	return meta
}

func (b *Bootstrapper) initialize(ctx context.Context) error {
	if b.seeder != nil {
		peers, err := b.seeder.Discover(ctx)
		if err != nil {
			b.logger.Warn("initial peer discovery failed", slog.Any("error", err))
		} else {
			b.discoveredPeers = peers
			b.logger.Info("discovered initial peers",
				slog.Int("count", len(peers)),
				slog.String("seeder", b.seeder.Name()))
		}
	}

	if b.secretsManager != nil {
		if err := b.secretsManager.LoadSecrets(); err != nil {
			b.logger.Warn("failed to load secrets, continuing without",
				slog.Any("error", err))
		}
	}

	if b.stateMachine != nil {
		if err := b.stateMachine.Initialize(); err != nil {
			return fmt.Errorf("initialize state machine: %w", err)
		}

		serverID := raft.ServerID(fmt.Sprintf("%s-%d", b.config.ServiceName, b.config.Ordinal))
		serverAddr := b.getOwnAddress()
		if err := b.stateMachine.SeedIdentity(serverID, serverAddr, b.config.Ordinal); err != nil {
			return fmt.Errorf("seed identity: %w", err)
		}
	}

	meta, err := b.metaStore.LoadMeta()
	if err != nil && err != ErrMetaNotFound {
		return fmt.Errorf("load meta: %w", err)
	}

	if meta != nil {
		b.logger.Info("loaded existing cluster metadata",
			slog.String("cluster_uuid", meta.ClusterUUID),
			slog.Uint64("fencing_epoch", meta.FencingEpoch),
			slog.String("state", string(meta.State)))

		if b.recoveryManager != nil && b.needsRecovery(meta) {
			b.logger.Info("initiating recovery from restart")
			if err := b.recoveryManager.RecoverFromRestart(ctx); err != nil {
				if _, ok := err.(*UnrecoverableError); ok {
					return fmt.Errorf("unrecoverable error: %w", err)
				}
				b.logger.Warn("recovery had issues but continuing",
					slog.Any("error", err))
			}
		}

		if b.fencingManager != nil {
			voters := b.fencingManager.Voters()
			if len(voters) > 0 {
				if err := b.fencingManager.Initialize(voters); err != nil {
					b.logger.Warn("fencing manager initialization had issues",
						slog.Any("error", err))
				}
			}
		}
	} else {
		b.logger.Info("no existing cluster metadata, fresh start")
	}

	return nil
}

func (b *Bootstrapper) needsRecovery(meta *ClusterMeta) bool {
	if meta == nil {
		return false
	}

	switch meta.State {
	case StateRecovering, StateJoining:
		return true
	case StateReady:
		return true
	default:
		return false
	}
}

func (b *Bootstrapper) determineInitialState(meta *ClusterMeta) NodeState {
	if meta == nil {
		return StateUninitialized
	}

	switch meta.State {
	case StateReady:
		return StateRecovering
	case StateJoining:
		return StateJoining
	case StateBootstrapping:
		return StateUninitialized
	default:
		return StateUninitialized
	}
}

func (b *Bootstrapper) runStateLoop(ctx context.Context) {
	ticker := time.NewTicker(b.stateCheckInterval)
	defer ticker.Stop()

	readyTimer := time.NewTimer(b.readyTimeout)
	defer readyTimer.Stop()

	readySignaled := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.checkAndHandleState(ctx)

			if !readySignaled && b.isOperationalState() {
				b.signalReady(nil)
				readySignaled = true
			}
		case <-readyTimer.C:
			if !readySignaled {
				b.signalReady(fmt.Errorf("ready timeout exceeded"))
				readySignaled = true
			}
		}
	}
}

func (b *Bootstrapper) isOperationalState() bool {
	state := b.CurrentState()
	return state == StateReady
}

func (b *Bootstrapper) signalReady(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	select {
	case <-b.readyCh:
		return
	default:
	}

	b.readyErr = err
	close(b.readyCh)

	if err != nil {
		b.logger.Warn("bootstrap ready with error", slog.Any("error", err))
	} else {
		b.logger.Info("bootstrap ready", slog.String("state", string(b.currentStateUnsafe())))
	}
}

func (b *Bootstrapper) checkAndHandleState(ctx context.Context) {
	state := b.CurrentState()

	switch state {
	case StateUninitialized:
		b.handleUninitialized(ctx)
	case StateBootstrapping:
		b.handleBootstrapping(ctx)
	case StateRecovering:
		b.handleRecovering(ctx)
	case StateJoining:
		b.handleJoining(ctx)
	case StateReady:
		b.handleReady(ctx)
	case StateDegraded:
		b.handleDegraded(ctx)
	case StateFenced:
		b.handleFenced(ctx)
	case StateAwaitingWipe:
		b.handleAwaitingWipe(ctx)
	}
}

func (b *Bootstrapper) handleUninitialized(ctx context.Context) {
	if b.checkAndExecuteForceBootstrap(ctx) {
		return
	}

	if len(b.discoveredPeers) == 0 && b.seeder != nil {
		peers, err := b.seeder.Discover(ctx)
		if err != nil {
			b.logger.Warn("peer discovery failed", slog.Any("error", err))
			return
		}
		b.discoveredPeers = peers
	}

	existingCluster := b.findExistingCluster(ctx)
	if existingCluster != nil {
		b.logger.Info("existing cluster found, transitioning to joining",
			slog.String("cluster_uuid", existingCluster.ClusterUUID))

		if b.stateMachine != nil {
			if err := b.stateMachine.SetClusterUUID(existingCluster.ClusterUUID); err != nil {
				b.logger.Error("failed to set ClusterUUID before joining", slog.Any("error", err))
				return
			}
		}

		if err := b.transitionState(StateJoining, "existing cluster discovered"); err != nil {
			b.logger.Error("failed to transition to joining", slog.Any("error", err))
		}
		return
	}

	quorum := b.config.CalculateQuorum()
	peersNeeded := quorum - 1

	if len(b.discoveredPeers) < peersNeeded {
		b.logger.Debug("waiting for quorum peers before bootstrap decision",
			slog.Int("discovered", len(b.discoveredPeers)),
			slog.Int("needed", peersNeeded))
		return
	}

	myServerID := b.getOwnServerID()
	if b.isLowestServerID(myServerID) {
		b.logger.Info("initiating bootstrap (lowest ServerID)",
			slog.String("server_id", string(myServerID)),
			slog.Int("discovered_peers", len(b.discoveredPeers)),
			slog.Int("quorum_size", quorum))

		if err := b.transitionState(StateBootstrapping, "lowest ServerID initiating bootstrap"); err != nil {
			b.logger.Error("failed to transition to bootstrapping", slog.Any("error", err))
		}
		return
	}

	if b.leaderWaitStartTime.IsZero() {
		b.leaderWaitStartTime = time.Now()
		b.logger.Info("not lowest ServerID, waiting for leader",
			slog.String("my_id", string(myServerID)),
			slog.Duration("timeout", b.config.LeaderWaitTimeout))
		return
	}

	elapsed := time.Since(b.leaderWaitStartTime)
	if elapsed < b.config.LeaderWaitTimeout {
		b.logger.Debug("waiting for leader",
			slog.Duration("elapsed", elapsed),
			slog.Duration("timeout", b.config.LeaderWaitTimeout))
		return
	}

	if b.fallbackElection != nil {
		b.logger.Warn("leader wait timeout, triggering fallback election")
		b.triggerFallbackElection(ctx)
	} else {
		b.logger.Warn("leader wait timeout, no fallback election - proceeding with bootstrap")
		if err := b.transitionState(StateBootstrapping, "leader wait timeout - insecure mode"); err != nil {
			b.logger.Error("failed to transition to bootstrapping", slog.Any("error", err))
		}
	}
}

func (b *Bootstrapper) checkAndExecuteForceBootstrap(ctx context.Context) bool {
	if b.forceBootstrapExecutor == nil {
		return false
	}

	found, token, err := b.forceBootstrapExecutor.CheckForToken(ctx)
	if err != nil {
		b.logger.Warn("failed to check for force-bootstrap token",
			slog.Any("error", err))
		return false
	}

	if !found || token == nil {
		return false
	}

	b.logger.Info("force-bootstrap token found, executing",
		slog.String("reason", token.Reason),
		slog.Bool("single_use", token.SingleUse),
		slog.Bool("disaster_recovery", token.DisasterRecoveryMode))

	if err := b.forceBootstrapExecutor.Execute(ctx, token); err != nil {
		b.logger.Error("force-bootstrap execution failed",
			slog.Any("error", err))
		return false
	}

	b.logger.Info("force-bootstrap executed successfully")

	if err := b.transitionState(StateBootstrapping, "force-bootstrap executed"); err != nil {
		b.logger.Error("failed to transition after force-bootstrap",
			slog.Any("error", err))
	}

	return true
}

func (b *Bootstrapper) getOwnServerID() raft.ServerID {
	return raft.ServerID(fmt.Sprintf("%s-%d", b.config.ServiceName, b.config.Ordinal))
}

func (b *Bootstrapper) isLowestServerID(myID raft.ServerID) bool {
	for _, peer := range b.discoveredPeers {
		if peer.ID != "" && peer.ID < string(myID) {
			return false
		}
	}
	return true
}

func (b *Bootstrapper) findExistingCluster(ctx context.Context) *ClusterMeta {
	if b.transport == nil {
		return nil
	}

	for _, peer := range b.discoveredPeers {
		addr := fmt.Sprintf("%s:%d", peer.Address, peer.Port)
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		meta, err := b.transport.GetClusterMeta(probeCtx, addr)
		cancel()

		if err != nil {
			continue
		}

		if meta != nil && meta.ClusterUUID != "" && meta.State == StateReady {
			return meta
		}
	}

	return nil
}

func (b *Bootstrapper) triggerFallbackElection(ctx context.Context) {
	if b.fallbackElection == nil {
		b.logger.Error("fallback election not configured")
		return
	}

	result, err := b.fallbackElection.RunElection(ctx)
	if err != nil {
		if errors.Is(err, ErrNotLowestServerID) {
			b.logger.Info("lost fallback election (not lowest ServerID)")
			return
		}
		if errors.Is(err, ErrExistingClusterFound) {
			b.logger.Info("fallback election aborted (existing cluster found)")
			return
		}
		b.logger.Error("fallback election failed", slog.Any("error", err))
		return
	}

	if result != nil {
		b.logger.Info("won fallback election, transitioning to bootstrapping",
			slog.String("server_id", string(result.ServerID)),
			slog.Int("votes", result.VotesFor))
		if err := b.transitionState(StateBootstrapping, "fallback election won"); err != nil {
			b.logger.Error("failed to transition to bootstrapping", slog.Any("error", err))
		}
	}
}

func (b *Bootstrapper) handleBootstrapping(ctx context.Context) {
	if b.fencingManager == nil {
		b.logger.Warn("no fencing manager, skipping fencing check")
		if err := b.transitionState(StateReady, "no fencing manager"); err != nil {
			b.logger.Error("failed to transition to ready", slog.Any("error", err))
		}
		return
	}

	if b.fencingManager.HasToken() {
		b.logger.Info("fencing token already acquired, transitioning to ready")
		if err := b.transitionState(StateReady, "fencing token acquired"); err != nil {
			b.logger.Error("failed to transition to ready", slog.Any("error", err))
		}
		return
	}

	serverID := raft.ServerID(fmt.Sprintf("%s-%d", b.config.ServiceName, b.config.Ordinal))
	serverAddr := b.getOwnAddress()

	token, err := b.fencingManager.AcquireToken(
		ctx,
		FencingModePrimary,
		serverID,
		serverAddr,
		b.transport,
	)

	if err != nil {
		var quorumErr *QuorumNotReachedError
		var epochErr *StaleEpochError

		switch {
		case errors.As(err, &quorumErr):
			b.logger.Warn("quorum not reached for fencing token",
				slog.Int("votes", quorumErr.Votes),
				slog.Int("required", quorumErr.Required))
		case errors.As(err, &epochErr):
			b.logger.Warn("CAS conflict during token acquisition",
				slog.Uint64("proposed", epochErr.ProposedEpoch),
				slog.Uint64("current", epochErr.CurrentEpoch))
		default:
			b.logger.Error("failed to acquire fencing token", slog.Any("error", err))
		}
		return
	}

	b.logger.Info("fencing token acquired",
		slog.Uint64("epoch", token.Epoch),
		slog.String("holder", string(token.HolderID)))

	if b.stateMachine != nil {
		if err := b.stateMachine.UpdateFencingInfo(token.Epoch); err != nil {
			b.logger.Error("failed to update fencing info", slog.Any("error", err))
		}
	}

	if err := b.transitionState(StateReady, "fencing token acquired"); err != nil {
		b.logger.Error("failed to transition to ready", slog.Any("error", err))
	}
}

func (b *Bootstrapper) handleRecovering(ctx context.Context) {
	if b.stateMachine != nil {
		if err := b.stateMachine.HandleStaleNode(ctx); err != nil {
			b.logger.Error("failed to check stale status in recovering state",
				slog.Any("error", err))
			return
		}

		if b.stateMachine.CurrentState() != StateRecovering {
			return
		}
	}

	if b.recoveryManager == nil {
		if err := b.transitionState(StateReady, "no recovery manager"); err != nil {
			b.logger.Error("failed to transition from recovering",
				slog.Any("error", err))
		}
		return
	}

	status := b.recoveryManager.GetRecoveryStatus()
	if status != nil && (status.WALLastIndex > 0 || status.LatestSnapshotIndex > 0) {
		b.logger.Info("recovery complete, transitioning to ready",
			slog.Uint64("wal_last_index", status.WALLastIndex),
			slog.Uint64("snapshot_index", status.LatestSnapshotIndex))
		if err := b.transitionState(StateReady, "recovery complete"); err != nil {
			b.logger.Error("failed to transition to ready",
				slog.Any("error", err))
		}
	}
}

func (b *Bootstrapper) handleJoining(ctx context.Context) {
	b.logger.Debug("in joining state, waiting for join completion")
}

func (b *Bootstrapper) handleReady(ctx context.Context) {
	if b.fencingManager != nil {
		token := b.fencingManager.CurrentToken()
		if token != nil && token.IsExpired() {
			b.logger.Warn("fencing token expired, may need re-election")
		}
	}

	if b.stateMachine != nil && time.Since(b.lastStaleCheck) >= b.staleCheckInterval {
		b.lastStaleCheck = time.Now()
		if err := b.stateMachine.HandleStaleNode(ctx); err != nil {
			b.logger.Error("failed to check stale status in ready state",
				slog.Any("error", err))
		}
	}
}

func (b *Bootstrapper) handleDegraded(ctx context.Context) {
	if b.checkAndExecuteForceBootstrap(ctx) {
		return
	}

	b.logger.Debug("in degraded state, waiting for recovery")

	if b.stateMachine != nil {
		if err := b.stateMachine.HandleStaleNode(ctx); err != nil {
			b.logger.Error("failed to check stale status in degraded state",
				slog.Any("error", err))
			return
		}

		if b.stateMachine.CurrentState() != StateDegraded {
			return
		}
	}

	reachablePeers := b.countReachablePeers(ctx)
	quorum := b.config.CalculateQuorum()
	if reachablePeers+1 >= quorum {
		if err := b.transitionState(StateRecovering, "quorum restored"); err != nil {
			b.logger.Error("failed to transition to recovering",
				slog.Any("error", err))
		}
	}
}

func (b *Bootstrapper) handleFenced(ctx context.Context) {
	if b.checkAndExecuteForceBootstrap(ctx) {
		return
	}

	b.logger.Warn("node is fenced, awaiting operator intervention")

	if b.stateMachine != nil {
		if err := b.stateMachine.HandleStaleNode(ctx); err != nil {
			b.logger.Error("failed to handle fenced node",
				slog.Any("error", err))
		}
	}
}

func (b *Bootstrapper) handleAwaitingWipe(ctx context.Context) {
	b.logger.Warn("node is awaiting wipe, requires operator intervention")
}

func (b *Bootstrapper) transitionState(newState NodeState, reason string) error {
	if b.stateMachine == nil {
		return nil
	}

	oldState := b.stateMachine.CurrentState()
	if err := b.stateMachine.TransitionTo(newState, reason); err != nil {
		return err
	}

	b.logger.Info("state transition",
		slog.String("from", string(oldState)),
		slog.String("to", string(newState)),
		slog.String("reason", reason))

	return nil
}

func (b *Bootstrapper) persistState() error {
	if b.metaStore == nil {
		return nil
	}

	exists, err := b.metaStore.Exists()
	if err != nil {
		return fmt.Errorf("check meta existence: %w", err)
	}

	if !exists {
		return nil
	}

	meta, err := b.metaStore.LoadMeta()
	if err != nil {
		return fmt.Errorf("load existing meta: %w", err)
	}

	meta.State = b.CurrentState()
	meta.UpdateChecksum()

	if b.fencingManager != nil {
		meta.FencingEpoch = b.fencingManager.CurrentEpoch()
	}

	return b.metaStore.SaveMeta(meta)
}

func (b *Bootstrapper) SetVoters(voters []VoterInfo) {
	if b.fencingManager != nil {
		b.fencingManager.SetVoters(voters)
	}
}

func (b *Bootstrapper) IsClusterInitiator() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.isLowestServerID(b.getOwnServerID())
}

func (b *Bootstrapper) GetDiscoveredPeers() []ports.Peer {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]ports.Peer, len(b.discoveredPeers))
	copy(result, b.discoveredPeers)
	return result
}

func (b *Bootstrapper) RegisterTransitionCallback(callback func(from, to NodeState)) {
	if b.stateMachine != nil {
		b.stateMachine.OnTransition(func(from, to NodeState, meta *ClusterMeta) {
			callback(from, to)
		})
	}
}

func (b *Bootstrapper) getOwnAddress() raft.ServerAddress {
	if b.config.HeadlessService != "" && b.config.ServiceName != "" {
		return raft.ServerAddress(fmt.Sprintf("%s-%d.%s:%d",
			b.config.ServiceName,
			b.config.Ordinal,
			b.config.HeadlessService,
			b.config.RaftPort))
	}
	addr := b.config.AdvertiseAddr
	if addr == "" {
		addr = b.config.BindAddr
	}
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		addr = host
	}
	return raft.ServerAddress(fmt.Sprintf("%s:%d", addr, b.config.RaftPort))
}

func (b *Bootstrapper) countReachablePeers(ctx context.Context) int {
	if b.transport == nil {
		return 0
	}

	count := 0
	for _, peer := range b.discoveredPeers {
		addr := fmt.Sprintf("%s:%d", peer.Address, peer.Port)
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := b.transport.GetFencingEpoch(probeCtx, addr)
		cancel()

		if err == nil {
			count++
		}
	}
	return count
}
