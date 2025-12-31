package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
)

type StateTransitionCallback func(from, to NodeState, meta *ClusterMeta)

type StateMachineDeps struct {
	MetaStore      MetaStore
	FencingManager *FencingManager
	StaleGuard     *StaleSingleNodeGuard
	UnfenceTracker *UnfenceTracker
	Logger         *slog.Logger
}

type StateMachine struct {
	mu             sync.RWMutex
	metaStore      MetaStore
	fencingManager *FencingManager
	staleGuard     *StaleSingleNodeGuard
	unfenceTracker *UnfenceTracker
	logger         *slog.Logger
	currentMeta    *ClusterMeta
	onTransition   []StateTransitionCallback
}

func NewStateMachine(deps StateMachineDeps) (*StateMachine, error) {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	sm := &StateMachine{
		metaStore:      deps.MetaStore,
		fencingManager: deps.FencingManager,
		staleGuard:     deps.StaleGuard,
		unfenceTracker: deps.UnfenceTracker,
		logger:         logger,
		onTransition:   make([]StateTransitionCallback, 0),
	}

	return sm, nil
}

func (m *StateMachine) Initialize() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	exists, err := m.metaStore.Exists()
	if err != nil {
		return fmt.Errorf("check meta existence: %w", err)
	}

	if exists {
		meta, err := m.metaStore.LoadMeta()
		if err != nil {
			return fmt.Errorf("load existing meta: %w", err)
		}
		m.currentMeta = meta
		m.logger.Info("state machine initialized with existing metadata",
			"state", meta.State,
			"cluster_uuid", meta.ClusterUUID,
		)
		return nil
	}

	m.currentMeta = &ClusterMeta{
		Version: CurrentMetaVersion,
		State:   StateUninitialized,
	}
	m.logger.Info("state machine initialized without existing metadata",
		"state", StateUninitialized,
	)
	return nil
}

func (m *StateMachine) SeedIdentity(serverID raft.ServerID, serverAddress raft.ServerAddress, ordinal int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentMeta == nil {
		return fmt.Errorf("state machine not initialized")
	}

	if m.currentMeta.ServerID != "" {
		m.logger.Debug("identity already seeded, skipping",
			"server_id", m.currentMeta.ServerID)
		return nil
	}

	m.currentMeta.ServerID = serverID
	m.currentMeta.ServerAddress = serverAddress
	m.currentMeta.Ordinal = ordinal

	m.logger.Info("seeded node identity",
		"server_id", serverID,
		"server_address", serverAddress,
		"ordinal", ordinal,
	)

	return nil
}

func (m *StateMachine) CurrentState() NodeState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.currentMeta == nil {
		return StateUninitialized
	}
	return m.currentMeta.State
}

func (m *StateMachine) CurrentMeta() *ClusterMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.currentMeta == nil {
		return nil
	}
	metaCopy := *m.currentMeta
	return &metaCopy
}

func (m *StateMachine) TransitionTo(target NodeState, reason string) error {
	var callbacks []StateTransitionCallback
	var from NodeState
	var metaCopy *ClusterMeta

	m.mu.Lock()

	if m.currentMeta == nil {
		m.mu.Unlock()
		return fmt.Errorf("state machine not initialized")
	}

	from = m.currentMeta.State

	if !from.CanTransitionTo(target) {
		m.mu.Unlock()
		return &StateTransitionError{
			From:    from,
			To:      target,
			Message: reason,
		}
	}

	if from == StateUninitialized && target == StateBootstrapping {
		if m.currentMeta.ClusterUUID == "" {
			m.currentMeta.ClusterUUID = uuid.New().String()
			m.currentMeta.BootstrapTime = time.Now()
			m.logger.Info("generated cluster UUID for bootstrap",
				"cluster_uuid", m.currentMeta.ClusterUUID)
		}
	}

	if from == StateUninitialized && target == StateJoining {
		if m.currentMeta.ClusterUUID == "" {
			m.mu.Unlock()
			return fmt.Errorf("ClusterUUID must be set before transitioning to joining state")
		}
		if m.currentMeta.JoinTime.IsZero() {
			m.currentMeta.JoinTime = time.Now()
		}
	}

	m.currentMeta.State = target
	m.currentMeta.UpdateChecksum()

	if err := m.metaStore.SaveMeta(m.currentMeta); err != nil {
		m.currentMeta.State = from
		m.mu.Unlock()
		return fmt.Errorf("persist state transition: %w", err)
	}

	m.logger.Info("state transition completed",
		"from", from,
		"to", target,
		"reason", reason,
	)

	callbacks = make([]StateTransitionCallback, len(m.onTransition))
	copy(callbacks, m.onTransition)
	metaCopy = m.currentMeta.Clone()

	m.mu.Unlock()

	for _, cb := range callbacks {
		cb(from, target, metaCopy)
	}

	return nil
}

func (m *StateMachine) OnTransition(cb StateTransitionCallback) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onTransition = append(m.onTransition, cb)
}

func (m *StateMachine) HandleStaleNode(ctx context.Context) error {
	if m.staleGuard == nil {
		return nil
	}

	status, details, err := m.staleGuard.Check(ctx)
	if err != nil {
		return fmt.Errorf("stale guard check: %w", err)
	}

	m.logger.Debug("stale node check completed",
		"status", status.String(),
		"reason", details.Reason,
	)

	switch status {
	case StaleStatusSafe:
		return nil

	case StaleStatusSafeToUnfence:
		m.logger.Info("auto-unfencing legitimate survivor",
			"reason", details.Reason,
		)
		if err := m.TransitionTo(StateRecovering, "auto-unfence after sustained isolation"); err != nil {
			return err
		}
		if m.unfenceTracker != nil {
			m.unfenceTracker.Reset()
		}
		return nil

	case StaleStatusQuestionable:
		if m.unfenceTracker != nil {
			m.unfenceTracker.RecordProbe(details.ReachablePeers, details.ExpectedVoters)
			if m.unfenceTracker.CanAutoUnfence() {
				m.logger.Info("auto-unfencing after sufficient probes")
				return m.TransitionTo(StateRecovering, "auto-unfence after accumulated probes")
			}
		}
		return nil

	case StaleStatusStale:
		m.logger.Warn("stale node detected, transitioning to awaiting wipe",
			"reason", details.Reason,
		)
		currentState := m.CurrentState()
		if currentState.CanTransitionTo(StateAwaitingWipe) {
			if err := m.TransitionTo(StateAwaitingWipe, details.Reason); err != nil {
				return err
			}
			if m.unfenceTracker != nil {
				m.unfenceTracker.Reset()
			}
			return nil
		}
		if currentState.CanTransitionTo(StateFenced) {
			if err := m.TransitionTo(StateFenced, details.Reason); err != nil {
				return err
			}
			if err := m.TransitionTo(StateAwaitingWipe, details.Reason); err != nil {
				return err
			}
			if m.unfenceTracker != nil {
				m.unfenceTracker.Reset()
			}
			return nil
		}
		return &StaleNodeError{
			Status:  status.String(),
			Reason:  details.Reason,
			Message: "cannot transition to awaiting wipe from current state",
		}

	case StaleStatusSplitBrain:
		m.logger.Error("split-brain detected, transitioning to fenced",
			"reason", details.Reason,
		)
		currentState := m.CurrentState()
		if currentState.CanTransitionTo(StateFenced) {
			if err := m.TransitionTo(StateFenced, details.Reason); err != nil {
				return err
			}
			if m.unfenceTracker != nil {
				m.unfenceTracker.Reset()
			}
			return nil
		}
		return &StaleNodeError{
			Status:  status.String(),
			Reason:  details.Reason,
			Message: "cannot transition to fenced from current state",
		}

	default:
		return fmt.Errorf("unexpected stale status: %s", status.String())
	}
}

func (m *StateMachine) SetMeta(meta *ClusterMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentMeta = meta
}

func (m *StateMachine) SetClusterUUID(clusterUUID string) error {
	return m.setClusterUUIDInternal(clusterUUID, false)
}

func (m *StateMachine) ForceSetClusterUUID(clusterUUID string) error {
	return m.setClusterUUIDInternal(clusterUUID, true)
}

func (m *StateMachine) setClusterUUIDInternal(clusterUUID string, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentMeta == nil {
		return fmt.Errorf("state machine not initialized")
	}

	if !force && m.currentMeta.ClusterUUID != "" && m.currentMeta.ClusterUUID != clusterUUID {
		return fmt.Errorf("ClusterUUID already set to %s, cannot change to %s",
			m.currentMeta.ClusterUUID, clusterUUID)
	}

	oldUUID := m.currentMeta.ClusterUUID
	m.currentMeta.ClusterUUID = clusterUUID
	m.currentMeta.UpdateChecksum()

	if m.metaStore != nil {
		if err := m.metaStore.SaveMeta(m.currentMeta); err != nil {
			m.currentMeta.ClusterUUID = oldUUID
			m.currentMeta.UpdateChecksum()
			return fmt.Errorf("persist cluster UUID: %w", err)
		}
	}

	if force && oldUUID != "" && oldUUID != clusterUUID {
		m.logger.Warn("force-override cluster UUID",
			"old_uuid", oldUUID,
			"new_uuid", clusterUUID)
	} else {
		m.logger.Info("set cluster UUID", "cluster_uuid", clusterUUID)
	}

	return nil
}

func (m *StateMachine) UpdateFencingInfo(epoch uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentMeta == nil {
		return fmt.Errorf("state machine not initialized")
	}

	m.currentMeta.FencingToken = epoch
	m.currentMeta.FencingEpoch = epoch
	m.currentMeta.UpdateChecksum()

	if err := m.metaStore.SaveMeta(m.currentMeta); err != nil {
		return fmt.Errorf("persist fencing info: %w", err)
	}

	m.logger.Info("updated fencing info",
		"fencing_token", epoch,
		"fencing_epoch", epoch,
	)

	return nil
}
