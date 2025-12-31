package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

func (m *MembershipManager) AddLearner(
	ctx context.Context,
	id raft.ServerID,
	addr raft.ServerAddress,
	joinReq *JoinRequest,
) error {
	if m.raft == nil {
		return ErrNotLeader
	}

	if !m.isLeader() {
		return ErrNotLeader
	}

	if err := m.rateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limited: %w", err)
	}

	select {
	case m.concurrencySem <- struct{}{}:
		defer func() { <-m.concurrencySem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	if joinReq != nil {
		if err := m.checkAdmissionControl(ctx, joinReq); err != nil {
			return err
		}
	}

	if err := m.checkDuplicateMember(id, addr); err != nil {
		return err
	}

	m.mu.Lock()
	if _, exists := m.pendingChanges[id]; exists {
		m.mu.Unlock()
		return ErrMembershipChangeInProgress
	}
	change := &MembershipChange{
		ServerID:  id,
		Address:   addr,
		Stage:     StageRequested,
		StartTime: time.Now(),
	}
	m.pendingChanges[id] = change
	m.mu.Unlock()

	change.Stage = StageLearner
	if err := m.raft.AddNonvoter(id, addr, 0, 0).Error(); err != nil {
		m.mu.Lock()
		delete(m.pendingChanges, id)
		m.mu.Unlock()
		change.Stage = StageFailed
		change.Error = err
		return fmt.Errorf("add learner failed: %w", err)
	}

	m.logger.Info("added learner",
		slog.String("server_id", string(id)),
		slog.String("address", string(addr)),
	)

	return nil
}

func (m *MembershipManager) PromoteLearnerToVoter(ctx context.Context, id raft.ServerID) error {
	if m.raft == nil {
		return ErrNotLeader
	}

	if !m.isLeader() {
		return ErrNotLeader
	}

	m.mu.Lock()
	change, exists := m.pendingChanges[id]
	if !exists {
		addr, err := m.getServerAddress(id)
		if err != nil {
			m.mu.Unlock()
			return err
		}
		change = &MembershipChange{
			ServerID:  id,
			Address:   addr,
			Stage:     StageLearner,
			StartTime: time.Now(),
		}
		m.pendingChanges[id] = change
	}
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.pendingChanges, id)
		m.mu.Unlock()
	}()

	change.Stage = StageCatchingUp
	if err := m.waitForCatchup(ctx, id, change); err != nil {
		_ = m.raft.RemoveServer(id, 0, 0).Error()
		change.Stage = StageFailed
		change.Error = err
		return fmt.Errorf("catchup failed: %w", err)
	}

	change.Stage = StagePromoting
	if err := m.raft.AddVoter(id, change.Address, 0, 0).Error(); err != nil {
		_ = m.raft.RemoveServer(id, 0, 0).Error()
		change.Stage = StageFailed
		change.Error = err
		return fmt.Errorf("promote failed: %w", err)
	}

	change.Stage = StageVoter
	m.logger.Info("promoted learner to voter",
		slog.String("server_id", string(id)),
	)

	return nil
}

func (m *MembershipManager) AddVoter(
	ctx context.Context,
	id raft.ServerID,
	addr raft.ServerAddress,
	joinReq *JoinRequest,
) error {
	if err := m.AddLearner(ctx, id, addr, joinReq); err != nil {
		return err
	}

	return m.PromoteLearnerToVoter(ctx, id)
}

func (m *MembershipManager) waitForCatchup(ctx context.Context, id raft.ServerID, change *MembershipChange) error {
	timeout := m.config.SnapshotCatchupTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}

	timeoutCh := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	snapshotComplete := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeoutCh:
			return ErrCatchupTimeout
		case <-ticker.C:
			leaderIdx := m.raft.AppliedIndex()
			followerState := m.getFollowerState(id)

			if followerState == nil {
				continue
			}

			if m.config.SnapshotCompleteGate && !snapshotComplete {
				if followerState.SnapshotInProgress {
					m.logger.Debug("waiting for snapshot transfer",
						slog.String("server_id", string(id)),
					)
					continue
				}
				if followerState.LastSnapshotIndex > 0 {
					snapshotComplete = true
					m.logger.Info("snapshot transfer complete",
						slog.String("server_id", string(id)),
						slog.Uint64("snapshot_index", followerState.LastSnapshotIndex),
					)
				}
			}

			logGap := leaderIdx - followerState.MatchIndex
			if logGap <= m.config.LearnerCatchupThreshold {
				if m.config.SnapshotCompleteGate && !snapshotComplete && followerState.LastSnapshotIndex == 0 {
					continue
				}
				m.logger.Info("learner caught up",
					slog.String("server_id", string(id)),
					slog.Uint64("leader_index", leaderIdx),
					slog.Uint64("match_index", followerState.MatchIndex),
					slog.Uint64("gap", logGap),
				)
				return nil
			}

			change.LastIndex = followerState.MatchIndex
			m.logger.Debug("learner catching up",
				slog.String("server_id", string(id)),
				slog.Uint64("leader_index", leaderIdx),
				slog.Uint64("match_index", followerState.MatchIndex),
				slog.Uint64("gap", logGap),
				slog.Uint64("threshold", m.config.LearnerCatchupThreshold),
			)
		}
	}
}

func (m *MembershipManager) getFollowerState(id raft.ServerID) *FollowerState {
	stats := m.raft.Stats()

	matchIndexKey := fmt.Sprintf("last_contact.%s.match_index", string(id))
	snapshotIndexKey := fmt.Sprintf("last_contact.%s.snapshot_index", string(id))

	matchIndexStr, ok := stats[matchIndexKey]
	if !ok {
		matchIndexStr = stats["last_log_index"]
		if matchIndexStr == "" {
			return nil
		}
	}

	matchIndex, err := strconv.ParseUint(matchIndexStr, 10, 64)
	if err != nil {
		for k, v := range stats {
			if strings.Contains(k, string(id)) && strings.Contains(k, "match") {
				if idx, e := strconv.ParseUint(v, 10, 64); e == nil {
					matchIndex = idx
					break
				}
			}
		}
		if matchIndex == 0 {
			return nil
		}
	}

	var lastSnapshotIndex uint64
	if snapshotStr, ok := stats[snapshotIndexKey]; ok {
		lastSnapshotIndex, _ = strconv.ParseUint(snapshotStr, 10, 64)
	}

	return &FollowerState{
		MatchIndex:         matchIndex,
		LastSnapshotIndex:  lastSnapshotIndex,
		SnapshotInProgress: false,
	}
}

func (m *MembershipManager) getServerAddress(id raft.ServerID) (raft.ServerAddress, error) {
	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return "", err
	}

	for _, server := range configFuture.Configuration().Servers {
		if server.ID == id {
			return server.Address, nil
		}
	}

	return "", ErrServerNotFound
}

func (m *MembershipManager) checkAdmissionControl(ctx context.Context, joinReq *JoinRequest) error {
	if m.config.MaxLogSizeForJoin > 0 {
		logSize := m.getLogSize()
		if logSize > m.config.MaxLogSizeForJoin {
			return &AdmissionControlError{
				Reason:    "log size too large",
				LogSize:   logSize,
				Threshold: float64(m.config.MaxLogSizeForJoin),
			}
		}
	}

	if m.config.DiskPressureThreshold > 0 {
		diskUsage := m.getDiskUsage()
		if diskUsage > m.config.DiskPressureThreshold {
			return &AdmissionControlError{
				Reason:    "disk pressure",
				DiskUsage: diskUsage,
				Threshold: m.config.DiskPressureThreshold,
			}
		}
	}

	if err := m.checkVersionCompatibility(joinReq); err != nil {
		return err
	}

	return nil
}

func (m *MembershipManager) getLogSize() uint64 {
	stats := m.raft.Stats()
	if sizeStr, ok := stats["log_size"]; ok {
		if size, err := strconv.ParseUint(sizeStr, 10, 64); err == nil {
			return size
		}
	}

	if lastIdxStr, ok := stats["last_log_index"]; ok {
		if firstIdxStr, ok := stats["first_log_index"]; ok {
			lastIdx, _ := strconv.ParseUint(lastIdxStr, 10, 64)
			firstIdx, _ := strconv.ParseUint(firstIdxStr, 10, 64)
			return lastIdx - firstIdx
		}
	}

	return 0
}

func (m *MembershipManager) getDiskUsage() float64 {
	if m.config.DataDir == "" {
		return 0.0
	}

	return getDiskUsageForPath(m.config.DataDir)
}

func (m *MembershipManager) checkVersionCompatibility(joinReq *JoinRequest) error {
	if joinReq == nil {
		return nil
	}

	compat := m.config.VersionCompatibility

	if compat.MinRaftProtocolVersion > 0 && joinReq.RaftVersion < compat.MinRaftProtocolVersion {
		return &VersionCompatibilityError{
			Field:       "raft_protocol_version",
			RemoteValue: joinReq.RaftVersion,
			MinRequired: compat.MinRaftProtocolVersion,
			MaxAllowed:  compat.MaxRaftProtocolVersion,
		}
	}

	if compat.MaxRaftProtocolVersion > 0 && joinReq.RaftVersion > compat.MaxRaftProtocolVersion {
		return &VersionCompatibilityError{
			Field:       "raft_protocol_version",
			RemoteValue: joinReq.RaftVersion,
			MinRequired: compat.MinRaftProtocolVersion,
			MaxAllowed:  compat.MaxRaftProtocolVersion,
		}
	}

	if compat.MinStateMachineVersion > 0 && joinReq.StateMachineVer < compat.MinStateMachineVersion {
		return &VersionCompatibilityError{
			Field:       "state_machine_version",
			RemoteValue: joinReq.StateMachineVer,
			MinRequired: compat.MinStateMachineVersion,
			MaxAllowed:  compat.MaxStateMachineVersion,
		}
	}

	if compat.MaxStateMachineVersion > 0 && joinReq.StateMachineVer > compat.MaxStateMachineVersion {
		return &VersionCompatibilityError{
			Field:       "state_machine_version",
			RemoteValue: joinReq.StateMachineVer,
			MinRequired: compat.MinStateMachineVersion,
			MaxAllowed:  compat.MaxStateMachineVersion,
		}
	}

	if len(compat.RequiredFeatureFlags) > 0 && joinReq.FeatureFlags != nil {
		remoteFlags := make(map[string]bool)
		for _, f := range joinReq.FeatureFlags {
			remoteFlags[f] = true
		}

		for _, required := range compat.RequiredFeatureFlags {
			if !remoteFlags[required] {
				return &AdmissionControlError{
					Reason: fmt.Sprintf("missing required feature flag: %s", required),
				}
			}
		}
	}

	if len(compat.BlockedVersionPatterns) > 0 {
		versionStr := fmt.Sprintf("%d.%d", joinReq.RaftVersion, joinReq.StateMachineVer)
		for _, pattern := range compat.BlockedVersionPatterns {
			matched, err := regexp.MatchString(pattern, versionStr)
			if err != nil {
				m.logger.Warn("invalid blocked version pattern",
					slog.String("pattern", pattern),
					slog.Any("error", err))
				continue
			}
			if matched {
				return &VersionCompatibilityError{
					Field:       "blocked_pattern",
					RemoteValue: joinReq.RaftVersion,
					Message:     fmt.Sprintf("version %s matches blocked pattern %s", versionStr, pattern),
				}
			}
		}
	}

	if !compat.AllowDowngrade {
		localRaftVersion := m.getRaftProtocolVersion()
		localSMVersion := m.getStateMachineVersion()

		if joinReq.RaftVersion < localRaftVersion {
			return &VersionCompatibilityError{
				Field:       "raft_protocol_version",
				LocalValue:  localRaftVersion,
				RemoteValue: joinReq.RaftVersion,
				Message:     "downgrade not allowed",
			}
		}
		if joinReq.StateMachineVer < localSMVersion {
			return &VersionCompatibilityError{
				Field:       "state_machine_version",
				LocalValue:  localSMVersion,
				RemoteValue: joinReq.StateMachineVer,
				Message:     "downgrade not allowed",
			}
		}
	}

	return nil
}

func (m *MembershipManager) checkDuplicateMember(id raft.ServerID, addr raft.ServerAddress) error {
	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("get configuration failed: %w", err)
	}

	for _, server := range configFuture.Configuration().Servers {
		if server.ID == id {
			return ErrDuplicateServerID
		}
		if server.Address == addr {
			return ErrDuplicateServerAddress
		}
	}

	return nil
}

func (m *MembershipManager) getRaftProtocolVersion() uint32 {
	stats := m.raft.Stats()
	if versionStr, ok := stats["protocol_version"]; ok {
		if version, err := strconv.ParseUint(versionStr, 10, 32); err == nil {
			return uint32(version)
		}
	}
	return 1
}

func (m *MembershipManager) getStateMachineVersion() uint32 {
	stats := m.raft.Stats()
	if versionStr, ok := stats["state_machine_version"]; ok {
		if version, err := strconv.ParseUint(versionStr, 10, 32); err == nil {
			return uint32(version)
		}
	}
	return m.config.VersionCompatibility.MinStateMachineVersion
}
