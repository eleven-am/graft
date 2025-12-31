package bootstrap

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hashicorp/raft"
)

func (m *MembershipManager) SafeRemoveVoter(ctx context.Context, id raft.ServerID) error {
	if m.raft == nil {
		return ErrNotLeader
	}

	if !m.isLeader() {
		return ErrNotLeader
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("get configuration failed: %w", err)
	}
	config := configFuture.Configuration()

	serverFound := false
	for _, server := range config.Servers {
		if server.ID == id {
			serverFound = true
			break
		}
	}
	if !serverFound {
		return ErrServerNotFound
	}

	currentVoters := m.countVoters(config)
	postRemovalVoters := currentVoters - 1
	postRemovalQuorum := (postRemovalVoters / 2) + 1

	reachableVoters := m.countReachableVoters(ctx, config, id)

	if reachableVoters < postRemovalQuorum {
		return &QuorumLossError{
			CurrentVoters:     currentVoters,
			PostRemovalVoters: postRemovalVoters,
			ReachableVoters:   reachableVoters,
			RequiredQuorum:    postRemovalQuorum,
		}
	}

	localID := m.localServerID()
	if m.isLeader() && id == localID {
		m.logger.Info("leader removing self, initiating leadership transfer",
			slog.String("server_id", string(id)),
		)
		if err := m.raft.LeadershipTransfer().Error(); err != nil {
			return fmt.Errorf("leadership transfer failed: %w", err)
		}
		return ErrLeadershipTransferred
	}

	if err := m.raft.RemoveServer(id, 0, 0).Error(); err != nil {
		return fmt.Errorf("remove server failed: %w", err)
	}

	m.logger.Info("removed voter",
		slog.String("server_id", string(id)),
		slog.Int("remaining_voters", postRemovalVoters),
	)

	return nil
}

func (m *MembershipManager) RemoveLearner(ctx context.Context, id raft.ServerID) error {
	if m.raft == nil {
		return ErrNotLeader
	}

	if !m.isLeader() {
		return ErrNotLeader
	}

	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("get configuration failed: %w", err)
	}

	for _, server := range configFuture.Configuration().Servers {
		if server.ID == id {
			if server.Suffrage == raft.Voter {
				return fmt.Errorf("server %s is a voter, use SafeRemoveVoter", id)
			}
			break
		}
	}

	if err := m.raft.RemoveServer(id, 0, 0).Error(); err != nil {
		return fmt.Errorf("remove learner failed: %w", err)
	}

	m.mu.Lock()
	delete(m.pendingChanges, id)
	m.mu.Unlock()

	m.logger.Info("removed learner",
		slog.String("server_id", string(id)),
	)

	return nil
}

func (m *MembershipManager) CanSafelyRemove(ctx context.Context, id raft.ServerID) error {
	if m.raft == nil {
		return ErrNotLeader
	}

	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	config := configFuture.Configuration()

	isVoter := false
	for _, server := range config.Servers {
		if server.ID == id {
			isVoter = server.Suffrage == raft.Voter
			break
		}
	}

	if !isVoter {
		return nil
	}

	currentVoters := m.countVoters(config)
	postRemovalVoters := currentVoters - 1
	postRemovalQuorum := (postRemovalVoters / 2) + 1
	reachableVoters := m.countReachableVoters(ctx, config, id)

	if reachableVoters < postRemovalQuorum {
		return &QuorumLossError{
			CurrentVoters:     currentVoters,
			PostRemovalVoters: postRemovalVoters,
			ReachableVoters:   reachableVoters,
			RequiredQuorum:    postRemovalQuorum,
		}
	}

	return nil
}

func (m *MembershipManager) countReachableVoters(ctx context.Context, config raft.Configuration, excludeID raft.ServerID) int {
	reachable := 0
	localID := m.localServerID()

	for _, server := range config.Servers {
		if server.Suffrage != raft.Voter {
			continue
		}
		if server.ID == excludeID {
			continue
		}

		if server.ID == localID {
			reachable++
			continue
		}

		if m.prober == nil {
			reachable++
			continue
		}

		result, err := m.prober.ProbePeer(ctx, server.Address)
		if err == nil && result != nil && result.Reachable {
			reachable++
		}
	}

	return reachable
}

func (m *MembershipManager) DemoteVoter(ctx context.Context, id raft.ServerID) error {
	if m.raft == nil {
		return ErrNotLeader
	}

	if !m.isLeader() {
		return ErrNotLeader
	}

	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("get configuration failed: %w", err)
	}

	var addr raft.ServerAddress
	for _, server := range configFuture.Configuration().Servers {
		if server.ID == id {
			if server.Suffrage != raft.Voter {
				return fmt.Errorf("server %s is not a voter", id)
			}
			addr = server.Address
			break
		}
	}

	if addr == "" {
		return ErrServerNotFound
	}

	if err := m.raft.AddNonvoter(id, addr, 0, 0).Error(); err != nil {
		return fmt.Errorf("demote to learner failed: %w", err)
	}

	m.logger.Info("demoted voter to learner",
		slog.String("server_id", string(id)),
	)

	return nil
}

func (m *MembershipManager) GetClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	if m.raft == nil {
		return nil, ErrNotLeader
	}

	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, err
	}
	config := configFuture.Configuration()

	status := &ClusterStatus{
		Voters:   make([]ServerStatus, 0),
		Learners: make([]ServerStatus, 0),
	}

	localID := m.localServerID()

	for _, server := range config.Servers {
		serverStatus := ServerStatus{
			ID:      server.ID,
			Address: server.Address,
		}

		if server.ID == localID {
			serverStatus.Reachable = true
			serverStatus.IsLocal = true
		} else if m.prober != nil {
			result, err := m.prober.ProbePeer(ctx, server.Address)
			serverStatus.Reachable = err == nil && result != nil && result.Reachable
		} else {
			serverStatus.Reachable = true
		}

		if server.Suffrage == raft.Voter {
			status.Voters = append(status.Voters, serverStatus)
		} else {
			status.Learners = append(status.Learners, serverStatus)
		}
	}

	status.TotalVoters = len(status.Voters)
	reachableCount := 0
	for _, v := range status.Voters {
		if v.Reachable {
			reachableCount++
		}
	}
	status.ReachableVoters = reachableCount
	status.CurrentQuorum = (status.TotalVoters / 2) + 1
	status.HasQuorum = reachableCount >= status.CurrentQuorum

	return status, nil
}

type ClusterStatus struct {
	Voters          []ServerStatus
	Learners        []ServerStatus
	TotalVoters     int
	ReachableVoters int
	CurrentQuorum   int
	HasQuorum       bool
}

type ServerStatus struct {
	ID        raft.ServerID
	Address   raft.ServerAddress
	Reachable bool
	IsLocal   bool
}
