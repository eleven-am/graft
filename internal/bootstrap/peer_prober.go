package bootstrap

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
)

type PeerProber interface {
	ProbePeer(ctx context.Context, addr raft.ServerAddress) (*PeerProbeResult, error)
	ProbeAll(ctx context.Context, peers []VoterInfo) []PeerProbeResult
}

type MockPeerProber struct {
	results map[raft.ServerAddress]*PeerProbeResult
	errors  map[raft.ServerAddress]error
}

func NewMockPeerProber() *MockPeerProber {
	return &MockPeerProber{
		results: make(map[raft.ServerAddress]*PeerProbeResult),
		errors:  make(map[raft.ServerAddress]error),
	}
}

func (m *MockPeerProber) SetResult(addr raft.ServerAddress, result *PeerProbeResult) {
	m.results[addr] = result
}

func (m *MockPeerProber) SetError(addr raft.ServerAddress, err error) {
	m.errors[addr] = err
}

func (m *MockPeerProber) SetReachable(addr raft.ServerAddress, serverID raft.ServerID, clusterUUID string, epoch uint64) {
	m.results[addr] = &PeerProbeResult{
		ServerID:     serverID,
		Address:      addr,
		Reachable:    true,
		ClusterUUID:  clusterUUID,
		FencingEpoch: epoch,
		ProbeTime:    time.Now(),
	}
}

func (m *MockPeerProber) SetUnreachable(addr raft.ServerAddress, serverID raft.ServerID, err error) {
	m.results[addr] = &PeerProbeResult{
		ServerID:  serverID,
		Address:   addr,
		Reachable: false,
		Error:     err,
		ProbeTime: time.Now(),
	}
}

func (m *MockPeerProber) ProbePeer(_ context.Context, addr raft.ServerAddress) (*PeerProbeResult, error) {
	if err, ok := m.errors[addr]; ok {
		return nil, err
	}
	if result, ok := m.results[addr]; ok {
		return result, nil
	}
	return &PeerProbeResult{
		Address:   addr,
		Reachable: false,
		Error:     context.DeadlineExceeded,
		ProbeTime: time.Now(),
	}, nil
}

func (m *MockPeerProber) ProbeAll(ctx context.Context, peers []VoterInfo) []PeerProbeResult {
	results := make([]PeerProbeResult, 0, len(peers))
	for _, peer := range peers {
		result, err := m.ProbePeer(ctx, peer.Address)
		if err != nil {
			results = append(results, PeerProbeResult{
				ServerID:  peer.ServerID,
				Address:   peer.Address,
				Reachable: false,
				Error:     err,
				ProbeTime: time.Now(),
			})
		} else {
			result.ServerID = peer.ServerID
			results = append(results, *result)
		}
	}
	return results
}

func (m *MockPeerProber) Reset() {
	m.results = make(map[raft.ServerAddress]*PeerProbeResult)
	m.errors = make(map[raft.ServerAddress]error)
}
