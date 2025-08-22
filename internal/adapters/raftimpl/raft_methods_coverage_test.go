package raftimpl

import (
	"io"
	"log/slog"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRaftNodeUncoveredMethods(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("Stats method", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		expectedStats := map[string]string{
			"state": "Follower",
			"term":  "1",
		}
		mockRaft.On("Stats").Return(expectedStats)

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		stats := node.Stats()
		assert.Equal(t, expectedStats, stats)
		mockRaft.AssertExpectations(t)
	})

	t.Run("LeaderAddr method with leader", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		expectedAddr := raft.ServerAddress("127.0.0.1:8300")
		expectedID := raft.ServerID("leader")
		mockRaft.On("LeaderWithID").Return(expectedAddr, expectedID)

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		addr := node.LeaderAddr()
		assert.Equal(t, "127.0.0.1:8300", addr)
		mockRaft.AssertExpectations(t)
	})

	t.Run("Shutdown method", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockFuture := new(MockGenericFuture)
		
		mockRaft.On("Shutdown").Return(mockFuture)
		mockFuture.On("Error").Return(nil)

		node := &RaftNode{
			raft:   mockRaft,
			logger: logger,
		}

		err := node.Shutdown()
		assert.NoError(t, err)
		
		mockRaft.AssertExpectations(t)
		mockFuture.AssertExpectations(t)
	})
}

type MockGenericFuture struct {
	mock.Mock
}

func (m *MockGenericFuture) Error() error {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(error)
}