package raftimpl

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockAdvancedTransportPort struct {
	mock.Mock
}

func (m *MockAdvancedTransportPort) Start(ctx context.Context, config ports.TransportConfig) error {
	args := m.Called(ctx, config)
	return args.Error(0)
}

func (m *MockAdvancedTransportPort) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockAdvancedTransportPort) JoinCluster(ctx context.Context, req ports.JoinRequest) (*ports.JoinResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*ports.JoinResponse), args.Error(1)
}

func (m *MockAdvancedTransportPort) LeaveCluster(ctx context.Context, req ports.LeaveRequest) (*ports.LeaveResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*ports.LeaveResponse), args.Error(1)
}

func (m *MockAdvancedTransportPort) GetLeader(ctx context.Context) (*ports.LeaderInfo, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ports.LeaderInfo), args.Error(1)
}

func (m *MockAdvancedTransportPort) ForwardToLeader(ctx context.Context, req ports.ForwardRequest) (*ports.ForwardResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*ports.ForwardResponse), args.Error(1)
}

func (m *MockAdvancedTransportPort) ForwardToNode(ctx context.Context, nodeID string, req ports.ForwardRequest) (*ports.ForwardResponse, error) {
	args := m.Called(ctx, nodeID, req)
	return args.Get(0).(*ports.ForwardResponse), args.Error(1)
}

func TestNewStorageAdapterWithConfig(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-config-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockRaft := new(MockRaftForAdapter)
	mockRaft.On("State").Return(raft.Leader)
	node := &RaftNode{raft: mockRaft, logger: logger}

	t.Run("valid config", func(t *testing.T) {
		config := DefaultStorageConfig()
		config.DefaultConsistencyLevel = ConsistencyQuorum
		
		adapter := NewStorageAdapterWithConfig(node, db, nil, logger, config)
		
		assert.NotNil(t, adapter)
		assert.Equal(t, ConsistencyQuorum, adapter.config.DefaultConsistencyLevel)
	})

	t.Run("invalid config falls back to default", func(t *testing.T) {
		config := StorageConfig{
		}
		
		adapter := NewStorageAdapterWithConfig(node, db, nil, logger, config)
		
		assert.NotNil(t, adapter)
		assert.Equal(t, ConsistencyLeader, adapter.config.DefaultConsistencyLevel)
	})
}

func TestGetWithConsistency(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "graft-consistency-test-"+time.Now().Format("20060102150405"))
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("test-key"), []byte("test-value"))
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("eventual consistency", func(t *testing.T) {
		node := &RaftNode{raft: nil, logger: logger}
		adapter := NewStorageAdapter(node, db, nil, logger)

		value, err := adapter.GetWithConsistency(context.Background(), "test-key", ConsistencyEventual)
		
		assert.NoError(t, err)
		assert.Equal(t, []byte("test-value"), value)
	})

	t.Run("leader consistency", func(t *testing.T) {
		mockRaft := new(MockRaftForAdapter)
		mockRaft.On("State").Return(raft.Leader)
		node := &RaftNode{raft: mockRaft, logger: logger}
		adapter := NewStorageAdapter(node, db, nil, logger)

		value, err := adapter.GetWithConsistency(context.Background(), "test-key", ConsistencyLeader)
		
		assert.NoError(t, err)
		assert.Equal(t, []byte("test-value"), value)
		mockRaft.AssertExpectations(t)
	})
}

func TestConfigurationMethods(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	adapter := NewStorageAdapter(nil, nil, nil, logger)

	t.Run("SetQuorumConfig", func(t *testing.T) {
		newConfig := QuorumReadConfig{
			MinReplicas:      3,
			ReadTimeout:      5 * time.Second,
			ConsistencyLevel: ConsistencyQuorum,
			RetryAttempts:    5,
		}

		adapter.SetQuorumConfig(newConfig)
		
		assert.Equal(t, 3, adapter.config.QuorumConfig.MinReplicas)
		assert.Equal(t, 5*time.Second, adapter.config.QuorumConfig.ReadTimeout)
		assert.Equal(t, 5, adapter.config.QuorumConfig.RetryAttempts)
	})

	t.Run("SetForwardingEnabled", func(t *testing.T) {
		adapter.SetForwardingEnabled(false)
		assert.False(t, adapter.config.ForwardingEnabled)

		adapter.SetForwardingEnabled(true)
		assert.True(t, adapter.config.ForwardingEnabled)
	})

	t.Run("SetCacheSize", func(t *testing.T) {
		adapter.SetCacheSize(2048 * 1024)
		assert.Equal(t, int64(2048*1024), adapter.config.CacheSize)
	})

	t.Run("UpdateConfig valid", func(t *testing.T) {
		newConfig := DefaultStorageConfig()
		newConfig.DefaultConsistencyLevel = ConsistencyEventual
		newConfig.CacheSize = 512 * 1024

		err := adapter.UpdateConfig(newConfig)
		
		assert.NoError(t, err)
		assert.Equal(t, ConsistencyEventual, adapter.config.DefaultConsistencyLevel)
		assert.Equal(t, int64(512*1024), adapter.config.CacheSize)
	})

	t.Run("UpdateConfig invalid", func(t *testing.T) {
		invalidConfig := StorageConfig{
		}

		err := adapter.UpdateConfig(invalidConfig)
		assert.Error(t, err)
	})

	t.Run("GetConfig", func(t *testing.T) {
		config := adapter.GetConfig()
		assert.Equal(t, adapter.config, config)
	})
}

func TestLeaderForwardingIntegration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("ForwardReadToLeader success", func(t *testing.T) {
		mockTransport := &MockAdvancedTransportPort{}
		mockTransport.On("GetLeader", mock.Anything).Return(&ports.LeaderInfo{
			NodeID:  "leader1",
			Address: "127.0.0.1:8080",
		}, nil)
		mockTransport.On("ForwardToLeader", mock.Anything, mock.Anything).Return(&ports.ForwardResponse{
			Success: true,
			Result:  []byte("forwarded-value"),
		}, nil)

		forwarder := NewReadForwarder(mockTransport, logger, DefaultStorageConfig())
		
		value, err := forwarder.ForwardReadToLeader(context.Background(), "test-key")
		
		assert.NoError(t, err)
		assert.Equal(t, []byte("forwarded-value"), value)
		mockTransport.AssertExpectations(t)
	})

	t.Run("ForwardReadToLeader transport error", func(t *testing.T) {
		mockTransport := &MockAdvancedTransportPort{}
		mockTransport.On("GetLeader", mock.Anything).Return(nil, errors.New("transport error"))

		forwarder := NewReadForwarder(mockTransport, logger, DefaultStorageConfig())
		
		value, err := forwarder.ForwardReadToLeader(context.Background(), "test-key")
		
		assert.Error(t, err)
		assert.Nil(t, value)
		assert.Contains(t, err.Error(), "failed to get current leader for read forwarding")
		mockTransport.AssertExpectations(t)
	})

	t.Run("ForwardReadToLeader no leader", func(t *testing.T) {
		mockTransport := &MockAdvancedTransportPort{}
		mockTransport.On("GetLeader", mock.Anything).Return(nil, nil)

		forwarder := NewReadForwarder(mockTransport, logger, DefaultStorageConfig())
		
		value, err := forwarder.ForwardReadToLeader(context.Background(), "test-key")
		
		assert.Error(t, err)
		assert.Nil(t, value)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeUnavailable, domainErr.Type)
		mockTransport.AssertExpectations(t)
	})

	t.Run("ForwardReadToLeaderWithRetry success after retry", func(t *testing.T) {
		mockTransport := &MockAdvancedTransportPort{}
		mockTransport.On("GetLeader", mock.Anything).Return(nil, errors.New("first failure")).Once()
		mockTransport.On("GetLeader", mock.Anything).Return(&ports.LeaderInfo{
			NodeID:  "leader1",
			Address: "127.0.0.1:8080",
		}, nil).Once()
		mockTransport.On("ForwardToLeader", mock.Anything, mock.Anything).Return(&ports.ForwardResponse{
			Success: true,
			Result:  []byte("retry-success"),
		}, nil)

		forwarder := NewReadForwarder(mockTransport, logger, DefaultStorageConfig())
		
		value, err := forwarder.ForwardReadToLeaderWithRetry(context.Background(), "test-key")
		
		assert.NoError(t, err)
		assert.Equal(t, []byte("retry-success"), value)
		mockTransport.AssertExpectations(t)
	})
}

func TestQuorumReadIntegration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("PerformQuorumRead success", func(t *testing.T) {
		mockTransport := &MockAdvancedTransportPort{}
		mockTransport.On("ForwardToNode", mock.Anything, mock.Anything, mock.Anything).Return(&ports.ForwardResponse{
			Success: true,
			Result:  []byte("quorum-value"),
		}, nil)

		reader := NewQuorumReader(mockTransport, logger)
		config := QuorumReadConfig{
			MinReplicas:   2,
			ReadTimeout:   3 * time.Second,
			RetryAttempts: 1,
		}
		nodeList := []string{"node1", "node2", "node3"}

		value, err := reader.PerformQuorumRead(context.Background(), "test-key", config, nodeList)
		
		assert.NoError(t, err)
		assert.Equal(t, []byte("quorum-value"), value)
	})

	t.Run("PerformQuorumRead insufficient nodes", func(t *testing.T) {
		mockTransport := &MockAdvancedTransportPort{}
		reader := NewQuorumReader(mockTransport, logger)
		config := QuorumReadConfig{
			MinReplicas:   3,
			ReadTimeout:   3 * time.Second,
			RetryAttempts: 1,
		}
		nodeList := []string{"node1"}

		value, err := reader.PerformQuorumRead(context.Background(), "test-key", config, nodeList)
		
		assert.Error(t, err)
		assert.Nil(t, value)
		domainErr, ok := err.(domain.Error)
		assert.True(t, ok)
		assert.Equal(t, domain.ErrorTypeUnavailable, domainErr.Type)
		assert.Contains(t, err.Error(), "insufficient nodes available for quorum read")
	})

	t.Run("PerformQuorumReadWithRetry", func(t *testing.T) {
		mockTransport := &MockAdvancedTransportPort{}
		mockTransport.On("ForwardToNode", mock.Anything, mock.Anything, mock.Anything).Return(&ports.ForwardResponse{
			Success: false,
			Error:   "first failure",
		}, nil).Once()
		mockTransport.On("ForwardToNode", mock.Anything, mock.Anything, mock.Anything).Return(&ports.ForwardResponse{
			Success: true,
			Result:  []byte("retry-quorum-value"),
		}, nil)

		reader := NewQuorumReader(mockTransport, logger)
		config := QuorumReadConfig{
			MinReplicas:   2,
			ReadTimeout:   1 * time.Second,
			RetryAttempts: 2,
		}
		nodeList := []string{"node1", "node2", "node3"}

		value, err := reader.PerformQuorumReadWithRetry(context.Background(), "test-key", config, nodeList)
		
		assert.NoError(t, err)
		assert.Equal(t, []byte("retry-quorum-value"), value)
		mockTransport.AssertExpectations(t)
	})
}

func TestConfigValidation(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		config := DefaultStorageConfig()
		err := config.Validate()
		assert.NoError(t, err)
	})

	t.Run("invalid quorum min replicas", func(t *testing.T) {
		config := DefaultStorageConfig()
		config.QuorumConfig.MinReplicas = 0
		
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "quorum_min_replicas")
	})

	t.Run("invalid timeout", func(t *testing.T) {
		config := DefaultStorageConfig()
		config.ForwardingTimeout = 0
		
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "forwarding_timeout")
	})

	t.Run("negative cache size", func(t *testing.T) {
		config := DefaultStorageConfig()
		config.CacheSize = -1
		
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cache_size")
	})

	t.Run("negative max retries", func(t *testing.T) {
		config := DefaultStorageConfig()
		config.MaxRetries = -1
		
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max_retries")
	})
}

func TestLeaderCaching(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockTransport := &MockAdvancedTransportPort{}

	t.Run("leader cache validation", func(t *testing.T) {
		forwarder := NewReadForwarder(mockTransport, logger, DefaultStorageConfig())
		
		cached := forwarder.GetCachedLeaderInfo()
		assert.Nil(t, cached)
		
		leaderInfo := &ports.LeaderInfo{
			NodeID:  "leader1",
			Address: "127.0.0.1:8080",
		}
		mockTransport.On("GetLeader", mock.Anything).Return(leaderInfo, nil).Once()

		leader, err := forwarder.getCurrentLeader(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, leaderInfo, leader)
		
		cached = forwarder.GetCachedLeaderInfo()
		assert.NotNil(t, cached)
		assert.Equal(t, "leader1", cached.NodeID)

		mockTransport.AssertExpectations(t)
	})

	t.Run("cache TTL and clearing", func(t *testing.T) {
		forwarder := NewReadForwarder(mockTransport, logger, DefaultStorageConfig())
		
		forwarder.SetLeaderCacheTTL(1 * time.Millisecond)
		
		leaderInfo := &ports.LeaderInfo{
			NodeID:  "leader1", 
			Address: "127.0.0.1:8080",
		}
		mockTransport.On("GetLeader", mock.Anything).Return(leaderInfo, nil).Once()

		_, err := forwarder.getCurrentLeader(context.Background())
		assert.NoError(t, err)
		
		time.Sleep(2 * time.Millisecond)
		
		cached := forwarder.GetCachedLeaderInfo()
		assert.Nil(t, cached)

		mockTransport.AssertExpectations(t)
	})

	t.Run("handle leader change", func(t *testing.T) {
		forwarder := NewReadForwarder(mockTransport, logger, DefaultStorageConfig())
		
		leaderInfo := &ports.LeaderInfo{
			NodeID:  "leader1",
			Address: "127.0.0.1:8080",
		}
		mockTransport.On("GetLeader", mock.Anything).Return(leaderInfo, nil).Once()
		
		_, err := forwarder.getCurrentLeader(context.Background())
		assert.NoError(t, err)
		
		cached := forwarder.GetCachedLeaderInfo()
		assert.NotNil(t, cached)
		
		forwarder.HandleLeaderChange("leader2")
		
		cached = forwarder.GetCachedLeaderInfo()
		assert.Nil(t, cached)

		mockTransport.AssertExpectations(t)
	})
}

func TestRetryConfiguration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("SetRetryConfig", func(t *testing.T) {
		mockTransport := &MockAdvancedTransportPort{}
		forwarder := NewReadForwarder(mockTransport, logger, DefaultStorageConfig())
		
		forwarder.SetRetryConfig(2, 10*time.Millisecond)
		
		mockTransport.On("GetLeader", mock.Anything).Return(nil, errors.New("failure")).Times(2)

		_, err := forwarder.ForwardReadToLeaderWithRetry(context.Background(), "test-key")
		assert.Error(t, err)
		
		mockTransport.AssertExpectations(t)
	})
}

func TestDefaultConfigurationMethods(t *testing.T) {
	t.Run("DefaultPerformanceConfig", func(t *testing.T) {
		config := DefaultPerformanceConfig()
		assert.True(t, config.ReadCacheEnabled)
		assert.Equal(t, int64(10*1024*1024), config.ReadCacheSize)
		assert.Equal(t, 1000, config.WriteBatchSize)
	})

	t.Run("DefaultConsistencyConfig", func(t *testing.T) {
		config := DefaultConsistencyConfig()
		assert.Equal(t, 5*time.Second, config.LeaderReadTimeout)
		assert.Equal(t, 2, config.QuorumMinReplicas)
	})

	t.Run("DefaultMonitoringConfig", func(t *testing.T) {
		config := DefaultMonitoringConfig()
		assert.True(t, config.MetricsEnabled)
		assert.Equal(t, 30*time.Second, config.MetricsInterval)
		assert.Equal(t, "info", config.LogLevel)
	})
}