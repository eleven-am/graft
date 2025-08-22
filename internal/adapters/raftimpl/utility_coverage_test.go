package raftimpl

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtilityCoverage(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	
	t.Run("raft logger write", func(t *testing.T) {
		raftLogger := &raftLogger{logger: logger}
		
		n, err := raftLogger.Write([]byte("test message"))
		assert.NoError(t, err)
		assert.Equal(t, 12, n)
	})
	
	t.Run("buffer sink methods", func(t *testing.T) {
		sink := &BufferSink{}
		
		n, err := sink.Write([]byte("test data"))
		assert.NoError(t, err)
		assert.Equal(t, 9, n)
		
		id := sink.ID()
		assert.Equal(t, "buffer-sink", id)
		
		err = sink.Close()
		assert.NoError(t, err)
		
		err = sink.Cancel()
		assert.NoError(t, err)
	})
	
	t.Run("default raft config", func(t *testing.T) {
		config := DefaultRaftConfig("test-node", "127.0.0.1:8300", "/tmp/test")
		
		assert.Equal(t, "test-node", config.NodeID)
		assert.Equal(t, "127.0.0.1:8300", config.BindAddr)
		assert.Equal(t, "/tmp/test/test-node", config.DataDir)
		assert.NotZero(t, config.HeartbeatTimeout)
		assert.NotZero(t, config.ElectionTimeout)
		assert.NotZero(t, config.CommitTimeout)
	})
}