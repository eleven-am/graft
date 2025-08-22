package raftimpl

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdditionalUtilityCoverage(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("CommandType String method coverage", func(t *testing.T) {
		assert.Equal(t, "PUT", CommandPut.String())
		assert.Equal(t, "DELETE", CommandDelete.String())
		assert.Equal(t, "BATCH", CommandBatch.String())
		assert.Equal(t, "STATE_UPDATE", CommandStateUpdate.String())
		assert.Equal(t, "QUEUE_OPERATION", CommandQueueOperation.String())

		unknownType := CommandType(99)
		assert.Equal(t, "UNKNOWN", unknownType.String())
	})

	t.Run("Command marshaling edge cases", func(t *testing.T) {
		deleteCmd := NewDeleteCommand("test-key")
		assert.Equal(t, CommandDelete, deleteCmd.Type)
		assert.Equal(t, "test-key", deleteCmd.Key)
		assert.Nil(t, deleteCmd.Value)

		data, err := deleteCmd.Marshal()
		assert.NoError(t, err)
		assert.NotEmpty(t, data)

		unmarshaled, err := UnmarshalCommand(data)
		assert.NoError(t, err)
		assert.Equal(t, deleteCmd.Type, unmarshaled.Type)
		assert.Equal(t, deleteCmd.Key, unmarshaled.Key)
	})

	t.Run("Storage adapter SetConsistencyLevel", func(t *testing.T) {
		adapter := &StorageAdapter{
			config: DefaultStorageConfig(),
			logger: logger,
		}

		adapter.SetConsistencyLevel(ConsistencyEventual)
		assert.Equal(t, ConsistencyEventual, adapter.config.DefaultConsistencyLevel)

		adapter.SetConsistencyLevel(ConsistencyQuorum)
		assert.Equal(t, ConsistencyQuorum, adapter.config.DefaultConsistencyLevel)
	})
}
