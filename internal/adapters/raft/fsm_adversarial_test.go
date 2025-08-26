package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestBadgerDB(t *testing.T) *badger.DB {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	return db
}

func TestFSM_ConcurrentApply(t *testing.T) {
	db := createTestBadgerDB(t)
	defer db.Close()

	fsm := NewFSM(db, "node1", "cluster1", slog.Default())

	const numGoroutines = 10
	const numOperations = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				cmd := domain.Command{
					Type:      domain.CommandPut,
					Key:       fmt.Sprintf("key-%d-%d", routineID, j),
					Value:     []byte(fmt.Sprintf("value-%d-%d", routineID, j)),
					RequestID: fmt.Sprintf("req-%d-%d", routineID, j),
					Timestamp: time.Now(),
				}

				cmdBytes, err := json.Marshal(cmd)
				require.NoError(t, err)

				log := &raft.Log{
					Data: cmdBytes,
				}

				result := fsm.Apply(log)
				cmdResult, ok := result.(*domain.CommandResult)
				require.True(t, ok)
				assert.True(t, cmdResult.Success)
			}
		}(i)
	}

	wg.Wait()

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		assert.GreaterOrEqual(t, count, numGoroutines*numOperations)
		return nil
	})
	require.NoError(t, err)
}

func TestFSM_LargeSnapshot(t *testing.T) {
	db := createTestBadgerDB(t)
	defer db.Close()

	fsm := NewFSM(db, "node1", "cluster1", slog.Default())

	const numEntries = 1000
	for i := 0; i < numEntries; i++ {
		cmd := domain.Command{
			Type:      domain.CommandPut,
			Key:       fmt.Sprintf("key-%05d", i),
			Value:     []byte(fmt.Sprintf("value-%05d", i)),
			RequestID: fmt.Sprintf("req-%d", i),
			Timestamp: time.Now(),
		}

		cmdBytes, err := json.Marshal(cmd)
		require.NoError(t, err)

		log := &raft.Log{
			Data: cmdBytes,
		}

		result := fsm.Apply(log)
		cmdResult, ok := result.(*domain.CommandResult)
		require.True(t, ok)
		assert.True(t, cmdResult.Success)
	}

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)

	sink := &MockSink{
		data: &bytes.Buffer{},
	}
	err = snap.Persist(sink)
	require.NoError(t, err)

	newDB := createTestBadgerDB(t)
	defer newDB.Close()
	
	newFSM := NewFSM(newDB, "node2", "cluster1", slog.Default())
	err = newFSM.Restore(io.NopCloser(bytes.NewReader(sink.data.Bytes())))
	require.NoError(t, err)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key-%05d", i)
		expectedValue := fmt.Sprintf("value-%05d", i)

		err := newDB.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				assert.Equal(t, []byte(expectedValue), val)
				return nil
			})
		})
		require.NoError(t, err)
	}
}

func TestFSM_InvalidCommands(t *testing.T) {
	db := createTestBadgerDB(t)
	defer db.Close()

	fsm := NewFSM(db, "node1", "cluster1", slog.Default())

	tests := []struct {
		name          string
		data          []byte
		expectSuccess bool
	}{
		{
			name:          "invalid_json",
			data:          []byte("not json"),
			expectSuccess: false,
		},
		{
			name:          "empty_data",
			data:          []byte{},
			expectSuccess: false,
		},
		{
			name:          "null_json",
			data:          []byte("null"),
			expectSuccess: false,
		},
		{
			name: "missing_type",
			data: func() []byte {
				cmd := map[string]interface{}{
					"key":   "",
					"value": []byte("test"),
				}
				b, _ := json.Marshal(cmd)
				return b
			}(),
			expectSuccess: false,
		},
		{
			name: "invalid_type",
			data: func() []byte {
				cmd := domain.Command{
					Type:  domain.CommandType(99),
					Key:   "test",
					Value: []byte("test"),
				}
				b, _ := json.Marshal(cmd)
				return b
			}(),
			expectSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &raft.Log{
				Data: tt.data,
			}

			result := fsm.Apply(log)
			cmdResult, ok := result.(*domain.CommandResult)
			
			if tt.expectSuccess {
				require.True(t, ok)
				assert.True(t, cmdResult.Success)
			} else {
				if ok {
					assert.False(t, cmdResult.Success, "Expected failure but got success: %+v", cmdResult)
				}
			}
		})
	}
}

func TestFSM_DeleteCommand(t *testing.T) {
	db := createTestBadgerDB(t)
	defer db.Close()

	fsm := NewFSM(db, "node1", "cluster1", slog.Default())

	putCmd := domain.Command{
		Type:      domain.CommandPut,
		Key:       "test-key",
		Value:     []byte("test-value"),
		RequestID: "put-1",
		Timestamp: time.Now(),
	}
	
	putBytes, err := json.Marshal(putCmd)
	require.NoError(t, err)
	
	putLog := &raft.Log{
		Data: putBytes,
	}
	
	result := fsm.Apply(putLog)
	cmdResult, ok := result.(*domain.CommandResult)
	require.True(t, ok)
	assert.True(t, cmdResult.Success)

	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("test-key"))
		return err
	})
	require.NoError(t, err)

	deleteCmd := domain.Command{
		Type:      domain.CommandDelete,
		Key:       "test-key",
		RequestID: "delete-1",
		Timestamp: time.Now(),
	}
	
	deleteBytes, err := json.Marshal(deleteCmd)
	require.NoError(t, err)
	
	deleteLog := &raft.Log{
		Data: deleteBytes,
	}
	
	result = fsm.Apply(deleteLog)
	cmdResult, ok = result.(*domain.CommandResult)
	require.True(t, ok)
	assert.True(t, cmdResult.Success)

	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("test-key"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestFSM_BatchCommand(t *testing.T) {
	db := createTestBadgerDB(t)
	defer db.Close()

	fsm := NewFSM(db, "node1", "cluster1", slog.Default())

	batchOps := []domain.BatchOp{
		{
			Type:  domain.CommandPut,
			Key:   "batch-key-1",
			Value: []byte("batch-value-1"),
		},
		{
			Type:  domain.CommandPut,
			Key:   "batch-key-2",
			Value: []byte("batch-value-2"),
		},
		{
			Type:  domain.CommandPut,
			Key:   "batch-key-3",
			Value: []byte("batch-value-3"),
		},
	}

	batchCmd := domain.Command{
		Type:      domain.CommandBatch,
		Batch:     batchOps,
		RequestID: "batch-1",
		Timestamp: time.Now(),
	}
	
	batchBytes, err := json.Marshal(batchCmd)
	require.NoError(t, err)
	
	batchLog := &raft.Log{
		Data: batchBytes,
	}
	
	result := fsm.Apply(batchLog)
	cmdResult, ok := result.(*domain.CommandResult)
	require.True(t, ok)
	assert.True(t, cmdResult.Success)

	for i := 1; i <= 3; i++ {
		key := fmt.Sprintf("batch-key-%d", i)
		expectedValue := fmt.Sprintf("batch-value-%d", i)
		
		err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				assert.Equal(t, []byte(expectedValue), val)
				return nil
			})
		})
		require.NoError(t, err)
	}
}

type MockSink struct {
	data *bytes.Buffer
}

func (m *MockSink) Write(p []byte) (n int, err error) {
	return m.data.Write(p)
}

func (m *MockSink) Close() error {
	return nil
}

func (m *MockSink) ID() string {
	return "mock-sink"
}

func (m *MockSink) Cancel() error {
	return nil
}