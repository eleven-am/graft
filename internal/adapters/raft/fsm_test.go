package raft

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestFSM(t *testing.T) (*FSM, *badger.DB, func()) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	require.NoError(t, err)

	fsm := NewFSM(db, nil, "test-node-1", "test-cluster", domain.ClusterPolicyRecover, nil)

	cleanup := func() {
		db.Close()
	}

	return fsm, db, cleanup
}

func TestFSM_ApplyPut(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	cmd := domain.Command{
		Type:      domain.CommandPut,
		Key:       "test-key",
		Value:     []byte("test-value"),
		RequestID: "req-1",
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
	assert.Equal(t, int64(1), cmdResult.Version)
	assert.Len(t, cmdResult.Events, 1)
	assert.Equal(t, domain.EventPut, cmdResult.Events[0].Type)
	assert.Equal(t, "test-key", cmdResult.Events[0].Key)

	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("test-key"))
		require.NoError(t, err)

		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		assert.Equal(t, []byte("test-value"), value)

		vItem, err := txn.Get([]byte("v:test-key"))
		require.NoError(t, err)

		var version int64
		vBytes, _ := vItem.ValueCopy(nil)
		json.Unmarshal(vBytes, &version)
		assert.Equal(t, int64(1), version)

		return nil
	})
	require.NoError(t, err)
}

func TestFSM_ApplyPut_MultipleTimes(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	for i := 1; i <= 5; i++ {
		cmd := domain.Command{
			Type:      domain.CommandPut,
			Key:       "counter-key",
			Value:     []byte{byte(i)},
			RequestID: "req-" + string(rune(i)),
			Timestamp: time.Now(),
		}

		cmdBytes, _ := json.Marshal(cmd)
		log := &raft.Log{Data: cmdBytes}

		result := fsm.Apply(log)
		cmdResult := result.(*domain.CommandResult)

		assert.True(t, cmdResult.Success)
		assert.Equal(t, int64(i), cmdResult.Version)
	}

	err := db.View(func(txn *badger.Txn) error {
		vItem, err := txn.Get([]byte("v:counter-key"))
		require.NoError(t, err)

		var version int64
		vBytes, _ := vItem.ValueCopy(nil)
		json.Unmarshal(vBytes, &version)
		assert.Equal(t, int64(5), version)

		return nil
	})
	require.NoError(t, err)
}

func TestFSM_ApplyDelete(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	putCmd := domain.Command{
		Type:      domain.CommandPut,
		Key:       "delete-test",
		Value:     []byte("to-be-deleted"),
		RequestID: "req-1",
		Timestamp: time.Now(),
	}
	putBytes, _ := json.Marshal(putCmd)
	fsm.Apply(&raft.Log{Data: putBytes})

	deleteCmd := domain.Command{
		Type:      domain.CommandDelete,
		Key:       "delete-test",
		RequestID: "req-2",
		Timestamp: time.Now(),
	}
	deleteBytes, _ := json.Marshal(deleteCmd)

	result := fsm.Apply(&raft.Log{Data: deleteBytes})
	cmdResult := result.(*domain.CommandResult)

	assert.True(t, cmdResult.Success)
	assert.Len(t, cmdResult.Events, 1)
	assert.Equal(t, domain.EventDelete, cmdResult.Events[0].Type)

	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("delete-test"))
		assert.Equal(t, badger.ErrKeyNotFound, err)

		_, err = txn.Get([]byte("v:delete-test"))
		assert.Equal(t, badger.ErrKeyNotFound, err)

		return nil
	})
	require.NoError(t, err)
}

func TestFSM_ApplyCAS_Success(t *testing.T) {
	fsm, _, cleanup := setupTestFSM(t)
	defer cleanup()

	putCmd := domain.Command{
		Type:      domain.CommandPut,
		Key:       "cas-test",
		Value:     []byte("initial"),
		RequestID: "req-1",
		Timestamp: time.Now(),
	}
	putBytes, _ := json.Marshal(putCmd)
	result := fsm.Apply(&raft.Log{Data: putBytes})
	putResult := result.(*domain.CommandResult)
	assert.Equal(t, int64(1), putResult.Version)

	casCmd := domain.Command{
		Type:      domain.CommandCAS,
		Key:       "cas-test",
		Expected:  []byte("initial"),
		Value:     []byte("updated"),
		RequestID: "req-2",
		Timestamp: time.Now(),
	}
	casBytes, _ := json.Marshal(casCmd)

	casResult := fsm.Apply(&raft.Log{Data: casBytes}).(*domain.CommandResult)

	assert.True(t, casResult.Success)
	assert.Equal(t, int64(2), casResult.Version)
	assert.Equal(t, int64(1), casResult.PrevVersion)
	assert.Len(t, casResult.Events, 1)
	assert.Equal(t, domain.EventCAS, casResult.Events[0].Type)
}

func TestFSM_ApplyCAS_VersionMismatch(t *testing.T) {
	fsm, _, cleanup := setupTestFSM(t)
	defer cleanup()

	putCmd := domain.Command{
		Type:      domain.CommandPut,
		Key:       "version-test",
		Value:     []byte("v1"),
		RequestID: "req-1",
		Timestamp: time.Now(),
	}
	putBytes, _ := json.Marshal(putCmd)
	fsm.Apply(&raft.Log{Data: putBytes})

	casCmd := domain.Command{
		Type:      domain.CommandCAS,
		Key:       "version-test",
		Version:   99,
		Value:     []byte("should-fail"),
		RequestID: "req-2",
		Timestamp: time.Now(),
	}
	casBytes, _ := json.Marshal(casCmd)

	result := fsm.Apply(&raft.Log{Data: casBytes}).(*domain.CommandResult)

	assert.False(t, result.Success)
	assert.Contains(t, result.Error, "version mismatch")
	assert.Equal(t, int64(1), result.PrevVersion)
}

func TestFSM_ApplyCAS_ValueMismatch(t *testing.T) {
	fsm, _, cleanup := setupTestFSM(t)
	defer cleanup()

	putCmd := domain.Command{
		Type:      domain.CommandPut,
		Key:       "value-test",
		Value:     []byte("original"),
		RequestID: "req-1",
		Timestamp: time.Now(),
	}
	putBytes, _ := json.Marshal(putCmd)
	fsm.Apply(&raft.Log{Data: putBytes})

	casCmd := domain.Command{
		Type:      domain.CommandCAS,
		Key:       "value-test",
		Expected:  []byte("wrong-value"),
		Value:     []byte("should-fail"),
		RequestID: "req-2",
		Timestamp: time.Now(),
	}
	casBytes, _ := json.Marshal(casCmd)

	result := fsm.Apply(&raft.Log{Data: casBytes}).(*domain.CommandResult)

	assert.False(t, result.Success)
	assert.Equal(t, "value mismatch", result.Error)
}

func TestFSM_ApplyBatch(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	batchCmd := domain.Command{
		Type: domain.CommandBatch,
		Batch: []domain.BatchOp{
			{
				Type:  domain.CommandPut,
				Key:   "batch-1",
				Value: []byte("value-1"),
			},
			{
				Type:  domain.CommandPut,
				Key:   "batch-2",
				Value: []byte("value-2"),
			},
			{
				Type:  domain.CommandCAS,
				Key:   "batch-3",
				Value: []byte("value-3"),
			},
			{
				Type: domain.CommandDelete,
				Key:  "batch-4",
			},
		},
		RequestID: "batch-req",
		Timestamp: time.Now(),
	}

	batchBytes, _ := json.Marshal(batchCmd)
	result := fsm.Apply(&raft.Log{Data: batchBytes}).(*domain.CommandResult)

	assert.True(t, result.Success)
	assert.Len(t, result.BatchResults, 4)
	assert.Len(t, result.Events, 4)

	for i, res := range result.BatchResults {
		assert.True(t, res.Success, "Operation %d failed", i)
		if i < 3 {
			assert.Equal(t, int64(1), res.Version)
		}
	}

	err := db.View(func(txn *badger.Txn) error {
		item1, err := txn.Get([]byte("batch-1"))
		require.NoError(t, err)
		val1, _ := item1.ValueCopy(nil)
		assert.Equal(t, []byte("value-1"), val1)

		item2, err := txn.Get([]byte("batch-2"))
		require.NoError(t, err)
		val2, _ := item2.ValueCopy(nil)
		assert.Equal(t, []byte("value-2"), val2)

		item3, err := txn.Get([]byte("batch-3"))
		require.NoError(t, err)
		val3, _ := item3.ValueCopy(nil)
		assert.Equal(t, []byte("value-3"), val3)

		_, err = txn.Get([]byte("batch-4"))
		assert.Equal(t, badger.ErrKeyNotFound, err)

		return nil
	})
	require.NoError(t, err)
}

func TestFSM_ApplyBatch_WithCASFailure(t *testing.T) {
	fsm, _, cleanup := setupTestFSM(t)
	defer cleanup()

	putCmd := domain.Command{
		Type:      domain.CommandPut,
		Key:       "existing",
		Value:     []byte("original"),
		RequestID: "req-1",
		Timestamp: time.Now(),
	}
	putBytes, _ := json.Marshal(putCmd)
	fsm.Apply(&raft.Log{Data: putBytes})

	batchCmd := domain.Command{
		Type: domain.CommandBatch,
		Batch: []domain.BatchOp{
			{
				Type:  domain.CommandPut,
				Key:   "new-key",
				Value: []byte("new-value"),
			},
			{
				Type:     domain.CommandCAS,
				Key:      "existing",
				Expected: []byte("wrong-value"),
				Value:    []byte("should-fail"),
			},
			{
				Type:  domain.CommandPut,
				Key:   "another-key",
				Value: []byte("another-value"),
			},
		},
		RequestID: "batch-req",
		Timestamp: time.Now(),
	}

	batchBytes, _ := json.Marshal(batchCmd)
	result := fsm.Apply(&raft.Log{Data: batchBytes}).(*domain.CommandResult)

	assert.True(t, result.Success)
	assert.Len(t, result.BatchResults, 3)

	assert.True(t, result.BatchResults[0].Success)
	assert.False(t, result.BatchResults[1].Success)
	assert.Equal(t, "value mismatch in batch", result.BatchResults[1].Error)
	assert.True(t, result.BatchResults[2].Success)
}

func TestFSM_Snapshot_Restore(t *testing.T) {
	fsm1, _, cleanup1 := setupTestFSM(t)
	defer cleanup1()

	for i := 0; i < 10; i++ {
		cmd := domain.Command{
			Type:      domain.CommandPut,
			Key:       string(rune('a' + i)),
			Value:     []byte{byte(i)},
			RequestID: "req-" + string(rune(i)),
			Timestamp: time.Now(),
		}
		cmdBytes, _ := json.Marshal(cmd)
		fsm1.Apply(&raft.Log{Data: cmdBytes})
	}

	snapshot, err := fsm1.Snapshot()
	require.NoError(t, err)

	var buf bytes.Buffer
	sink := &mockSnapshotSink{Buffer: &buf}
	err = snapshot.Persist(sink)
	require.NoError(t, err)

	fsm2, db2, cleanup2 := setupTestFSM(t)
	defer cleanup2()

	rc := &mockReadCloser{Buffer: bytes.NewBuffer(buf.Bytes())}
	err = fsm2.Restore(rc)
	require.NoError(t, err)

	err = db2.View(func(txn *badger.Txn) error {
		for i := 0; i < 10; i++ {
			key := string(rune('a' + i))
			item, err := txn.Get([]byte(key))
			require.NoError(t, err)

			val, _ := item.ValueCopy(nil)
			assert.Equal(t, []byte{byte(i)}, val)

			vItem, err := txn.Get([]byte("v:" + key))
			require.NoError(t, err)

			var version int64
			vBytes, _ := vItem.ValueCopy(nil)
			json.Unmarshal(vBytes, &version)
			assert.Equal(t, int64(1), version)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestFSM_InvalidCommand(t *testing.T) {
	fsm, _, cleanup := setupTestFSM(t)
	defer cleanup()

	log := &raft.Log{
		Data: []byte("invalid json"),
	}

	result := fsm.Apply(log)
	cmdResult := result.(*domain.CommandResult)

	assert.False(t, cmdResult.Success)
	assert.Contains(t, cmdResult.Error, "failed to unmarshal")
}

func TestFSM_UnknownCommandType(t *testing.T) {
	fsm, _, cleanup := setupTestFSM(t)
	defer cleanup()

	cmd := domain.Command{
		Type:      99,
		Key:       "test",
		Value:     []byte("test"),
		RequestID: "req-1",
		Timestamp: time.Now(),
	}

	cmdBytes, _ := json.Marshal(cmd)
	result := fsm.Apply(&raft.Log{Data: cmdBytes})
	cmdResult := result.(*domain.CommandResult)

	assert.False(t, cmdResult.Success)
	assert.Contains(t, cmdResult.Error, "unknown command type")
}

func TestFSM_ConcurrentOperations(t *testing.T) {
	fsm, db, cleanup := setupTestFSM(t)
	defer cleanup()

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			cmd := domain.Command{
				Type:      domain.CommandPut,
				Key:       "concurrent-key",
				Value:     []byte{byte(idx)},
				RequestID: "req-concurrent-" + string(rune(idx)),
				Timestamp: time.Now(),
			}
			cmdBytes, _ := json.Marshal(cmd)
			fsm.Apply(&raft.Log{Data: cmdBytes})
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	err := db.View(func(txn *badger.Txn) error {
		vItem, err := txn.Get([]byte("v:concurrent-key"))
		require.NoError(t, err)

		var version int64
		vBytes, _ := vItem.ValueCopy(nil)
		json.Unmarshal(vBytes, &version)
		assert.Equal(t, int64(10), version)

		return nil
	})
	require.NoError(t, err)
}

type mockSnapshotSink struct {
	*bytes.Buffer
}

func (m *mockSnapshotSink) ID() string {
	return "mock"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

func (m *mockSnapshotSink) Close() error {
	return nil
}

type mockReadCloser struct {
	*bytes.Buffer
}

func (m *mockReadCloser) Close() error {
	return nil
}
