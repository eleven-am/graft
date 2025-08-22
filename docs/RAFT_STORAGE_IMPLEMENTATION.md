# Raft Storage Adapter Implementation Instructions for Team 1

## 🔴 CRITICAL: COMPLIANCE REQUIREMENTS

**MANDATORY**: This implementation MUST strictly follow:
1. `/CLAUDE.md` - All behavioral rules, especially:
   - NO comments except actionable `TODO:` statements
   - Test everything after implementation
   - Use existing packages over custom implementations
2. `/CODING_GUIDELINES.md` - All coding standards and patterns

**FAILURE TO COMPLY WITH THESE FILES WILL RESULT IN REJECTION**

## 📋 Executive Summary

Replace the current BadgerDB storage adapter entirely with a Raft-consensus-based storage adapter. This will provide distributed consensus for all storage operations, ensuring consistency across all Graft instances.

### What This Resolves
✅ **State Consistency**: All nodes see the same state through Raft consensus
✅ **Distributed Queue**: Queue operations become consistent across cluster
✅ **Atomic Operations**: Batch operations guaranteed atomic via Raft log
✅ **Split-brain Prevention**: Raft leader election prevents conflicting writes
✅ **Ordered Operations**: Raft log provides total ordering of all mutations

### What Remains Unresolved
❌ **Work Claiming**: Still needs atomic claim mechanism for node execution
❌ **Event-driven Evaluation**: Still uses polling for pending nodes
❌ **Panic Recovery**: No recovery mechanism for failed node executions
❌ **Lifecycle Hooks**: No support for workflow lifecycle events

## 🏗️ Implementation Structure

```
internal/adapters/storage/
├── raft/
│   ├── storage.go          # Main Raft storage adapter
│   ├── fsm.go              # Raft FSM for applying commands
│   ├── config.go           # Raft configuration
│   ├── commands.go         # Command types for Raft log
│   └── storage_test.go     # Unit tests
```

## 📝 Complete Implementation Code

### 1. Command Types (`internal/adapters/storage/raft/commands.go`)

```go
package raft

import "encoding/json"

type CommandType uint8

const (
    CommandPut CommandType = iota
    CommandDelete
    CommandBatch
)

type Command struct {
    Type  CommandType     `json:"type"`
    Key   string          `json:"key,omitempty"`
    Value []byte          `json:"value,omitempty"`
    Ops   []BatchOp       `json:"ops,omitempty"`
}

type BatchOp struct {
    Type  CommandType `json:"type"`
    Key   string      `json:"key"`
    Value []byte      `json:"value,omitempty"`
}

func (c *Command) Encode() ([]byte, error) {
    return json.Marshal(c)
}

func DecodeCommand(data []byte) (*Command, error) {
    var cmd Command
    err := json.Unmarshal(data, &cmd)
    return &cmd, err
}
```

### 2. Raft FSM (`internal/adapters/storage/raft/fsm.go`)

```go
package raft

import (
    "encoding/json"
    "io"
    "sync"

    "github.com/dgraph-io/badger/v3"
    "github.com/hashicorp/raft"
)

type FSM struct {
    mu sync.RWMutex
    db *badger.DB
}

func NewFSM(db *badger.DB) *FSM {
    return &FSM{
        db: db,
    }
}

func (f *FSM) Apply(log *raft.Log) interface{} {
    cmd, err := DecodeCommand(log.Data)
    if err != nil {
        return err
    }

    f.mu.Lock()
    defer f.mu.Unlock()

    switch cmd.Type {
    case CommandPut:
        return f.applyPut(cmd.Key, cmd.Value)
    case CommandDelete:
        return f.applyDelete(cmd.Key)
    case CommandBatch:
        return f.applyBatch(cmd.Ops)
    default:
        return ErrUnknownCommand
    }
}

func (f *FSM) applyPut(key string, value []byte) error {
    return f.db.Update(func(txn *badger.Txn) error {
        return txn.Set([]byte(key), value)
    })
}

func (f *FSM) applyDelete(key string) error {
    return f.db.Update(func(txn *badger.Txn) error {
        return txn.Delete([]byte(key))
    })
}

func (f *FSM) applyBatch(ops []BatchOp) error {
    return f.db.Update(func(txn *badger.Txn) error {
        for _, op := range ops {
            switch op.Type {
            case CommandPut:
                if err := txn.Set([]byte(op.Key), op.Value); err != nil {
                    return err
                }
            case CommandDelete:
                if err := txn.Delete([]byte(op.Key)); err != nil {
                    return err
                }
            }
        }
        return nil
    })
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    
    return &FSMSnapshot{db: f.db}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
    defer rc.Close()
    
    f.mu.Lock()
    defer f.mu.Unlock()
    
    var snapshot map[string][]byte
    if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
        return err
    }
    
    return f.db.Update(func(txn *badger.Txn) error {
        for key, value := range snapshot {
            if err := txn.Set([]byte(key), value); err != nil {
                return err
            }
        }
        return nil
    })
}

type FSMSnapshot struct {
    db *badger.DB
}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
    defer sink.Close()
    
    snapshot := make(map[string][]byte)
    
    err := s.db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        it := txn.NewIterator(opts)
        defer it.Close()
        
        for it.Rewind(); it.Valid(); it.Next() {
            item := it.Item()
            key := string(item.Key())
            value, err := item.ValueCopy(nil)
            if err != nil {
                return err
            }
            snapshot[key] = value
        }
        return nil
    })
    
    if err != nil {
        sink.Cancel()
        return err
    }
    
    if err := json.NewEncoder(sink).Encode(snapshot); err != nil {
        sink.Cancel()
        return err
    }
    
    return nil
}

func (s *FSMSnapshot) Release() {}
```

### 3. Main Storage Adapter (`internal/adapters/storage/raft/storage.go`)

```go
package raft

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "net"
    "os"
    "path/filepath"
    "time"

    "github.com/dgraph-io/badger/v3"
    "github.com/hashicorp/raft"
    raftboltdb "github.com/hashicorp/raft-boltdb"
    "github.com/your-org/graft/internal/ports"
)

var (
    ErrNotLeader      = errors.New("not the leader")
    ErrUnknownCommand = errors.New("unknown command type")
    ErrTimeout        = errors.New("operation timeout")
)

type Config struct {
    NodeID      string
    BindAddr    string
    DataDir     string
    Bootstrap   bool
    JoinAddress string
}

type Storage struct {
    raft   *raft.Raft
    fsm    *FSM
    db     *badger.DB
    config Config
}

func New(cfg Config) (*Storage, error) {
    dataDir := filepath.Join(cfg.DataDir, cfg.NodeID)
    if err := os.MkdirAll(dataDir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create data dir: %w", err)
    }

    dbPath := filepath.Join(dataDir, "badger")
    db, err := badger.Open(badger.DefaultOptions(dbPath))
    if err != nil {
        return nil, fmt.Errorf("failed to open badger: %w", err)
    }

    fsm := NewFSM(db)

    raftConfig := raft.DefaultConfig()
    raftConfig.LocalID = raft.ServerID(cfg.NodeID)
    raftConfig.SnapshotInterval = 120 * time.Second
    raftConfig.SnapshotThreshold = 2

    addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
    if err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to resolve address: %w", err)
    }

    transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
    if err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to create transport: %w", err)
    }

    logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.db"))
    if err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to create log store: %w", err)
    }

    stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.db"))
    if err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to create stable store: %w", err)
    }

    snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
    if err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to create snapshot store: %w", err)
    }

    r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
    if err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to create raft: %w", err)
    }

    if cfg.Bootstrap {
        configuration := raft.Configuration{
            Servers: []raft.Server{
                {
                    ID:      raft.ServerID(cfg.NodeID),
                    Address: transport.LocalAddr(),
                },
            },
        }
        r.BootstrapCluster(configuration)
    }

    return &Storage{
        raft:   r,
        fsm:    fsm,
        db:     db,
        config: cfg,
    }, nil
}

func (s *Storage) Put(ctx context.Context, key string, value []byte) error {
    if s.raft.State() != raft.Leader {
        return ErrNotLeader
    }

    cmd := &Command{
        Type:  CommandPut,
        Key:   key,
        Value: value,
    }

    data, err := cmd.Encode()
    if err != nil {
        return err
    }

    future := s.raft.Apply(data, 5*time.Second)
    if err := future.Error(); err != nil {
        return err
    }

    if err, ok := future.Response().(error); ok {
        return err
    }

    return nil
}

func (s *Storage) Get(ctx context.Context, key string) ([]byte, error) {
    var value []byte
    err := s.db.View(func(txn *badger.Txn) error {
        item, err := txn.Get([]byte(key))
        if err != nil {
            return err
        }
        value, err = item.ValueCopy(nil)
        return err
    })
    
    if errors.Is(err, badger.ErrKeyNotFound) {
        return nil, ports.ErrNotFound
    }
    
    return value, err
}

func (s *Storage) Delete(ctx context.Context, key string) error {
    if s.raft.State() != raft.Leader {
        return ErrNotLeader
    }

    cmd := &Command{
        Type: CommandDelete,
        Key:  key,
    }

    data, err := cmd.Encode()
    if err != nil {
        return err
    }

    future := s.raft.Apply(data, 5*time.Second)
    if err := future.Error(); err != nil {
        return err
    }

    if err, ok := future.Response().(error); ok {
        return err
    }

    return nil
}

func (s *Storage) List(ctx context.Context, prefix string) ([]ports.KeyValue, error) {
    var results []ports.KeyValue
    
    err := s.db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        opts.Prefix = []byte(prefix)
        it := txn.NewIterator(opts)
        defer it.Close()
        
        for it.Rewind(); it.Valid(); it.Next() {
            item := it.Item()
            key := string(item.Key())
            value, err := item.ValueCopy(nil)
            if err != nil {
                return err
            }
            results = append(results, ports.KeyValue{
                Key:   key,
                Value: value,
            })
        }
        return nil
    })
    
    return results, err
}

func (s *Storage) Batch(ctx context.Context, ops []ports.Operation) error {
    if s.raft.State() != raft.Leader {
        return ErrNotLeader
    }

    batchOps := make([]BatchOp, 0, len(ops))
    for _, op := range ops {
        var cmdType CommandType
        switch op.Type {
        case ports.OpPut:
            cmdType = CommandPut
        case ports.OpDelete:
            cmdType = CommandDelete
        default:
            return fmt.Errorf("unknown operation type: %v", op.Type)
        }
        
        batchOps = append(batchOps, BatchOp{
            Type:  cmdType,
            Key:   op.Key,
            Value: op.Value,
        })
    }

    cmd := &Command{
        Type: CommandBatch,
        Ops:  batchOps,
    }

    data, err := cmd.Encode()
    if err != nil {
        return err
    }

    future := s.raft.Apply(data, 5*time.Second)
    if err := future.Error(); err != nil {
        return err
    }

    if err, ok := future.Response().(error); ok {
        return err
    }

    return nil
}

func (s *Storage) Close() error {
    if err := s.raft.Shutdown().Error(); err != nil {
        return err
    }
    return s.db.Close()
}

func (s *Storage) Join(nodeID, addr string) error {
    configFuture := s.raft.GetConfiguration()
    if err := configFuture.Error(); err != nil {
        return err
    }

    for _, srv := range configFuture.Configuration().Servers {
        if srv.ID == raft.ServerID(nodeID) {
            return nil
        }
    }

    f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
    return f.Error()
}

func (s *Storage) Leave(nodeID string) error {
    f := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
    return f.Error()
}

func (s *Storage) IsLeader() bool {
    return s.raft.State() == raft.Leader
}

func (s *Storage) LeaderAddress() string {
    _, leader := s.raft.LeaderWithID()
    return string(leader)
}

func (s *Storage) WaitForLeader(timeout time.Duration) error {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()
    
    timer := time.NewTimer(timeout)
    defer timer.Stop()
    
    for {
        select {
        case <-ticker.C:
            if leader, _ := s.raft.LeaderWithID(); leader != "" {
                return nil
            }
        case <-timer.C:
            return ErrTimeout
        }
    }
}
```

### 4. Configuration (`internal/adapters/storage/raft/config.go`)

```go
package raft

import (
    "os"
    "strconv"
)

func ConfigFromEnv() Config {
    bootstrap, _ := strconv.ParseBool(os.Getenv("RAFT_BOOTSTRAP"))
    
    return Config{
        NodeID:      getEnvOrDefault("RAFT_NODE_ID", "node-1"),
        BindAddr:    getEnvOrDefault("RAFT_BIND_ADDR", "127.0.0.1:7000"),
        DataDir:     getEnvOrDefault("RAFT_DATA_DIR", "/tmp/graft-raft"),
        Bootstrap:   bootstrap,
        JoinAddress: os.Getenv("RAFT_JOIN_ADDRESS"),
    }
}

func getEnvOrDefault(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}
```

## 🧪 Testing Requirements

### Unit Tests (`internal/adapters/storage/raft/storage_test.go`)

```go
package raft

import (
    "context"
    "io/ioutil"
    "os"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "github.com/your-org/graft/internal/ports"
)

func TestStorage_SingleNode(t *testing.T) {
    dir, err := ioutil.TempDir("", "raft-test")
    require.NoError(t, err)
    defer os.RemoveAll(dir)

    cfg := Config{
        NodeID:    "test-node",
        BindAddr:  "127.0.0.1:0",
        DataDir:   dir,
        Bootstrap: true,
    }

    storage, err := New(cfg)
    require.NoError(t, err)
    defer storage.Close()

    err = storage.WaitForLeader(5 * time.Second)
    require.NoError(t, err)

    ctx := context.Background()

    t.Run("Put and Get", func(t *testing.T) {
        err := storage.Put(ctx, "key1", []byte("value1"))
        assert.NoError(t, err)

        value, err := storage.Get(ctx, "key1")
        assert.NoError(t, err)
        assert.Equal(t, []byte("value1"), value)
    })

    t.Run("Delete", func(t *testing.T) {
        err := storage.Put(ctx, "key2", []byte("value2"))
        assert.NoError(t, err)

        err = storage.Delete(ctx, "key2")
        assert.NoError(t, err)

        _, err = storage.Get(ctx, "key2")
        assert.ErrorIs(t, err, ports.ErrNotFound)
    })

    t.Run("List with Prefix", func(t *testing.T) {
        storage.Put(ctx, "prefix/key1", []byte("value1"))
        storage.Put(ctx, "prefix/key2", []byte("value2"))
        storage.Put(ctx, "other/key3", []byte("value3"))

        results, err := storage.List(ctx, "prefix/")
        assert.NoError(t, err)
        assert.Len(t, results, 2)
    })

    t.Run("Batch Operations", func(t *testing.T) {
        ops := []ports.Operation{
            {Type: ports.OpPut, Key: "batch1", Value: []byte("value1")},
            {Type: ports.OpPut, Key: "batch2", Value: []byte("value2")},
            {Type: ports.OpDelete, Key: "key1"},
        }

        err := storage.Batch(ctx, ops)
        assert.NoError(t, err)

        value, err := storage.Get(ctx, "batch1")
        assert.NoError(t, err)
        assert.Equal(t, []byte("value1"), value)

        _, err = storage.Get(ctx, "key1")
        assert.ErrorIs(t, err, ports.ErrNotFound)
    })
}

func TestStorage_ThreeNodeCluster(t *testing.T) {
    // TODO: Implement three-node cluster test
}

func TestStorage_LeaderElection(t *testing.T) {
    // TODO: Implement leader failure and re-election test
}

func TestStorage_Snapshot(t *testing.T) {
    // TODO: Implement snapshot and restore test
}
```

### Integration Tests

Create `internal/adapters/storage/raft/integration_test.go`:

```go
// +build integration

package raft

import (
    "context"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
)

func TestRaftClusterIntegration(t *testing.T) {
    // TODO: Test full cluster operations
}

func TestRaftNetworkPartition(t *testing.T) {
    // TODO: Test network partition scenarios
}

func TestRaftConcurrentWrites(t *testing.T) {
    // TODO: Test concurrent write consistency
}
```

## 🔄 Migration Plan

### Phase 1: Parallel Deployment (Week 1)
1. Deploy Raft storage adapter alongside existing BadgerDB adapter
2. Use feature flag `STORAGE_BACKEND=raft` to enable
3. Run in shadow mode (writes to both, reads from BadgerDB)
4. Monitor Raft cluster health metrics

### Phase 2: Gradual Migration (Week 2)
1. Switch read traffic to Raft for 10% of requests
2. Monitor latency and error rates
3. Gradually increase to 50%, then 100%
4. Keep BadgerDB as fallback

### Phase 3: Complete Cutover (Week 3)
1. Remove BadgerDB adapter code
2. Update all configurations to use Raft
3. Remove feature flags
4. Archive BadgerDB data for rollback

### Phase 4: Cleanup (Week 4)
1. Remove old BadgerDB dependencies
2. Update documentation
3. Remove migration code
4. Performance optimization

## 🔙 Rollback Plan

If issues arise:

1. **Immediate Rollback** (< 5 minutes):
   - Switch feature flag back to `STORAGE_BACKEND=badger`
   - All traffic returns to BadgerDB

2. **Data Recovery**:
   - Export Raft data using snapshot
   - Import into fresh BadgerDB instance
   - Verify data integrity

3. **Cluster Issues**:
   - If Raft cluster fails, single-node fallback mode
   - Reconstruct cluster from latest snapshot

## ✅ Success Criteria

### Functional Requirements
- [ ] All storage operations work through Raft consensus
- [ ] Three-node cluster maintains quorum with one failure
- [ ] Snapshots created every 2 minutes
- [ ] Leader election completes within 5 seconds
- [ ] No data loss during leader changes

### Performance Requirements
- [ ] Put latency < 50ms (p99)
- [ ] Get latency < 10ms (p99)
- [ ] Batch operations < 100ms for 100 items
- [ ] Cluster can handle 1000 ops/sec

### Testing Requirements
- [ ] Unit test coverage > 80%
- [ ] Integration tests pass
- [ ] Load tests show no degradation
- [ ] Chaos testing survives network partitions

## 📊 Monitoring and Metrics

Add these metrics to your monitoring system:

```go
type RaftMetrics struct {
    IsLeader           bool
    Term               uint64
    LastLogIndex       uint64
    CommitIndex        uint64
    AppliedIndex       uint64
    NumPeers           int
    SnapshotCount      int64
    LastSnapshotTime   time.Time
    ApplyLatency       time.Duration
    CommitLatency      time.Duration
    LeaderChanges      int64
}
```

## 🚨 Critical Notes

1. **NO COMMENTS** in code except `TODO:` with specific implementation details
2. **TEST EVERYTHING** - Run full test suite after each change
3. **USE HASHICORP/RAFT** - Do not implement consensus yourself
4. **REPLACE COMPLETELY** - Remove all BadgerDB direct usage
5. **FOLLOW INTERFACES** - Implement ports.StoragePort exactly

## 📅 Timeline

- Day 1-2: Implement core Raft storage adapter
- Day 3: Write comprehensive tests
- Day 4: Integration testing with engine
- Day 5: Performance testing and optimization
- Day 6: Documentation and deployment guide
- Day 7: Code review and fixes

## 🎯 Definition of Done

- [ ] All code follows CLAUDE.md and CODING_GUIDELINES.md
- [ ] Zero comments except actionable TODOs
- [ ] All tests pass
- [ ] Integration with engine verified
- [ ] Performance benchmarks met
- [ ] Old adapter completely removed
- [ ] Documentation updated
- [ ] Deployed to test environment

---

**REMINDER**: This implementation MUST follow all rules in CLAUDE.md and CODING_GUIDELINES.md. Any deviation will result in rejection. Test everything, use existing packages, and include NO comments except actionable TODOs.