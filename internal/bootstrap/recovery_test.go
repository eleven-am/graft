package bootstrap

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

type mockLogStore struct {
	logs       map[uint64]*raft.Log
	firstIndex uint64
	lastIndex  uint64
	currentTerm uint64
	getLogErr  error
	storeErr   error
	deleteErr  error
}

func newMockLogStore() *mockLogStore {
	return &mockLogStore{
		logs: make(map[uint64]*raft.Log),
	}
}

func (m *mockLogStore) FirstIndex() (uint64, error) {
	return m.firstIndex, nil
}

func (m *mockLogStore) LastIndex() (uint64, error) {
	return m.lastIndex, nil
}

func (m *mockLogStore) GetLog(index uint64, log *raft.Log) error {
	if m.getLogErr != nil {
		return m.getLogErr
	}
	if l, ok := m.logs[index]; ok {
		*log = *l
		return nil
	}
	return raft.ErrLogNotFound
}

func (m *mockLogStore) StoreLog(log *raft.Log) error {
	if m.storeErr != nil {
		return m.storeErr
	}
	m.logs[log.Index] = log
	if log.Index > m.lastIndex {
		m.lastIndex = log.Index
	}
	if m.firstIndex == 0 || log.Index < m.firstIndex {
		m.firstIndex = log.Index
	}
	return nil
}

func (m *mockLogStore) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		if err := m.StoreLog(log); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockLogStore) DeleteRange(min, max uint64) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	for i := min; i <= max; i++ {
		delete(m.logs, i)
	}
	if min <= m.firstIndex && max >= m.lastIndex {
		m.firstIndex = 0
		m.lastIndex = 0
	}
	return nil
}

func (m *mockLogStore) GetUint64(key []byte) (uint64, error) {
	if string(key) == "CurrentTerm" {
		return m.currentTerm, nil
	}
	return 0, errors.New("key not found")
}

func (m *mockLogStore) SetLogs(logs ...*raft.Log) {
	for _, log := range logs {
		m.logs[log.Index] = log
		if log.Index > m.lastIndex {
			m.lastIndex = log.Index
		}
		if m.firstIndex == 0 || log.Index < m.firstIndex {
			m.firstIndex = log.Index
		}
	}
}

type mockSnapshotStore struct {
	snapshots []*raft.SnapshotMeta
	listErr   error
	openErr   error
}

func newMockSnapshotStore() *mockSnapshotStore {
	return &mockSnapshotStore{
		snapshots: make([]*raft.SnapshotMeta, 0),
	}
}

func (m *mockSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.snapshots, nil
}

func (m *mockSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	if m.openErr != nil {
		return nil, nil, m.openErr
	}
	for _, snap := range m.snapshots {
		if snap.ID == id {
			return snap, io.NopCloser(&mockReader{}), nil
		}
	}
	return nil, nil, errors.New("snapshot not found")
}

func (m *mockSnapshotStore) AddSnapshot(meta *raft.SnapshotMeta) {
	m.snapshots = append([]*raft.SnapshotMeta{meta}, m.snapshots...)
}

type mockRecoverySnapshotStore struct {
	*mockSnapshotStore
	createErr   error
	createdMeta *SnapshotMeta
	writtenData []byte
	sinkID      string
}

func newMockRecoverySnapshotStore() *mockRecoverySnapshotStore {
	return &mockRecoverySnapshotStore{
		mockSnapshotStore: newMockSnapshotStore(),
		sinkID:            "mock-sink-id",
	}
}

func (m *mockRecoverySnapshotStore) CreateRecoverySink(meta *SnapshotMeta) (RecoverySnapshotSink, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}
	m.createdMeta = meta
	return &mockRecoverySnapshotSink{
		store: m,
		meta:  meta,
	}, nil
}

type mockRecoverySnapshotSink struct {
	store    *mockRecoverySnapshotStore
	meta     *SnapshotMeta
	data     []byte
	closed   bool
	canceled bool
	writeErr error
	closeErr error
}

func (s *mockRecoverySnapshotSink) Write(p []byte) (n int, err error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	s.data = append(s.data, p...)
	return len(p), nil
}

func (s *mockRecoverySnapshotSink) Close() error {
	if s.closeErr != nil {
		return s.closeErr
	}
	s.closed = true
	s.store.writtenData = s.data
	s.store.AddSnapshot(&raft.SnapshotMeta{
		ID:    s.store.sinkID,
		Index: s.meta.Index,
		Term:  s.meta.Term,
	})
	return nil
}

func (s *mockRecoverySnapshotSink) Cancel() error {
	s.canceled = true
	return nil
}

func (s *mockRecoverySnapshotSink) ID() string {
	return s.store.sinkID
}

type mockMetaStore struct {
	meta    *ClusterMeta
	saveErr error
	loadErr error
	exists  bool
}

func newMockMetaStore() *mockMetaStore {
	return &mockMetaStore{}
}

func (m *mockMetaStore) SaveMeta(meta *ClusterMeta) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.meta = meta
	m.exists = true
	return nil
}

func (m *mockMetaStore) LoadMeta() (*ClusterMeta, error) {
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	if m.meta == nil {
		return nil, ErrMetaNotFound
	}
	return m.meta, nil
}

func (m *mockMetaStore) Exists() (bool, error) {
	return m.exists, nil
}

type mockReader struct{}

func (m *mockReader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

type mockRecoveryTransport struct {
	fetchResult    []raft.Log
	fetchPrevIndex uint64
	fetchPrevTerm  uint64
	fetchErr       error
	fetchCalls     int
	fetchFn        func(fromIdx, toIdx uint64) ([]raft.Log, uint64, uint64, error)
	getTermResult  uint64
	getTermErr     error
	getTermFn      func(index uint64) (uint64, error)
	snapshotStream SnapshotStream
	snapshotErr    error
	clusterMeta    *ClusterMeta
	metaErr        error
}

func newMockRecoveryTransport() *mockRecoveryTransport {
	return &mockRecoveryTransport{}
}

func (m *mockRecoveryTransport) FetchLogEntries(_ context.Context, _ raft.ServerAddress, fromIdx, toIdx uint64) ([]raft.Log, uint64, uint64, error) {
	m.fetchCalls++
	if m.fetchFn != nil {
		return m.fetchFn(fromIdx, toIdx)
	}
	if m.fetchErr != nil {
		return nil, 0, 0, m.fetchErr
	}
	return m.fetchResult, m.fetchPrevIndex, m.fetchPrevTerm, nil
}

func (m *mockRecoveryTransport) GetLogTerm(_ context.Context, _ raft.ServerAddress, index uint64) (uint64, error) {
	if m.getTermFn != nil {
		return m.getTermFn(index)
	}
	if m.getTermErr != nil {
		return 0, m.getTermErr
	}
	return m.getTermResult, nil
}

func (m *mockRecoveryTransport) RequestSnapshot(_ context.Context, _ raft.ServerAddress) (SnapshotStream, error) {
	if m.snapshotErr != nil {
		return nil, m.snapshotErr
	}
	return m.snapshotStream, nil
}

func (m *mockRecoveryTransport) GetClusterMeta(_ context.Context, _ string) (*ClusterMeta, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return m.clusterMeta, nil
}

type mockSnapshotStream struct {
	meta     *SnapshotMeta
	data     []byte
	readIdx  int
	metaErr  error
	closeErr error
}

func (m *mockSnapshotStream) ReceiveMeta() (*SnapshotMeta, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return m.meta, nil
}

func (m *mockSnapshotStream) Read(p []byte) (n int, err error) {
	if m.readIdx >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.readIdx:])
	m.readIdx += n
	return n, nil
}

func (m *mockSnapshotStream) Close() error {
	return m.closeErr
}

func TestNewRecoveryManager(t *testing.T) {
	logStore := newMockLogStore()
	snapStore := newMockSnapshotStore()

	deps := RecoveryManagerDeps{
		LogStore:      logStore,
		SnapshotStore: snapStore,
	}

	manager := NewRecoveryManager(deps)

	if manager == nil {
		t.Fatal("NewRecoveryManager returned nil")
	}

	if manager.logStore != logStore {
		t.Error("logStore not set correctly")
	}

	if manager.snapshotStore != snapStore {
		t.Error("snapshotStore not set correctly")
	}
}

func TestRecoveryManager_ValidateWAL_Empty(t *testing.T) {
	logStore := newMockLogStore()
	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore: logStore,
	})

	ok, err := manager.validateWAL()
	if err != nil {
		t.Fatalf("validateWAL() error = %v", err)
	}
	if !ok {
		t.Error("validateWAL() should return true for empty WAL")
	}
}

func TestRecoveryManager_ValidateWAL_WithLogs(t *testing.T) {
	logStore := newMockLogStore()
	logStore.SetLogs(
		&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 2, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 3, Term: 2, Type: raft.LogCommand},
	)

	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore: logStore,
	})

	ok, err := manager.validateWAL()
	if err != nil {
		t.Fatalf("validateWAL() error = %v", err)
	}
	if !ok {
		t.Error("validateWAL() should return true for valid WAL")
	}
}

func TestRecoveryManager_ValidateWAL_Corrupted(t *testing.T) {
	logStore := newMockLogStore()
	logStore.lastIndex = 5
	logStore.getLogErr = errors.New("corruption detected")

	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore: logStore,
	})

	ok, err := manager.validateWAL()
	if err == nil {
		t.Fatal("validateWAL() should return error for corrupted WAL")
	}
	if ok {
		t.Error("validateWAL() should return false for corrupted WAL")
	}
}

func TestRecoveryManager_ValidateSnapshot_Empty(t *testing.T) {
	snapStore := newMockSnapshotStore()
	manager := NewRecoveryManager(RecoveryManagerDeps{
		SnapshotStore: snapStore,
	})

	ok, err := manager.validateSnapshot()
	if err != nil {
		t.Fatalf("validateSnapshot() error = %v", err)
	}
	if !ok {
		t.Error("validateSnapshot() should return true for no snapshots")
	}
}

func TestRecoveryManager_ValidateSnapshot_WithSnapshot(t *testing.T) {
	snapStore := newMockSnapshotStore()
	snapStore.AddSnapshot(&raft.SnapshotMeta{
		ID:    "snap-1",
		Index: 100,
		Term:  5,
	})

	manager := NewRecoveryManager(RecoveryManagerDeps{
		SnapshotStore: snapStore,
	})

	ok, err := manager.validateSnapshot()
	if err != nil {
		t.Fatalf("validateSnapshot() error = %v", err)
	}
	if !ok {
		t.Error("validateSnapshot() should return true for valid snapshot")
	}
}

func TestRecoveryManager_RecoverFromPeers_NoPeers(t *testing.T) {
	discovery := NewMockPeerDiscovery()
	manager := NewRecoveryManager(RecoveryManagerDeps{
		Discovery: discovery,
	})

	ctx := context.Background()
	err := manager.recoverFromPeers(ctx)
	if !errors.Is(err, ErrNoPeersForRecovery) {
		t.Errorf("recoverFromPeers() error = %v, want ErrNoPeersForRecovery", err)
	}
}

func TestRecoveryManager_FetchMissingEntries_Success(t *testing.T) {
	logStore := newMockLogStore()
	logStore.SetLogs(
		&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 2, Term: 1, Type: raft.LogCommand},
	)

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{
		{ServerID: "node-1", Address: "localhost:8001", Ordinal: 1},
	})

	transport := newMockRecoveryTransport()
	transport.fetchResult = []raft.Log{
		{Index: 3, Term: 2, Type: raft.LogCommand},
		{Index: 4, Term: 2, Type: raft.LogCommand},
	}
	transport.fetchPrevIndex = 2
	transport.fetchPrevTerm = 1

	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore:  logStore,
		Discovery: discovery,
		Transport: transport,
	})

	ctx := context.Background()
	err := manager.fetchMissingEntries(ctx, 3, 4)
	if err != nil {
		t.Fatalf("fetchMissingEntries() error = %v", err)
	}

	if len(logStore.logs) != 4 {
		t.Errorf("expected 4 logs, got %d", len(logStore.logs))
	}
}

func TestRecoveryManager_FetchMissingEntries_NoPeers(t *testing.T) {
	logStore := newMockLogStore()
	discovery := NewMockPeerDiscovery()

	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore:  logStore,
		Discovery: discovery,
	})

	ctx := context.Background()
	err := manager.fetchMissingEntries(ctx, 1, 10)
	if !errors.Is(err, ErrNoPeersForRecovery) {
		t.Errorf("fetchMissingEntries() error = %v, want ErrNoPeersForRecovery", err)
	}
}

func TestRecoveryManager_FindCommonAncestor(t *testing.T) {
	logStore := newMockLogStore()
	logStore.SetLogs(
		&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 2, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 3, Term: 2, Type: raft.LogCommand},
	)

	transport := newMockRecoveryTransport()
	transport.getTermResult = 1

	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore:  logStore,
		Transport: transport,
	})

	ctx := context.Background()
	peer := PeerInfo{ServerID: "node-1", Address: "localhost:8001"}

	commonIdx, err := manager.findCommonAncestor(ctx, peer, 2)
	if err != nil {
		t.Fatalf("findCommonAncestor() error = %v", err)
	}

	if commonIdx != 2 {
		t.Errorf("findCommonAncestor() = %d, want 2", commonIdx)
	}
}

func TestRecoveryManager_FetchSnapshotFromPeer_Success(t *testing.T) {
	data := []byte("snapshot data")
	checksum := sha256.Sum256(data)

	stream := &mockSnapshotStream{
		meta: &SnapshotMeta{
			Index:    100,
			Term:     5,
			Size:     int64(len(data)),
			Checksum: checksum[:],
		},
		data: data,
	}

	transport := newMockRecoveryTransport()
	transport.snapshotStream = stream

	manager := NewRecoveryManager(RecoveryManagerDeps{
		Transport: transport,
	})

	ctx := context.Background()
	peer := PeerInfo{ServerID: "node-1", Address: "localhost:8001"}

	err := manager.fetchSnapshotFromPeer(ctx, peer)
	if err != nil {
		t.Fatalf("fetchSnapshotFromPeer() error = %v", err)
	}
}

func TestRecoveryManager_FetchSnapshotFromPeer_ChecksumMismatch(t *testing.T) {
	data := []byte("snapshot data")
	wrongChecksum := sha256.Sum256([]byte("wrong data"))

	stream := &mockSnapshotStream{
		meta: &SnapshotMeta{
			Index:    100,
			Term:     5,
			Size:     int64(len(data)),
			Checksum: wrongChecksum[:],
		},
		data: data,
	}

	transport := newMockRecoveryTransport()
	transport.snapshotStream = stream

	manager := NewRecoveryManager(RecoveryManagerDeps{
		Transport: transport,
	})

	ctx := context.Background()
	peer := PeerInfo{ServerID: "node-1", Address: "localhost:8001"}

	err := manager.fetchSnapshotFromPeer(ctx, peer)
	if !errors.Is(err, ErrSnapshotChecksumMismatch) {
		t.Errorf("fetchSnapshotFromPeer() error = %v, want ErrSnapshotChecksumMismatch", err)
	}
}

func TestRecoveryManager_VerifySnapshotChecksum(t *testing.T) {
	manager := NewRecoveryManager(RecoveryManagerDeps{})

	data := []byte("test data")
	validChecksum := sha256.Sum256(data)
	invalidChecksum := sha256.Sum256([]byte("other data"))

	if !manager.verifySnapshotChecksum(data, validChecksum[:]) {
		t.Error("verifySnapshotChecksum() should return true for valid checksum")
	}

	if manager.verifySnapshotChecksum(data, invalidChecksum[:]) {
		t.Error("verifySnapshotChecksum() should return false for invalid checksum")
	}
}

func TestRecoveryManager_GetRecoveryStatus(t *testing.T) {
	logStore := newMockLogStore()
	logStore.SetLogs(
		&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 10, Term: 3, Type: raft.LogCommand},
	)

	snapStore := newMockSnapshotStore()
	snapStore.AddSnapshot(&raft.SnapshotMeta{
		ID:    "snap-1",
		Index: 50,
		Term:  2,
	})

	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore:      logStore,
		SnapshotStore: snapStore,
	})

	status := manager.GetRecoveryStatus()

	if status.WALFirstIndex != 1 {
		t.Errorf("WALFirstIndex = %d, want 1", status.WALFirstIndex)
	}
	if status.WALLastIndex != 10 {
		t.Errorf("WALLastIndex = %d, want 10", status.WALLastIndex)
	}
	if status.LatestSnapshotIndex != 50 {
		t.Errorf("LatestSnapshotIndex = %d, want 50", status.LatestSnapshotIndex)
	}
	if status.LatestSnapshotTerm != 2 {
		t.Errorf("LatestSnapshotTerm = %d, want 2", status.LatestSnapshotTerm)
	}
}

func TestUnrecoverableError(t *testing.T) {
	walErr := errors.New("wal error")
	snapErr := errors.New("snapshot error")
	peerErr := errors.New("peer error")

	err := &UnrecoverableError{
		WALError:      walErr,
		SnapshotError: snapErr,
		PeerError:     peerErr,
	}

	expected := "unrecoverable state: WAL error=wal error, snapshot error=snapshot error, peer recovery error=peer error"
	if err.Error() != expected {
		t.Errorf("UnrecoverableError.Error() = %q, want %q", err.Error(), expected)
	}

	if !errors.Is(err.Unwrap(), peerErr) {
		t.Error("Unwrap() should return peerErr when set")
	}
}

func TestLogIndexMismatchError(t *testing.T) {
	err := &LogIndexMismatchError{
		Expected: 10,
		Got:      15,
		Peer:     "node-1",
	}

	expected := "log index mismatch from peer node-1: expected 10, got 15"
	if err.Error() != expected {
		t.Errorf("LogIndexMismatchError.Error() = %q, want %q", err.Error(), expected)
	}
}

func TestTermRegressionError(t *testing.T) {
	err := &TermRegressionError{
		Index:        5,
		PreviousTerm: 3,
		CurrentTerm:  2,
		Peer:         "node-1",
	}

	expected := "term regression at index 5 from peer node-1: term went from 3 to 2"
	if err.Error() != expected {
		t.Errorf("TermRegressionError.Error() = %q, want %q", err.Error(), expected)
	}
}

func TestLogConflictError(t *testing.T) {
	err := &LogConflictError{
		Index:          10,
		LocalTerm:      3,
		ExpectedTerm:   4,
		Peer:           "node-1",
		ConflictAction: "truncate_and_retry",
	}

	expected := "log conflict at index 10: local term 3 != expected term 4 from peer node-1 (action: truncate_and_retry)"
	if err.Error() != expected {
		t.Errorf("LogConflictError.Error() = %q, want %q", err.Error(), expected)
	}
}

func TestRecoveryManager_StoreEntriesWithValidation_IndexMismatch(t *testing.T) {
	logStore := newMockLogStore()
	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore: logStore,
	})

	entries := []raft.Log{
		{Index: 5, Term: 1, Type: raft.LogCommand},
	}
	peer := PeerInfo{ServerID: "node-1"}

	err := manager.storeEntriesWithValidation(entries, peer, 1, 1)
	var idxErr *LogIndexMismatchError
	if !errors.As(err, &idxErr) {
		t.Errorf("expected LogIndexMismatchError, got %T", err)
	}
}

func TestRecoveryManager_StoreEntriesWithValidation_TermRegression(t *testing.T) {
	logStore := newMockLogStore()
	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore: logStore,
	})

	entries := []raft.Log{
		{Index: 2, Term: 1, Type: raft.LogCommand},
	}
	peer := PeerInfo{ServerID: "node-1"}

	err := manager.storeEntriesWithValidation(entries, peer, 1, 5)
	var termErr *TermRegressionError
	if !errors.As(err, &termErr) {
		t.Errorf("expected TermRegressionError, got %T", err)
	}
}

func TestRecoveryManager_FetchMissingEntries_TruncationWithRealignment(t *testing.T) {
	logStore := newMockLogStore()
	logStore.SetLogs(
		&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 2, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 3, Term: 2, Type: raft.LogCommand},
		&raft.Log{Index: 4, Term: 2, Type: raft.LogCommand},
		&raft.Log{Index: 5, Term: 3, Type: raft.LogCommand},
	)

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{
		{ServerID: "node-1", Address: "localhost:8001", Ordinal: 1},
	})

	fetchCallCount := 0
	transport := newMockRecoveryTransport()
	transport.fetchFn = func(fromIdx, toIdx uint64) ([]raft.Log, uint64, uint64, error) {
		fetchCallCount++
		if fetchCallCount == 1 {
			return []raft.Log{
				{Index: 6, Term: 4, Type: raft.LogCommand},
				{Index: 7, Term: 4, Type: raft.LogCommand},
			}, 5, 4, nil
		}
		return []raft.Log{
			{Index: 3, Term: 3, Type: raft.LogCommand},
			{Index: 4, Term: 3, Type: raft.LogCommand},
			{Index: 5, Term: 4, Type: raft.LogCommand},
			{Index: 6, Term: 4, Type: raft.LogCommand},
			{Index: 7, Term: 4, Type: raft.LogCommand},
		}, 2, 1, nil
	}

	transport.getTermFn = func(index uint64) (uint64, error) {
		switch index {
		case 1, 2:
			return 1, nil
		case 3, 4:
			return 3, nil
		case 5:
			return 4, nil
		default:
			return 4, nil
		}
	}

	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore:  logStore,
		Discovery: discovery,
		Transport: transport,
	})

	ctx := context.Background()
	err := manager.fetchMissingEntries(ctx, 6, 7)
	if err != nil {
		t.Fatalf("fetchMissingEntries() error = %v", err)
	}

	if fetchCallCount != 2 {
		t.Errorf("expected 2 fetch calls (initial + after truncation), got %d", fetchCallCount)
	}

	if len(logStore.logs) != 7 {
		t.Errorf("expected 7 logs after reconciliation, got %d", len(logStore.logs))
	}

	for i := uint64(3); i <= 5; i++ {
		log, ok := logStore.logs[i]
		if !ok {
			t.Errorf("missing log at index %d", i)
			continue
		}
		if i < 5 && log.Term != 3 {
			t.Errorf("log at index %d should have term 3 (from peer), got %d", i, log.Term)
		}
		if i == 5 && log.Term != 4 {
			t.Errorf("log at index 5 should have term 4 (from peer), got %d", log.Term)
		}
	}
}

func TestRecoveryManager_FetchMissingEntries_RefetchMismatchSkipsPeer(t *testing.T) {
	logStore := newMockLogStore()
	logStore.SetLogs(
		&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 2, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 3, Term: 2, Type: raft.LogCommand},
	)

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{
		{ServerID: "node-1", Address: "localhost:8001", Ordinal: 1},
	})

	fetchCallCount := 0
	transport := newMockRecoveryTransport()
	transport.fetchFn = func(fromIdx, toIdx uint64) ([]raft.Log, uint64, uint64, error) {
		fetchCallCount++
		if fetchCallCount == 1 {
			return []raft.Log{
				{Index: 4, Term: 3, Type: raft.LogCommand},
			}, 3, 3, nil
		}
		return []raft.Log{
			{Index: 3, Term: 3, Type: raft.LogCommand},
			{Index: 4, Term: 3, Type: raft.LogCommand},
		}, 999, 999, nil
	}

	transport.getTermFn = func(index uint64) (uint64, error) {
		if index <= 2 {
			return 1, nil
		}
		return 2, nil
	}

	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore:  logStore,
		Discovery: discovery,
		Transport: transport,
	})

	ctx := context.Background()
	err := manager.fetchMissingEntries(ctx, 4, 4)

	if !errors.Is(err, ErrCannotFetchMissingEntries) {
		t.Errorf("expected ErrCannotFetchMissingEntries when refetch returns mismatched prevLogIndex, got %v", err)
	}
}

func TestRecoveryManager_FetchSnapshotFromPeer_PersistsSnapshot(t *testing.T) {
	data := []byte("snapshot data")
	checksum := sha256.Sum256(data)

	stream := &mockSnapshotStream{
		meta: &SnapshotMeta{
			Index:    100,
			Term:     5,
			Size:     int64(len(data)),
			Checksum: checksum[:],
		},
		data: data,
	}

	transport := newMockRecoveryTransport()
	transport.snapshotStream = stream

	recoverySnapStore := newMockRecoverySnapshotStore()

	manager := NewRecoveryManager(RecoveryManagerDeps{
		Transport:             transport,
		RecoverySnapshotStore: recoverySnapStore,
	})

	ctx := context.Background()
	peer := PeerInfo{ServerID: "node-1", Address: "localhost:8001"}

	err := manager.fetchSnapshotFromPeer(ctx, peer)
	if err != nil {
		t.Fatalf("fetchSnapshotFromPeer() error = %v", err)
	}

	if recoverySnapStore.createdMeta == nil {
		t.Fatal("snapshot was not persisted - CreateRecoverySink was not called")
	}

	if recoverySnapStore.createdMeta.Index != 100 {
		t.Errorf("persisted snapshot index = %d, want 100", recoverySnapStore.createdMeta.Index)
	}

	if recoverySnapStore.createdMeta.Term != 5 {
		t.Errorf("persisted snapshot term = %d, want 5", recoverySnapStore.createdMeta.Term)
	}

	if string(recoverySnapStore.writtenData) != string(data) {
		t.Errorf("persisted data = %q, want %q", recoverySnapStore.writtenData, data)
	}

	if manager.LastRecoveredSnapshot() == nil {
		t.Error("LastRecoveredSnapshot() should return the recovered snapshot meta")
	}

	if manager.LastRecoveredSnapshot().Index != 100 {
		t.Errorf("LastRecoveredSnapshot().Index = %d, want 100", manager.LastRecoveredSnapshot().Index)
	}
}

func TestRecoveryManager_SyncWALWithSnapshot(t *testing.T) {
	tests := []struct {
		name              string
		walLogs           []*raft.Log
		snapshotIndex     uint64
		expectLogsCleared bool
		expectLogCount    int
	}{
		{
			name:              "no WAL entries",
			walLogs:           nil,
			snapshotIndex:     100,
			expectLogsCleared: false,
			expectLogCount:    0,
		},
		{
			name: "WAL covered by snapshot",
			walLogs: []*raft.Log{
				{Index: 1, Term: 1},
				{Index: 2, Term: 1},
				{Index: 50, Term: 2},
			},
			snapshotIndex:     100,
			expectLogsCleared: true,
			expectLogCount:    0,
		},
		{
			name: "WAL extends beyond snapshot",
			walLogs: []*raft.Log{
				{Index: 1, Term: 1},
				{Index: 50, Term: 2},
				{Index: 100, Term: 3},
				{Index: 150, Term: 4},
			},
			snapshotIndex:     100,
			expectLogsCleared: true,
			expectLogCount:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logStore := newMockLogStore()
			if tt.walLogs != nil {
				logStore.SetLogs(tt.walLogs...)
			}

			manager := NewRecoveryManager(RecoveryManagerDeps{
				LogStore: logStore,
			})

			if tt.snapshotIndex > 0 {
				manager.lastRecoveredSnapshot = &SnapshotMeta{
					Index: tt.snapshotIndex,
					Term:  5,
				}
			}

			err := manager.syncWALWithSnapshot()
			if err != nil {
				t.Fatalf("syncWALWithSnapshot() error = %v", err)
			}

			if tt.expectLogsCleared && len(logStore.logs) != tt.expectLogCount {
				t.Errorf("expected %d logs remaining, got %d", tt.expectLogCount, len(logStore.logs))
			}
		})
	}
}

func TestRecoveryManager_PersistSnapshot_SizeMismatch(t *testing.T) {
	data := []byte("snapshot data")
	checksum := sha256.Sum256(data)

	stream := &mockSnapshotStream{
		meta: &SnapshotMeta{
			Index:    100,
			Term:     5,
			Size:     999,
			Checksum: checksum[:],
		},
		data: data,
	}

	transport := newMockRecoveryTransport()
	transport.snapshotStream = stream

	recoverySnapStore := newMockRecoverySnapshotStore()

	manager := NewRecoveryManager(RecoveryManagerDeps{
		Transport:             transport,
		RecoverySnapshotStore: recoverySnapStore,
	})

	ctx := context.Background()
	peer := PeerInfo{ServerID: "node-1", Address: "localhost:8001"}

	err := manager.fetchSnapshotFromPeer(ctx, peer)
	if err == nil {
		t.Fatal("expected error for size mismatch")
	}

	if manager.LastRecoveredSnapshot() != nil {
		t.Error("LastRecoveredSnapshot() should be nil on failure")
	}
}

func TestRecoveryManager_RecoverFromPeers_FullRecovery(t *testing.T) {
	data := []byte("snapshot data")
	checksum := sha256.Sum256(data)

	stream := &mockSnapshotStream{
		meta: &SnapshotMeta{
			Index:    100,
			Term:     5,
			Size:     int64(len(data)),
			Checksum: checksum[:],
		},
		data: data,
	}

	transport := newMockRecoveryTransport()
	transport.snapshotStream = stream
	transport.clusterMeta = &ClusterMeta{
		ClusterUUID: "test-cluster",
		State:       StateReady,
	}

	discovery := NewMockPeerDiscovery()
	discovery.SetHealthyPeers([]PeerInfo{
		{ServerID: "node-1", Address: "localhost:8001", Ordinal: 1},
	})

	logStore := newMockLogStore()
	logStore.SetLogs(
		&raft.Log{Index: 1, Term: 1},
		&raft.Log{Index: 2, Term: 1},
		&raft.Log{Index: 50, Term: 2},
	)

	recoverySnapStore := newMockRecoverySnapshotStore()
	metaStore := newMockMetaStore()

	manager := NewRecoveryManager(RecoveryManagerDeps{
		LogStore:              logStore,
		Discovery:             discovery,
		Transport:             transport,
		RecoverySnapshotStore: recoverySnapStore,
		MetaStore:             metaStore,
	})

	ctx := context.Background()
	err := manager.recoverFromPeers(ctx)
	if err != nil {
		t.Fatalf("recoverFromPeers() error = %v", err)
	}

	if recoverySnapStore.createdMeta == nil {
		t.Error("snapshot was not persisted")
	}

	if len(logStore.logs) != 0 {
		t.Errorf("expected WAL to be cleared, got %d logs", len(logStore.logs))
	}

	savedMeta, err := metaStore.LoadMeta()
	if err != nil {
		t.Fatalf("LoadMeta() error = %v", err)
	}
	if savedMeta.ClusterUUID != "test-cluster" {
		t.Errorf("cluster UUID = %q, want %q", savedMeta.ClusterUUID, "test-cluster")
	}
}
