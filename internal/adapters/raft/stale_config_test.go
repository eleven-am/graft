package raft

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

func TestRuntime_IsStaleReconciliationMode_Default(t *testing.T) {
	t.Parallel()

	rt := NewRuntime(RuntimeDeps{})

	if rt.IsStaleReconciliationMode() {
		t.Fatal("expected stale reconciliation mode to be false by default")
	}
}

func TestRuntime_SetReconciliationState(t *testing.T) {
	t.Parallel()

	rt := NewRuntime(RuntimeDeps{})

	rt.SetReconciliationState("pending")
	rt.mu.RLock()
	state := rt.reconciliationState
	rt.mu.RUnlock()

	if state != "pending" {
		t.Fatalf("expected reconciliation state 'pending', got %s", state)
	}

	rt.SetReconciliationState("succeeded")
	rt.mu.RLock()
	state = rt.reconciliationState
	rt.mu.RUnlock()

	if state != "succeeded" {
		t.Fatalf("expected reconciliation state 'succeeded', got %s", state)
	}
}

func TestRuntime_GetStaleConfigInfo(t *testing.T) {
	t.Parallel()

	rt := NewRuntime(RuntimeDeps{})

	rt.mu.Lock()
	rt.staleConfigDetected = true
	rt.persistedMemberCount = 1
	rt.expectedMemberCount = 3
	rt.reconciliationState = "pending"
	rt.mu.Unlock()

	detected, persisted, expected, state := rt.GetStaleConfigInfo()

	if !detected {
		t.Fatal("expected stale config detected to be true")
	}
	if persisted != 1 {
		t.Fatalf("expected persisted count 1, got %d", persisted)
	}
	if expected != 3 {
		t.Fatalf("expected expected count 3, got %d", expected)
	}
	if state != "pending" {
		t.Fatalf("expected state 'pending', got %s", state)
	}
}

func TestRuntime_Health_IncludesStaleConfigInfo(t *testing.T) {
	t.Parallel()

	rt := NewRuntime(RuntimeDeps{})

	rt.mu.Lock()
	rt.staleConfigDetected = true
	rt.persistedMemberCount = 1
	rt.expectedMemberCount = 3
	rt.reconciliationState = "reconciling"
	rt.mu.Unlock()

	health := rt.Health()

	if !health.StaleConfigDetected {
		t.Fatal("expected health to report stale config detected")
	}
	if health.PersistedMemberCount != 1 {
		t.Fatalf("expected persisted member count 1, got %d", health.PersistedMemberCount)
	}
	if health.ExpectedMemberCount != 3 {
		t.Fatalf("expected member count 3, got %d", health.ExpectedMemberCount)
	}
	if health.ReconciliationState != "reconciling" {
		t.Fatalf("expected reconciliation state 'reconciling', got %s", health.ReconciliationState)
	}
}

func TestRuntime_Health_NoStaleConfig(t *testing.T) {
	t.Parallel()

	rt := NewRuntime(RuntimeDeps{})

	health := rt.Health()

	if health.StaleConfigDetected {
		t.Fatal("expected health to not report stale config detected")
	}
	if health.PersistedMemberCount != 0 {
		t.Fatalf("expected persisted member count 0, got %d", health.PersistedMemberCount)
	}
	if health.ExpectedMemberCount != 0 {
		t.Fatalf("expected member count 0, got %d", health.ExpectedMemberCount)
	}
	if health.ReconciliationState != "" {
		t.Fatalf("expected empty reconciliation state, got %s", health.ReconciliationState)
	}
}

func TestRuntime_StaleConfigDetection_SingleVsMultiNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		persistedCount int
		expectedCount  int
		shouldBeStale  bool
	}{
		{
			name:           "single node persisted, multi node expected",
			persistedCount: 1,
			expectedCount:  3,
			shouldBeStale:  true,
		},
		{
			name:           "single node persisted, single node expected",
			persistedCount: 1,
			expectedCount:  1,
			shouldBeStale:  false,
		},
		{
			name:           "multi node persisted, multi node expected",
			persistedCount: 3,
			expectedCount:  3,
			shouldBeStale:  false,
		},
		{
			name:           "two nodes persisted, three expected",
			persistedCount: 2,
			expectedCount:  3,
			shouldBeStale:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isStale := tc.persistedCount == 1 && tc.expectedCount > 1
			if isStale != tc.shouldBeStale {
				t.Fatalf("expected stale=%v for persisted=%d, expected=%d",
					tc.shouldBeStale, tc.persistedCount, tc.expectedCount)
			}
		})
	}
}

func TestMapLeadershipStateToNodeState_Reconciling(t *testing.T) {
	t.Parallel()

	result := mapLeadershipStateToNodeState(ports.RaftLeadershipReconciling)

	if result != ports.NodeCandidate {
		t.Fatalf("expected reconciling to map to NodeCandidate, got %v", result)
	}
}

func TestController_StartReconciliation_NoTransport(t *testing.T) {
	t.Parallel()

	mockRuntime := &mockNodeRuntime{
		staleMode:    true,
		localAddress: "node1:9000",
	}

	mockCoordinator := &mockCoordinator{
		eventCh: make(chan ports.BootstrapEvent, 10),
	}

	c, err := NewController(ControllerDeps{
		Runtime:     mockRuntime,
		Coordinator: mockCoordinator,
		Transport:   nil,
	})
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	if c.reconciler != nil {
		t.Fatal("expected reconciler to be nil when no transport")
	}
}

type mockNodeRuntime struct {
	staleMode           bool
	localAddress        string
	reconciliationState string
	clusterInfo         ports.ClusterInfo
}

func (m *mockNodeRuntime) Start(_ context.Context, _ domain.RaftControllerOptions) error {
	return nil
}
func (m *mockNodeRuntime) Stop() error { return nil }
func (m *mockNodeRuntime) Apply(_ domain.Command, _ time.Duration) (*domain.CommandResult, error) {
	return &domain.CommandResult{Success: true}, nil
}
func (m *mockNodeRuntime) Demote(_ context.Context, _ ports.RaftPeer) error { return nil }
func (m *mockNodeRuntime) LeadershipInfo() ports.RaftLeadershipInfo {
	return ports.RaftLeadershipInfo{}
}
func (m *mockNodeRuntime) ClusterInfo() ports.ClusterInfo {
	return m.clusterInfo
}
func (m *mockNodeRuntime) IsStaleReconciliationMode() bool {
	return m.staleMode
}
func (m *mockNodeRuntime) SetReconciliationState(state string) {
	m.reconciliationState = state
}
func (m *mockNodeRuntime) LocalAddress() string {
	return m.localAddress
}

type mockCoordinator struct {
	eventCh chan ports.BootstrapEvent
	started bool
}

func (m *mockCoordinator) Start(_ context.Context) error {
	m.started = true
	return nil
}
func (m *mockCoordinator) Stop() error { return nil }
func (m *mockCoordinator) Events() <-chan ports.BootstrapEvent {
	return m.eventCh
}
