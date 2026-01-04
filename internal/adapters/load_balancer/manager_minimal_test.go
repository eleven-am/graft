package load_balancer

import (
	"testing"

	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/mock"
)

// Test that least-active selection prefers the node with fewer active executions
func TestLeastActiveSelection(t *testing.T) {
	events := &mocks.MockEventManager{}

	events.On("OnNodeStarted", mock.Anything).Return(nil)
	events.On("OnNodeCompleted", mock.Anything).Return(nil)
	events.On("OnNodeError", mock.Anything).Return(nil)

	cfg := &Config{}
	m := NewManager(events, "node-a", nil, cfg, nil)

	m.executionUnits["wf-1"] = 1
	m.totalWeight = 1

	_ = m.ReceiveLoadUpdate(ports.LoadUpdate{NodeID: "node-b", ActiveWorkflows: 2, TotalWeight: 2, RecentLatencyMs: 0, RecentErrorRate: 0, Pressure: 0})

	should, err := m.ShouldExecuteNode("node-a", "wf-2", "n1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !should {
		t.Fatalf("expected local node to execute when it has fewer actives")
	}

	m.executionUnits = map[string]float64{"wf-1": 1, "wf-2": 1}
	m.totalWeight = 2
	_ = m.ReceiveLoadUpdate(ports.LoadUpdate{NodeID: "node-b", ActiveWorkflows: 1, TotalWeight: 1, Pressure: 0})

	should, err = m.ShouldExecuteNode("node-a", "wf-3", "n1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if should {
		t.Fatalf("expected peer to execute when local has more actives")
	}
}

// Test that tie-break falls back to lexicographic NodeID when pressures are equal/unknown
func TestTieBreakLexOrder(t *testing.T) {
	events := &mocks.MockEventManager{}
	events.On("OnNodeStarted", mock.Anything).Return(nil)
	events.On("OnNodeCompleted", mock.Anything).Return(nil)
	events.On("OnNodeError", mock.Anything).Return(nil)

	cfg := &Config{}

	m := NewManager(events, "node-a", nil, cfg, nil)
	m.pressureFn = func() float64 { return 0 }

	m.executionUnits["wf-1"] = 1
	m.totalWeight = 1
	_ = m.ReceiveLoadUpdate(ports.LoadUpdate{NodeID: "node-b", ActiveWorkflows: 1, TotalWeight: 1, Pressure: 0})

	should, err := m.ShouldExecuteNode("node-a", "wf-2", "n1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !should {
		t.Fatalf("expected local node to win lexicographic tie-break")
	}
}

// Test that pressure tie-break prefers the node with lower pressure when active counts are equal
func TestPressureTieBreakPrefersLowerPressure(t *testing.T) {
	events := &mocks.MockEventManager{}
	events.On("OnNodeStarted", mock.Anything).Return(nil)
	events.On("OnNodeCompleted", mock.Anything).Return(nil)
	events.On("OnNodeError", mock.Anything).Return(nil)

	cfg := &Config{}
	m := NewManager(events, "node-a", nil, cfg, nil)

	m.executionUnits["wf-1"] = 1
	m.totalWeight = 1

	m.pressureFn = func() float64 { return 1.0 }
	_ = m.ReceiveLoadUpdate(ports.LoadUpdate{NodeID: "node-b", ActiveWorkflows: 1, TotalWeight: 1, Pressure: 0.0})

	should, err := m.ShouldExecuteNode("node-a", "wf-2", "n1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if should {
		t.Fatalf("expected peer to win due to lower pressure in tie")
	}
}

func TestConnectorLoadInfluencesScheduling(t *testing.T) {
	events := &mocks.MockEventManager{}
	events.On("OnNodeStarted", mock.Anything).Return(nil)
	events.On("OnNodeCompleted", mock.Anything).Return(nil)
	events.On("OnNodeError", mock.Anything).Return(nil)

	cfg := &Config{}
	m := NewManager(events, "node-a", nil, cfg, nil)
	m.pressureFn = func() float64 { return 0 }

	if err := m.RegisterConnectorLoad("conn-1", 1); err != nil {
		t.Fatalf("unexpected error registering connector load: %v", err)
	}

	_ = m.ReceiveLoadUpdate(ports.LoadUpdate{NodeID: "node-b", ActiveWorkflows: 0, TotalWeight: 0, Pressure: 0})

	should, err := m.ShouldExecuteNode("node-a", "wf-connector", "connector-node")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if should {
		t.Fatalf("expected peer to execute when local node already owns connector load")
	}

	if err := m.DeregisterConnectorLoad("conn-1"); err != nil {
		t.Fatalf("unexpected error deregistering connector load: %v", err)
	}

	should, err = m.ShouldExecuteNode("node-a", "wf-connector", "connector-node")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !should {
		t.Fatalf("expected local node to execute once connector load removed")
	}
}
