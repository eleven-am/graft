package load_balancer

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestTelemetryPublishAffectsPeerSelection simulates node A publishing load to node B
// and verifies B's selection prefers itself when A reports higher load.
func TestTelemetryPublishAffectsPeerSelection(t *testing.T) {
	ctx := context.Background()

	// Events (no-op handlers)
	eventsA := &mocks.MockEventManager{}
	eventsB := &mocks.MockEventManager{}
	for _, ev := range []*mocks.MockEventManager{eventsA, eventsB} {
		ev.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
		ev.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
		ev.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)
	}

	// Managers for node-a and node-b with explicit algorithm
	configA := &Config{
		FailurePolicy:   "fail-open",
		Algorithm:       domain.AlgorithmLeastConnections, // Use a simpler algorithm
		PublishInterval: 50 * time.Millisecond,
		PublishDebounce: 10 * time.Millisecond,
	}
	configB := &Config{
		FailurePolicy: "fail-open",
		Algorithm:     domain.AlgorithmLeastConnections,
	}
	lbA := NewManager(eventsA, "node-a", nil, configA, nil)
	lbB := NewManager(eventsB, "node-b", nil, configB, nil)

	// Mock transport for A; when A publishes to B, directly deliver to B's ReceiveLoadUpdate
	tp := &mocks.MockTransportPort{}
	tp.On("SendPublishLoad", mock.Anything, "b-addr", mock.AnythingOfType("ports.LoadUpdate")).
		Run(func(args mock.Arguments) {
			update := args.Get(2).(ports.LoadUpdate)
			// Deliver update to B as if received via RPC
			_ = lbB.ReceiveLoadUpdate(update)
		}).
		Return(nil).Maybe()

	// Wire transport and peer provider into A
	lbA.SetTransport(tp)
	lbA.SetPeerAddrProvider(func() []string { return []string{"b-addr"} })

	// Start managers to enable publisherLoop on A
	assert.NoError(t, lbA.Start(ctx))
	defer lbA.Stop()
	assert.NoError(t, lbB.Start(ctx))
	defer lbB.Stop()

	// Make A appear busy: start one workflow on A -> increases weight and triggers publish
	lbA.onNodeStarted(&domain.NodeStartedEvent{
		WorkflowID: "wf-123",
		NodeName:   "test-node",
		NodeID:     "node-a",
		StartedAt:  time.Now(),
	})

	// Wait for debounce + publish to occur
	time.Sleep(200 * time.Millisecond)

	// Verify that B has received A's load update
	clusterLoad := lbB.getInMemoryClusterLoad()
	assert.GreaterOrEqual(t, len(clusterLoad), 2, "B should see both nodes")

	// Now B should see A's higher load via ReceiveLoadUpdate and prefer itself
	// Since A has 1 active workflow and B has 0, B should be selected for new work
	should, err := lbB.ShouldExecuteNode("node-b", "wf-decision", "test-node")
	assert.NoError(t, err)
	assert.True(t, should, "node-b should be selected given node-a reports higher load")
}

// Test periodic publisher tick sends updates even without event triggers.
func TestTelemetryPeriodicPublish(t *testing.T) {
	ctx := context.Background()

	eventsA := &mocks.MockEventManager{}
	eventsB := &mocks.MockEventManager{}
	for _, ev := range []*mocks.MockEventManager{eventsA, eventsB} {
		ev.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
		ev.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
		ev.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)
	}

	// Use a short publish interval for fast test
	cfgA := &Config{FailurePolicy: "fail-open", PublishInterval: 100 * time.Millisecond}
	lbA := NewManager(eventsA, "node-a", nil, cfgA, nil)
	lbB := NewManager(eventsB, "node-b", nil, &Config{FailurePolicy: "fail-open"}, nil)

	var sent int32
	tp := &mocks.MockTransportPort{}
	tp.On("SendPublishLoad", mock.Anything, "b-addr", mock.AnythingOfType("ports.LoadUpdate")).
		Run(func(args mock.Arguments) {
			// Deliver to B to simulate RPC
			_ = lbB.ReceiveLoadUpdate(args.Get(2).(ports.LoadUpdate))
			// Count calls
			// not using sync/atomic; test is single-threaded for assertions
			sent++
		}).
		Return(nil).Maybe()

	lbA.SetTransport(tp)
	lbA.SetPeerAddrProvider(func() []string { return []string{"b-addr"} })

	assert.NoError(t, lbA.Start(ctx))
	defer lbA.Stop()
	assert.NoError(t, lbB.Start(ctx))
	defer lbB.Stop()

	// Wait up to ~600ms for at least one publish
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		if sent > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, int(sent), 1, "expected at least one periodic publish")
}
