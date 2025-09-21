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

	eventsA := &mocks.MockEventManager{}
	eventsB := &mocks.MockEventManager{}
	for _, ev := range []*mocks.MockEventManager{eventsA, eventsB} {
		ev.On("OnNodeStarted", mock.AnythingOfType("func(*domain.NodeStartedEvent)")).Return(nil)
		ev.On("OnNodeCompleted", mock.AnythingOfType("func(*domain.NodeCompletedEvent)")).Return(nil)
		ev.On("OnNodeError", mock.AnythingOfType("func(*domain.NodeErrorEvent)")).Return(nil)
	}

	configA := &Config{
		FailurePolicy:   "fail-open",
		Algorithm:       domain.AlgorithmLeastConnections,
		PublishInterval: 50 * time.Millisecond,
		PublishDebounce: 10 * time.Millisecond,
	}
	configB := &Config{
		FailurePolicy: "fail-open",
		Algorithm:     domain.AlgorithmLeastConnections,
	}
	lbA := NewManager(eventsA, "node-a", nil, configA, nil)
	lbB := NewManager(eventsB, "node-b", nil, configB, nil)

	tp := &mocks.MockTransportPort{}
	tp.On("SendPublishLoad", mock.Anything, "b-addr", mock.AnythingOfType("ports.LoadUpdate")).
		Run(func(args mock.Arguments) {
			update := args.Get(2).(ports.LoadUpdate)

			_ = lbB.ReceiveLoadUpdate(update)
		}).
		Return(nil).Maybe()

	lbA.SetTransport(tp)
	lbA.SetPeerAddrProvider(func() []string { return []string{"b-addr"} })

	assert.NoError(t, lbA.Start(ctx))
	defer lbA.Stop()
	assert.NoError(t, lbB.Start(ctx))
	defer lbB.Stop()

	lbA.onNodeStarted(&domain.NodeStartedEvent{
		WorkflowID: "wf-123",
		NodeName:   "test-node",
		NodeID:     "node-a",
		StartedAt:  time.Now(),
	})

	time.Sleep(200 * time.Millisecond)

	clusterLoad := lbB.getInMemoryClusterLoad()
	assert.GreaterOrEqual(t, len(clusterLoad), 2, "B should see both nodes")

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

	cfgA := &Config{FailurePolicy: "fail-open", PublishInterval: 100 * time.Millisecond}
	lbA := NewManager(eventsA, "node-a", nil, cfgA, nil)
	lbB := NewManager(eventsB, "node-b", nil, &Config{FailurePolicy: "fail-open"}, nil)

	var sent int32
	tp := &mocks.MockTransportPort{}
	tp.On("SendPublishLoad", mock.Anything, "b-addr", mock.AnythingOfType("ports.LoadUpdate")).
		Run(func(args mock.Arguments) {

			_ = lbB.ReceiveLoadUpdate(args.Get(2).(ports.LoadUpdate))

			sent++
		}).
		Return(nil).Maybe()

	lbA.SetTransport(tp)
	lbA.SetPeerAddrProvider(func() []string { return []string{"b-addr"} })

	assert.NoError(t, lbA.Start(ctx))
	defer lbA.Stop()
	assert.NoError(t, lbB.Start(ctx))
	defer lbB.Stop()

	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		if sent > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, int(sent), 1, "expected at least one periodic publish")
}
