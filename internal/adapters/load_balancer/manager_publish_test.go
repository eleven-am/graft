package load_balancer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestManagerPublishLocalMetricsUsesManagedContext(t *testing.T) {
	events := &mocks.MockEventManager{}
	events.On("OnNodeStarted", mock.Anything).Return(nil)
	events.On("OnNodeCompleted", mock.Anything).Return(nil)
	events.On("OnNodeError", mock.Anything).Return(nil)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(events, "node-a", nil, &Config{}, logger)
	mgr.executionUnits["wf-1"] = 1
	mgr.totalWeight = 1
	mgr.recentLatencyMs = 25
	mgr.pressureFn = func() float64 { return 0.5 }

	tp := mocks.NewMockTransportPort(t)
	defer tp.AssertExpectations(t)
	mgr.transport = tp
	mgr.getPeerAddrs = func() []string { return []string{"addr1", "addr2"} }
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())

	calls := 0
	tp.EXPECT().SendPublishLoad(mock.Anything, "addr1", mock.Anything).Run(func(ctx context.Context, _ string, _ ports.LoadUpdate) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatalf("expected context with deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > time.Second {
			t.Fatalf("unexpected timeout window: %v", remaining)
		}
	}).Return(errors.New("send failed"))
	tp.EXPECT().SendPublishLoad(mock.Anything, "addr2", mock.Anything).Run(func(ctx context.Context, _ string, _ ports.LoadUpdate) {
		calls++
		if ctx.Err() != nil {
			t.Fatalf("expected active context for second publish")
		}
	}).Return(nil)

	mgr.publishLocalMetrics()
	require.Equal(t, 2, calls)

	mgr.cancel()
	mgr.publishLocalMetrics()
	require.Equal(t, 2, calls, "no additional publishes expected after cancellation")
}

func TestManagerConvertToNodeMetrics(t *testing.T) {
	mgr := NewManager(nil, "node-a", nil, &Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	now := time.Now().Unix()
	cluster := map[string]*ports.NodeLoad{
		"node-b": {
			NodeID:          "node-b",
			TotalWeight:     3.5,
			ExecutionUnits:  map[string]float64{"wf-1": 1.5, "wf-2": 2},
			RecentLatencyMs: 42,
			RecentErrorRate: 0.25,
			LastUpdated:     now,
		},
		"node-c": {
			NodeID:          "node-c",
			TotalWeight:     0,
			ExecutionUnits:  map[string]float64{},
			RecentLatencyMs: 5,
			RecentErrorRate: 0,
			LastUpdated:     0,
		},
	}

	metrics := mgr.convertToNodeMetrics(cluster)
	require.Len(t, metrics, 2)
	require.Equal(t, "node-b", metrics[0].NodeID)
	require.Equal(t, 2, metrics[0].ActiveWorkflows)
	require.InDelta(t, 3.5, metrics[0].Pressure, 0.0001)
	require.Equal(t, true, metrics[0].Available)
	require.Equal(t, "node-c", metrics[1].NodeID)
	require.Equal(t, 0, metrics[1].ActiveWorkflows)
	require.Equal(t, false, metrics[1].Available)
}
