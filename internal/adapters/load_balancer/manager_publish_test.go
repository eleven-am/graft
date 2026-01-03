package load_balancer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/mocks"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestManagerPublishLocalMetricsUsesBroadcaster(t *testing.T) {
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
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())

	var broadcasted []byte
	mgr.broadcaster = func(msg []byte) {
		broadcasted = msg
	}

	mgr.publishLocalMetrics()

	require.NotNil(t, broadcasted, "expected broadcaster to be called")

	var update ports.LoadUpdate
	err := json.Unmarshal(broadcasted, &update)
	require.NoError(t, err)
	require.Equal(t, "node-a", update.NodeID)
	require.Equal(t, 1, update.ActiveWorkflows)
	require.InDelta(t, 1.0, update.TotalWeight, 0.0001)
	require.InDelta(t, 25.0, update.RecentLatencyMs, 0.0001)
	require.InDelta(t, 0.5, update.Pressure, 0.0001)

	mgr.cancel()
	broadcasted = nil
	mgr.publishLocalMetrics()
	require.Nil(t, broadcasted, "no broadcast expected after context cancellation")
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
