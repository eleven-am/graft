package ports

import "context"

type LoadBalancer interface {
	Start(ctx context.Context) error
	Stop() error

	ShouldExecuteNode(nodeID string, workflowID string, nodeName string) (bool, error)

	StartDraining() error
	StopDraining() error
	IsDraining() bool
	WaitForDraining(ctx context.Context) error
}

type LoadSink interface {
	ReceiveLoadUpdate(update LoadUpdate) error
}

type NodeLoad struct {
	NodeID          string             `json:"node_id"`
	TotalWeight     float64            `json:"total_weight"`
	ExecutionUnits  map[string]float64 `json:"execution_units"`
	RecentLatencyMs float64            `json:"recent_latency_ms"`
	RecentErrorRate float64            `json:"recent_error_rate"`
	LastUpdated     int64              `json:"last_updated"`
}
