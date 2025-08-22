package ports

import (
	"context"
	"github.com/eleven-am/graft/internal/domain"
)

type ClusterPort interface {
	RegisterNode(node NodePort) error
	ProcessTrigger(trigger WorkflowTrigger) error
	GetWorkflowStatus(workflowID string) (*WorkflowStatus, error)
	Start(ctx context.Context) error
	Stop() error
	OnComplete(handler CompletionHandler)
	OnError(handler ErrorHandler)
	GetClusterInfo() ClusterInfo
}

type CompletionHandler func(ctx context.Context, data domain.WorkflowCompletionData) error
type ErrorHandler func(workflowID string, finalState interface{}, err error)

type ClusterInfo struct {
	NodeID          string          `json:"node_id"`
	RegisteredNodes []string        `json:"registered_nodes"`
	ActiveWorkflows int64           `json:"active_workflows"`
	ResourceLimits  ResourceConfig  `json:"resource_limits"`
	ExecutionStats  ExecutionStats  `json:"execution_stats"`
	EngineMetrics   EngineMetrics   `json:"engine_metrics"`
	ClusterMembers  []ClusterMember `json:"cluster_members"`
	IsLeader        bool            `json:"is_leader"`
}

type ClusterMember struct {
	NodeID   string `json:"node_id"`
	Address  string `json:"address"`
	Status   string `json:"status"`
	IsLeader bool   `json:"is_leader"`
}
