package ports

import (
	"context"
	"time"
)

type TransportPort interface {
	Start(ctx context.Context, config TransportConfig) error
	Stop() error
	JoinCluster(ctx context.Context, req JoinRequest) (*JoinResponse, error)
	LeaveCluster(ctx context.Context, req LeaveRequest) (*LeaveResponse, error)
	GetLeader(ctx context.Context) (*LeaderInfo, error)
	ForwardToLeader(ctx context.Context, req ForwardRequest) (*ForwardResponse, error)
	ForwardToNode(ctx context.Context, nodeID string, req ForwardRequest) (*ForwardResponse, error)
}

type TransportConfig struct {
	BindAddress string
	BindPort    int
	TLS         *TLSConfig
}

type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
	CAFile   string
}

type JoinRequest struct {
	NodeID   string
	Address  string
	Metadata map[string]string
}

type JoinResponse struct {
	Success bool
	Leader  string
	Error   string
}

type LeaveRequest struct {
	NodeID string
}

type LeaveResponse struct {
	Success bool
	Error   string
}

type LeaderInfo struct {
	NodeID  string
	Address string
}

type ForwardRequest struct {
	Type    string
	Payload []byte
}

type ForwardResponse struct {
	Success bool
	Result  []byte
	Error   string
}

type PeerTracker interface {
	GetPeers() map[string]PeerInfo
}

type PeerInfo struct {
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	IsHealthy bool      `json:"is_healthy"`
	IsLeader  bool      `json:"is_leader"`
	LastSeen  time.Time `json:"last_seen"`
}