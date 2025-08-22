package domain

import (
	"time"
)

type ClusterNode struct {
	ID          string
	Address     string
	Port        int
	IsLeader    bool
	Status      ClusterNodeStatus
	LastSeen    time.Time
	JoinedAt    time.Time
	Metadata    map[string]string
}

type ClusterNodeStatus string

const (
	ClusterNodeStatusActive   ClusterNodeStatus = "active"
	ClusterNodeStatusInactive ClusterNodeStatus = "inactive"
	ClusterNodeStatusJoining  ClusterNodeStatus = "joining"
	ClusterNodeStatusLeaving  ClusterNodeStatus = "leaving"
	ClusterNodeStatusFailed   ClusterNodeStatus = "failed"
)

type ClusterConfig struct {
	NodeID              string
	BindAddress         string
	BindPort            int
	AdvertiseAddress    string
	AdvertisePort       int
	DataDir             string
	MaxConcurrentNodes  int
	WorkerPoolSize      int
	EnableTLS           bool
	LogLevel            string
}