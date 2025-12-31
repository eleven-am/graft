package bootstrap

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/hashicorp/raft"
)

const CurrentMetaVersion uint32 = 1

type NodeState string

const (
	StateUninitialized NodeState = "uninitialized"
	StateBootstrapping NodeState = "bootstrapping"
	StateJoining       NodeState = "joining"
	StateReady         NodeState = "ready"
	StateRecovering    NodeState = "recovering"
	StateDegraded      NodeState = "degraded"
	StateFenced        NodeState = "fenced"
	StateAwaitingWipe  NodeState = "awaiting_wipe"
)

func (s NodeState) String() string {
	return string(s)
}

func (s NodeState) IsTerminal() bool {
	return s == StateFenced || s == StateAwaitingWipe
}

func (s NodeState) CanTransitionTo(target NodeState) bool {
	switch s {
	case StateUninitialized:
		return target == StateBootstrapping || target == StateJoining
	case StateBootstrapping:
		return target == StateReady || target == StateFenced
	case StateJoining:
		return target == StateReady || target == StateFenced
	case StateReady:
		return target == StateRecovering || target == StateDegraded || target == StateFenced
	case StateRecovering:
		return target == StateReady || target == StateFenced
	case StateDegraded:
		return target == StateReady || target == StateRecovering || target == StateFenced
	case StateFenced:
		return target == StateAwaitingWipe
	case StateAwaitingWipe:
		return target == StateUninitialized
	default:
		return false
	}
}

type ClusterMeta struct {
	Version       uint32             `json:"version"`
	ClusterUUID   string             `json:"cluster_uuid"`
	FencingToken  uint64             `json:"fencing_token"`
	FencingEpoch  uint64             `json:"fencing_epoch"`
	ServerID      raft.ServerID      `json:"server_id"`
	ServerAddress raft.ServerAddress `json:"server_address"`
	AdvertiseAddr raft.ServerAddress `json:"advertise_addr,omitempty"`
	Ordinal       int                `json:"ordinal"`
	BootstrapTime time.Time          `json:"bootstrap_time,omitempty"`
	JoinTime      time.Time          `json:"join_time,omitempty"`
	State         NodeState          `json:"state"`
	LastRaftIndex uint64             `json:"last_raft_index"`
	LastRaftTerm  uint64             `json:"last_raft_term"`
	Checksum      uint32             `json:"checksum"`
}

func (m *ClusterMeta) Clone() *ClusterMeta {
	if m == nil {
		return nil
	}
	copy := *m
	return &copy
}

func (m *ClusterMeta) ComputeChecksum() uint32 {
	h := crc32.NewIEEE()
	binary.Write(h, binary.BigEndian, m.Version)
	h.Write([]byte(m.ClusterUUID))
	binary.Write(h, binary.BigEndian, m.FencingToken)
	binary.Write(h, binary.BigEndian, m.FencingEpoch)
	h.Write([]byte(m.ServerID))
	h.Write([]byte(m.ServerAddress))
	h.Write([]byte(m.AdvertiseAddr))
	binary.Write(h, binary.BigEndian, int32(m.Ordinal))
	binary.Write(h, binary.BigEndian, m.BootstrapTime.UnixNano())
	binary.Write(h, binary.BigEndian, m.JoinTime.UnixNano())
	h.Write([]byte(m.State))
	binary.Write(h, binary.BigEndian, m.LastRaftIndex)
	binary.Write(h, binary.BigEndian, m.LastRaftTerm)
	return h.Sum32()
}

func (m *ClusterMeta) Validate() error {
	if m.Version == 0 {
		return fmt.Errorf("version must be set")
	}
	if m.Version > CurrentMetaVersion {
		return fmt.Errorf("unsupported meta version %d (max supported: %d)", m.Version, CurrentMetaVersion)
	}
	if m.ClusterUUID == "" && m.State != StateUninitialized {
		return fmt.Errorf("cluster_uuid is required except in uninitialized state")
	}
	if m.ServerID == "" {
		return fmt.Errorf("server_id is required")
	}
	if m.ServerAddress == "" {
		return fmt.Errorf("server_address is required")
	}
	if m.Ordinal < 0 {
		return fmt.Errorf("ordinal must be non-negative")
	}
	if m.State == "" {
		return fmt.Errorf("state is required")
	}
	return nil
}

func (m *ClusterMeta) VerifyChecksum() bool {
	return m.Checksum == m.ComputeChecksum()
}

func (m *ClusterMeta) UpdateChecksum() {
	m.Checksum = m.ComputeChecksum()
}

type VoterInfo struct {
	ServerID raft.ServerID      `json:"server_id"`
	Address  raft.ServerAddress `json:"address"`
	Ordinal  int                `json:"ordinal"`
}

func (v *VoterInfo) Validate() error {
	if v.ServerID == "" {
		return fmt.Errorf("server_id is required")
	}
	if v.Address == "" {
		return fmt.Errorf("address is required")
	}
	if v.Ordinal < 0 {
		return fmt.Errorf("ordinal must be non-negative")
	}
	return nil
}
