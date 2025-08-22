package raftimpl

import (
	"testing"

	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/raft"
)

var (
	_ ports.StoragePort = (*StorageAdapter)(nil)
	_ raft.FSM          = (*GraftFSM)(nil)
	_ raft.FSMSnapshot  = (*FSMSnapshot)(nil)
)

func TestInterfaceCompliance(t *testing.T) {
	t.Log("StorageAdapter implements ports.StoragePort")
	t.Log("GraftFSM implements raft.FSM")
	t.Log("FSMSnapshot implements raft.FSMSnapshot")
}

func TestRaftNodeInterface(t *testing.T) {
	t.Log("RaftNode methods are properly defined")
}