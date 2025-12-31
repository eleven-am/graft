package bootstrap

import (
	"time"

	"github.com/hashicorp/raft"
)

type StaleStatus int

const (
	StaleStatusUnknown StaleStatus = iota
	StaleStatusSafe
	StaleStatusStale
	StaleStatusQuestionable
	StaleStatusSplitBrain
	StaleStatusSafeToUnfence
)

func (s StaleStatus) String() string {
	switch s {
	case StaleStatusUnknown:
		return "unknown"
	case StaleStatusSafe:
		return "safe"
	case StaleStatusStale:
		return "stale"
	case StaleStatusQuestionable:
		return "questionable"
	case StaleStatusSplitBrain:
		return "split_brain"
	case StaleStatusSafeToUnfence:
		return "safe_to_unfence"
	default:
		return "unknown"
	}
}

func (s StaleStatus) RequiresAction() bool {
	switch s {
	case StaleStatusStale, StaleStatusSplitBrain, StaleStatusSafeToUnfence:
		return true
	default:
		return false
	}
}

func (s StaleStatus) IsTerminal() bool {
	return s == StaleStatusStale || s == StaleStatusSplitBrain
}

type PeerProbeResult struct {
	ServerID     raft.ServerID
	Address      raft.ServerAddress
	Reachable    bool
	ClusterUUID  string
	FencingEpoch uint64
	Error        error
	ProbeTime    time.Time
}

type StaleCheckDetails struct {
	PersistedVoters   int
	ExpectedVoters    int
	ReachablePeers    int
	LocalFencingEpoch uint64
	LocalClusterUUID  string
	BootstrapTime     time.Time
	PeerResults       []PeerProbeResult
	Reason            string
	CheckTime         time.Time
}

func (d *StaleCheckDetails) CountReachable() int {
	count := 0
	for _, p := range d.PeerResults {
		if p.Reachable {
			count++
		}
	}
	return count
}

func (d *StaleCheckDetails) HasSplitBrain() (bool, *PeerProbeResult) {
	for i, p := range d.PeerResults {
		if p.Reachable && p.ClusterUUID != "" && p.ClusterUUID != d.LocalClusterUUID {
			return true, &d.PeerResults[i]
		}
	}
	return false, nil
}

func (d *StaleCheckDetails) HasHigherEpoch() (bool, *PeerProbeResult) {
	for i, p := range d.PeerResults {
		if p.Reachable && p.FencingEpoch > d.LocalFencingEpoch {
			return true, &d.PeerResults[i]
		}
	}
	return false, nil
}
