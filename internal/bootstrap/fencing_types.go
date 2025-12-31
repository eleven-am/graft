package bootstrap

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

type FencingMode string

const (
	FencingModePrimary  FencingMode = "primary"
	FencingModeFallback FencingMode = "fallback"
	FencingModeRecovery FencingMode = "recovery"
)

func (m FencingMode) String() string {
	return string(m)
}

func (m FencingMode) IsValid() bool {
	switch m {
	case FencingModePrimary, FencingModeFallback, FencingModeRecovery:
		return true
	default:
		return false
	}
}

type FencingProposal struct {
	ProposedEpoch   uint64             `json:"proposed_epoch"`
	ExpectedEpoch   uint64             `json:"expected_epoch"`
	ProposerID      raft.ServerID      `json:"proposer_id"`
	ProposerAddress raft.ServerAddress `json:"proposer_address"`
	VoterSetHash    [32]byte           `json:"voter_set_hash"`
	QuorumSize      int                `json:"quorum_size"`
	Mode            FencingMode        `json:"mode"`
	Timestamp       time.Time          `json:"timestamp"`
	Signature       []byte             `json:"signature"`
}

func (p *FencingProposal) Validate() error {
	if p.ProposedEpoch == 0 {
		return fmt.Errorf("proposed_epoch must be greater than 0")
	}
	if p.ProposedEpoch != p.ExpectedEpoch+1 {
		return fmt.Errorf("proposed_epoch must be expected_epoch + 1")
	}
	if p.ProposerID == "" {
		return fmt.Errorf("proposer_id is required")
	}
	if p.ProposerAddress == "" {
		return fmt.Errorf("proposer_address is required")
	}
	if p.QuorumSize < 1 {
		return fmt.Errorf("quorum_size must be at least 1")
	}
	if !p.Mode.IsValid() {
		return fmt.Errorf("invalid fencing mode: %s", p.Mode)
	}
	if p.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	return nil
}

func (p *FencingProposal) SigningPayload() []byte {
	buf := make([]byte, 0, 256)

	epochBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(epochBytes, p.ProposedEpoch)
	buf = append(buf, epochBytes...)

	expectedEpochBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(expectedEpochBytes, p.ExpectedEpoch)
	buf = append(buf, expectedEpochBytes...)

	buf = append(buf, []byte(p.ProposerID)...)
	buf = append(buf, []byte(p.ProposerAddress)...)
	buf = append(buf, p.VoterSetHash[:]...)

	quorumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(quorumBytes, uint32(p.QuorumSize))
	buf = append(buf, quorumBytes...)

	buf = append(buf, []byte(p.Mode)...)

	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(p.Timestamp.UnixNano()))
	buf = append(buf, tsBytes...)

	return buf
}

type FencingAck struct {
	ProposedEpoch uint64        `json:"proposed_epoch"`
	VoterID       raft.ServerID `json:"voter_id"`
	Accepted      bool          `json:"accepted"`
	CurrentEpoch  uint64        `json:"current_epoch"`
	RejectReason  string        `json:"reject_reason,omitempty"`
	Timestamp     time.Time     `json:"timestamp"`
	Signature     []byte        `json:"signature"`
}

func (a *FencingAck) Validate() error {
	if a.ProposedEpoch == 0 {
		return fmt.Errorf("proposed_epoch must be greater than 0")
	}
	if a.VoterID == "" {
		return fmt.Errorf("voter_id is required")
	}
	if a.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	return nil
}

func (a *FencingAck) SigningPayload() []byte {
	buf := make([]byte, 0, 128)

	epochBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(epochBytes, a.ProposedEpoch)
	buf = append(buf, epochBytes...)

	buf = append(buf, []byte(a.VoterID)...)

	if a.Accepted {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	currentEpochBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(currentEpochBytes, a.CurrentEpoch)
	buf = append(buf, currentEpochBytes...)

	buf = append(buf, []byte(a.RejectReason)...)

	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(a.Timestamp.UnixNano()))
	buf = append(buf, tsBytes...)

	return buf
}

type FencingResult struct {
	Success       bool
	Token         *FencingToken
	AcksReceived  int
	AcksRequired  int
	RejectReasons []string
}
