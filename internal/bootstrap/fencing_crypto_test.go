package bootstrap

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestSignProposal_Verify(t *testing.T) {
	key := []byte("test-fencing-key-32-bytes-long!!")

	proposal := &FencingProposal{
		ProposedEpoch:   1,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    [32]byte{1, 2, 3},
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Now(),
	}

	if err := SignProposal(proposal, key); err != nil {
		t.Fatalf("SignProposal() error = %v", err)
	}

	if len(proposal.Signature) == 0 {
		t.Error("SignProposal() did not set signature")
	}

	if !VerifyProposalSignature(proposal, key) {
		t.Error("VerifyProposalSignature() = false for valid signature")
	}

	wrongKey := []byte("wrong-fencing-key-32-bytes-long!")
	if VerifyProposalSignature(proposal, wrongKey) {
		t.Error("VerifyProposalSignature() = true for wrong key")
	}

	proposal.ProposedEpoch = 2
	if VerifyProposalSignature(proposal, key) {
		t.Error("VerifyProposalSignature() = true for tampered proposal")
	}
}

func TestSignProposal_EmptySignature(t *testing.T) {
	key := []byte("test-key")

	proposal := &FencingProposal{
		ProposedEpoch: 1,
		ProposerID:    raft.ServerID("node-0"),
		Timestamp:     time.Now(),
	}

	if VerifyProposalSignature(proposal, key) {
		t.Error("VerifyProposalSignature() = true for empty signature")
	}
}

func TestSignToken_Verify(t *testing.T) {
	key := []byte("test-fencing-key-32-bytes-long!!")

	token := &FencingToken{
		Version:      TokenVersion,
		Epoch:        1,
		HolderID:     raft.ServerID("node-0"),
		VoterSetHash: [32]byte{1, 2, 3},
		QuorumSize:   2,
		Mode:         FencingModePrimary,
		AcquiredAt:   time.Now(),
	}
	token.UpdateChecksum()

	if err := SignToken(token, key); err != nil {
		t.Fatalf("SignToken() error = %v", err)
	}

	if len(token.Signature) == 0 {
		t.Error("SignToken() did not set signature")
	}

	if !VerifyTokenSignature(token, key) {
		t.Error("VerifyTokenSignature() = false for valid signature")
	}

	wrongKey := []byte("wrong-fencing-key-32-bytes-long!")
	if VerifyTokenSignature(token, wrongKey) {
		t.Error("VerifyTokenSignature() = true for wrong key")
	}

	token.Epoch = 2
	if VerifyTokenSignature(token, key) {
		t.Error("VerifyTokenSignature() = true for tampered token")
	}
}

func TestSignToken_EmptySignature(t *testing.T) {
	key := []byte("test-key")

	token := &FencingToken{
		Version:    TokenVersion,
		Epoch:      1,
		HolderID:   raft.ServerID("node-0"),
		AcquiredAt: time.Now(),
	}

	if VerifyTokenSignature(token, key) {
		t.Error("VerifyTokenSignature() = true for empty signature")
	}
}

func TestSignAck_Verify(t *testing.T) {
	key := []byte("test-fencing-key-32-bytes-long!!")

	ack := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-0"),
		Accepted:      true,
		CurrentEpoch:  1,
		Timestamp:     time.Now(),
	}

	if err := SignAck(ack, key); err != nil {
		t.Fatalf("SignAck() error = %v", err)
	}

	if len(ack.Signature) == 0 {
		t.Error("SignAck() did not set signature")
	}

	if !VerifyAckSignature(ack, key) {
		t.Error("VerifyAckSignature() = false for valid signature")
	}

	wrongKey := []byte("wrong-fencing-key-32-bytes-long!")
	if VerifyAckSignature(ack, wrongKey) {
		t.Error("VerifyAckSignature() = true for wrong key")
	}

	ack.Accepted = false
	if VerifyAckSignature(ack, key) {
		t.Error("VerifyAckSignature() = true for tampered ack")
	}
}

func TestSignAck_EmptySignature(t *testing.T) {
	key := []byte("test-key")

	ack := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-0"),
		Timestamp:     time.Now(),
	}

	if VerifyAckSignature(ack, key) {
		t.Error("VerifyAckSignature() = true for empty signature")
	}
}

func TestComputeVoterSetHash_Deterministic(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	hash1 := ComputeVoterSetHash(voters)
	hash2 := ComputeVoterSetHash(voters)

	if hash1 != hash2 {
		t.Error("ComputeVoterSetHash() not deterministic")
	}
}

func TestComputeVoterSetHash_OrderIndependent(t *testing.T) {
	voters1 := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	voters2 := []VoterInfo{
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	hash1 := ComputeVoterSetHash(voters1)
	hash2 := ComputeVoterSetHash(voters2)

	if hash1 != hash2 {
		t.Error("ComputeVoterSetHash() should be order-independent")
	}
}

func TestComputeVoterSetHash_DifferentVoters(t *testing.T) {
	voters1 := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	voters2 := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	hash1 := ComputeVoterSetHash(voters1)
	hash2 := ComputeVoterSetHash(voters2)

	if hash1 == hash2 {
		t.Error("ComputeVoterSetHash() should differ for different voters")
	}
}

func TestComputeVoterSetHash_EmptyVoters(t *testing.T) {
	var voters []VoterInfo
	hash := ComputeVoterSetHash(voters)

	if hash == [32]byte{} {
		t.Error("ComputeVoterSetHash() should return non-zero hash for empty voters")
	}
}

func TestValidateTokenVoterSet_Match(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	token := &FencingToken{
		VoterSetHash: ComputeVoterSetHash(voters),
		QuorumSize:   2,
	}

	if err := ValidateTokenVoterSet(token, voters); err != nil {
		t.Errorf("ValidateTokenVoterSet() = %v, want nil", err)
	}
}

func TestValidateTokenVoterSet_Mismatch(t *testing.T) {
	voters1 := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1:7946"), Ordinal: 1},
	}

	voters2 := []VoterInfo{
		{ServerID: raft.ServerID("node-0"), Address: raft.ServerAddress("node-0:7946"), Ordinal: 0},
		{ServerID: raft.ServerID("node-2"), Address: raft.ServerAddress("node-2:7946"), Ordinal: 2},
	}

	token := &FencingToken{
		VoterSetHash: ComputeVoterSetHash(voters1),
		QuorumSize:   2,
	}

	err := ValidateTokenVoterSet(token, voters2)
	if err == nil {
		t.Error("ValidateTokenVoterSet() should fail for mismatched voters")
	}

	if _, ok := err.(*VoterSetMismatchError); !ok {
		t.Errorf("ValidateTokenVoterSet() error type = %T, want *VoterSetMismatchError", err)
	}
}
