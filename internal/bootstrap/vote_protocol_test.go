package bootstrap

import (
	"bytes"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestHashVoterSet_Deterministic(t *testing.T) {
	voters := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	hash1 := HashVoterSet(voters)
	hash2 := HashVoterSet(voters)

	if !bytes.Equal(hash1, hash2) {
		t.Error("HashVoterSet should return same hash for same input")
	}
}

func TestHashVoterSet_OrderIndependent(t *testing.T) {
	voters1 := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	voters2 := []VoterInfo{
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
	}

	hash1 := HashVoterSet(voters1)
	hash2 := HashVoterSet(voters2)

	if !bytes.Equal(hash1, hash2) {
		t.Error("HashVoterSet should be order-independent")
	}
}

func TestHashVoterSet_DifferentVoters(t *testing.T) {
	voters1 := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-1", Address: "10.0.0.2:7946", Ordinal: 1},
	}

	voters2 := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946", Ordinal: 0},
		{ServerID: "node-2", Address: "10.0.0.3:7946", Ordinal: 2},
	}

	hash1 := HashVoterSet(voters1)
	hash2 := HashVoterSet(voters2)

	if bytes.Equal(hash1, hash2) {
		t.Error("HashVoterSet should return different hash for different voters")
	}
}

func TestExtractOrdinal(t *testing.T) {
	tests := []struct {
		serverID raft.ServerID
		expected int
	}{
		{"myservice-0", 0},
		{"myservice-1", 1},
		{"myservice-10", 10},
		{"my-complex-service-name-5", 5},
		{"node0", -1},
		{"node", -1},
		{"", -1},
		{"-1", 1},
	}

	for _, tt := range tests {
		t.Run(string(tt.serverID), func(t *testing.T) {
			result := ExtractOrdinal(tt.serverID)
			if result != tt.expected {
				t.Errorf("ExtractOrdinal(%q) = %d, want %d", tt.serverID, result, tt.expected)
			}
		})
	}
}

func TestSignVerifyVoteRequest(t *testing.T) {
	key := []byte("test-fencing-key-32-bytes-long!!")

	req := &VoteRequest{
		CandidateID:      "node-1",
		CandidateOrdinal: 1,
		ElectionReason:   "ordinal_0_unreachable",
		Timestamp:        time.Now(),
		VoterSetHash:     []byte("voter-set-hash"),
		RequiredQuorum:   2,
	}

	err := SignVoteRequest(req, key)
	if err != nil {
		t.Fatalf("SignVoteRequest failed: %v", err)
	}

	if len(req.Signature) == 0 {
		t.Error("Signature should not be empty")
	}

	if !VerifyVoteRequestSignature(req, key) {
		t.Error("VerifyVoteRequestSignature should return true for valid signature")
	}
}

func TestSignVerifyVoteRequest_InvalidKey(t *testing.T) {
	key1 := []byte("test-fencing-key-32-bytes-long!!")
	key2 := []byte("different-key-32-bytes-long!!!!!")

	req := &VoteRequest{
		CandidateID:      "node-1",
		CandidateOrdinal: 1,
		ElectionReason:   "ordinal_0_unreachable",
		Timestamp:        time.Now(),
		VoterSetHash:     []byte("voter-set-hash"),
		RequiredQuorum:   2,
	}

	_ = SignVoteRequest(req, key1)

	if VerifyVoteRequestSignature(req, key2) {
		t.Error("VerifyVoteRequestSignature should return false for wrong key")
	}
}

func TestSignVerifyVoteRequest_NoKey(t *testing.T) {
	req := &VoteRequest{
		CandidateID:      "node-1",
		CandidateOrdinal: 1,
		ElectionReason:   "ordinal_0_unreachable",
		Timestamp:        time.Now(),
		VoterSetHash:     []byte("voter-set-hash"),
		RequiredQuorum:   2,
	}

	err := SignVoteRequest(req, nil)
	if err != nil {
		t.Fatalf("SignVoteRequest with nil key should succeed: %v", err)
	}

	if len(req.Signature) != 0 {
		t.Error("Signature should be empty when no key provided")
	}

	if !VerifyVoteRequestSignature(req, nil) {
		t.Error("VerifyVoteRequestSignature should return true when no key")
	}
}

func TestSignVerifyVoteRequest_TamperedData(t *testing.T) {
	key := []byte("test-fencing-key-32-bytes-long!!")

	req := &VoteRequest{
		CandidateID:      "node-1",
		CandidateOrdinal: 1,
		ElectionReason:   "ordinal_0_unreachable",
		Timestamp:        time.Now(),
		VoterSetHash:     []byte("voter-set-hash"),
		RequiredQuorum:   2,
	}

	_ = SignVoteRequest(req, key)

	req.CandidateOrdinal = 0

	if VerifyVoteRequestSignature(req, key) {
		t.Error("VerifyVoteRequestSignature should return false for tampered data")
	}
}

func TestSignVerifyVoteResponse(t *testing.T) {
	key := []byte("test-fencing-key-32-bytes-long!!")

	resp := &VoteResponse{
		VoteGranted:  true,
		Reason:       "",
		VoterID:      "node-2",
		VoterSetHash: []byte("voter-set-hash"),
	}

	err := SignVoteResponse(resp, key)
	if err != nil {
		t.Fatalf("SignVoteResponse failed: %v", err)
	}

	if len(resp.Signature) == 0 {
		t.Error("Signature should not be empty")
	}

	if !VerifyVoteResponseSignature(resp, key) {
		t.Error("VerifyVoteResponseSignature should return true for valid signature")
	}
}

func TestSignVerifyVoteResponse_InvalidKey(t *testing.T) {
	key1 := []byte("test-fencing-key-32-bytes-long!!")
	key2 := []byte("different-key-32-bytes-long!!!!!")

	resp := &VoteResponse{
		VoteGranted:  true,
		VoterID:      "node-2",
		VoterSetHash: []byte("voter-set-hash"),
	}

	_ = SignVoteResponse(resp, key1)

	if VerifyVoteResponseSignature(resp, key2) {
		t.Error("VerifyVoteResponseSignature should return false for wrong key")
	}
}

func TestSignVerifyVoteResponse_TamperedData(t *testing.T) {
	key := []byte("test-fencing-key-32-bytes-long!!")

	resp := &VoteResponse{
		VoteGranted:  true,
		VoterID:      "node-2",
		VoterSetHash: []byte("voter-set-hash"),
	}

	_ = SignVoteResponse(resp, key)

	resp.VoteGranted = false

	if VerifyVoteResponseSignature(resp, key) {
		t.Error("VerifyVoteResponseSignature should return false for tampered data")
	}
}

func TestMockBootstrapTransport(t *testing.T) {
	transport := NewMockBootstrapTransport()

	addr := raft.ServerAddress("10.0.0.1:7946")
	expectedResp := &VoteResponse{
		VoteGranted: true,
		VoterID:     "node-1",
	}

	transport.SetVoteResponse(addr, expectedResp)

	resp, err := transport.RequestVote(nil, addr, nil)
	if err != nil {
		t.Fatalf("RequestVote failed: %v", err)
	}

	if resp != expectedResp {
		t.Error("RequestVote should return configured response")
	}
}

func TestMockMembershipStore(t *testing.T) {
	store := NewMockMembershipStore()

	config := &raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
		},
	}

	store.SetConfiguration(config)

	result, err := store.GetLastCommittedConfiguration()
	if err != nil {
		t.Fatalf("GetLastCommittedConfiguration failed: %v", err)
	}

	if len(result.Servers) != 2 {
		t.Errorf("Expected 2 servers, got %d", len(result.Servers))
	}
}

func TestMockPeerDiscovery(t *testing.T) {
	discovery := NewMockPeerDiscovery()

	discovery.SetAddress(0, "10.0.0.1:7946")
	discovery.SetAddress(1, "10.0.0.2:7946")

	addr := discovery.AddressForOrdinal(0)
	if addr != "10.0.0.1:7946" {
		t.Errorf("AddressForOrdinal(0) = %q, want %q", addr, "10.0.0.1:7946")
	}

	addr = discovery.AddressForOrdinal(2)
	if addr != "" {
		t.Errorf("AddressForOrdinal(2) = %q, want empty string", addr)
	}
}
