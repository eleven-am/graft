package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func newTestFallbackElection(
	meta *ClusterMeta,
	membershipStore MembershipStore,
	transport BootstrapTransport,
	prober PeerProber,
) *FallbackElection {
	return NewFallbackElection(FallbackElectionDeps{
		Config: &BootstrapConfig{
			FallbackElectionWindow: 5 * time.Second,
		},
		Meta:            meta,
		MembershipStore: membershipStore,
		Transport:       transport,
		Prober:          prober,
	})
}

func TestFallbackElection_RunElection_NoCommittedConfig(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	membershipStore := NewMockMembershipStore()
	membershipStore.SetError(errors.New("no committed config"))

	election := newTestFallbackElection(meta, membershipStore, nil, nil)

	_, err := election.RunElection(context.Background())

	var ncErr *NoCommittedConfigError
	if !errors.As(err, &ncErr) {
		t.Errorf("Expected NoCommittedConfigError, got %T: %v", err, err)
	}
}

func TestFallbackElection_RunElection_EmptyVoterSet(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{},
	})

	election := newTestFallbackElection(meta, membershipStore, nil, nil)

	_, err := election.RunElection(context.Background())

	var ncErr *NoCommittedConfigError
	if !errors.As(err, &ncErr) {
		t.Errorf("Expected NoCommittedConfigError for empty voter set, got %T: %v", err, err)
	}
}

func TestFallbackElection_RunElection_ExistingClusterFound(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetReachable("10.0.0.1:7946", "node-0", "cluster-uuid", 1)
	prober.SetReachable("10.0.0.3:7946", "node-2", "cluster-uuid", 1)

	transport := NewMockBootstrapTransport()
	transport.SetClusterMeta("10.0.0.1:7946", &ClusterMeta{
		ClusterUUID: "cluster-uuid",
		State:       StateReady,
	})

	election := newTestFallbackElection(meta, membershipStore, transport, prober)

	_, err := election.RunElection(context.Background())

	if !errors.Is(err, ErrExistingClusterFound) {
		t.Errorf("Expected ErrExistingClusterFound, got %v", err)
	}
}

func TestFallbackElection_RunElection_NotLowestServerID(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-2",
		ServerAddress: "10.0.0.3:7946",
	}

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetReachable("10.0.0.2:7946", "node-1", "", 0)

	election := newTestFallbackElection(meta, membershipStore, nil, prober)

	_, err := election.RunElection(context.Background())

	if !errors.Is(err, ErrNotLowestServerID) {
		t.Errorf("Expected ErrNotLowestServerID, got %v", err)
	}
}

func TestFallbackElection_RunElection_InsufficientPeers(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetUnreachable("10.0.0.3:7946", "node-2", errors.New("unreachable"))

	election := newTestFallbackElection(meta, membershipStore, nil, prober)

	_, err := election.RunElection(context.Background())

	var ipErr *InsufficientPeersError
	if !errors.As(err, &ipErr) {
		t.Errorf("Expected InsufficientPeersError, got %T: %v", err, err)
	}
}

func TestFallbackElection_RunElection_NilProber(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	election := newTestFallbackElection(meta, membershipStore, nil, nil)

	_, err := election.RunElection(context.Background())

	if !errors.Is(err, ErrProberNotConfigured) {
		t.Errorf("Expected ErrProberNotConfigured, got %v", err)
	}
}

func TestFallbackElection_RunElection_QuorumNotReached(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	voterSet := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946"},
		{ServerID: "node-1", Address: "10.0.0.2:7946"},
		{ServerID: "node-2", Address: "10.0.0.3:7946"},
	}
	voterSetHash := HashVoterSet(voterSet)

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetReachable("10.0.0.3:7946", "node-2", "", 0)

	transport := NewMockBootstrapTransport()
	transport.SetVoteResponse("10.0.0.3:7946", &VoteResponse{
		VoteGranted:  false,
		Reason:       "already in cluster",
		VoterID:      "node-2",
		VoterSetHash: voterSetHash,
	})

	election := newTestFallbackElection(meta, membershipStore, transport, prober)

	_, err := election.RunElection(context.Background())

	var qnrErr *QuorumNotReachedError
	if !errors.As(err, &qnrErr) {
		t.Errorf("Expected QuorumNotReachedError, got %T: %v", err, err)
	}
}

func TestFallbackElection_RunElection_Success(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	voterSet := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946"},
		{ServerID: "node-1", Address: "10.0.0.2:7946"},
		{ServerID: "node-2", Address: "10.0.0.3:7946"},
	}
	voterSetHash := HashVoterSet(voterSet)

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetReachable("10.0.0.3:7946", "node-2", "", 0)

	transport := NewMockBootstrapTransport()
	transport.SetVoteResponse("10.0.0.3:7946", &VoteResponse{
		VoteGranted:  true,
		VoterID:      "node-2",
		VoterSetHash: voterSetHash,
	})

	election := newTestFallbackElection(meta, membershipStore, transport, prober)

	candidate, err := election.RunElection(context.Background())
	if err != nil {
		t.Fatalf("RunElection failed: %v", err)
	}

	if candidate.ServerID != "node-1" {
		t.Errorf("Expected candidate ServerID node-1, got %s", candidate.ServerID)
	}

	if candidate.VotesFor != 2 {
		t.Errorf("Expected 2 votes (self + node-2), got %d", candidate.VotesFor)
	}

	if candidate.VotesRequired != 2 {
		t.Errorf("Expected quorum of 2, got %d", candidate.VotesRequired)
	}
}

func TestFallbackElection_RunElection_LowestServerIDWins(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	voterSet := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946"},
		{ServerID: "node-1", Address: "10.0.0.2:7946"},
		{ServerID: "node-2", Address: "10.0.0.3:7946"},
		{ServerID: "node-3", Address: "10.0.0.4:7946"},
		{ServerID: "node-4", Address: "10.0.0.5:7946"},
	}
	voterSetHash := HashVoterSet(voterSet)

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
			{ID: "node-3", Address: "10.0.0.4:7946", Suffrage: raft.Voter},
			{ID: "node-4", Address: "10.0.0.5:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetReachable("10.0.0.3:7946", "node-2", "", 0)
	prober.SetReachable("10.0.0.4:7946", "node-3", "", 0)
	prober.SetReachable("10.0.0.5:7946", "node-4", "", 0)

	transport := NewMockBootstrapTransport()
	transport.SetVoteResponse("10.0.0.3:7946", &VoteResponse{VoteGranted: true, VoterID: "node-2", VoterSetHash: voterSetHash})
	transport.SetVoteResponse("10.0.0.4:7946", &VoteResponse{VoteGranted: true, VoterID: "node-3", VoterSetHash: voterSetHash})
	transport.SetVoteResponse("10.0.0.5:7946", &VoteResponse{VoteGranted: true, VoterID: "node-4", VoterSetHash: voterSetHash})

	election := newTestFallbackElection(meta, membershipStore, transport, prober)

	candidate, err := election.RunElection(context.Background())
	if err != nil {
		t.Fatalf("RunElection failed: %v", err)
	}

	if candidate.ServerID != "node-1" {
		t.Errorf("Expected lowest reachable ServerID (node-1) to win, got %s", candidate.ServerID)
	}

	if candidate.VotesFor != 4 {
		t.Errorf("Expected 4 votes, got %d", candidate.VotesFor)
	}
}

func TestFallbackElection_ConcurrentElection(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetReachable("10.0.0.3:7946", "node-2", "", 0)

	transport := NewMockBootstrapTransport()
	transport.SetVoteResponse("10.0.0.3:7946", &VoteResponse{VoteGranted: true, VoterID: "node-2"})

	election := newTestFallbackElection(meta, membershipStore, transport, prober)

	election.electionMu.Lock()
	election.inElection = true
	election.electionMu.Unlock()

	_, err := election.RunElection(context.Background())

	if !errors.Is(err, ErrElectionInProgress) {
		t.Errorf("Expected ErrElectionInProgress, got %v", err)
	}
}

func TestFallbackElection_HandleVoteRequest_NoMTLS(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-2",
		ServerAddress: "10.0.0.3:7946",
		State:         StateBootstrapping,
	}

	election := newTestFallbackElection(meta, nil, nil, nil)

	req := &VoteRequest{
		CandidateID: "node-1",
	}

	resp, err := election.HandleVoteRequest(context.Background(), req, nil, nil)
	if err != nil {
		t.Fatalf("HandleVoteRequest failed: %v", err)
	}

	if resp.VoteGranted {
		t.Error("Vote should be denied without mTLS")
	}

	if resp.Reason != "mTLS required" {
		t.Errorf("Expected reason 'mTLS required', got %q", resp.Reason)
	}
}

func TestFallbackElection_HandleVoteRequest_NoCert(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-2",
		ServerAddress: "10.0.0.3:7946",
		State:         StateBootstrapping,
	}

	election := newTestFallbackElection(meta, nil, nil, nil)

	req := &VoteRequest{
		CandidateID: "node-1",
	}

	tlsConfig := &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
	}

	resp, err := election.HandleVoteRequest(context.Background(), req, tlsConfig, nil)
	if err != nil {
		t.Fatalf("HandleVoteRequest failed: %v", err)
	}

	if resp.VoteGranted {
		t.Error("Vote should be denied without client certificate")
	}
}

func TestFallbackElection_HandleVoteRequest_AlreadyInCluster(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-2",
		ServerAddress: "10.0.0.3:7946",
		State:         StateReady,
	}

	election := newTestFallbackElection(meta, nil, nil, nil)

	req := &VoteRequest{
		CandidateID: "node-1",
	}

	tlsConfig := &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
	}

	peerCert := &x509.Certificate{
		DNSNames: []string{"10.0.0.2"},
	}

	resp, err := election.HandleVoteRequest(context.Background(), req, tlsConfig, peerCert)
	if err != nil {
		t.Fatalf("HandleVoteRequest failed: %v", err)
	}

	if resp.VoteGranted {
		t.Error("Vote should be denied when already in cluster")
	}

	if resp.Reason != "already in cluster" {
		t.Errorf("Expected reason 'already in cluster', got %q", resp.Reason)
	}
}

func TestFallbackElection_HandleVoteRequest_VoterSetMismatch(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-2",
		ServerAddress: "10.0.0.3:7946",
		State:         StateBootstrapping,
	}

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))

	election := newTestFallbackElection(meta, membershipStore, nil, prober)

	req := &VoteRequest{
		CandidateID:  "node-1",
		VoterSetHash: []byte("different-hash"),
	}

	tlsConfig := &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
	}

	peerCert := &x509.Certificate{
		DNSNames: []string{"10.0.0.2"},
	}

	resp, err := election.HandleVoteRequest(context.Background(), req, tlsConfig, peerCert)
	if err != nil {
		t.Fatalf("HandleVoteRequest failed: %v", err)
	}

	if resp.VoteGranted {
		t.Error("Vote should be denied on voter set mismatch")
	}

	if resp.Reason != "voter set mismatch" {
		t.Errorf("Expected reason 'voter set mismatch', got %q", resp.Reason)
	}
}

func TestFallbackElection_HandleVoteRequest_Success(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-2",
		ServerAddress: "10.0.0.3:7946",
		State:         StateBootstrapping,
	}

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))

	election := newTestFallbackElection(meta, nil, nil, prober)

	req := &VoteRequest{
		CandidateID: "node-1",
	}

	tlsConfig := &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
	}

	peerCert := &x509.Certificate{
		DNSNames: []string{"10.0.0.2"},
	}

	resp, err := election.HandleVoteRequest(context.Background(), req, tlsConfig, peerCert)
	if err != nil {
		t.Fatalf("HandleVoteRequest failed: %v", err)
	}

	if !resp.VoteGranted {
		t.Errorf("Vote should be granted, got reason: %s", resp.Reason)
	}

	if resp.VoterID != "node-2" {
		t.Errorf("Expected VoterID node-2, got %s", resp.VoterID)
	}
}

func TestFallbackElection_ValidatePeerCertAgainstVoterSet(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-2",
		ServerAddress: "10.0.0.3:7946",
	}

	election := newTestFallbackElection(meta, nil, nil, nil)

	voterSet := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946"},
		{ServerID: "node-1", Address: "10.0.0.2:7946"},
		{ServerID: "node-2", Address: "10.0.0.3:7946"},
	}

	t.Run("valid DNS name", func(t *testing.T) {
		peerCert := &x509.Certificate{
			DNSNames: []string{"10.0.0.2"},
		}
		err := election.validatePeerCertAgainstVoterSet("node-1", peerCert, voterSet)
		if err != nil {
			t.Errorf("Expected valid cert, got error: %v", err)
		}
	})

	t.Run("valid IP address", func(t *testing.T) {
		peerCert := &x509.Certificate{
			IPAddresses: []net.IP{net.ParseIP("10.0.0.2")},
		}
		err := election.validatePeerCertAgainstVoterSet("node-1", peerCert, voterSet)
		if err != nil {
			t.Errorf("Expected valid cert, got error: %v", err)
		}
	})

	t.Run("valid common name", func(t *testing.T) {
		peerCert := &x509.Certificate{}
		peerCert.Subject.CommonName = "10.0.0.2"
		err := election.validatePeerCertAgainstVoterSet("node-1", peerCert, voterSet)
		if err != nil {
			t.Errorf("Expected valid cert, got error: %v", err)
		}
	})

	t.Run("peer not in voter set", func(t *testing.T) {
		peerCert := &x509.Certificate{
			DNSNames: []string{"10.0.0.5"},
		}
		err := election.validatePeerCertAgainstVoterSet("node-5", peerCert, voterSet)
		if err == nil {
			t.Error("Expected error for peer not in voter set")
		}
	})

	t.Run("SAN mismatch", func(t *testing.T) {
		peerCert := &x509.Certificate{
			DNSNames: []string{"wrong-host"},
		}
		err := election.validatePeerCertAgainstVoterSet("node-1", peerCert, voterSet)

		var sanErr *CertificateSANMismatchError
		if !errors.As(err, &sanErr) {
			t.Errorf("Expected CertificateSANMismatchError, got %T: %v", err, err)
		}
	})

	t.Run("nil certificate", func(t *testing.T) {
		err := election.validatePeerCertAgainstVoterSet("node-1", nil, voterSet)
		if err == nil {
			t.Error("Expected error for nil certificate")
		}
	})
}

func TestFallbackElection_IsInElection(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	election := newTestFallbackElection(meta, nil, nil, nil)

	if election.IsInElection() {
		t.Error("Expected IsInElection to be false initially")
	}

	election.electionMu.Lock()
	election.inElection = true
	election.electionMu.Unlock()

	if !election.IsInElection() {
		t.Error("Expected IsInElection to be true after setting flag")
	}
}

func TestFallbackElection_VoteResponseValidation_InvalidSignature(t *testing.T) {
	fencingKey := []byte("test-fencing-key-32-bytes-long!!")

	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	voterSet := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946"},
		{ServerID: "node-1", Address: "10.0.0.2:7946"},
		{ServerID: "node-2", Address: "10.0.0.3:7946"},
	}
	voterSetHash := HashVoterSet(voterSet)

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetReachable("10.0.0.3:7946", "node-2", "", 0)

	transport := NewMockBootstrapTransport()
	transport.SetVoteResponse("10.0.0.3:7946", &VoteResponse{
		VoteGranted:  true,
		VoterID:      "node-2",
		VoterSetHash: voterSetHash,
		Signature:    []byte("invalid-signature"),
	})

	secrets := NewMockSecretsManager()
	secrets.SetFencingKey(fencingKey)

	election := NewFallbackElection(FallbackElectionDeps{
		Config: &BootstrapConfig{
			FallbackElectionWindow: 5 * time.Second,
		},
		Meta:            meta,
		MembershipStore: membershipStore,
		Transport:       transport,
		Prober:          prober,
		Secrets:         secrets,
	})

	_, err := election.RunElection(context.Background())

	var qnrErr *QuorumNotReachedError
	if !errors.As(err, &qnrErr) {
		t.Errorf("Expected QuorumNotReachedError (vote with invalid signature rejected), got %T: %v", err, err)
	}
}

func TestFallbackElection_VoteResponseValidation_VoterSetHashMismatch(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetReachable("10.0.0.3:7946", "node-2", "", 0)

	transport := NewMockBootstrapTransport()
	transport.SetVoteResponse("10.0.0.3:7946", &VoteResponse{
		VoteGranted:  true,
		VoterID:      "node-2",
		VoterSetHash: []byte("wrong-voter-set-hash"),
	})

	election := newTestFallbackElection(meta, membershipStore, transport, prober)

	_, err := election.RunElection(context.Background())

	var qnrErr *QuorumNotReachedError
	if !errors.As(err, &qnrErr) {
		t.Errorf("Expected QuorumNotReachedError (vote with mismatched hash rejected), got %T: %v", err, err)
	}
}

func TestFallbackElection_VoteResponseValidation_NonMemberVoterID(t *testing.T) {
	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	voterSet := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946"},
		{ServerID: "node-1", Address: "10.0.0.2:7946"},
		{ServerID: "node-2", Address: "10.0.0.3:7946"},
	}
	voterSetHash := HashVoterSet(voterSet)

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetReachable("10.0.0.3:7946", "node-2", "", 0)

	transport := NewMockBootstrapTransport()
	transport.SetVoteResponse("10.0.0.3:7946", &VoteResponse{
		VoteGranted:  true,
		VoterID:      "node-99",
		VoterSetHash: voterSetHash,
	})

	election := newTestFallbackElection(meta, membershipStore, transport, prober)

	_, err := election.RunElection(context.Background())

	var qnrErr *QuorumNotReachedError
	if !errors.As(err, &qnrErr) {
		t.Errorf("Expected QuorumNotReachedError (vote from non-member rejected), got %T: %v", err, err)
	}
}

func TestFallbackElection_VoteResponseValidation_ValidSignature(t *testing.T) {
	fencingKey := []byte("test-fencing-key-32-bytes-long!!")

	meta := &ClusterMeta{
		ServerID:      "node-1",
		ServerAddress: "10.0.0.2:7946",
	}

	voterSet := []VoterInfo{
		{ServerID: "node-0", Address: "10.0.0.1:7946"},
		{ServerID: "node-1", Address: "10.0.0.2:7946"},
		{ServerID: "node-2", Address: "10.0.0.3:7946"},
	}
	voterSetHash := HashVoterSet(voterSet)

	membershipStore := NewMockMembershipStore()
	membershipStore.SetConfiguration(&raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-0", Address: "10.0.0.1:7946", Suffrage: raft.Voter},
			{ID: "node-1", Address: "10.0.0.2:7946", Suffrage: raft.Voter},
			{ID: "node-2", Address: "10.0.0.3:7946", Suffrage: raft.Voter},
		},
	})

	prober := NewMockPeerProber()
	prober.SetUnreachable("10.0.0.1:7946", "node-0", errors.New("unreachable"))
	prober.SetReachable("10.0.0.3:7946", "node-2", "", 0)

	voteResp := &VoteResponse{
		VoteGranted:  true,
		VoterID:      "node-2",
		VoterSetHash: voterSetHash,
	}
	_ = SignVoteResponse(voteResp, fencingKey)

	transport := NewMockBootstrapTransport()
	transport.SetVoteResponse("10.0.0.3:7946", voteResp)

	secrets := NewMockSecretsManager()
	secrets.SetFencingKey(fencingKey)

	election := NewFallbackElection(FallbackElectionDeps{
		Config: &BootstrapConfig{
			FallbackElectionWindow: 5 * time.Second,
		},
		Meta:            meta,
		MembershipStore: membershipStore,
		Transport:       transport,
		Prober:          prober,
		Secrets:         secrets,
	})

	candidate, err := election.RunElection(context.Background())
	if err != nil {
		t.Fatalf("RunElection failed: %v", err)
	}

	if candidate.VotesFor != 2 {
		t.Errorf("Expected 2 votes with valid signature, got %d", candidate.VotesFor)
	}
}
