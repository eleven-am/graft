package bootstrap

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestFencingMode_IsValid(t *testing.T) {
	tests := []struct {
		mode  FencingMode
		valid bool
	}{
		{FencingModePrimary, true},
		{FencingModeFallback, true},
		{FencingModeRecovery, true},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			if got := tt.mode.IsValid(); got != tt.valid {
				t.Errorf("FencingMode.IsValid() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestFencingProposal_Validate(t *testing.T) {
	validProposal := func() *FencingProposal {
		return &FencingProposal{
			ProposedEpoch:   1,
			ProposerID:      raft.ServerID("node-0"),
			ProposerAddress: raft.ServerAddress("node-0:7946"),
			VoterSetHash:    [32]byte{1, 2, 3},
			QuorumSize:      2,
			Mode:            FencingModePrimary,
			Timestamp:       time.Now(),
		}
	}

	t.Run("valid proposal", func(t *testing.T) {
		p := validProposal()
		if err := p.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("missing proposed_epoch", func(t *testing.T) {
		p := validProposal()
		p.ProposedEpoch = 0
		if err := p.Validate(); err == nil {
			t.Error("Validate() should fail for missing proposed_epoch")
		}
	})

	t.Run("missing proposer_id", func(t *testing.T) {
		p := validProposal()
		p.ProposerID = ""
		if err := p.Validate(); err == nil {
			t.Error("Validate() should fail for missing proposer_id")
		}
	})

	t.Run("missing proposer_address", func(t *testing.T) {
		p := validProposal()
		p.ProposerAddress = ""
		if err := p.Validate(); err == nil {
			t.Error("Validate() should fail for missing proposer_address")
		}
	})

	t.Run("invalid quorum_size", func(t *testing.T) {
		p := validProposal()
		p.QuorumSize = 0
		if err := p.Validate(); err == nil {
			t.Error("Validate() should fail for invalid quorum_size")
		}
	})

	t.Run("invalid mode", func(t *testing.T) {
		p := validProposal()
		p.Mode = "invalid"
		if err := p.Validate(); err == nil {
			t.Error("Validate() should fail for invalid mode")
		}
	})

	t.Run("missing timestamp", func(t *testing.T) {
		p := validProposal()
		p.Timestamp = time.Time{}
		if err := p.Validate(); err == nil {
			t.Error("Validate() should fail for missing timestamp")
		}
	})
}

func TestFencingProposal_SigningPayload(t *testing.T) {
	p := &FencingProposal{
		ProposedEpoch:   1,
		ProposerID:      raft.ServerID("node-0"),
		ProposerAddress: raft.ServerAddress("node-0:7946"),
		VoterSetHash:    [32]byte{1, 2, 3},
		QuorumSize:      2,
		Mode:            FencingModePrimary,
		Timestamp:       time.Unix(1000000000, 0),
	}

	payload1 := p.SigningPayload()
	payload2 := p.SigningPayload()

	if len(payload1) == 0 {
		t.Error("SigningPayload() returned empty slice")
	}

	if string(payload1) != string(payload2) {
		t.Error("SigningPayload() not deterministic")
	}

	p.ProposedEpoch = 2
	payload3 := p.SigningPayload()

	if string(payload1) == string(payload3) {
		t.Error("SigningPayload() should change when fields change")
	}
}

func TestFencingAck_Validate(t *testing.T) {
	validAck := func() *FencingAck {
		return &FencingAck{
			ProposedEpoch: 1,
			VoterID:       raft.ServerID("node-0"),
			Accepted:      true,
			CurrentEpoch:  1,
			Timestamp:     time.Now(),
		}
	}

	t.Run("valid ack", func(t *testing.T) {
		a := validAck()
		if err := a.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("missing proposed_epoch", func(t *testing.T) {
		a := validAck()
		a.ProposedEpoch = 0
		if err := a.Validate(); err == nil {
			t.Error("Validate() should fail for missing proposed_epoch")
		}
	})

	t.Run("missing voter_id", func(t *testing.T) {
		a := validAck()
		a.VoterID = ""
		if err := a.Validate(); err == nil {
			t.Error("Validate() should fail for missing voter_id")
		}
	})

	t.Run("missing timestamp", func(t *testing.T) {
		a := validAck()
		a.Timestamp = time.Time{}
		if err := a.Validate(); err == nil {
			t.Error("Validate() should fail for missing timestamp")
		}
	})

	t.Run("reject ack with reason", func(t *testing.T) {
		a := validAck()
		a.Accepted = false
		a.RejectReason = "stale epoch"
		if err := a.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil for reject ack", err)
		}
	})
}

func TestFencingAck_SigningPayload(t *testing.T) {
	a := &FencingAck{
		ProposedEpoch: 1,
		VoterID:       raft.ServerID("node-0"),
		Accepted:      true,
		CurrentEpoch:  1,
		Timestamp:     time.Unix(1000000000, 0),
	}

	payload1 := a.SigningPayload()
	payload2 := a.SigningPayload()

	if len(payload1) == 0 {
		t.Error("SigningPayload() returned empty slice")
	}

	if string(payload1) != string(payload2) {
		t.Error("SigningPayload() not deterministic")
	}

	a.Accepted = false
	payload3 := a.SigningPayload()

	if string(payload1) == string(payload3) {
		t.Error("SigningPayload() should change when fields change")
	}
}
