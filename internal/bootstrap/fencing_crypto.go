package bootstrap

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

func SignProposal(proposal *FencingProposal, key []byte) error {
	payload := proposal.SigningPayload()
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	proposal.Signature = mac.Sum(nil)
	return nil
}

func VerifyProposalSignature(proposal *FencingProposal, key []byte) bool {
	if len(proposal.Signature) == 0 {
		return false
	}
	expected := proposal.Signature
	payload := proposal.SigningPayload()
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	computed := mac.Sum(nil)
	return hmac.Equal(expected, computed)
}

func SignToken(token *FencingToken, key []byte) error {
	payload := token.SigningPayload()
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	token.Signature = mac.Sum(nil)
	return nil
}

func VerifyTokenSignature(token *FencingToken, key []byte) bool {
	if len(token.Signature) == 0 {
		return false
	}
	expected := token.Signature
	payload := token.SigningPayload()
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	computed := mac.Sum(nil)
	return hmac.Equal(expected, computed)
}

func SignAck(ack *FencingAck, key []byte) error {
	payload := ack.SigningPayload()
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	ack.Signature = mac.Sum(nil)
	return nil
}

func VerifyAckSignature(ack *FencingAck, key []byte) bool {
	if len(ack.Signature) == 0 {
		return false
	}
	expected := ack.Signature
	payload := ack.SigningPayload()
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	computed := mac.Sum(nil)
	return hmac.Equal(expected, computed)
}

func ComputeVoterSetHash(voters []VoterInfo) [32]byte {
	sorted := make([]VoterInfo, len(voters))
	copy(sorted, voters)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ServerID < sorted[j].ServerID
	})

	h := sha256.New()
	for _, v := range sorted {
		h.Write([]byte(v.ServerID))
		h.Write([]byte(v.Address))
		ordinalBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(ordinalBytes, uint32(v.Ordinal))
		h.Write(ordinalBytes)
	}

	var hash [32]byte
	copy(hash[:], h.Sum(nil))
	return hash
}

func ValidateTokenVoterSet(token *FencingToken, currentVoters []VoterInfo) error {
	currentHash := ComputeVoterSetHash(currentVoters)
	if token.VoterSetHash != currentHash {
		return &VoterSetMismatchError{
			TokenVoterSetHash:   token.VoterSetHash[:],
			CurrentVoterSetHash: currentHash[:],
			TokenQuorumSize:     token.QuorumSize,
			CurrentVoterCount:   len(currentVoters),
			Message:             "token voter set does not match current voter set",
		}
	}
	return nil
}
