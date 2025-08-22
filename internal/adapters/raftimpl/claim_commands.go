package raftimpl

import (
	"encoding/json"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type ClaimWorkCommand struct {
	WorkItemID string        `json:"work_item_id"`
	NodeID     string        `json:"node_id"`
	Duration   time.Duration `json:"duration"`
	Timestamp  time.Time     `json:"timestamp"`
}

type ReleaseClaimCommand struct {
	WorkItemID string    `json:"work_item_id"`
	NodeID     string    `json:"node_id"`
	Timestamp  time.Time `json:"timestamp"`
}

func NewClaimWorkCommand(workItemID, nodeID string, duration time.Duration) *Command {
	claimCmd := ClaimWorkCommand{
		WorkItemID: workItemID,
		NodeID:     nodeID,
		Duration:   duration,
		Timestamp:  time.Now(),
	}

	cmdData, _ := json.Marshal(claimCmd)

	return &Command{
		Type:      CommandClaimWork,
		Key:       domain.GenerateClaimKey(workItemID),
		Value:     cmdData,
		Timestamp: time.Now(),
	}
}

func NewReleaseClaimCommand(workItemID, nodeID string) *Command {
	releaseCmd := ReleaseClaimCommand{
		WorkItemID: workItemID,
		NodeID:     nodeID,
		Timestamp:  time.Now(),
	}

	cmdData, _ := json.Marshal(releaseCmd)

	return &Command{
		Type:      CommandReleaseClaim,
		Key:       domain.GenerateClaimKey(workItemID),
		Value:     cmdData,
		Timestamp: time.Now(),
	}
}

func (c *ClaimWorkCommand) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func (c *ReleaseClaimCommand) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func UnmarshalClaimWorkCommand(data []byte) (*ClaimWorkCommand, error) {
	var cmd ClaimWorkCommand
	err := json.Unmarshal(data, &cmd)
	return &cmd, err
}

func UnmarshalReleaseClaimCommand(data []byte) (*ReleaseClaimCommand, error) {
	var cmd ReleaseClaimCommand
	err := json.Unmarshal(data, &cmd)
	return &cmd, err
}

func (c *ClaimWorkCommand) Validate() error {
	if c.WorkItemID == "" {
		return domain.NewValidationError("work_item_id", "cannot be empty")
	}
	if c.NodeID == "" {
		return domain.NewValidationError("node_id", "cannot be empty")
	}
	if c.Duration <= 0 {
		return domain.NewValidationError("duration", "must be positive")
	}
	return nil
}

func (c *ReleaseClaimCommand) Validate() error {
	if c.WorkItemID == "" {
		return domain.NewValidationError("work_item_id", "cannot be empty")
	}
	if c.NodeID == "" {
		return domain.NewValidationError("node_id", "cannot be empty")
	}
	return nil
}