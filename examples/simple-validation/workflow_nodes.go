package main

import (
	"context"
	"fmt"

	"github.com/eleven-am/graft"
)

type WorkflowDocument struct {
	ID          string                 `json:"id"`
	Status      string                 `json:"status"`
	NeedsRepair bool                   `json:"needs_repair"`
	History     []string               `json:"history"`
	Metadata    map[string]interface{} `json:"metadata"`
}

type WorkflowConfig struct {
	ProcessorName string `json:"processor_name"`
	AllowRepair   bool   `json:"allow_repair"`
}

type IntakeNode struct{}

func (n *IntakeNode) GetName() string {
	return "intake_node"
}

func (n *IntakeNode) CanStart(ctx context.Context, doc WorkflowDocument, cfg WorkflowConfig) bool {
	return doc.Status == "new"
}

func (n *IntakeNode) Execute(ctx context.Context, doc WorkflowDocument, cfg WorkflowConfig) (*graft.NodeResult, error) {
	doc.Status = "validated_pending"
	doc.History = append(doc.History, fmt.Sprintf("%s:intake", cfg.ProcessorName))
	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "validator_node", Config: cfg}},
	}, nil
}

type ValidatorNode struct{}

func (n *ValidatorNode) GetName() string {
	return "validator_node"
}

func (n *ValidatorNode) CanStart(ctx context.Context, doc WorkflowDocument, cfg WorkflowConfig) bool {
	return doc.Status == "validated_pending"
}

func (n *ValidatorNode) Execute(ctx context.Context, doc WorkflowDocument, cfg WorkflowConfig) (*graft.NodeResult, error) {
	doc.History = append(doc.History, fmt.Sprintf("%s:validate", cfg.ProcessorName))
	if doc.NeedsRepair && cfg.AllowRepair {
		doc.Status = "repair_needed"
		return &graft.NodeResult{
			GlobalState: doc,
			NextNodes:   []graft.NextNode{{NodeName: "repair_node", Config: cfg}},
		}, nil
	}
	doc.NeedsRepair = false
	doc.Status = "processing_pending"
	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "processor_node", Config: cfg}},
	}, nil
}

type RepairNode struct{}

func (n *RepairNode) GetName() string {
	return "repair_node"
}

func (n *RepairNode) CanStart(ctx context.Context, doc WorkflowDocument, cfg WorkflowConfig) bool {
	return doc.Status == "repair_needed" && cfg.AllowRepair
}

func (n *RepairNode) Execute(ctx context.Context, doc WorkflowDocument, cfg WorkflowConfig) (*graft.NodeResult, error) {
	doc.History = append(doc.History, fmt.Sprintf("%s:repair", cfg.ProcessorName))
	doc.NeedsRepair = false
	doc.Status = "validated_pending"
	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "validator_node", Config: cfg}},
	}, nil
}

type ProcessorNode struct{}

func (n *ProcessorNode) GetName() string {
	return "processor_node"
}

func (n *ProcessorNode) CanStart(ctx context.Context, doc WorkflowDocument, cfg WorkflowConfig) bool {
	return doc.Status == "processing_pending"
}

func (n *ProcessorNode) Execute(ctx context.Context, doc WorkflowDocument, cfg WorkflowConfig) (*graft.NodeResult, error) {
	doc.History = append(doc.History, fmt.Sprintf("%s:process", cfg.ProcessorName))
	doc.Status = "completed"
	return &graft.NodeResult{GlobalState: doc}, nil
}

func registerNodes(manager *graft.Manager) error {
	nodes := []interface{}{
		&IntakeNode{},
		&ValidatorNode{},
		&RepairNode{},
		&ProcessorNode{},
	}
	for _, node := range nodes {
		if err := manager.RegisterNode(node); err != nil {
			return err
		}
	}
	return nil
}
